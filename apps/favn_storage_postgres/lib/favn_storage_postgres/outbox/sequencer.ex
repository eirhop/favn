defmodule FavnStoragePostgres.Outbox.Sequencer do
  @moduledoc """
  Assigns commit-safe publication IDs to committed durable outbox rows.

  PostgreSQL identity values are deliberately not replay cursors. Every node may
  run this worker; the singleton publication-state row serializes short batches.
  """

  use GenServer

  alias Ecto.Adapters.SQL
  alias FavnStoragePostgres.Repo

  @default_batch_size 1_000
  @default_interval_ms 30_000

  @type publication :: %{
          outbox_event_id: pos_integer(),
          publication_id: pos_integer(),
          workspace_id: String.t(),
          event_kind: String.t(),
          aggregate_kind: String.t(),
          aggregate_id: String.t()
        }

  @doc "Starts one node-local sequencer wake-up worker."
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @doc "Sequences one bounded batch and returns its assigned publication identities."
  @spec sequence_batch(pos_integer()) :: {:ok, [publication()]} | {:error, term()}
  def sequence_batch(limit \\ @default_batch_size)
      when is_integer(limit) and limit > 0 and limit <= 5_000 do
    Repo.transaction(fn ->
      %{rows: [[last_publication_id]]} =
        SQL.query!(
          Repo,
          """
          SELECT last_publication_id
          FROM favn_control.outbox_publication_state
          WHERE singleton_id = 1
          FOR UPDATE
          """,
          []
        )

      %{rows: rows} =
        SQL.query!(
          Repo,
          """
          SELECT outbox_event_id
          FROM favn_control.outbox_events
          WHERE publication_id IS NULL
          ORDER BY outbox_event_id
          LIMIT $1
          FOR UPDATE
          """,
          [limit]
        )

      ids = Enum.map(rows, fn [id] -> id end)

      if ids == [] do
        []
      else
        %{rows: published_rows} =
          SQL.query!(
            Repo,
            """
            WITH selected AS (
              SELECT id, ordinality
              FROM unnest($1::bigint[]) WITH ORDINALITY AS selected(id, ordinality)
            )
            UPDATE favn_control.outbox_events AS event
            SET publication_id = $2 + selected.ordinality,
                published_at = clock_timestamp()
            FROM selected
            WHERE event.outbox_event_id = selected.id
            RETURNING event.outbox_event_id, event.publication_id, event.workspace_id,
                      event.event_kind, event.aggregate_kind, event.aggregate_id
            """,
            [ids, last_publication_id]
          )

        new_last = last_publication_id + length(ids)

        SQL.query!(
          Repo,
          """
          UPDATE favn_control.outbox_publication_state
          SET last_publication_id = $1, updated_at = clock_timestamp()
          WHERE singleton_id = 1
          """,
          [new_last]
        )

        SQL.query!(Repo, "SELECT pg_notify('favn_outbox_published', '')", [])

        published_rows
        |> Enum.map(fn [
                         outbox_id,
                         publication_id,
                         workspace_id,
                         event_kind,
                         aggregate_kind,
                         aggregate_id
                       ] ->
          %{
            outbox_event_id: outbox_id,
            publication_id: publication_id,
            workspace_id: workspace_id,
            event_kind: event_kind,
            aggregate_kind: aggregate_kind,
            aggregate_id: aggregate_id
          }
        end)
        |> Enum.sort_by(& &1.publication_id)
      end
    end)
  end

  @impl true
  def init(opts) do
    state = %{
      batch_size: Keyword.get(opts, :batch_size, @default_batch_size),
      interval_ms: Keyword.get(opts, :interval_ms, @default_interval_ms),
      timer_ref: nil
    }

    {:ok, schedule(state, 0)}
  end

  @impl true
  def handle_cast(:wake, %{timer_ref: :pending} = state), do: {:noreply, state}

  def handle_cast(:wake, %{timer_ref: timer_ref} = state) do
    timer_already_delivered? =
      is_reference(timer_ref) and Process.cancel_timer(timer_ref) == false

    unless timer_already_delivered?, do: send(self(), :sequence)
    {:noreply, %{state | timer_ref: :pending}}
  end

  @impl true
  def handle_info(:sequence, state) do
    state = %{state | timer_ref: nil}
    started_at = System.monotonic_time()

    delay =
      case sequence_batch(state.batch_size) do
        {:ok, publications} ->
          :telemetry.execute(
            [:favn, :storage, :outbox, :sequence],
            %{duration: System.monotonic_time() - started_at, count: length(publications)},
            %{}
          )

          if(length(publications) == state.batch_size, do: 0, else: state.interval_ms)

        {:error, reason} ->
          :telemetry.execute(
            [:favn, :storage, :outbox, :sequence, :error],
            %{duration: System.monotonic_time() - started_at, count: 1},
            %{reason: redacted_reason(reason)}
          )

          state.interval_ms
      end

    {:noreply, schedule(state, delay)}
  end

  defp schedule(state, delay),
    do: %{state | timer_ref: Process.send_after(self(), :sequence, delay)}

  defp redacted_reason(%Postgrex.Error{postgres: %{code: code}}), do: code
  defp redacted_reason(%DBConnection.ConnectionError{}), do: :connection_error
  defp redacted_reason(_reason), do: :unknown
end
