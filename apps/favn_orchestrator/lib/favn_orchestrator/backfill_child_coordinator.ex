defmodule FavnOrchestrator.BackfillChildCoordinator do
  @moduledoc """
  Coordinates finite child-run admission for pipeline backfills.

  Backfill submission persists the parent run and all pending window rows before
  this coordinator is invoked. When an operator supplies
  `:backfill_child_concurrency`, the coordinator submits only that many child
  window runs at a time and advances the queue as backfill-window ledger rows
  become terminal. This keeps the public submit call non-blocking while applying
  admission before child pipeline runs start.
  """

  use GenServer

  alias FavnOrchestrator.Backfill.BackfillWindow
  alias FavnOrchestrator.Backfill.Projector, as: BackfillProjector
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage
  alias FavnOrchestrator.TransitionWriter

  @poll_ms 100
  @restart_error {:backfill_child_admission_interrupted, :coordinator_restarted}
  @active_window_statuses [:pending, :running]
  @terminal_window_statuses [:ok, :partial, :cancelled, :timed_out, :error]

  @type child_spec :: %{
          required(:window_key) => String.t(),
          required(:child_opts) => keyword()
        }

  @type submitter :: (keyword() -> {:ok, String.t()} | {:error, term()})
  @type failure_handler :: (String.t(), term() -> :ok | {:error, term()})

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, %{}, name: name)
  end

  @doc false
  @spec start_backfill(
          FavnOrchestrator.RunState.t(),
          module(),
          [child_spec()],
          keyword()
        ) :: :ok | {:error, term()}
  def start_backfill(parent, pipeline_module, child_specs, opts)
      when is_list(child_specs) and is_list(opts) do
    GenServer.call(__MODULE__, {:start_backfill, parent, pipeline_module, child_specs, opts})
  end

  @impl true
  def init(state) do
    _ = terminalize_interrupted_finite_backfills()
    {:ok, state}
  end

  @impl true
  def handle_call(
        {:start_backfill, parent, pipeline_module, child_specs, opts},
        _from,
        state
      ) do
    limit = Keyword.fetch!(opts, :concurrency)
    submitter = Keyword.fetch!(opts, :submitter)
    failure_handler = Keyword.fetch!(opts, :failure_handler)

    entry = %{
      pipeline_module: pipeline_module,
      queue: :queue.from_list(child_specs),
      active: MapSet.new(),
      limit: limit,
      submitter: submitter,
      failure_handler: failure_handler
    }

    state = Map.put(state, parent.id, entry)

    case submit_available(parent.id, state) do
      {:ok, state} ->
        schedule_poll(parent.id)
        {:reply, :ok, state}

      {{:error, _reason} = error, state} ->
        {:reply, error, Map.delete(state, parent.id)}
    end
  end

  @impl true
  def handle_info({:poll_backfill, backfill_run_id}, state) do
    case Map.fetch(state, backfill_run_id) do
      {:ok, entry} ->
        state = refresh_active_windows(backfill_run_id, entry, state)

        case submit_available(backfill_run_id, state) do
          {:ok, state} ->
            if Map.has_key?(state, backfill_run_id), do: schedule_poll(backfill_run_id)
            {:noreply, state}

          {{:error, reason}, state} ->
            state = fail_backfill(backfill_run_id, reason, state)
            {:noreply, Map.delete(state, backfill_run_id)}
        end

      :error ->
        {:noreply, state}
    end
  end

  defp submit_available(backfill_run_id, state) do
    case Map.fetch(state, backfill_run_id) do
      {:ok, entry} ->
        available = max(entry.limit - MapSet.size(entry.active), 0)
        do_submit_available(backfill_run_id, entry, state, available)

      :error ->
        {:ok, state}
    end
  end

  defp do_submit_available(backfill_run_id, entry, state, 0) do
    {:ok, Map.put(state, backfill_run_id, entry)}
  end

  defp do_submit_available(backfill_run_id, entry, state, available) do
    case :queue.out(entry.queue) do
      {{:value, %{window_key: window_key, child_opts: child_opts}}, queue} ->
        case entry.submitter.(child_opts) do
          {:ok, _child_run_id} ->
            entry = %{entry | queue: queue, active: MapSet.put(entry.active, window_key)}
            do_submit_available(backfill_run_id, entry, state, available - 1)

          {:error, _reason} = error ->
            {error, Map.put(state, backfill_run_id, %{entry | queue: queue})}
        end

      {:empty, _queue} ->
        state =
          if MapSet.size(entry.active) == 0 do
            Map.delete(state, backfill_run_id)
          else
            Map.put(state, backfill_run_id, entry)
          end

        {:ok, state}
    end
  end

  defp refresh_active_windows(backfill_run_id, entry, state) do
    case BackfillProjector.list_all_backfill_windows(backfill_run_id: backfill_run_id) do
      {:ok, windows} ->
        terminal_keys =
          windows
          |> Enum.filter(&terminal_window?(entry.pipeline_module, &1))
          |> MapSet.new(& &1.window_key)

        entry = %{entry | active: MapSet.difference(entry.active, terminal_keys)}
        Map.put(state, backfill_run_id, entry)

      {:error, _reason} ->
        state
    end
  end

  defp terminal_window?(pipeline_module, %BackfillWindow{} = window) do
    window.pipeline_module == pipeline_module and window.status in @terminal_window_statuses
  end

  defp terminalize_interrupted_finite_backfills do
    case Storage.list_runs(status: :running) do
      {:ok, runs} ->
        runs
        |> Enum.filter(&finite_backfill_parent?/1)
        |> Enum.each(&terminalize_interrupted_backfill/1)

      {:error, _reason} ->
        :ok
    end
  end

  defp finite_backfill_parent?(%RunState{submit_kind: :backfill_pipeline, metadata: metadata}) do
    case backfill_metadata(metadata) do
      %{backfill_child_concurrency: value} when is_integer(value) and value > 0 -> true
      %{"backfill_child_concurrency" => value} when is_integer(value) and value > 0 -> true
      _other -> false
    end
  end

  defp finite_backfill_parent?(%RunState{}), do: false

  defp backfill_metadata(metadata) when is_map(metadata) do
    Map.get(metadata, :backfill) || Map.get(metadata, "backfill") || %{}
  end

  defp backfill_metadata(_metadata), do: %{}

  defp terminalize_interrupted_backfill(%RunState{} = parent) do
    with {:ok, windows} <- BackfillProjector.list_all_backfill_windows(backfill_run_id: parent.id),
         active_windows <- Enum.filter(windows, &(&1.status in @active_window_statuses)),
         true <- active_windows != [],
         :ok <- put_interrupted_windows(active_windows),
         {:ok, updated_windows} <-
           BackfillProjector.list_all_backfill_windows(backfill_run_id: parent.id) do
      status = BackfillProjector.parent_status(updated_windows)

      parent
      |> RunState.transition(
        status: status,
        error: @restart_error,
        result: %{status: status, backfill_windows: length(updated_windows)}
      )
      |> TransitionWriter.persist_transition(parent_event_type(status), %{
        status: status,
        error: @restart_error,
        window_counts: window_counts(updated_windows)
      })
    else
      false -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp put_interrupted_windows(windows) do
    now = DateTime.utc_now()

    Enum.reduce_while(windows, :ok, fn %BackfillWindow{} = window, :ok ->
      updated = %{
        window
        | status: :error,
          last_error: @restart_error,
          errors: window.errors ++ [@restart_error],
          finished_at: window.finished_at || now,
          updated_at: now
      }

      case Storage.put_backfill_window(updated) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp parent_event_type(:ok), do: :backfill_finished
  defp parent_event_type(:partial), do: :backfill_partial
  defp parent_event_type(:cancelled), do: :backfill_cancelled
  defp parent_event_type(:timed_out), do: :backfill_timed_out
  defp parent_event_type(:error), do: :backfill_failed
  defp parent_event_type(:running), do: :backfill_progressed

  defp window_counts(windows) do
    Enum.reduce(windows, %{}, fn %BackfillWindow{status: status}, acc ->
      Map.update(acc, status, 1, &(&1 + 1))
    end)
  end

  defp fail_backfill(backfill_run_id, reason, state) do
    case Map.fetch(state, backfill_run_id) do
      {:ok, %{failure_handler: failure_handler}} ->
        _ = failure_handler.(backfill_run_id, reason)
        state

      :error ->
        state
    end
  end

  defp schedule_poll(backfill_run_id) do
    Process.send_after(self(), {:poll_backfill, backfill_run_id}, @poll_ms)
  end
end
