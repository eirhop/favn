defmodule Favn.Scheduler.Storage.SQLite do
  @moduledoc false

  alias Favn.Scheduler.State
  alias Favn.Storage.SQLite.Repo

  @spec get_state(module()) :: {:ok, State.t() | nil} | {:error, term()}
  def get_state(pipeline_module) when is_atom(pipeline_module) do
    with :ok <- ensure_repo_started() do
      sql = """
      SELECT schedule_id, schedule_fingerprint, last_evaluated_at, last_due_at, last_submitted_due_at, in_flight_run_id, queued_due_at, updated_at
      FROM scheduler_states
      WHERE pipeline_module = ?1
      LIMIT 1
      """

      case Ecto.Adapters.SQL.query(Repo, sql, [Atom.to_string(pipeline_module)]) do
        {:ok, %{rows: []}} -> {:ok, nil}
        {:ok, %{rows: [row]}} -> {:ok, from_row(pipeline_module, row)}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @spec put_state(State.t()) :: :ok | {:error, term()}
  def put_state(%State{} = state) do
    with :ok <- ensure_repo_started() do
      sql = """
      INSERT INTO scheduler_states (
        pipeline_module,
        schedule_id,
        schedule_fingerprint,
        last_evaluated_at,
        last_due_at,
        last_submitted_due_at,
        in_flight_run_id,
        queued_due_at,
        updated_at
      ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
      ON CONFLICT(pipeline_module) DO UPDATE SET
        schedule_id = excluded.schedule_id,
        schedule_fingerprint = excluded.schedule_fingerprint,
        last_evaluated_at = excluded.last_evaluated_at,
        last_due_at = excluded.last_due_at,
        last_submitted_due_at = excluded.last_submitted_due_at,
        in_flight_run_id = excluded.in_flight_run_id,
        queued_due_at = excluded.queued_due_at,
        updated_at = excluded.updated_at
      """

      params = [
        Atom.to_string(state.pipeline_module),
        if(is_atom(state.schedule_id), do: Atom.to_string(state.schedule_id), else: nil),
        state.schedule_fingerprint,
        iso(state.last_evaluated_at),
        iso(state.last_due_at),
        iso(state.last_submitted_due_at),
        state.in_flight_run_id,
        iso(state.queued_due_at),
        iso(DateTime.utc_now())
      ]

      case Ecto.Adapters.SQL.query(Repo, sql, params) do
        {:ok, _} -> :ok
        {:error, reason} -> {:error, reason}
      end
    end
  end

  defp from_row(pipeline_module, [
         schedule_id,
         schedule_fingerprint,
         last_eval,
         last_due,
         last_sub,
         in_flight,
         queued,
         updated
       ]) do
    %State{
      pipeline_module: pipeline_module,
      schedule_id: parse_schedule_id(schedule_id),
      schedule_fingerprint: schedule_fingerprint,
      last_evaluated_at: from_iso(last_eval),
      last_due_at: from_iso(last_due),
      last_submitted_due_at: from_iso(last_sub),
      in_flight_run_id: in_flight,
      queued_due_at: from_iso(queued),
      updated_at: from_iso(updated)
    }
  end

  defp ensure_repo_started do
    if Process.whereis(Repo) == nil, do: {:error, :sqlite_repo_not_started}, else: :ok
  end

  defp iso(nil), do: nil
  defp iso(%DateTime{} = dt), do: DateTime.to_iso8601(dt)

  defp from_iso(nil), do: nil
  defp from_iso(value) when is_binary(value), do: DateTime.from_iso8601(value) |> elem(1)

  defp parse_schedule_id(nil), do: nil

  defp parse_schedule_id(value) when is_binary(value) do
    String.to_existing_atom(value)
  rescue
    ArgumentError -> nil
  end
end
