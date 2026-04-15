defmodule FavnOrchestrator.Storage.SchedulerStateCodec do
  @moduledoc false

  alias Favn.Scheduler.State, as: SchedulerState

  @type key :: {module(), atom() | nil}

  @spec normalize_key(term()) :: {:ok, key()} | {:error, term()}
  def normalize_key({pipeline_module, schedule_id}) when is_atom(pipeline_module) do
    if is_atom(schedule_id) or is_nil(schedule_id) do
      {:ok, {pipeline_module, schedule_id}}
    else
      {:error, {:invalid_scheduler_key, {pipeline_module, schedule_id}}}
    end
  end

  def normalize_key(value), do: {:error, {:invalid_scheduler_key, value}}

  @spec normalize_state(map() | SchedulerState.t()) :: {:ok, map()} | {:error, term()}
  def normalize_state(%SchedulerState{} = scheduler_state) do
    scheduler_state
    |> Map.from_struct()
    |> Map.drop([:pipeline_module, :schedule_id])
    |> normalize_state()
  end

  def normalize_state(state) when is_map(state) do
    with :ok <-
           validate_optional_binary(:schedule_fingerprint, Map.get(state, :schedule_fingerprint)),
         :ok <- validate_optional_datetime(:last_evaluated_at, Map.get(state, :last_evaluated_at)),
         :ok <- validate_optional_datetime(:last_due_at, Map.get(state, :last_due_at)),
         :ok <-
           validate_optional_datetime(
             :last_submitted_due_at,
             Map.get(state, :last_submitted_due_at)
           ),
         :ok <- validate_optional_binary(:in_flight_run_id, Map.get(state, :in_flight_run_id)),
         :ok <- validate_optional_datetime(:queued_due_at, Map.get(state, :queued_due_at)),
         :ok <- validate_optional_datetime(:updated_at, Map.get(state, :updated_at)),
         :ok <- validate_optional_version(Map.get(state, :version)) do
      {:ok,
       %{
         schedule_fingerprint: Map.get(state, :schedule_fingerprint),
         last_evaluated_at: Map.get(state, :last_evaluated_at),
         last_due_at: Map.get(state, :last_due_at),
         last_submitted_due_at: Map.get(state, :last_submitted_due_at),
         in_flight_run_id: Map.get(state, :in_flight_run_id),
         queued_due_at: Map.get(state, :queued_due_at),
         updated_at: Map.get(state, :updated_at),
         version: Map.get(state, :version)
       }}
    end
  end

  def normalize_state(value), do: {:error, {:invalid_scheduler_state, value}}

  defp validate_optional_binary(_field, nil), do: :ok
  defp validate_optional_binary(_field, value) when is_binary(value), do: :ok

  defp validate_optional_binary(field, value),
    do: {:error, {:invalid_scheduler_field, field, value}}

  defp validate_optional_datetime(_field, nil), do: :ok
  defp validate_optional_datetime(_field, %DateTime{}), do: :ok

  defp validate_optional_datetime(field, value),
    do: {:error, {:invalid_scheduler_field, field, value}}

  defp validate_optional_version(nil), do: :ok
  defp validate_optional_version(value) when is_integer(value) and value > 0, do: :ok
  defp validate_optional_version(value), do: {:error, {:invalid_scheduler_field, :version, value}}
end
