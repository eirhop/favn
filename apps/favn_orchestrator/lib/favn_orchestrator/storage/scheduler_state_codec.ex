defmodule FavnOrchestrator.Storage.SchedulerStateCodec do
  @moduledoc false

  alias Favn.Scheduler.State, as: SchedulerState

  @format "favn.scheduler_state.storage"
  @schema_version 1

  @state_dto_fields [
    "schedule_fingerprint",
    "last_evaluated_at",
    "last_due_at",
    "last_submitted_due_at",
    "in_flight_run_id",
    "queued_due_at",
    "updated_at"
  ]

  @type key :: {module(), atom() | nil}

  @spec encode_state(map() | SchedulerState.t()) :: {:ok, String.t()} | {:error, term()}
  def encode_state(state) do
    with {:ok, normalized} <- normalize_state(state) do
      payload = %{
        "format" => @format,
        "schema_version" => @schema_version,
        "state" => state_to_dto(Map.delete(normalized, :version))
      }

      {:ok, Jason.encode!(payload)}
    end
  rescue
    error -> {:error, {:scheduler_state_encode_failed, error}}
  end

  @spec decode_state(String.t()) :: {:ok, map()} | {:error, term()}
  def decode_state(payload) when is_binary(payload) do
    case Jason.decode(payload) do
      {:ok, %{"format" => @format, "schema_version" => @schema_version, "state" => state}}
      when is_map(state) ->
        with :ok <- reject_unknown_state_fields(state),
             {:ok, decoded} <- state_from_dto(state) do
          normalize_state(decoded)
        end

      {:ok, %{"format" => @format, "schema_version" => version}}
      when is_integer(version) ->
        {:error, {:unsupported_scheduler_state_schema_version, version}}

      {:ok, other} ->
        {:error, {:invalid_scheduler_state_dto, other}}

      {:error, reason} ->
        {:error, {:invalid_scheduler_state_json, reason}}
    end
  end

  def decode_state(value), do: {:error, {:invalid_scheduler_state_payload, value}}

  @spec build_state(key(), pos_integer(), map()) :: {:ok, SchedulerState.t()} | {:error, term()}
  def build_state(key, version, state) when is_integer(version) and version > 0 do
    with {:ok, {pipeline_module, schedule_id}} <- normalize_key(key),
         {:ok, normalized} <- normalize_state(Map.put(state, :version, version)) do
      {:ok,
       struct(
         SchedulerState,
         Map.merge(normalized, %{pipeline_module: pipeline_module, schedule_id: schedule_id})
       )}
    end
  end

  def build_state(_key, version, _state),
    do: {:error, {:invalid_scheduler_field, :version, version}}

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

  defp state_to_dto(state) do
    %{
      "schedule_fingerprint" => Map.get(state, :schedule_fingerprint),
      "last_evaluated_at" => datetime_to_dto(Map.get(state, :last_evaluated_at)),
      "last_due_at" => datetime_to_dto(Map.get(state, :last_due_at)),
      "last_submitted_due_at" => datetime_to_dto(Map.get(state, :last_submitted_due_at)),
      "in_flight_run_id" => Map.get(state, :in_flight_run_id),
      "queued_due_at" => datetime_to_dto(Map.get(state, :queued_due_at)),
      "updated_at" => datetime_to_dto(Map.get(state, :updated_at))
    }
  end

  defp state_from_dto(state) do
    with {:ok, last_evaluated_at} <- datetime_from_dto(:last_evaluated_at, state),
         {:ok, last_due_at} <- datetime_from_dto(:last_due_at, state),
         {:ok, last_submitted_due_at} <- datetime_from_dto(:last_submitted_due_at, state),
         {:ok, queued_due_at} <- datetime_from_dto(:queued_due_at, state),
         {:ok, updated_at} <- datetime_from_dto(:updated_at, state) do
      {:ok,
       %{
         schedule_fingerprint: Map.get(state, "schedule_fingerprint"),
         last_evaluated_at: last_evaluated_at,
         last_due_at: last_due_at,
         last_submitted_due_at: last_submitted_due_at,
         in_flight_run_id: Map.get(state, "in_flight_run_id"),
         queued_due_at: queued_due_at,
         updated_at: updated_at
       }}
    end
  end

  defp reject_unknown_state_fields(state) do
    unknown = state |> Map.keys() |> Kernel.--(@state_dto_fields) |> Enum.sort()

    case unknown do
      [] -> :ok
      fields -> {:error, {:unknown_scheduler_state_fields, fields}}
    end
  end

  defp datetime_to_dto(nil), do: nil
  defp datetime_to_dto(%DateTime{} = datetime), do: DateTime.to_iso8601(datetime)

  defp datetime_from_dto(field, state) do
    case Map.get(state, Atom.to_string(field)) do
      nil ->
        {:ok, nil}

      value when is_binary(value) ->
        case DateTime.from_iso8601(value) do
          {:ok, datetime, _offset} -> {:ok, datetime}
          _ -> {:error, {:invalid_scheduler_state_field, field, value}}
        end

      value ->
        {:error, {:invalid_scheduler_state_field, field, value}}
    end
  end

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
