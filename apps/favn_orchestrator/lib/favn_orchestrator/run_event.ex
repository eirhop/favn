defmodule FavnOrchestrator.RunEvent do
  @moduledoc """
  Canonical operator-facing run event published by the orchestrator.
  """

  @type t :: %__MODULE__{
          schema_version: pos_integer(),
          run_id: String.t(),
          sequence: pos_integer(),
          event_type: atom() | String.t(),
          entity: :run | :step,
          occurred_at: DateTime.t(),
          status: atom() | String.t() | nil,
          manifest_version_id: String.t() | nil,
          manifest_content_hash: String.t() | nil,
          asset_ref: Favn.Ref.t() | nil,
          stage: non_neg_integer() | nil,
          data: map()
        }

  defstruct [
    :run_id,
    :sequence,
    :event_type,
    :occurred_at,
    schema_version: 1,
    entity: :run,
    status: nil,
    manifest_version_id: nil,
    manifest_content_hash: nil,
    asset_ref: nil,
    stage: nil,
    data: %{}
  ]

  @spec from_map(map()) :: t()
  def from_map(event) when is_map(event) do
    event_type = Map.get(event, :event_type)
    entity = infer_entity(Map.get(event, :entity), event_type)

    %__MODULE__{
      schema_version: normalize_schema_version(Map.get(event, :schema_version)),
      run_id: Map.get(event, :run_id),
      sequence: Map.get(event, :sequence),
      event_type: event_type,
      entity: entity,
      occurred_at: normalize_occurred_at(Map.get(event, :occurred_at)),
      status: Map.get(event, :status),
      manifest_version_id: Map.get(event, :manifest_version_id),
      manifest_content_hash: Map.get(event, :manifest_content_hash),
      asset_ref: normalize_asset_ref(Map.get(event, :asset_ref), Map.get(event, :data), entity),
      stage: normalize_stage(Map.get(event, :stage), Map.get(event, :data), entity),
      data: normalize_data(Map.get(event, :data))
    }
  end

  @spec to_map(t()) :: map()
  def to_map(%__MODULE__{} = event) do
    %{
      schema_version: event.schema_version,
      run_id: event.run_id,
      sequence: event.sequence,
      event_type: event.event_type,
      entity: event.entity,
      occurred_at: event.occurred_at,
      status: event.status,
      manifest_version_id: event.manifest_version_id,
      manifest_content_hash: event.manifest_content_hash,
      asset_ref: event.asset_ref,
      stage: event.stage,
      data: event.data
    }
  end

  defp normalize_schema_version(value) when is_integer(value) and value > 0, do: value
  defp normalize_schema_version(_value), do: 1

  defp normalize_occurred_at(%DateTime{} = occurred_at), do: occurred_at

  defp normalize_occurred_at(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, occurred_at, _offset} -> occurred_at
      _ -> DateTime.utc_now()
    end
  end

  defp normalize_occurred_at(_value), do: DateTime.utc_now()

  defp normalize_asset_ref(_value, _data, :run), do: nil

  defp normalize_asset_ref({module, name} = ref, _data, :step)
       when is_atom(module) and is_atom(name),
       do: ref

  defp normalize_asset_ref(_value, data, :step) when is_map(data) do
    case Map.get(data, :asset_ref) do
      {module, name} = ref when is_atom(module) and is_atom(name) -> ref
      _ -> nil
    end
  end

  defp normalize_asset_ref(_value, _data, _entity), do: nil

  defp normalize_stage(_value, _data, :run), do: nil

  defp normalize_stage(value, _data, :step) when is_integer(value) and value >= 0, do: value

  defp normalize_stage(_value, data, :step) when is_map(data) do
    case Map.get(data, :stage) do
      stage when is_integer(stage) and stage >= 0 -> stage
      _ -> nil
    end
  end

  defp normalize_stage(_value, _data, _entity), do: nil

  defp normalize_data(data) when is_map(data), do: data
  defp normalize_data(_value), do: %{}

  defp infer_entity(:run, _event_type), do: :run
  defp infer_entity(:step, _event_type), do: :step

  defp infer_entity(_entity, event_type) when is_atom(event_type) do
    if String.starts_with?(Atom.to_string(event_type), "step_"), do: :step, else: :run
  end

  defp infer_entity(_entity, event_type) when is_binary(event_type) do
    if String.starts_with?(event_type, "step_"), do: :step, else: :run
  end

  defp infer_entity(_entity, _event_type), do: :run
end
