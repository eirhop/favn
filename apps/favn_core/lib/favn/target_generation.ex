defmodule Favn.TargetGeneration do
  @moduledoc """
  Stable identity for one physical generation of a persisted SQL target.

  A target generation is scoped by workspace and logical target. Its UUID is
  independent of manifest releases so compatible manifests may continue using
  the same active physical data.
  """

  alias Favn.GenerationDataPlaneMarker

  @type status :: :building | :active | :retired | :failed | :discarded

  @type t :: %__MODULE__{
          workspace_id: String.t(),
          target_id: String.t(),
          target_generation_id: String.t(),
          creating_manifest_id: String.t(),
          creating_descriptor_hash: String.t(),
          active_descriptor_hash: String.t() | nil,
          logical_relation: map(),
          physical_relation: map(),
          physical_schema_fingerprint: String.t() | nil,
          data_plane_marker: GenerationDataPlaneMarker.t() | nil,
          status: status(),
          rebuild_operation_id: String.t() | nil,
          version: pos_integer(),
          created_at: DateTime.t(),
          activated_at: DateTime.t() | nil,
          retired_at: DateTime.t() | nil,
          updated_at: DateTime.t()
        }

  @enforce_keys [
    :workspace_id,
    :target_id,
    :target_generation_id,
    :creating_manifest_id,
    :creating_descriptor_hash,
    :logical_relation,
    :physical_relation,
    :status,
    :version,
    :created_at,
    :updated_at
  ]
  defstruct [
    :workspace_id,
    :target_id,
    :target_generation_id,
    :creating_manifest_id,
    :creating_descriptor_hash,
    :active_descriptor_hash,
    :logical_relation,
    :physical_relation,
    :physical_schema_fingerprint,
    :data_plane_marker,
    :status,
    :rebuild_operation_id,
    :version,
    :created_at,
    :activated_at,
    :retired_at,
    :updated_at
  ]

  @statuses [:building, :active, :retired, :failed, :discarded]
  @uuid_pattern ~r/\A[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\z/
  @hash_pattern ~r/\A[0-9a-f]{64}\z/

  @doc "Builds and validates a target-generation value returned by the control plane."
  @spec new(map() | keyword()) :: {:ok, t()} | {:error, term()}
  def new(attrs) when is_map(attrs) or is_list(attrs) do
    generation = struct(__MODULE__, Map.new(attrs))

    with :ok <- validate_identifier(:workspace_id, generation.workspace_id),
         :ok <- validate_identifier(:target_id, generation.target_id),
         :ok <- validate_uuid(generation.target_generation_id),
         :ok <- validate_identifier(:creating_manifest_id, generation.creating_manifest_id),
         :ok <- validate_hash(:creating_descriptor_hash, generation.creating_descriptor_hash),
         :ok <- validate_optional_hash(:active_descriptor_hash, generation.active_descriptor_hash),
         :ok <-
           validate_optional_hash(
             :physical_schema_fingerprint,
             generation.physical_schema_fingerprint
           ),
         :ok <- validate_data_plane_marker(generation),
         :ok <-
           validate_optional_identifier(:rebuild_operation_id, generation.rebuild_operation_id),
         :ok <- validate_relations(generation),
         :ok <- validate_status(generation.status),
         :ok <- validate_version(generation.version),
         :ok <- validate_timestamps(generation) do
      {:ok, generation}
    end
  rescue
    KeyError -> {:error, :invalid_target_generation}
  end

  def new(_attrs), do: {:error, :invalid_target_generation}

  @doc "Returns the supported target-generation lifecycle states."
  @spec statuses() :: [status()]
  def statuses, do: @statuses

  defp validate_identifier(_field, value)
       when is_binary(value) and byte_size(value) in 1..255,
       do: :ok

  defp validate_identifier(field, value),
    do: {:error, {:invalid_target_generation_field, field, value}}

  defp validate_optional_identifier(_field, nil), do: :ok
  defp validate_optional_identifier(field, value), do: validate_identifier(field, value)

  defp validate_uuid(value) when is_binary(value) do
    if Regex.match?(@uuid_pattern, value),
      do: :ok,
      else: {:error, {:invalid_target_generation_id, value}}
  end

  defp validate_uuid(value), do: {:error, {:invalid_target_generation_id, value}}

  defp validate_hash(_field, value) when is_binary(value) do
    if Regex.match?(@hash_pattern, value),
      do: :ok,
      else: {:error, {:invalid_target_generation_hash, value}}
  end

  defp validate_hash(field, value), do: {:error, {:invalid_target_generation_field, field, value}}

  defp validate_optional_hash(_field, nil), do: :ok
  defp validate_optional_hash(field, value), do: validate_hash(field, value)

  defp validate_data_plane_marker(%__MODULE__{data_plane_marker: nil}), do: :ok

  defp validate_data_plane_marker(%__MODULE__{} = generation) do
    GenerationDataPlaneMarker.validate(
      generation.data_plane_marker,
      generation.target_id,
      generation.target_generation_id
    )
  end

  defp validate_relations(%__MODULE__{logical_relation: logical, physical_relation: physical})
       when is_map(logical) and map_size(logical) > 0 and is_map(physical) and
              map_size(physical) > 0,
       do: :ok

  defp validate_relations(_generation), do: {:error, :invalid_target_generation_relations}

  defp validate_status(status) when status in @statuses, do: :ok
  defp validate_status(status), do: {:error, {:invalid_target_generation_status, status}}

  defp validate_version(version) when is_integer(version) and version > 0, do: :ok
  defp validate_version(version), do: {:error, {:invalid_target_generation_version, version}}

  defp validate_timestamps(%__MODULE__{
         created_at: %DateTime{},
         activated_at: activated_at,
         retired_at: retired_at,
         updated_at: %DateTime{}
       }) do
    if optional_datetime?(activated_at) and optional_datetime?(retired_at),
      do: :ok,
      else: {:error, :invalid_target_generation_timestamps}
  end

  defp validate_timestamps(_generation), do: {:error, :invalid_target_generation_timestamps}

  defp optional_datetime?(nil), do: true
  defp optional_datetime?(%DateTime{}), do: true
  defp optional_datetime?(_value), do: false
end
