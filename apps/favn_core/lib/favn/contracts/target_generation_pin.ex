defmodule Favn.Contracts.TargetGenerationPin do
  @moduledoc """
  Immutable target-generation identity pinned into runner work.

  Pins are used for persisted SQL inputs. They tell the runner which physical
  generation and relation a dependency read must use; a runner must not resolve
  a newer active binding on its own.
  """

  alias Favn.RelationRef
  alias Favn.Manifest.TargetDescriptor

  @uuid_pattern ~r/\A[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\z/
  @hash_pattern ~r/\A[0-9a-f]{64}\z/

  @enforce_keys [:asset_ref, :target_id, :target_generation_id, :relation, :descriptor_hash]
  defstruct [:asset_ref, :target_id, :target_generation_id, :relation, :descriptor_hash]

  @type t :: %__MODULE__{
          asset_ref: Favn.Ref.t(),
          target_id: String.t(),
          target_generation_id: String.t(),
          relation: RelationRef.t(),
          descriptor_hash: String.t()
        }

  @doc "Validates one immutable input-generation pin."
  @spec validate(t()) :: :ok | {:error, term()}
  def validate(%__MODULE__{} = pin) do
    with :ok <- validate_asset_ref(pin.asset_ref),
         :ok <- validate_identifier(:target_id, pin.target_id),
         :ok <- validate_generation_id(pin.target_generation_id),
         :ok <- validate_relation(pin.relation),
         :ok <- validate_hash(:descriptor_hash, pin.descriptor_hash) do
      :ok
    end
  end

  def validate(value), do: {:error, {:invalid_target_generation_pin, value}}

  @doc "Matches an upstream pin to its manifest target descriptor."
  @spec validate_target_identity(t(), TargetDescriptor.t()) :: :ok | {:error, term()}
  def validate_target_identity(%__MODULE__{} = pin, %TargetDescriptor{} = descriptor) do
    with :ok <- validate(pin),
         {:ok, descriptor} <- TargetDescriptor.validate(descriptor),
         :ok <- match_field(:target_id, pin.target_id, descriptor.target_id),
         :ok <- match_field(:descriptor_hash, pin.descriptor_hash, descriptor.descriptor_hash) do
      match_field(
        :relation,
        relation_identity(pin.relation),
        relation_identity(descriptor.relation)
      )
    end
  end

  def validate_target_identity(pin, descriptor),
    do: {:error, {:invalid_target_generation_identity, pin, descriptor}}

  defp validate_asset_ref({module, name}) when is_atom(module) and is_atom(name), do: :ok
  defp validate_asset_ref(value), do: {:error, {:invalid_generation_pin_asset_ref, value}}

  defp validate_identifier(_field, value)
       when is_binary(value) and byte_size(value) in 1..255,
       do: :ok

  defp validate_identifier(field, value),
    do: {:error, {:invalid_generation_pin_field, field, value}}

  defp validate_generation_id(value) when is_binary(value) do
    if Regex.match?(@uuid_pattern, value),
      do: :ok,
      else: {:error, {:invalid_target_generation_id, value}}
  end

  defp validate_generation_id(value), do: {:error, {:invalid_target_generation_id, value}}

  defp validate_hash(_field, value) when is_binary(value) do
    if Regex.match?(@hash_pattern, value),
      do: :ok,
      else: {:error, {:invalid_generation_pin_hash, value}}
  end

  defp validate_hash(field, value), do: {:error, {:invalid_generation_pin_field, field, value}}

  defp validate_relation(%RelationRef{} = relation) do
    RelationRef.validate!(relation)
    :ok
  rescue
    ArgumentError -> {:error, {:invalid_generation_pin_relation, relation}}
  end

  defp validate_relation(value), do: {:error, {:invalid_generation_pin_relation, value}}

  defp relation_identity(%RelationRef{} = relation) do
    %{
      connection: identifier_value(relation.connection),
      catalog: relation.catalog,
      schema: relation.schema,
      name: relation.name
    }
  end

  defp relation_identity(relation) when is_map(relation) do
    %{
      connection:
        identifier_value(Map.get(relation, :connection, Map.get(relation, "connection"))),
      catalog: Map.get(relation, :catalog, Map.get(relation, "catalog")),
      schema: Map.get(relation, :schema, Map.get(relation, "schema")),
      name: Map.get(relation, :name, Map.get(relation, "name"))
    }
  end

  defp identifier_value(value) when is_atom(value), do: Atom.to_string(value)
  defp identifier_value(value), do: value

  defp match_field(_field, value, value), do: :ok

  defp match_field(field, actual, expected),
    do: {:error, {:target_generation_pin_mismatch, field, actual, expected}}
end
