defmodule Favn.Contracts.GenerationMarker do
  @moduledoc """
  Generation marker stored beside a target in its data system.

  The marker is the data-plane authority used to reconcile an activation whose
  outcome was not observed by the orchestrator. It contains no credentials or
  control-plane connection data.
  """

  alias Favn.RelationRef

  @uuid_pattern ~r/\A[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\z/

  @enforce_keys [
    :target_id,
    :active_relation,
    :active_generation_id,
    :activation_operation_id,
    :activation_token,
    :activated_at
  ]
  defstruct [
    :target_id,
    :active_relation,
    :active_generation_id,
    :activation_operation_id,
    :activation_token,
    :activated_at
  ]

  @type t :: %__MODULE__{
          target_id: String.t(),
          active_relation: RelationRef.t(),
          active_generation_id: String.t(),
          activation_operation_id: String.t(),
          activation_token: String.t(),
          activated_at: DateTime.t()
        }

  @doc "Validates a complete data-plane generation marker."
  @spec validate(t()) :: :ok | {:error, term()}
  def validate(%__MODULE__{} = marker) do
    with :ok <- identifier(:target_id, marker.target_id),
         :ok <- relation(marker.active_relation),
         :ok <- generation_id(marker.active_generation_id),
         :ok <- identifier(:activation_operation_id, marker.activation_operation_id),
         :ok <- identifier(:activation_token, marker.activation_token),
         :ok <- datetime(marker.activated_at) do
      :ok
    end
  end

  def validate(value), do: {:error, {:invalid_generation_marker, value}}

  defp identifier(_field, value) when is_binary(value) and byte_size(value) in 1..255, do: :ok
  defp identifier(field, value), do: {:error, {:invalid_generation_marker_field, field, value}}

  defp generation_id(value) when is_binary(value) do
    if Regex.match?(@uuid_pattern, value),
      do: :ok,
      else: {:error, {:invalid_target_generation_id, value}}
  end

  defp generation_id(value), do: {:error, {:invalid_target_generation_id, value}}

  defp relation(%RelationRef{} = relation) do
    RelationRef.validate!(relation)
    :ok
  rescue
    ArgumentError -> {:error, {:invalid_generation_marker_relation, relation}}
  end

  defp relation(value), do: {:error, {:invalid_generation_marker_relation, value}}

  defp datetime(%DateTime{}), do: :ok
  defp datetime(value), do: {:error, {:invalid_generation_marker_timestamp, value}}
end
