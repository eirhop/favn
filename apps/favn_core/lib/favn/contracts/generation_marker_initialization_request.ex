defmodule Favn.Contracts.GenerationMarkerInitializationRequest do
  @moduledoc """
  Idempotent request to establish the sidecar marker for an initial generation.

  The initial ordinary materialization already has durable control-plane
  evidence. The runner may write this marker only when the stable relation's
  physical fingerprint still matches that evidence. Reusing the same operation
  identity and token is an exact replay, never a second initialization.
  """

  alias Favn.Contracts.RunnerReleaseBinding
  alias Favn.RelationRef

  @uuid_pattern ~r/\A[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\z/
  @hash_pattern ~r/\A[0-9a-f]{64}\z/

  @enforce_keys [
    :manifest_version_id,
    :manifest_content_hash,
    :required_runner_release_id,
    :target_id,
    :target_generation_id,
    :active_relation,
    :expected_physical_fingerprint,
    :initialization_operation_id,
    :initialization_token
  ]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          manifest_version_id: String.t(),
          manifest_content_hash: String.t(),
          required_runner_release_id: String.t(),
          target_id: String.t(),
          target_generation_id: String.t(),
          active_relation: RelationRef.t(),
          expected_physical_fingerprint: String.t(),
          initialization_operation_id: String.t(),
          initialization_token: String.t()
        }

  @doc "Validates the exact initial-generation marker identity."
  @spec validate(t()) :: :ok | {:error, term()}
  def validate(%__MODULE__{} = request) do
    with :ok <- identifier(:manifest_version_id, request.manifest_version_id),
         :ok <- hash(:manifest_content_hash, request.manifest_content_hash),
         :ok <- RunnerReleaseBinding.validate(request.required_runner_release_id),
         :ok <- identifier(:target_id, request.target_id),
         :ok <- generation_id(request.target_generation_id),
         :ok <- relation(request.active_relation),
         :ok <- hash(:expected_physical_fingerprint, request.expected_physical_fingerprint),
         :ok <- identifier(:initialization_operation_id, request.initialization_operation_id) do
      identifier(:initialization_token, request.initialization_token)
    end
  end

  def validate(value), do: {:error, {:invalid_generation_marker_initialization_request, value}}

  defp identifier(_field, value) when is_binary(value) and byte_size(value) in 1..255, do: :ok
  defp identifier(field, value), do: {:error, {:invalid_initialization_field, field, value}}

  defp generation_id(value) when is_binary(value) do
    if Regex.match?(@uuid_pattern, value),
      do: :ok,
      else: {:error, {:invalid_target_generation_id, value}}
  end

  defp generation_id(value), do: {:error, {:invalid_target_generation_id, value}}

  defp hash(_field, value) when is_binary(value) do
    if Regex.match?(@hash_pattern, value),
      do: :ok,
      else: {:error, {:invalid_initialization_hash, value}}
  end

  defp hash(field, value), do: {:error, {:invalid_initialization_field, field, value}}

  defp relation(%RelationRef{} = relation) do
    RelationRef.validate!(relation)
    :ok
  rescue
    ArgumentError -> {:error, {:invalid_initialization_relation, relation}}
  end

  defp relation(value), do: {:error, {:invalid_initialization_relation, value}}
end
