defmodule Favn.Contracts.GenerationActivationRequest do
  @moduledoc """
  Idempotent request to atomically activate one candidate SQL generation.

  The runner forwards this identity to a generation-capable SQL adapter. The
  orchestrator persists the activation token before dispatch, and may later use
  the same request for marker reconciliation. No control-plane credentials are
  part of this value.
  """

  alias Favn.Contracts.GenerationMarker
  alias Favn.Contracts.RunnerReleaseBinding
  alias Favn.RelationRef

  @uuid_pattern ~r/\A[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\z/
  @hash_pattern ~r/\A[0-9a-f]{64}\z/

  @enforce_keys [
    :manifest_version_id,
    :manifest_content_hash,
    :required_runner_release_id,
    :rebuild_operation_id,
    :rebuild_action_id,
    :target_id,
    :previous_generation_id,
    :candidate_generation_id,
    :active_relation,
    :candidate_relation,
    :retired_relation,
    :expected_candidate_fingerprint,
    :activation_token,
    :expected_marker
  ]
  defstruct [
    :manifest_version_id,
    :manifest_content_hash,
    :required_runner_release_id,
    :rebuild_operation_id,
    :rebuild_action_id,
    :target_id,
    :previous_generation_id,
    :candidate_generation_id,
    :active_relation,
    :candidate_relation,
    :retired_relation,
    :expected_candidate_fingerprint,
    :activation_token,
    :expected_marker
  ]

  @type t :: %__MODULE__{
          manifest_version_id: String.t(),
          manifest_content_hash: String.t(),
          required_runner_release_id: String.t(),
          rebuild_operation_id: String.t(),
          rebuild_action_id: String.t(),
          target_id: String.t(),
          previous_generation_id: String.t(),
          candidate_generation_id: String.t(),
          active_relation: RelationRef.t(),
          candidate_relation: RelationRef.t(),
          retired_relation: RelationRef.t(),
          expected_candidate_fingerprint: String.t(),
          activation_token: String.t(),
          expected_marker: GenerationMarker.t()
        }

  @doc "Validates all identities required before an activation may execute."
  @spec validate(t()) :: :ok | {:error, term()}
  def validate(%__MODULE__{} = request) do
    with :ok <- identifier(:manifest_version_id, request.manifest_version_id),
         :ok <- hash(request.manifest_content_hash),
         :ok <- RunnerReleaseBinding.validate(request.required_runner_release_id),
         :ok <- identifier(:rebuild_operation_id, request.rebuild_operation_id),
         :ok <- identifier(:rebuild_action_id, request.rebuild_action_id),
         :ok <- identifier(:target_id, request.target_id),
         :ok <- generation_id(:previous_generation_id, request.previous_generation_id),
         :ok <- generation_id(:candidate_generation_id, request.candidate_generation_id),
         :ok <- distinct_generations(request),
         :ok <- relation(:active_relation, request.active_relation),
         :ok <- relation(:candidate_relation, request.candidate_relation),
         :ok <- relation(:retired_relation, request.retired_relation),
         :ok <- related_relations(request),
         :ok <- fingerprint(request.expected_candidate_fingerprint),
         :ok <- identifier(:activation_token, request.activation_token) do
      validate_expected_marker(request)
    end
  end

  def validate(value), do: {:error, {:invalid_generation_activation_request, value}}

  defp validate_expected_marker(
         %__MODULE__{expected_marker: %GenerationMarker{} = marker} = request
       ) do
    with :ok <- GenerationMarker.validate(marker),
         :ok <- match_field(:target_id, marker.target_id, request.target_id),
         :ok <-
           match_field(
             :active_generation_id,
             marker.active_generation_id,
             request.previous_generation_id
           ) do
      match_field(:active_relation, marker.active_relation, request.active_relation)
    end
  end

  defp validate_expected_marker(%__MODULE__{expected_marker: marker}),
    do: {:error, {:invalid_expected_generation_marker, marker}}

  defp distinct_generations(%__MODULE__{
         previous_generation_id: generation_id,
         candidate_generation_id: generation_id
       }),
       do: {:error, :candidate_generation_must_be_distinct}

  defp distinct_generations(%__MODULE__{}), do: :ok

  defp related_relations(%__MODULE__{} = request) do
    relations = [request.active_relation, request.candidate_relation, request.retired_relation]
    namespaces = Enum.map(relations, &relation_namespace/1)
    names = Enum.map(relations, & &1.name)

    cond do
      length(Enum.uniq(namespaces)) != 1 -> {:error, :generation_relation_namespace_mismatch}
      length(Enum.uniq(names)) != 3 -> {:error, :generation_relation_names_must_be_distinct}
      true -> :ok
    end
  end

  defp relation_namespace(relation),
    do: {relation.connection, relation.catalog, relation.schema}

  defp identifier(_field, value) when is_binary(value) and byte_size(value) in 1..255, do: :ok
  defp identifier(field, value), do: {:error, {:invalid_activation_field, field, value}}

  defp hash(value) when is_binary(value) do
    if Regex.match?(@hash_pattern, value),
      do: :ok,
      else: {:error, {:invalid_manifest_content_hash, value}}
  end

  defp hash(value), do: {:error, {:invalid_manifest_content_hash, value}}

  defp fingerprint(value) when is_binary(value) do
    if Regex.match?(@hash_pattern, value),
      do: :ok,
      else: {:error, {:invalid_expected_candidate_fingerprint, value}}
  end

  defp fingerprint(value), do: {:error, {:invalid_expected_candidate_fingerprint, value}}

  defp generation_id(field, value) when is_binary(value) do
    if Regex.match?(@uuid_pattern, value),
      do: :ok,
      else: {:error, {:invalid_activation_field, field, value}}
  end

  defp generation_id(field, value), do: {:error, {:invalid_activation_field, field, value}}

  defp relation(_field, %RelationRef{} = relation) do
    RelationRef.validate!(relation)
    :ok
  rescue
    ArgumentError -> {:error, {:invalid_activation_relation, relation}}
  end

  defp relation(field, value), do: {:error, {:invalid_activation_field, field, value}}

  defp match_field(_field, value, value), do: :ok

  defp match_field(field, actual, expected),
    do: {:error, {:generation_activation_identity_mismatch, field, actual, expected}}
end
