defmodule Favn.Contracts.GenerationDiscardRequest do
  @moduledoc """
  Idempotent request to discard one non-active candidate or retired relation.

  The adapter must inspect its generation marker and reject a request when the
  candidate generation is active. This protects a committed activation whose
  runner reply was lost.
  """

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
    :candidate_generation_id,
    :active_relation,
    :candidate_relation,
    :discard_token
  ]
  defstruct [
    :manifest_version_id,
    :manifest_content_hash,
    :required_runner_release_id,
    :rebuild_operation_id,
    :rebuild_action_id,
    :target_id,
    :candidate_generation_id,
    :active_relation,
    :candidate_relation,
    :discard_token,
    relation_kind: :candidate
  ]

  @type t :: %__MODULE__{
          manifest_version_id: String.t(),
          manifest_content_hash: String.t(),
          required_runner_release_id: String.t(),
          rebuild_operation_id: String.t(),
          rebuild_action_id: String.t(),
          target_id: String.t(),
          candidate_generation_id: String.t(),
          active_relation: RelationRef.t(),
          candidate_relation: RelationRef.t(),
          discard_token: String.t(),
          relation_kind: :candidate | :retired
        }

  @doc "Validates the exact candidate identity to discard."
  @spec validate(t()) :: :ok | {:error, term()}
  def validate(%__MODULE__{} = request) do
    with :ok <- identifier(:manifest_version_id, request.manifest_version_id),
         :ok <- hash(request.manifest_content_hash),
         :ok <- RunnerReleaseBinding.validate(request.required_runner_release_id),
         :ok <- identifier(:rebuild_operation_id, request.rebuild_operation_id),
         :ok <- identifier(:rebuild_action_id, request.rebuild_action_id),
         :ok <- identifier(:target_id, request.target_id),
         :ok <- generation_id(request.candidate_generation_id),
         :ok <- relation_kind(request.relation_kind),
         :ok <- relation(request.active_relation),
         :ok <- relation(request.candidate_relation),
         :ok <- related_relations(request) do
      identifier(:discard_token, request.discard_token)
    end
  end

  def validate(value), do: {:error, {:invalid_generation_discard_request, value}}

  defp identifier(_field, value) when is_binary(value) and byte_size(value) in 1..255, do: :ok
  defp identifier(field, value), do: {:error, {:invalid_discard_field, field, value}}

  defp hash(value) when is_binary(value) do
    if Regex.match?(@hash_pattern, value),
      do: :ok,
      else: {:error, {:invalid_manifest_content_hash, value}}
  end

  defp hash(value), do: {:error, {:invalid_manifest_content_hash, value}}

  defp generation_id(value) when is_binary(value) do
    if Regex.match?(@uuid_pattern, value),
      do: :ok,
      else: {:error, {:invalid_candidate_generation_id, value}}
  end

  defp generation_id(value), do: {:error, {:invalid_candidate_generation_id, value}}

  defp relation_kind(kind) when kind in [:candidate, :retired], do: :ok
  defp relation_kind(kind), do: {:error, {:invalid_discard_relation_kind, kind}}

  defp relation(%RelationRef{} = relation) do
    RelationRef.validate!(relation)
    :ok
  rescue
    ArgumentError -> {:error, {:invalid_candidate_relation, relation}}
  end

  defp relation(value), do: {:error, {:invalid_candidate_relation, value}}

  defp related_relations(%__MODULE__{active_relation: active, candidate_relation: candidate}) do
    if {active.connection, active.catalog, active.schema} ==
         {candidate.connection, candidate.catalog, candidate.schema} and
         active.name != candidate.name,
       do: :ok,
       else: {:error, :invalid_discard_relation_identity}
  end
end
