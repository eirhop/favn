defmodule Favn.SQL.GenerationRelation do
  @moduledoc """
  Deterministic physical relation names for target generations.

  Candidate and retired names retain the logical relation namespace. The name
  suffix includes the generation UUID and is shortened with a stable hash when
  necessary to remain inside the adapter's identifier-byte limit.
  """

  alias Favn.RelationRef
  alias Favn.TargetGenerationRelation

  @doc "Returns the deterministic candidate relation for a generation."
  @spec candidate(RelationRef.t(), String.t(), pos_integer()) :: RelationRef.t()
  def candidate(%RelationRef{} = logical, generation_id, max_identifier_bytes)
      when is_binary(generation_id) and generation_id != "" do
    TargetGenerationRelation.candidate(logical, generation_id, max_identifier_bytes)
  end

  @doc "Returns the deterministic retired relation for a generation."
  @spec retired(RelationRef.t(), String.t(), pos_integer()) :: RelationRef.t()
  def retired(%RelationRef{} = logical, generation_id, max_identifier_bytes)
      when is_binary(generation_id) and generation_id != "" do
    TargetGenerationRelation.retired(logical, generation_id, max_identifier_bytes)
  end

  @doc "Returns the sidecar marker relation in the logical target namespace."
  @spec marker(RelationRef.t()) :: RelationRef.t()
  def marker(%RelationRef{} = logical), do: TargetGenerationRelation.marker(logical)
end
