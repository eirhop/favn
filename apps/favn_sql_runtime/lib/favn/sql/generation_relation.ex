defmodule Favn.SQL.GenerationRelation do
  @moduledoc """
  Deterministic physical relation names for target generations.

  Candidate and retired names retain the logical relation namespace. The name
  suffix includes the generation UUID and is shortened with a stable hash when
  necessary to remain inside the adapter's identifier-byte limit.
  """

  alias Favn.RelationRef

  @marker_name "__favn_target_generation_markers"

  @doc "Returns the deterministic candidate relation for a generation."
  @spec candidate(RelationRef.t(), String.t(), pos_integer()) :: RelationRef.t()
  def candidate(%RelationRef{} = logical, generation_id, max_identifier_bytes)
      when is_binary(generation_id) and generation_id != "" do
    with_generation_name(logical, generation_id, "candidate", max_identifier_bytes)
  end

  @doc "Returns the deterministic retired relation for a generation."
  @spec retired(RelationRef.t(), String.t(), pos_integer()) :: RelationRef.t()
  def retired(%RelationRef{} = logical, generation_id, max_identifier_bytes)
      when is_binary(generation_id) and generation_id != "" do
    with_generation_name(logical, generation_id, "retired", max_identifier_bytes)
  end

  @doc "Returns the sidecar marker relation in the logical target namespace."
  @spec marker(RelationRef.t()) :: RelationRef.t()
  def marker(%RelationRef{} = logical), do: %{logical | name: @marker_name}

  defp with_generation_name(logical, generation_id, kind, max_identifier_bytes)
       when is_integer(max_identifier_bytes) and max_identifier_bytes >= 48 do
    generation_fragment = String.replace(generation_id, "-", "")
    suffix = "__favn_#{kind}_#{generation_fragment}"
    full_name = logical.name <> suffix

    name =
      if byte_size(full_name) <= max_identifier_bytes do
        full_name
      else
        hash = digest([logical.name, kind, generation_id])
        short_suffix = "__favn_#{String.first(kind)}_#{hash}"
        prefix = utf8_prefix(logical.name, max_identifier_bytes - byte_size(short_suffix))
        prefix <> short_suffix
      end

    %{logical | name: name}
  end

  defp digest(parts) do
    parts
    |> Enum.intersperse(<<0>>)
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
    |> binary_part(0, 24)
  end

  defp utf8_prefix(value, max_bytes) do
    value
    |> String.codepoints()
    |> Enum.reduce_while({[], 0}, fn codepoint, {acc, size} ->
      next_size = size + byte_size(codepoint)

      if next_size <= max_bytes,
        do: {:cont, {[codepoint | acc], next_size}},
        else: {:halt, {acc, size}}
    end)
    |> elem(0)
    |> Enum.reverse()
    |> IO.iodata_to_binary()
  end
end
