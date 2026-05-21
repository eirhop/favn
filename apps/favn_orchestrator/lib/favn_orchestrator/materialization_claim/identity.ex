defmodule FavnOrchestrator.MaterializationClaim.Identity do
  @moduledoc """
  Deterministic materialization claim identity helpers.

  Claims are scoped by asset ref, freshness key, and the exact upstream freshness
  versions consumed by the materialization attempt.

  Execution code uses this identity before submitting runner work. Storage then
  admits only one active claim for the same identity, lets overlapping runs wait
  instead of duplicating materialization, and lets later runs skip when a matching
  claim has already succeeded.
  """

  @doc """
  Returns a stable fingerprint for consumed input versions.
  """
  @spec input_fingerprint(term()) :: String.t()
  def input_fingerprint(input_versions) do
    input_versions
    |> normalize_term()
    |> :erlang.term_to_binary()
    |> sha256_base16()
  end

  @doc """
  Returns a deterministic claim key for one asset/freshness/input tuple.
  """
  @spec claim_key(Favn.Ref.t(), String.t(), String.t()) :: String.t()
  def claim_key({module, name}, freshness_key, input_fingerprint)
      when is_atom(module) and is_atom(name) and is_binary(freshness_key) and
             is_binary(input_fingerprint) do
    [Atom.to_string(module), Atom.to_string(name), freshness_key, input_fingerprint]
    |> :erlang.term_to_binary()
    |> sha256_base16()
  end

  defp normalize_term(term) when is_map(term) do
    term
    |> Enum.map(fn {key, value} -> {normalize_term(key), normalize_term(value)} end)
    |> Enum.sort_by(&inspect/1)
  end

  defp normalize_term(term) when is_list(term), do: Enum.map(term, &normalize_term/1)
  defp normalize_term(term) when is_tuple(term), do: term |> Tuple.to_list() |> normalize_term()
  defp normalize_term(term), do: term

  defp sha256_base16(term) do
    :crypto.hash(:sha256, term)
    |> Base.encode16(case: :lower)
  end
end
