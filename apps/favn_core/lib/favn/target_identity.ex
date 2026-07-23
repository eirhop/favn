defmodule Favn.TargetIdentity do
  @moduledoc """
  Canonical bounded identities for manifest targets.

  These identities are shared by manifest descriptors and control-plane rows.
  """

  @doc "Returns the canonical identity for one manifest asset reference."
  @spec for_asset(Favn.Ref.t()) :: String.t()
  def for_asset({module, name}) when is_atom(module) and is_atom(name) do
    "asset:" <> Atom.to_string(module) <> ":" <> Atom.to_string(name)
  end

  @doc "Returns the canonical identity for one named manifest pipeline."
  @spec for_pipeline({module(), atom()}) :: String.t()
  def for_pipeline({module, name}) when is_atom(module) and is_atom(name) do
    "pipeline:" <> Atom.to_string(module) <> ":" <> Atom.to_string(name)
  end
end
