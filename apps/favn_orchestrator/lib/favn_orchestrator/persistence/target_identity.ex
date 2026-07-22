defmodule FavnOrchestrator.Persistence.TargetIdentity do
  @moduledoc """
  Stable, bounded identities for targets persisted by the control plane.

  Pipeline identity includes both module and manifest name. A module alone is
  not unique because one module may publish multiple named pipeline definitions.
  """

  @doc "Returns the canonical identity for one manifest asset reference."
  @spec for_asset(Favn.Ref.t()) :: String.t()
  defdelegate for_asset(ref), to: Favn.TargetIdentity

  @doc "Returns the canonical identity for one named manifest pipeline."
  @spec for_pipeline({module(), atom()}) :: String.t()
  defdelegate for_pipeline(ref), to: Favn.TargetIdentity
end
