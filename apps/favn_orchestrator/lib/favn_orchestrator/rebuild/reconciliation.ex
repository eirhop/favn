defmodule FavnOrchestrator.Rebuild.Reconciliation do
  @moduledoc false

  @spec next(boolean(), :activation | :items) :: :cancel | :activate | :build
  def next(true, phase) when phase in [:activation, :items], do: :cancel
  def next(false, :activation), do: :activate
  def next(false, :items), do: :build
end
