defmodule Favn.Runtime.Engine do
  @moduledoc """
  Runtime engine facade.

  The initial v0.2 foundation keeps a synchronous public run API while using
  a run-scoped coordinator process internally.
  """

  @spec run_sync(Favn.asset_ref(), keyword()) ::
          {:ok, Favn.Run.t()} | {:error, Favn.Run.t() | term()}
  def run_sync(target_ref, opts \\ []) when is_list(opts) do
    Favn.Runtime.Coordinator.run_sync(target_ref, opts)
  end
end
