defmodule Favn.Runtime.Runner do
  @moduledoc """
  Backward-compatible runtime runner facade.

  The v0.2 foundation delegates orchestration to `Favn.Runtime.Engine`, which
  executes runs via a coordinator/executor split with explicit state machines.
  """

  @typedoc """
  Options supported by the runtime runner.
  """
  @type run_opts :: [
          dependencies: Favn.dependencies_mode(),
          params: map()
        ]

  @spec run(Favn.asset_ref(), run_opts()) :: {:ok, Favn.Run.t()} | {:error, Favn.Run.t() | term()}
  def run(target_ref, opts \\ []) when is_list(opts) do
    Favn.Runtime.Engine.run_sync(target_ref, opts)
  end
end
