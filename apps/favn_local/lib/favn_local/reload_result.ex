defmodule FavnLocal.ReloadResult do
  @moduledoc """
  Successful local reload result shared by the runtime, caller, and Mix command.

  `:unchanged` skips deployment; `:manifest_deployed` keeps the current runner;
  `:runner_replaced` activates the new release and drains the previous runner.
  Phase durations are milliseconds. Build and total time are filled by the caller.
  """

  @type classification :: :unchanged | :manifest_deployed | :runner_replaced
  @type drain_status :: :not_replaced | :drained | {:warning, :old_runner_drain_timeout}
  @type phases :: %{
          manifest_build_ms: non_neg_integer(),
          execution_packages_ms: non_neg_integer(),
          manifest_publication_ms: non_neg_integer(),
          manifest_activation_ms: non_neg_integer(),
          deployment_ms: non_neg_integer()
        }
  @type t :: %{
          reload_status: classification(),
          status: :ready,
          workspace_id: String.t(),
          runner_release_id: String.t(),
          runner_releases: %{String.t() => String.t()},
          runner_node: node(),
          operator_node: node(),
          orchestrator_url: String.t(),
          view_url: String.t(),
          manifest_version_id: String.t(),
          deployment_id: String.t(),
          execution_packages: %{provided: non_neg_integer(), registered: non_neg_integer()},
          phases: phases(),
          duration_ms: non_neg_integer(),
          old_runner: drain_status()
        }

  @doc "Combines a successful deployment with the ready runtime summary."
  @spec new(classification(), map(), map(), drain_status()) :: t()
  def new(classification, deployment, runtime, old_runner \\ :not_replaced) do
    deployment
    |> Map.merge(runtime)
    |> Map.merge(%{
      reload_status: classification,
      old_runner: old_runner,
      phases: Map.put(deployment.phases, :manifest_build_ms, 0),
      duration_ms: 0
    })
  end
end
