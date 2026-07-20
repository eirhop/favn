defmodule FavnOrchestrator.RunServer.Execution.StageEntry do
  @moduledoc false

  alias Favn.Manifest.Version

  @fields [
    :run_id,
    :asset_step_id,
    :asset_ref,
    :node_key,
    :window,
    :execution_id,
    :runner_execution_id,
    :ownership,
    :decision,
    :attempt,
    :stage,
    :lease,
    :materialization_claim,
    :execution_pool,
    :resource_circuit_permits,
    :freshness_key,
    :version,
    :freshness_context
  ]
  @type t :: %{
          required(:run_id) => String.t(),
          required(:asset_step_id) => String.t(),
          required(:asset_ref) => Favn.Ref.t(),
          required(:node_key) => Favn.Plan.node_key(),
          required(:window) => term(),
          required(:execution_id) => term(),
          required(:runner_execution_id) => term(),
          required(:ownership) => term(),
          required(:decision) => map(),
          required(:attempt) => pos_integer(),
          required(:stage) => non_neg_integer(),
          required(:lease) => term(),
          required(:materialization_claim) => term(),
          required(:execution_pool) => term(),
          required(:resource_circuit_permits) => [
            FavnOrchestrator.Persistence.Results.ResourceCircuitPermit.t()
          ],
          required(:freshness_key) => term(),
          required(:version) => Version.t(),
          required(:freshness_context) => map()
        }

  @spec new!(map()) :: t()
  def new!(attrs) when is_map(attrs) do
    case @fields -- Map.keys(attrs) do
      [] -> Map.take(attrs, @fields)
      missing -> raise ArgumentError, "missing stage entry fields: #{inspect(missing)}"
    end
  end
end
