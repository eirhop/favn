defmodule Favn.Manifest do
  @moduledoc """
  Canonical runtime manifest payload.

  `%Favn.Manifest{}` is the stable payload that gets serialized, hashed, and
  pinned into `%Favn.Manifest.Version{}`. Build-only fields such as timestamps
  and diagnostics do not belong here. Every valid current manifest is bound to
  the operator-selected runner builds through `runner_releases`; that required
  pool-to-release map participates in canonical serialization and identity.
  """

  alias Favn.Manifest.ContractVersions
  alias Favn.Manifest.Graph
  alias Favn.Manifest.Environment

  @schema_version ContractVersions.manifest_schema_version()
  @runner_contract_version ContractVersions.runner_contract_version()

  @type t :: %__MODULE__{
          schema_version: pos_integer(),
          runner_contract_version: pos_integer(),
          runner_releases: Favn.RunnerPool.releases(),
          execution_pools: Favn.ExecutionPool.PolicySet.t(),
          environment: Environment.t(),
          assets: [Favn.Manifest.Asset.t()],
          pipelines: [Favn.Manifest.Pipeline.t()],
          schedules: [Favn.Manifest.Schedule.t()],
          graph: Graph.t(),
          metadata: map()
        }

  defstruct schema_version: @schema_version,
            runner_contract_version: @runner_contract_version,
            runner_releases: %{},
            execution_pools: %{},
            environment: %Environment{
              default_timezone: "Etc/UTC",
              default_timezone_source: :utc_fallback
            },
            assets: [],
            pipelines: [],
            schedules: [],
            graph: %Graph{},
            metadata: %{}
end
