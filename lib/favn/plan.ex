defmodule Favn.Plan do
  @moduledoc """
  Deterministic execution plan for one logical run request.

  Plans are built from a target set and a dependency mode. Nodes are deduplicated
  by canonical asset reference so shared dependencies execute at most once.

  `stages` groups refs by topological depth so each stage can run in parallel
  after all refs in previous stages are satisfied.
  """

  alias Favn.Ref

  @typedoc """
  Execution action for one planned node.
  """
  @type action :: :run

  @typedoc """
  One planned node keyed by canonical ref.
  """
  @type node_key :: {Ref.t(), term() | nil}

  @type plan_node :: %{
          ref: Ref.t(),
          node_key: node_key(),
          upstream: [Ref.t()],
          downstream: [Ref.t()],
          stage: non_neg_integer(),
          action: action()
        }

  @typedoc """
  Topologically ordered plan stages.
  """
  @type stage :: [Ref.t()]

  @type t :: %__MODULE__{
          target_refs: [Ref.t()],
          dependencies: Favn.dependencies_mode(),
          nodes: %{required(Ref.t()) => plan_node()},
          topo_order: [Ref.t()],
          stages: [stage()]
        }

  defstruct target_refs: [],
            dependencies: :all,
            nodes: %{},
            topo_order: [],
            stages: []
end
