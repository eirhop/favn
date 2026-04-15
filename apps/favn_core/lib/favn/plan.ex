defmodule Favn.Plan do
  @moduledoc """
  Deterministic execution plan for one logical run request.

  Plans are built from a target set and a dependency mode. Nodes are deduplicated
  by canonical asset reference so shared dependencies execute at most once.

  `stages` groups refs by topological depth so each stage can run in parallel
  after all refs in previous stages are satisfied.

  `target_node_keys` carries the concrete planned target identities. During the
  current scaffold phase this is `{ref, nil}` per target ref.
  """

  alias Favn.Ref

  @typedoc """
  Execution action for one planned node.
  """
  @type action :: :run | :observe

  @typedoc """
  One planned node keyed by canonical ref.
  """
  @type node_key :: {Ref.t(), term() | nil}

  @type plan_node :: %{
          ref: Ref.t(),
          node_key: node_key(),
          window: Favn.Window.Runtime.t() | nil,
          upstream: [node_key()],
          downstream: [node_key()],
          stage: non_neg_integer(),
          action: action()
        }

  @typedoc """
  Topologically ordered plan stages.
  """
  @type stage :: [Ref.t()]
  @type node_stage :: [node_key()]
  @type dependencies_mode :: :all | :none

  @type t :: %__MODULE__{
          target_refs: [Ref.t()],
          target_node_keys: [node_key()],
          dependencies: dependencies_mode(),
          nodes: %{required(node_key()) => plan_node()},
          topo_order: [Ref.t()],
          stages: [stage()],
          node_stages: [node_stage()]
        }

  defstruct target_refs: [],
            target_node_keys: [],
            dependencies: :all,
            nodes: %{},
            topo_order: [],
            stages: [],
            node_stages: []
end
