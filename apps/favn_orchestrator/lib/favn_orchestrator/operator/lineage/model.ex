defmodule FavnOrchestrator.Operator.Lineage.Model do
  @moduledoc false

  alias FavnOrchestrator.Operator.Lineage.AssetNode
  alias FavnOrchestrator.Operator.Lineage.Edge
  alias FavnOrchestrator.Operator.Lineage.Graph
  alias FavnOrchestrator.Operator.Lineage.GroupNode

  @enforce_keys [
    :graph,
    :groups,
    :groups_by_id,
    :edges,
    :edges_by_id,
    :asset_nodes_by_id,
    :group_assets_by_id
  ]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          graph: Graph.t(),
          groups: [GroupNode.t()],
          groups_by_id: %{String.t() => GroupNode.t()},
          edges: [Edge.t()],
          edges_by_id: %{String.t() => Edge.t()},
          asset_nodes_by_id: %{String.t() => AssetNode.t()},
          group_assets_by_id: %{String.t() => [AssetNode.t()]}
        }
end
