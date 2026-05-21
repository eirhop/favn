defmodule Favn.Plan.NodeIdentity do
  @moduledoc """
  Manifest/planning-owned identity for one planned node.

  This struct deliberately contains only data produced from a pinned manifest
  version and a plan. Runner lifecycle data such as execution IDs, attempts, and
  storage state belongs outside this contract.
  """

  alias Favn.Plan
  alias Favn.Ref

  @type node_key :: Plan.node_key()

  @type t :: %__MODULE__{
          manifest_version_id: String.t(),
          node_key: node_key(),
          target_refs: [Ref.t()],
          planned_asset_refs: [Ref.t()],
          window: Favn.Window.Runtime.t() | nil,
          execution_pool: atom() | nil
        }

  defstruct [
    :manifest_version_id,
    :node_key,
    target_refs: [],
    planned_asset_refs: [],
    window: nil,
    execution_pool: nil
  ]

  @doc """
  Builds node identity from a pinned manifest version id and plan node key.
  """
  @spec from_plan(String.t(), Plan.t(), node_key()) :: {:ok, t()} | {:error, :plan_node_not_found}
  def from_plan(manifest_version_id, %Plan{} = plan, node_key)
      when is_binary(manifest_version_id) do
    case Map.fetch(plan.nodes, node_key) do
      {:ok, node} ->
        {:ok,
         %__MODULE__{
           manifest_version_id: manifest_version_id,
           node_key: node_key,
           target_refs: plan.target_refs,
           planned_asset_refs: plan.topo_order,
           window: Map.get(node, :window),
           execution_pool: Map.get(node, :execution_pool)
         }}

      :error ->
        {:error, :plan_node_not_found}
    end
  end
end
