defmodule Favn.Plan.NodeIdentity do
  @moduledoc """
  Planned-node identity shared by orchestrator and runner contracts.

  This struct carries manifest/planning-owned fields only. Runner lifecycle
  fields such as attempt number, retry state, admission state, and cancellation
  state belong to runner/orchestrator runtime contracts instead.
  """

  alias Favn.Plan
  alias Favn.Ref
  alias Favn.Window.Runtime

  @type t :: %__MODULE__{
          manifest_version_id: String.t(),
          node_key: Plan.node_key(),
          target_refs: [Ref.t()],
          planned_asset_refs: [Ref.t()],
          window: Runtime.t() | nil,
          execution_pool: atom() | nil
        }

  defstruct manifest_version_id: nil,
            node_key: nil,
            target_refs: [],
            planned_asset_refs: [],
            window: nil,
            execution_pool: nil

  @doc """
  Builds a planned-node identity from validated fields.
  """
  @spec new(map() | keyword()) :: {:ok, t()} | {:error, term()}
  def new(fields) when is_map(fields) or is_list(fields) do
    fields = Map.new(fields)

    with :ok <- validate_manifest_version_id(Map.get(fields, :manifest_version_id)),
         :ok <- validate_node_key(Map.get(fields, :node_key)),
         :ok <- validate_refs(:target_refs, Map.get(fields, :target_refs, [])),
         :ok <- validate_refs(:planned_asset_refs, Map.get(fields, :planned_asset_refs, [])),
         :ok <- validate_execution_pool(Map.get(fields, :execution_pool)) do
      {:ok, struct!(__MODULE__, fields)}
    end
  end

  @doc """
  Builds a planned-node identity or raises on invalid fields.
  """
  @spec new!(map() | keyword()) :: t()
  def new!(fields) do
    case new(fields) do
      {:ok, identity} -> identity
      {:error, reason} -> raise ArgumentError, "invalid node identity: #{inspect(reason)}"
    end
  end

  defp validate_manifest_version_id(value) when is_binary(value) and value != "", do: :ok
  defp validate_manifest_version_id(value), do: {:error, {:invalid_manifest_version_id, value}}

  defp validate_node_key({_ref, _window_key}), do: :ok
  defp validate_node_key(value), do: {:error, {:invalid_node_key, value}}

  defp validate_refs(_field, refs) when is_list(refs) do
    if Enum.all?(refs, &valid_ref?/1), do: :ok, else: {:error, {:invalid_refs, refs}}
  end

  defp validate_refs(field, value), do: {:error, {field, value}}

  defp valid_ref?({module, name}) when is_atom(module) and is_atom(name), do: true
  defp valid_ref?(_ref), do: false

  defp validate_execution_pool(nil), do: :ok
  defp validate_execution_pool(value) when is_atom(value), do: :ok
  defp validate_execution_pool(value), do: {:error, {:invalid_execution_pool, value}}
end
