defmodule FavnOrchestrator.ResourceConfiguration do
  @moduledoc """
  Resolves Favn-owned circuit policy from an explicit run snapshot.

  Both execution-pool and connection policies originate in immutable workspace
  deployment configuration. This module never reads the consumer application's
  local environment.
  """

  alias Favn.Connection.CircuitPolicySet
  alias Favn.ExecutionPool.PolicySet
  alias Favn.Resource.Ref

  @enforce_keys [:execution_pools, :connection_circuits]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          execution_pools: PolicySet.t(),
          connection_circuits: CircuitPolicySet.t()
        }

  @doc "Validates one complete run policy snapshot for constant-time resource lookups."
  @spec new(map()) :: {:ok, t()} | {:error, term()}
  def new(policies) when is_map(policies) do
    with {:ok, execution_pools} <-
           policies |> field(:execution_pools, %{}) |> PolicySet.new(),
         {:ok, connection_circuits} <-
           policies |> field(:connection_circuits, %{}) |> CircuitPolicySet.new() do
      {:ok,
       %__MODULE__{
         execution_pools: execution_pools,
         connection_circuits: connection_circuits
       }}
    end
  end

  def new(_policies), do: {:error, :invalid_resource_configuration}

  @doc "Returns the snapshotted circuit-breaker policy for a resource."
  @spec circuit_breaker(Ref.t(), t()) :: {:ok, Favn.CircuitBreaker.Policy.t() | nil}
  def circuit_breaker(
        %Ref{kind: :execution_pool, name: name},
        %__MODULE__{execution_pools: execution_pools}
      ) do
    case Map.fetch(execution_pools, name) do
      {:ok, execution_pool_policy} -> {:ok, execution_pool_policy.circuit_breaker}
      :error -> {:ok, nil}
    end
  end

  def circuit_breaker(
        %Ref{kind: :connection, name: name},
        %__MODULE__{connection_circuits: connections}
      ) do
    {:ok, Map.get(connections, to_string(name))}
  end

  @doc "Returns a resource only when its snapshotted circuit breaker is enabled."
  @spec enabled_resource(Ref.t(), t()) ::
          {:ok, {Ref.t(), Favn.CircuitBreaker.Policy.t()} | nil}
  def enabled_resource(%Ref{} = ref, %__MODULE__{} = configuration) do
    case circuit_breaker(ref, configuration) do
      {:ok, %Favn.CircuitBreaker.Policy{} = policy} -> {:ok, {ref, policy}}
      {:ok, nil} -> {:ok, nil}
    end
  end

  defp field(map, key, default) when is_map(map),
    do: Map.get(map, key, Map.get(map, Atom.to_string(key), default))
end
