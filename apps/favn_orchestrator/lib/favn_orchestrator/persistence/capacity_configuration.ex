defmodule FavnOrchestrator.Persistence.CapacityConfiguration do
  @moduledoc """
  Frozen capacity scopes derived from one validated deployment policy.

  Deployments persist these limits, while execution admission derives only the
  matching stable scope identities. PostgreSQL remains the counter authority.
  """

  alias FavnOrchestrator.Persistence.CapacityIdentity
  alias FavnOrchestrator.Persistence.Commands.DeploymentCapacityScope

  @doc "Returns configured workspace and pool scopes for one deployment."
  @spec deployment_scopes(String.t(), Favn.ExecutionPool.PolicySet.t()) ::
          [DeploymentCapacityScope.t()]
  def deployment_scopes(workspace_id, policies)
      when is_binary(workspace_id) and is_map(policies) do
    policies
    |> Enum.map(fn {name, policy} ->
      deployment_scope(workspace_id, name, policy.max_concurrency)
    end)
    |> Enum.sort_by(& &1.scope_id)
  end

  @doc "Returns the configured scope for a work pool, if it is bounded."
  @spec execution_scope(String.t(), atom() | String.t() | nil, map()) ::
          {:ok, map()} | :unlimited
  def execution_scope(_workspace_id, nil, _policies), do: :unlimited

  def execution_scope(workspace_id, pool, policies)
      when is_binary(workspace_id) and (is_atom(pool) or is_binary(pool)) and is_map(policies) do
    key = to_string(pool)

    case Map.fetch(policies, key) do
      {:ok, policy} ->
        kind = if key == "global", do: :workspace, else: :pool

        {:ok,
         %{
           scope_id: CapacityIdentity.scope_id(workspace_id, kind, key),
           kind: if(key == "global", do: :global, else: :pool),
           key: key,
           limit: policy.max_concurrency
         }}

      :error ->
        :unlimited
    end
  end

  @doc "Returns whether the named pool exists in runtime configuration."
  @spec configured_pool?(map(), atom() | String.t()) :: boolean()
  def configured_pool?(policies, pool)
      when is_map(policies) and (is_atom(pool) or is_binary(pool)),
      do: Map.has_key?(policies, to_string(pool))

  defp deployment_scope(workspace_id, "global" = key, limit) do
    %DeploymentCapacityScope{
      scope_id: CapacityIdentity.scope_id(workspace_id, :workspace, key),
      scope_kind: :workspace,
      scope_key: key,
      capacity_limit: limit
    }
  end

  defp deployment_scope(workspace_id, key, limit) do
    %DeploymentCapacityScope{
      scope_id: CapacityIdentity.scope_id(workspace_id, :pool, key),
      scope_kind: :pool,
      scope_key: key,
      capacity_limit: limit
    }
  end
end
