defmodule FavnOrchestrator.Persistence.CapacityConfiguration do
  @moduledoc """
  Frozen capacity scopes derived from Favn execution-pool configuration.

  Deployments persist these limits, while execution admission derives only the
  matching stable scope identities. PostgreSQL remains the counter authority.
  """

  alias FavnOrchestrator.Persistence.CapacityIdentity
  alias FavnOrchestrator.Persistence.Commands.DeploymentCapacityScope

  @doc "Returns configured workspace and pool scopes for one deployment."
  @spec deployment_scopes(String.t()) :: [DeploymentCapacityScope.t()]
  def deployment_scopes(workspace_id) when is_binary(workspace_id) do
    execution_pools()
    |> Enum.map(fn {name, limit} -> deployment_scope(workspace_id, name, limit) end)
    |> Enum.sort_by(& &1.scope_id)
  end

  @doc "Returns the configured scope for a work pool, if it is bounded."
  @spec execution_scope(String.t(), atom() | String.t() | nil) ::
          {:ok, map()} | :unlimited
  def execution_scope(_workspace_id, nil), do: :unlimited

  def execution_scope(workspace_id, pool)
      when is_binary(workspace_id) and (is_atom(pool) or is_binary(pool)) do
    key = to_string(pool)

    case Map.fetch(execution_pools(), key) do
      {:ok, limit} ->
        kind = if key == "global", do: :workspace, else: :pool

        {:ok,
         %{
           scope_id: CapacityIdentity.scope_id(workspace_id, kind, key),
           kind: if(key == "global", do: :global, else: :pool),
           key: key,
           limit: limit
         }}

      :error ->
        :unlimited
    end
  end

  @doc "Returns whether the named pool exists in runtime configuration."
  @spec configured_pool?(atom() | String.t()) :: boolean()
  def configured_pool?(pool) when is_atom(pool) or is_binary(pool),
    do: Map.has_key?(execution_pools(), to_string(pool))

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

  defp execution_pools do
    case Application.get_env(:favn, :execution_pools, []) do
      pools when is_list(pools) or is_map(pools) ->
        Enum.reduce(pools, %{}, fn
          {name, opts}, acc when is_atom(name) or is_binary(name) ->
            case limit(opts) do
              value when is_integer(value) and value > 0 -> Map.put(acc, to_string(name), value)
              _invalid -> acc
            end

          _invalid, acc ->
            acc
        end)

      _invalid ->
        %{}
    end
  end

  defp limit(value) when is_integer(value), do: value
  defp limit(value) when is_list(value), do: Keyword.get(value, :max_concurrency)

  defp limit(value) when is_map(value),
    do: Map.get(value, :max_concurrency) || value["max_concurrency"]

  defp limit(_value), do: nil
end
