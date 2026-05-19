defmodule Favn.SQL.Admission do
  @moduledoc false

  alias Favn.SQL.Admission.Limiter
  alias Favn.SQL.{ConcurrencyPolicies, ConcurrencyPolicy, Error, Session, WritePlan}

  @permit_key {__MODULE__, :permits}
  @write_prefixes ~w(
    alter analyze attach checkpoint copy create delete detach drop export import insert install
    load merge pragma set truncate update vacuum
  )

  @spec with_permit(Session.t(), atom(), iodata() | term(), (-> term())) :: term()
  def with_permit(
        %Session{} = session,
        operation,
        payload,
        fun
      )
      when is_function(fun, 0) do
    policy = policy_for(session, operation, payload)

    if permit_required?(policy, operation, payload) do
      acquire_and_run(policy, operation, fun)
    else
      fun.()
    end
  end

  def with_permit(%Session{}, _operation, _payload, fun) when is_function(fun, 0), do: fun.()

  @spec acquire_session(ConcurrencyPolicy.t() | ConcurrencyPolicies.t() | nil, keyword()) ::
          term()
  def acquire_session(policy, opts \\ [])

  def acquire_session(%ConcurrencyPolicies{} = policies, opts) when is_list(opts) do
    case required_catalogs(opts) do
      [] ->
        acquire_unscoped_session(policies, opts)

      catalogs ->
        policies
        |> catalog_connect_policies(catalogs)
        |> acquire_session_policies(:connect)
    end
  end

  def acquire_session(%ConcurrencyPolicy{scope: scope} = policy, _opts) do
    cond do
      not permit_required?(policy, :connect, nil) ->
        nil

      already_holding?(scope) ->
        increment_held(scope)
        {:borrowed, scope, self()}

      true ->
        acquire_lease(policy)
    end
  end

  def acquire_session(_policy, _opts), do: nil

  @spec release_session(term()) :: :ok
  def release_session({:held, scope, owner}) when owner == self() do
    release_held_scope(scope)
    :ok
  end

  def release_session({:held, scope, owner}) do
    Limiter.release(scope, owner)
    :ok
  end

  def release_session({:borrowed, scope, owner}) when owner == self() do
    release_held_scope(scope)
    :ok
  end

  def release_session({:borrowed, _scope, _owner}), do: :ok

  def release_session(leases) when is_list(leases) do
    Enum.each(Enum.reverse(leases), &release_session/1)
    :ok
  end

  def release_session(_lease), do: :ok

  defp required_catalogs(opts) do
    opts
    |> Keyword.get(:required_catalogs)
    |> List.wrap()
    |> Enum.map(&to_string/1)
    |> Enum.reject(&(&1 == ""))
    |> Enum.uniq()
  end

  defp catalog_connect_policies(%ConcurrencyPolicies{} = policies, catalogs) do
    catalogs
    |> Enum.map(&ConcurrencyPolicies.catalog_policy(policies, &1))
    |> Enum.reject(&is_nil/1)
    |> Enum.uniq_by(& &1.scope)
    |> Enum.sort_by(&inspect(&1.scope))
  end

  defp acquire_unscoped_session(%ConcurrencyPolicies{catalog: catalog} = policies, opts) do
    case catalog |> Map.values() |> Enum.sort_by(&inspect(&1.scope)) do
      [] -> acquire_session(policies.default, opts)
      catalog_policies -> acquire_session_policies(catalog_policies, :connect)
    end
  end

  defp acquire_session_policies([], _operation), do: nil

  defp acquire_session_policies(policies, operation) do
    Enum.reduce_while(policies, [], fn policy, leases ->
      case acquire_catalog_connect_lease(policy, operation) do
        {:error, %Error{}} = error ->
          release_session(leases)
          {:halt, error}

        nil ->
          {:cont, leases}

        lease ->
          {:cont, [lease | leases]}
      end
    end)
    |> case do
      {:error, %Error{}} = error -> error
      leases when is_list(leases) -> Enum.reverse(leases)
    end
  end

  defp acquire_catalog_connect_lease(%ConcurrencyPolicy{limit: :unlimited}, _operation), do: nil

  defp acquire_catalog_connect_lease(%ConcurrencyPolicy{scope: scope} = policy, operation) do
    if already_holding?(scope) do
      increment_held(scope)
      {:borrowed, scope, self()}
    else
      acquire_lease(policy, operation)
    end
  end

  defp policy_for(
         %Session{concurrency_policies: %ConcurrencyPolicies{} = policies},
         operation,
         payload
       ) do
    case catalog_target(operation, payload) do
      {_connection, catalog} when is_binary(catalog) ->
        ConcurrencyPolicies.catalog_policy(policies, catalog) || policies.default

      _target ->
        policies.default
    end
  end

  defp policy_for(
         %Session{concurrency_policy: %ConcurrencyPolicy{} = policy},
         _operation,
         _payload
       ) do
    policy
  end

  defp policy_for(%Session{}, _operation, _payload), do: nil

  defp catalog_target(:materialize, %WritePlan{
         connection: connection,
         target: %{catalog: catalog}
       })
       when is_atom(connection) and is_binary(catalog),
       do: {connection, catalog}

  defp catalog_target(:materialize, %WritePlan{target: %{catalog: catalog}})
       when is_binary(catalog),
       do: {nil, catalog}

  defp catalog_target(_operation, _payload), do: nil

  defp permit_required?(%ConcurrencyPolicy{limit: :unlimited}, _operation, _payload), do: false
  defp permit_required?(%ConcurrencyPolicy{applies_to: :all}, _operation, _payload), do: true
  defp permit_required?(nil, _operation, _payload), do: false

  defp permit_required?(%ConcurrencyPolicy{applies_to: :writes}, operation, payload) do
    operation in [:execute, :materialize, :transaction] or write_query?(operation, payload)
  end

  defp write_query?(:query, statement) do
    statement
    |> IO.iodata_to_binary()
    |> String.trim_leading()
    |> String.downcase()
    |> first_token()
    |> then(&(&1 in @write_prefixes or is_nil(&1)))
  rescue
    _ -> true
  end

  defp write_query?(_operation, _payload), do: false

  defp first_token(""), do: nil

  defp first_token(sql) do
    sql
    |> String.split(~r/\s+/, parts: 2)
    |> List.first()
  end

  defp acquire_and_run(%ConcurrencyPolicy{scope: scope} = policy, operation, fun) do
    if already_holding?(scope) do
      fun.()
    else
      case acquire_lease(policy, operation) do
        {:error, %Error{}} = error ->
          error

        _lease ->
          try do
            fun.()
          after
            release_held_scope(scope)
          end
      end
    end
  end

  defp acquire_lease(%ConcurrencyPolicy{} = policy), do: acquire_lease(policy, :connect)

  defp acquire_lease(%ConcurrencyPolicy{scope: scope, limit: limit} = policy, operation) do
    case Limiter.acquire(scope, limit, policy.admission_timeout_ms) do
      :ok ->
        increment_held(scope)
        {:held, scope, self()}

      {:error, :admission_timeout} ->
        {:error, admission_timeout_error(policy, operation)}
    end
  end

  defp admission_timeout_error(%ConcurrencyPolicy{} = policy, operation) do
    %Error{
      type: :admission_timeout,
      message: "SQL admission timed out",
      connection: policy.connection,
      operation: operation,
      retryable?: true,
      details: %{scope: policy.scope, timeout_ms: policy.admission_timeout_ms}
    }
  end

  defp already_holding?(scope), do: Map.get(held(), scope, 0) > 0

  defp increment_held(scope),
    do: Process.put(@permit_key, Map.update(held(), scope, 1, &(&1 + 1)))

  defp decrement_held(scope) do
    next =
      held()
      |> Map.update(scope, 0, &max(&1 - 1, 0))
      |> Enum.reject(fn {_scope, count} -> count == 0 end)
      |> Map.new()

    Process.put(@permit_key, next)
    Map.get(next, scope, 0)
  end

  defp release_held_scope(scope) do
    if decrement_held(scope) == 0 do
      Limiter.release(scope)
    end
  end

  defp held do
    case Process.get(@permit_key, %{}) do
      value when is_map(value) -> value
      _ -> %{}
    end
  end
end
