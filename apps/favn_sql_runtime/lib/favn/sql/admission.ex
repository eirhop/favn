defmodule Favn.SQL.Admission do
  @moduledoc false

  alias Favn.SQL.Admission.Limiter
  alias Favn.SQL.{ConcurrencyPolicies, ConcurrencyPolicy, Error, Observability, Session, WritePlan}

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
    session
    |> policies_for(operation, payload)
    |> acquire_and_run(operation, payload, fun)
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
  def release_session({:external, scope, owner}) do
    Limiter.release(scope, owner)
    :ok
  end

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

  @spec detach_session(term()) :: :ok
  def detach_session({:held, scope, owner}) when owner == self() do
    decrement_held(scope)
    :ok
  end

  def detach_session({:borrowed, scope, owner}) when owner == self() do
    decrement_held(scope)
    :ok
  end

  def detach_session(leases) when is_list(leases) do
    Enum.each(Enum.reverse(leases), &detach_session/1)
    :ok
  end

  def detach_session(_lease), do: :ok

  @spec externalize_session(term(), pid()) :: {:ok, term()} | {:error, term()}
  def externalize_session(lease, owner) when is_pid(owner) do
    transfer_session(lease, owner, :external)
  end

  @spec adopt_session(term()) :: {:ok, term()} | {:error, term()}
  def adopt_session(lease), do: transfer_session(lease, self(), :held)

  defp required_catalogs(opts) do
    opts
    |> Keyword.get(:required_catalogs)
    |> List.wrap()
    |> Enum.map(&to_string/1)
    |> Enum.reject(&(&1 == ""))
    |> Enum.uniq()
  end

  defp transfer_session(nil, _owner, _target), do: {:ok, nil}

  defp transfer_session({:borrowed, _scope, _owner}, _new_owner, _target),
    do: {:error, :borrowed_lease}

  defp transfer_session(lease, new_owner, target) when is_tuple(lease) do
    with {:ok, transfers, transferred, held_scopes} <- transfer_plan([lease], new_owner, target),
         :ok <- Limiter.transfer_many(transfers) do
      Enum.each(held_scopes, &increment_held/1)
      {:ok, List.first(transferred)}
    end
  end

  defp transfer_session(leases, new_owner, target) when is_list(leases) do
    with {:ok, transfers, transferred, held_scopes} <- transfer_plan(leases, new_owner, target),
         :ok <- Limiter.transfer_many(transfers) do
      Enum.each(held_scopes, &increment_held/1)
      {:ok, transferred}
    end
  end

  defp transfer_session(_lease, _new_owner, _target), do: {:error, :invalid_lease}

  defp transfer_plan(leases, new_owner, target) do
    Enum.reduce_while(leases, {:ok, [], [], []}, fn lease,
                                                    {:ok, transfers, transferred, held_scopes} ->
      case transfer_plan_lease(lease, new_owner, target) do
        {:ok, transfer, next_lease, held_scope} ->
          transfers = if transfer, do: [transfer | transfers], else: transfers
          held_scopes = if held_scope, do: [held_scope | held_scopes], else: held_scopes

          {:cont, {:ok, transfers, [next_lease | transferred], held_scopes}}

        {:error, reason} ->
          {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, transfers, transferred, held_scopes} ->
        {:ok, Enum.reverse(transfers), Enum.reverse(transferred), Enum.reverse(held_scopes)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp transfer_plan_lease(nil, _new_owner, _target), do: {:ok, nil, nil, nil}

  defp transfer_plan_lease({:external, scope, owner}, new_owner, :held),
    do: {:ok, {scope, owner, new_owner}, {:held, scope, new_owner}, scope}

  defp transfer_plan_lease({:held, scope, owner}, new_owner, :external),
    do: {:ok, {scope, owner, new_owner}, {:external, scope, new_owner}, nil}

  defp transfer_plan_lease({:borrowed, _scope, _owner}, _new_owner, _target),
    do: {:error, :borrowed_lease}

  defp transfer_plan_lease(_lease, _new_owner, _target), do: {:error, :invalid_lease}

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

  defp policies_for(
         %Session{concurrency_policies: %ConcurrencyPolicies{} = policies} = session,
         operation,
         payload
       ) do
    catalog_policies =
      session
      |> catalog_targets(operation, payload)
      |> Enum.map(&(ConcurrencyPolicies.catalog_policy(policies, &1) || policies.default))
      |> Enum.reject(&is_nil/1)

    case catalog_policies do
      [] -> List.wrap(policies.default)
      policies -> Enum.uniq_by(policies, & &1.scope)
    end
  end

  defp policies_for(
         %Session{concurrency_policy: %ConcurrencyPolicy{} = policy},
         _operation,
         _payload
       ) do
    [policy]
  end

  defp policies_for(%Session{}, _operation, _payload), do: []

  defp catalog_targets(%Session{} = session, operation, payload) do
    case catalog_target(operation, payload) do
      {_connection, catalog} when is_binary(catalog) ->
        [catalog]

      _target ->
        operation_catalog_scope(session, operation, payload)
    end
  end

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

  defp operation_catalog_scope(%Session{} = session, operation, payload) do
    case explicit_catalogs(payload) do
      [] -> session_required_catalogs(session, operation, payload)
      catalogs -> catalogs
    end
  end

  defp explicit_catalogs({_statement, opts}) when is_list(opts), do: explicit_catalogs(opts)

  defp explicit_catalogs(opts) when is_list(opts) do
    if Keyword.keyword?(opts) do
      cond do
        Keyword.has_key?(opts, :catalog) ->
          opts |> Keyword.get(:catalog) |> normalize_catalog_list()

        Keyword.has_key?(opts, :target) ->
          opts |> Keyword.get(:target) |> target_catalogs()

        Keyword.has_key?(opts, :required_catalogs) ->
          opts |> Keyword.get(:required_catalogs) |> normalize_catalog_list()

        true ->
          []
      end
    else
      []
    end
  end

  defp explicit_catalogs(_payload), do: []

  defp target_catalogs({:catalog, catalog}), do: normalize_catalog_list(catalog)
  defp target_catalogs(%{catalog: catalog}), do: normalize_catalog_list(catalog)
  defp target_catalogs(_target), do: []

  defp session_required_catalogs(%Session{required_catalogs: catalogs}, :execute, _payload), do: catalogs

  defp session_required_catalogs(%Session{required_catalogs: catalogs}, :transaction, _payload),
    do: catalogs

  defp session_required_catalogs(%Session{required_catalogs: catalogs}, :query, payload) do
    statement = statement_payload(payload)

    if write_query?(:query, statement), do: catalogs, else: []
  end

  defp session_required_catalogs(%Session{}, _operation, _payload), do: []

  defp statement_payload({statement, opts}) when is_list(opts), do: statement
  defp statement_payload(statement), do: statement

  defp normalize_catalog_list(catalogs) do
    catalogs
    |> List.wrap()
    |> Enum.map(&to_string/1)
    |> Enum.reject(&(&1 == ""))
    |> Enum.uniq()
  end

  defp permit_required?(%ConcurrencyPolicy{limit: :unlimited}, _operation, _payload), do: false
  defp permit_required?(%ConcurrencyPolicy{applies_to: :all}, _operation, _payload), do: true
  defp permit_required?(nil, _operation, _payload), do: false

  defp permit_required?(%ConcurrencyPolicy{applies_to: :writes}, operation, payload) do
    write_operation?(operation, payload)
  end

  defp write_operation?(operation, _payload) when operation in [:execute, :materialize, :transaction],
    do: true

  defp write_operation?(operation, payload), do: write_query?(operation, payload)

  defp write_query?(:query, {statement, opts}) when is_list(opts), do: write_query?(:query, statement)

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

  defp acquire_and_run(policies, operation, payload, fun) do
    policies =
      policies
      |> Enum.filter(&permit_required?(&1, operation, payload))
      |> Enum.sort_by(&inspect(&1.scope))

    if policies == [] do
      run_and_emit(policies, operation, fun)
    else
      started_at = monotonic_ms()

      case acquire_operation_leases(policies, operation) do
        {:error, %Error{}} = error ->
          emit_operation_admission_wait(:error, started_at, operation, policies)
          error

        leases ->
          emit_operation_admission_wait(:ok, started_at, operation, policies)

          try do
            run_and_emit(policies, operation, fun)
          after
            release_session(leases)
          end
      end
    end
  end

  defp run_and_emit(policies, operation, fun) do
    started_at = monotonic_ms()

    try do
      result = fun.()
      emit_operation_run(result_status(result), started_at, operation, policies)
      result
    rescue
      error ->
        emit_operation_run(:raised, started_at, operation, policies)
        reraise error, __STACKTRACE__
    catch
      kind, reason ->
        emit_operation_run(kind, started_at, operation, policies)
        :erlang.raise(kind, reason, __STACKTRACE__)
    end
  end

  defp result_status({:ok, _value}), do: :ok
  defp result_status({:error, _reason}), do: :error
  defp result_status(_result), do: :unknown

  defp emit_operation_admission_wait(result, started_at, operation, policies) do
    Observability.emit(
      [:admission, :operation, :wait],
      %{wait_time_ms: monotonic_ms() - started_at},
      Map.put(operation_policy_metadata(operation, policies), :result, result)
    )
  end

  defp emit_operation_run(result, started_at, operation, policies) do
    Observability.emit(
      [:operation, :run],
      %{duration_ms: monotonic_ms() - started_at},
      Map.put(operation_policy_metadata(operation, policies), :result, result)
    )
  end

  defp operation_policy_metadata(operation, policies) do
    %{
      operation: operation,
      policy_count: length(policies),
      policy_targets: Enum.map(policies, & &1.target),
      policy_scopes: Enum.map(policies, &inspect(&1.scope))
    }
  end

  defp acquire_operation_leases(policies, operation) do
    Enum.reduce_while(policies, [], fn %ConcurrencyPolicy{scope: scope} = policy, leases ->
      cond do
        already_holding?(scope) ->
          {:cont, leases}

        true ->
          case acquire_lease(policy, operation) do
            {:error, %Error{}} = error ->
              release_session(leases)
              {:halt, error}

            lease ->
              {:cont, [lease | leases]}
          end
      end
    end)
    |> case do
      {:error, %Error{}} = error -> error
      leases -> leases
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

  defp monotonic_ms, do: System.monotonic_time(:millisecond)
end
