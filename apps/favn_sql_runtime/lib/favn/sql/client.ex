defmodule Favn.SQL.Client do
  @moduledoc """
  Shared SQL runtime client for named Favn connections.

  DuckDB/ADBC connections use runner-local pooling by default when the adapter is
  poolable. Disable per connection with `pool: [enabled: false]`, or tune with
  `pool: [enabled: true, max_idle_per_key: 1, idle_timeout_ms: 300_000]`.

  The pool is local to one runner BEAM and does not increase catalog/write
  concurrency. Checked-out sessions are exclusive, and reuse requires matching
  connection identity/config, required catalog set, and adapter fingerprint. A
  pooled session is process-affine: only the checkout owner may run operations or
  disconnect it; non-owner use returns `:invalid_checkout_owner` and marks the
  checkout for discard.
  Automatic retries are limited to session creation/bootstrap and read-only
  inspection/query paths; writes, materialization, transactions, and unknown
  outcome failures are not blindly retried. Raw execute/materialize/transaction
  paths discard pooled sessions after mutation unless explicitly marked
  internally as pool-safe.

  Idle pooled sessions keep their catalog admission leases until reuse or idle
  eviction. With finite catalog concurrency, an idle session for one pool key can
  block a new incompatible pool key that needs the same catalog until the idle
  session is reused or closed.

  SQL sessions retain their normalized `:required_catalogs` scope. Raw write
  operations use explicit operation catalog targets when provided and otherwise
  use that retained session scope for catalog admission; arbitrary SQL text is not
  parsed to infer target catalogs.
  """

  alias Favn.Connection.Loader
  alias Favn.Connection.Registry
  alias Favn.Connection.Resolved
  alias Favn.RelationRef
  alias Favn.SQL.Admission
  alias Favn.SQL.ConcurrencyPolicy
  alias Favn.SQL.Error
  alias Favn.SQL.Observability
  alias Favn.SQL.PoolConfig
  alias Favn.SQL.PoolKey
  alias Favn.SQL.Retry
  alias Favn.SQL.Session
  alias Favn.SQL.SessionPool
  alias Favn.SQL.SessionPool.Checkout
  alias Favn.SQL.WritePlan

  @resolution_opt_keys [:registry_name]
  @default_required_catalogs_key {__MODULE__, :default_required_catalogs_by_connection}

  @type operation_result :: {:ok, term()} | {:error, term()}

  @spec connect(atom(), keyword()) :: {:ok, Session.t()} | {:error, term()}
  def connect(connection, opts \\ [])

  def connect(connection, opts) when is_atom(connection) and is_list(opts) do
    {resolution_opts, adapter_opts} = split_connect_opts(opts)
    adapter_opts = maybe_put_default_required_catalogs(connection, adapter_opts)

    with {:ok, %Resolved{} = resolved} <- fetch_connection(connection, resolution_opts),
         {:ok, concurrency_policies} <- ConcurrencyPolicy.resolve(resolved) do
      connect_with_admission(resolved, concurrency_policies, adapter_opts)
    end
  rescue
    error -> {:error, normalize_runtime_error(:connect, error)}
  catch
    :exit, reason -> {:error, normalize_runtime_error(:connect, reason)}
  end

  def connect(connection, _opts), do: {:error, invalid_connection_error(connection)}

  @doc false
  @spec with_default_required_catalogs(atom(), [atom() | String.t()], (-> result)) :: result
        when result: var
  def with_default_required_catalogs(connection, catalogs, fun)
      when is_atom(connection) and is_list(catalogs) and is_function(fun, 0) do
    previous = Process.get(@default_required_catalogs_key, %{})
    catalogs = normalize_catalogs(catalogs)

    next =
      if catalogs == [] do
        Map.delete(previous, connection)
      else
        Map.put(previous, connection, catalogs)
      end

    Process.put(@default_required_catalogs_key, next)

    try do
      fun.()
    after
      Process.put(@default_required_catalogs_key, previous)
    end
  end

  @spec disconnect(Session.t()) :: :ok | {:error, Error.t()}
  def disconnect(%Session{pool_checkout: %Checkout{} = checkout} = session) do
    case checkout_owner_error(session, :disconnect) do
      nil ->
        disconnect_pooled_session(session)

      %Error{} = error ->
        SessionPool.mark_discard(checkout.token, discard_reason(:disconnect, error))
        {:error, error}
    end
  end

  def disconnect(%Session{adapter: adapter, conn: conn, admission_lease: lease}) do
    _ = adapter.disconnect(conn, [])
    Admission.release_session(lease)
    :ok
  rescue
    _error ->
      Admission.release_session(lease)
      :ok
  catch
    :exit, _ ->
      Admission.release_session(lease)
      :ok
  end

  def disconnect(_session), do: :ok

  defp disconnect_pooled_session(%Session{} = session) do
    case checkin_pooled_session(session) do
      :ok ->
        Admission.detach_session(session.admission_lease)
        :ok

      {:error, _reason} ->
        disconnect_directly(session)
    end
  end

  defp checkin_pooled_session(%Session{} = session) do
    SessionPool.checkin(session, :ok)
  rescue
    error -> {:error, error}
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp disconnect_directly(%Session{adapter: adapter, conn: conn, admission_lease: lease}) do
    try do
      _ = adapter.disconnect(conn, [])
      Admission.release_session(lease)
      :ok
    rescue
      _error ->
        Admission.release_session(lease)
        :ok
    catch
      :exit, _reason ->
        Admission.release_session(lease)
        :ok
    end
  end

  @spec capabilities(Session.t()) :: {:ok, Favn.SQL.Capabilities.t()} | {:error, term()}
  def capabilities(%Session{capabilities: capabilities}), do: {:ok, capabilities}
  def capabilities(_session), do: {:error, invalid_session_error()}

  @spec query(Session.t(), iodata(), keyword()) :: operation_result()
  def query(%Session{} = session, statement, opts) when is_list(opts) do
    session
    |> run_session_operation(:query, statement, opts, fn ->
      run_with_optional_retry(:query, opts, fn ->
        Admission.with_permit(session, :query, {statement, opts}, fn ->
          session.adapter.query(session.conn, statement, opts)
        end)
      end)
    end)
  rescue
    error -> {:error, normalize_runtime_error(:query, error)}
  catch
    :exit, reason -> {:error, normalize_runtime_error(:query, reason)}
  end

  def query(_session, _statement, _opts), do: {:error, invalid_session_error()}

  @spec execute(Session.t(), iodata(), keyword()) :: operation_result()
  def execute(%Session{} = session, statement, opts) when is_list(opts) do
    session
    |> run_session_operation(:execute, statement, opts, fn ->
      Admission.with_permit(session, :execute, {statement, opts}, fn ->
        session.adapter.execute(session.conn, statement, opts)
      end)
    end)
  rescue
    error -> {:error, normalize_runtime_error(:execute, error)}
  catch
    :exit, reason -> {:error, normalize_runtime_error(:execute, reason)}
  end

  def execute(_session, _statement, _opts), do: {:error, invalid_session_error()}

  @spec materialize(Session.t(), WritePlan.t(), keyword()) :: operation_result()
  def materialize(%Session{} = session, %WritePlan{} = write_plan, opts)
      when is_list(opts) do
    session
    |> run_session_operation(:materialize, write_plan, opts, fn ->
      Admission.with_permit(session, :materialize, write_plan, fn ->
        session.adapter.materialize(session.conn, write_plan, opts)
      end)
    end)
  rescue
    error -> {:error, normalize_runtime_error(:materialize, error)}
  catch
    :exit, reason -> {:error, normalize_runtime_error(:materialize, reason)}
  end

  def materialize(_session, _write_plan, _opts), do: {:error, invalid_session_error()}

  @spec relation(Session.t(), RelationRef.t()) :: operation_result()
  def relation(%Session{} = session, %RelationRef{} = relation_ref) do
    session
    |> run_session_operation(:relation, relation_ref, [], fn ->
      run_with_optional_retry(:relation, [], fn ->
        Admission.with_permit(session, :relation, relation_ref, fn ->
          session.adapter.relation(session.conn, relation_ref, [])
        end)
      end)
    end)
  rescue
    error -> {:error, normalize_runtime_error(:relation, error)}
  catch
    :exit, reason -> {:error, normalize_runtime_error(:relation, reason)}
  end

  def relation(_session, _relation_ref), do: {:error, invalid_session_error()}

  @spec columns(Session.t(), RelationRef.t()) :: operation_result()
  def columns(%Session{} = session, %RelationRef{} = relation_ref) do
    session
    |> run_session_operation(:columns, relation_ref, [], fn ->
      run_with_optional_retry(:columns, [], fn ->
        Admission.with_permit(session, :columns, relation_ref, fn ->
          session.adapter.columns(session.conn, relation_ref, [])
        end)
      end)
    end)
  rescue
    error -> {:error, normalize_runtime_error(:columns, error)}
  catch
    :exit, reason -> {:error, normalize_runtime_error(:columns, reason)}
  end

  def columns(_session, _relation_ref), do: {:error, invalid_session_error()}

  @spec row_count(Session.t(), RelationRef.t()) :: operation_result()
  def row_count(%Session{} = session, %RelationRef{} = relation_ref) do
    if function_exported?(session.adapter, :row_count, 3) do
      session
      |> run_session_operation(:row_count, relation_ref, [], fn ->
        run_with_optional_retry(:row_count, [], fn ->
          Admission.with_permit(session, :row_count, relation_ref, fn ->
            session.adapter.row_count(session.conn, relation_ref, [])
          end)
        end)
      end)
    else
      {:error, unsupported_introspection_error(session, :row_count)}
    end
  rescue
    error -> {:error, normalize_runtime_error(:row_count, error)}
  catch
    :exit, reason -> {:error, normalize_runtime_error(:row_count, reason)}
  end

  def row_count(_session, _relation_ref), do: {:error, invalid_session_error()}

  @spec sample(Session.t(), RelationRef.t(), keyword()) :: operation_result()
  def sample(session, relation_ref, opts \\ [])

  def sample(%Session{} = session, %RelationRef{} = relation_ref, opts) when is_list(opts) do
    with {:ok, limit} <- sample_limit(opts) do
      if function_exported?(session.adapter, :sample, 3) do
        session
        |> run_session_operation(:sample, relation_ref, [], fn ->
          run_with_optional_retry(:sample, [], fn ->
            Admission.with_permit(session, :sample, relation_ref, fn ->
              session.adapter.sample(session.conn, relation_ref, limit: limit)
            end)
          end)
        end)
      else
        {:error, unsupported_introspection_error(session, :sample)}
      end
    end
  rescue
    error -> {:error, normalize_runtime_error(:sample, error)}
  catch
    :exit, reason -> {:error, normalize_runtime_error(:sample, reason)}
  end

  def sample(_session, _relation_ref, _opts), do: {:error, invalid_session_error()}

  @spec table_metadata(Session.t(), RelationRef.t()) :: operation_result()
  def table_metadata(%Session{} = session, %RelationRef{} = relation_ref) do
    if function_exported?(session.adapter, :table_metadata, 3) do
      session
      |> run_session_operation(:table_metadata, relation_ref, [], fn ->
        run_with_optional_retry(:table_metadata, [], fn ->
          Admission.with_permit(session, :table_metadata, relation_ref, fn ->
            session.adapter.table_metadata(session.conn, relation_ref, [])
          end)
        end)
      end)
    else
      {:error, unsupported_introspection_error(session, :table_metadata)}
    end
  rescue
    error -> {:error, normalize_runtime_error(:table_metadata, error)}
  catch
    :exit, reason -> {:error, normalize_runtime_error(:table_metadata, reason)}
  end

  def table_metadata(_session, _relation_ref), do: {:error, invalid_session_error()}

  @spec transaction(Session.t(), (Session.t() -> operation_result()), keyword()) ::
          operation_result()
  def transaction(session, fun, opts \\ [])

  def transaction(%Session{} = session, fun, opts)
      when is_function(fun, 1) and is_list(opts) do
    run_transaction(session, fun, opts)
  rescue
    error -> {:error, normalize_runtime_error(:transaction, error)}
  catch
    :exit, reason -> {:error, normalize_runtime_error(:transaction, reason)}
  end

  def transaction(_session, _fun, _opts), do: {:error, invalid_session_error()}

  defp run_transaction(%Session{adapter: adapter, conn: conn} = session, fun, opts) do
    if function_exported?(adapter, :transaction, 3) do
      session
      |> run_session_operation(:transaction, nil, opts, fn ->
        Admission.with_permit(session, :transaction, opts, fn ->
          adapter.transaction(
            conn,
            fn tx_conn -> fun.(%Session{session | conn: tx_conn}) end,
            opts
          )
        end)
      end)
    else
      {:error, unsupported_transaction_error(session)}
    end
  end

  defp split_connect_opts(opts) do
    Keyword.split(opts, @resolution_opt_keys)
  end

  defp maybe_put_default_required_catalogs(connection, adapter_opts) do
    cond do
      Keyword.has_key?(adapter_opts, :required_catalogs) ->
        adapter_opts

      true ->
        @default_required_catalogs_key
        |> Process.get(%{})
        |> Map.get(connection, [])
        |> case do
          [] -> adapter_opts
          catalogs -> Keyword.put(adapter_opts, :required_catalogs, catalogs)
        end
    end
  end

  defp fetch_connection(connection, opts) do
    registry_name = Keyword.get(opts, :registry_name)

    if is_atom(registry_name) and not is_nil(registry_name) do
      fetch_from_registry(connection, registry_name)
    else
      fetch_from_config(connection)
    end
  end

  defp fetch_from_registry(connection, registry_name) do
    case Registry.fetch(connection, registry_name: registry_name) do
      {:ok, %Resolved{} = resolved} -> {:ok, resolved}
      :error -> {:error, invalid_connection_error(connection)}
    end
  catch
    :exit, reason -> {:error, normalize_runtime_error(:connect, reason)}
  end

  defp fetch_from_config(connection) do
    with {:ok, connections} <- Loader.load() do
      case Map.fetch(connections, connection) do
        {:ok, %Resolved{} = resolved} -> {:ok, resolved}
        :error -> {:error, invalid_connection_error(connection)}
      end
    end
  end

  defp connect_with_admission(%Resolved{} = resolved, concurrency_policies, adapter_opts) do
    pool_config = Map.get(resolved.config || %{}, :pool, %PoolConfig{})

    if pool_enabled?(resolved, adapter_opts, pool_config) do
      connect_with_pool(resolved, concurrency_policies, adapter_opts, pool_config)
    else
      connect_without_pool(resolved, concurrency_policies, adapter_opts)
    end
  end

  defp connect_without_pool(%Resolved{} = resolved, concurrency_policies, adapter_opts) do
    case Admission.acquire_session(concurrency_policies, adapter_opts) do
      {:error, %Error{}} = error ->
        error

      lease ->
        try do
          connect_and_build_session(resolved, adapter_opts, concurrency_policies, lease)
        rescue
          error ->
            Admission.release_session(lease)
            reraise error, __STACKTRACE__
        catch
          kind, reason ->
            Admission.release_session(lease)
            :erlang.raise(kind, reason, __STACKTRACE__)
        end
    end
  end

  defp connect_with_pool(%Resolved{} = resolved, concurrency_policies, adapter_opts, pool_config) do
    key = pool_key(resolved, adapter_opts)

    case checkout_or_create_session(key, resolved, concurrency_policies, adapter_opts) do
      {:ok, %Session{} = session} ->
        {:ok, session}

      :create ->
        create_pooled_session(resolved, concurrency_policies, adapter_opts, pool_config, key)

      {:error, %Error{}} = error ->
        error
    end
  end

  defp checkout_or_create_session(key, resolved, concurrency_policies, adapter_opts) do
    case SessionPool.checkout_or_create(key) do
      {:ok, %Session{} = session} ->
        case prepare_warm_session(session, resolved, concurrency_policies, adapter_opts) do
          {:ok, %Session{} = session} -> {:ok, session}
          :miss -> checkout_or_create_session(key, resolved, concurrency_policies, adapter_opts)
          {:error, %Error{}} = error -> error
        end

      :create ->
        :create
    end
  end

  defp prepare_warm_session(%Session{} = session, resolved, concurrency_policies, adapter_opts) do
    case checkout_warm_session_lease(session, concurrency_policies, adapter_opts) do
      {:ok, lease} ->
        session = put_session_runtime(session, resolved, concurrency_policies, adapter_opts, lease)
        :ok = SessionPool.update_checkout(session)

        with :ok <- validate_pooled_session(session, adapter_opts),
             :ok <- reset_pooled_session(session, resolved, adapter_opts) do
          {:ok, session}
        else
          {:error, reason} ->
            Admission.release_session(lease)
            SessionPool.discard(%Session{session | admission_lease: nil}, reason)
            :miss

          other ->
            Admission.release_session(lease)
            SessionPool.discard(%Session{session | admission_lease: nil}, other)
            :miss
        end

      {:error, %Error{}} = error ->
        SessionPool.discard(session, :admission_failed)
        error

      {:error, reason} ->
        SessionPool.discard(session, {:admission_adopt_failed, reason})
        :miss
    end
  end

  defp create_pooled_session(resolved, concurrency_policies, adapter_opts, pool_config, key) do
    try do
      Retry.run(
        fn ->
          with {:ok, lease} <- acquire_checkout_lease(concurrency_policies, adapter_opts),
               {:ok, session} <-
                 connect_and_build_session(resolved, adapter_opts, concurrency_policies, lease) do
            session =
              session
              |> SessionPool.attach_checkout(key, pool_config)
              |> put_session_runtime(resolved, concurrency_policies, adapter_opts, lease)

            :ok = SessionPool.track_checkout(session)

            Observability.emit(
              [:pool, :session, :created],
              %{},
              pool_session_metadata(key, resolved)
            )

            {:ok, session}
          end
        end,
        phase: :session_bootstrap
      )
    after
      SessionPool.creation_finished(key)
    end
  end

  defp acquire_checkout_lease(concurrency_policies, adapter_opts) do
    started_at = monotonic_ms()

    case Admission.acquire_session(concurrency_policies, adapter_opts) do
      {:error, %Error{} = error} ->
        emit_admission_wait(:error, started_at, concurrency_policies, adapter_opts)
        {:error, error}

      lease ->
        emit_admission_wait(:ok, started_at, concurrency_policies, adapter_opts)
        {:ok, lease}
    end
  end

  defp checkout_warm_session_lease(
         %Session{admission_lease: nil},
         concurrency_policies,
         adapter_opts
       ) do
    acquire_checkout_lease(concurrency_policies, adapter_opts)
  end

  defp checkout_warm_session_lease(
         %Session{admission_lease: lease},
         _concurrency_policies,
         _adapter_opts
       ) do
    Admission.adopt_session(lease)
  end

  defp pool_session_metadata(%PoolKey{} = key, %Resolved{} = resolved) do
    %{
      key_hash: key.hash,
      connection: resolved.name,
      adapter: inspect(resolved.adapter)
    }
  end

  defp emit_admission_wait(result, started_at, _concurrency_policies, adapter_opts) do
    Observability.emit(
      [:admission, :wait],
      %{wait_time_ms: monotonic_ms() - started_at},
      %{
        result: result,
        required_catalogs: normalized_required_catalogs(adapter_opts)
      }
    )
  end

  defp normalized_required_catalogs(adapter_opts) do
    adapter_opts
    |> Keyword.get(:required_catalogs, [])
    |> List.wrap()
    |> normalize_catalogs()
  end

  defp normalize_catalogs(catalogs) when is_list(catalogs) do
    catalogs
    |> Enum.map(&to_string/1)
    |> Enum.reject(&(&1 == ""))
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp pool_enabled?(%Resolved{adapter: adapter} = resolved, adapter_opts, %PoolConfig{
         enabled: true
       }) do
    function_exported?(adapter, :poolable?, 2) and adapter.poolable?(resolved, adapter_opts)
  rescue
    _ -> false
  catch
    :exit, _ -> false
  end

  defp pool_enabled?(_resolved, _adapter_opts, _pool_config), do: false

  defp pool_key(%Resolved{adapter: adapter} = resolved, adapter_opts) do
    required_catalogs = Keyword.get(adapter_opts, :required_catalogs, []) |> List.wrap()

    PoolKey.build(
      resolved,
      adapter_opts,
      required_catalogs,
      adapter_fingerprint(adapter, resolved, adapter_opts)
    )
  end

  defp adapter_fingerprint(adapter, resolved, adapter_opts) do
    if function_exported?(adapter, :pool_fingerprint, 2) do
      adapter.pool_fingerprint(resolved, adapter_opts)
    else
      adapter
    end
  rescue
    _ -> adapter
  catch
    :exit, _ -> adapter
  end

  defp put_session_runtime(%Session{} = session, resolved, concurrency_policies, adapter_opts, lease) do
    %Session{
      session
      | resolved: resolved,
        concurrency_policy: singular_policy(concurrency_policies),
        concurrency_policies: policy_container(concurrency_policies),
        required_catalogs: normalized_required_catalogs(adapter_opts),
        admission_lease: lease
    }
  end

  defp validate_pooled_session(%Session{adapter: adapter, conn: conn}, adapter_opts) do
    if function_exported?(adapter, :validate_session, 2) do
      adapter.validate_session(conn, adapter_opts)
    else
      {:error, :missing_validate_session_callback}
    end
  end

  defp reset_pooled_session(%Session{adapter: adapter, conn: conn}, resolved, adapter_opts) do
    if function_exported?(adapter, :reset_session, 3) do
      adapter.reset_session(conn, resolved, adapter_opts)
    else
      {:error, :missing_reset_session_callback}
    end
  end

  defp connect_and_build_session(
         %Resolved{} = resolved,
         adapter_opts,
         concurrency_policies,
         lease
       ) do
    case resolved.adapter.connect(resolved, adapter_opts) do
      {:ok, conn} ->
        bootstrap_and_build_session(resolved, adapter_opts, concurrency_policies, lease, conn)

      {:error, _reason} = error ->
        release_lease_and_return(error, lease)

      other ->
        release_lease_and_return(other, lease)
    end
  end

  defp bootstrap_and_build_session(resolved, adapter_opts, concurrency_policies, lease, conn) do
    case bootstrap_connection_with_cleanup(resolved, conn, adapter_opts) do
      :ok ->
        build_session_with_capabilities(resolved, adapter_opts, concurrency_policies, lease, conn)

      {:error, _reason} = error ->
        disconnect_after_connect_error(resolved.adapter, conn, lease, error)

      other ->
        disconnect_after_connect_error(resolved.adapter, conn, lease, other)
    end
  end

  defp bootstrap_connection_with_cleanup(resolved, conn, adapter_opts) do
    bootstrap_connection(resolved, conn, adapter_opts)
  rescue
    error ->
      _ = resolved.adapter.disconnect(conn, [])
      reraise error, __STACKTRACE__
  catch
    kind, reason ->
      _ = resolved.adapter.disconnect(conn, [])
      :erlang.raise(kind, reason, __STACKTRACE__)
  end

  defp bootstrap_connection(%Resolved{adapter: adapter} = resolved, conn, adapter_opts) do
    if function_exported?(adapter, :bootstrap, 3) do
      adapter.bootstrap(conn, resolved, adapter_opts)
    else
      :ok
    end
  end

  defp build_session_with_capabilities(resolved, adapter_opts, concurrency_policies, lease, conn) do
    case resolved.adapter.capabilities(resolved, adapter_opts) do
      {:ok, capabilities} ->
        {:ok,
         %Session{
           adapter: resolved.adapter,
           resolved: resolved,
           conn: conn,
            capabilities: capabilities,
            concurrency_policy: singular_policy(concurrency_policies),
            concurrency_policies: policy_container(concurrency_policies),
            required_catalogs: normalized_required_catalogs(adapter_opts),
            admission_lease: lease
          }}

      {:error, _reason} = error ->
        disconnect_after_connect_error(resolved.adapter, conn, lease, error)

      other ->
        disconnect_after_connect_error(resolved.adapter, conn, lease, other)
    end
  rescue
    error ->
      _ = resolved.adapter.disconnect(conn, [])
      reraise error, __STACKTRACE__
  catch
    kind, reason ->
      _ = resolved.adapter.disconnect(conn, [])
      :erlang.raise(kind, reason, __STACKTRACE__)
  end

  defp singular_policy(%Favn.SQL.ConcurrencyPolicy{} = policy), do: policy
  defp singular_policy(_policies), do: nil

  defp policy_container(%Favn.SQL.ConcurrencyPolicies{} = policies), do: policies
  defp policy_container(_policy), do: nil

  defp disconnect_after_connect_error(adapter, conn, lease, error) do
    _ = adapter.disconnect(conn, [])
    release_lease_and_return(error, lease)
  end

  defp release_lease_and_return(error, lease) do
    Admission.release_session(lease)
    error
  end

  defp run_session_operation(%Session{} = session, operation, payload, opts, fun)
       when is_function(fun, 0) do
    case checkout_owner_error(session, operation) do
      nil ->
        run_owned_session_operation(session, operation, payload, opts, fun)

      %Error{} = error ->
        maybe_mark_pooled_session_discard(session, operation, payload, error)
        {:error, error}
    end
  end

  defp run_owned_session_operation(%Session{} = session, operation, payload, opts, fun)
       when is_function(fun, 0) do
    case fun.() do
      {:ok, _value} = result ->
        maybe_mark_pooled_session_success(session, operation, opts)
        result

      {:error, %Error{} = error} = result ->
        maybe_mark_pooled_session_discard(session, operation, payload, error)
        result

      other ->
        other
    end
  end

  defp maybe_mark_pooled_session_success(
         %Session{pool_checkout: %Checkout{} = checkout},
         operation,
         opts
       ) do
    if discard_pooled_session_after_success?(operation, opts) do
      SessionPool.mark_discard(checkout.token, %{operation: operation, status: :success})
    end

    :ok
  end

  defp maybe_mark_pooled_session_success(_session, _operation, _opts), do: :ok

  defp discard_pooled_session_after_success?(operation, opts) when is_list(opts) do
    cond do
      Keyword.get(opts, :pool_safe?) == true -> false
      operation == :query -> Keyword.get(opts, :read_only?) != true
      true -> operation in [:execute, :materialize, :transaction]
    end
  end

  defp discard_pooled_session_after_success?(operation, _opts),
    do: operation in [:execute, :materialize, :transaction]

  defp run_with_optional_retry(:query, opts, fun) when is_function(fun, 0) do
    if Keyword.get(opts, :read_only?) == true do
      fun
      |> Retry.run(phase: :read_only)
      |> put_retry_operation(:query)
    else
      fun.()
    end
  end

  defp run_with_optional_retry(operation, _opts, fun) when is_function(fun, 0) do
    if operation in [:relation, :columns, :row_count, :sample, :table_metadata] do
      fun
      |> Retry.run(phase: :read_only)
      |> put_retry_operation(operation)
    else
      fun.()
    end
  end

  defp put_retry_operation({:error, %Error{operation: nil} = error}, operation),
    do: {:error, %Error{error | operation: operation}}

  defp put_retry_operation(result, _operation), do: result

  defp maybe_mark_pooled_session_discard(
         %Session{pool_checkout: %Checkout{} = checkout},
         operation,
         payload,
         %Error{} = error
       ) do
    if discard_pooled_session?(operation, payload, error) do
      SessionPool.mark_discard(checkout.token, discard_reason(operation, error))
    end

    :ok
  end

  defp maybe_mark_pooled_session_discard(_session, _operation, _payload, _error), do: :ok

  defp checkout_owner_error(%Session{pool_checkout: %Checkout{owner: owner}}, operation)
       when owner != self() do
    %Error{
      type: :invalid_checkout_owner,
      message: "pooled SQL session is checked out by another process",
      operation: operation,
      details: %{owner: inspect(owner), caller: inspect(self())}
    }
  end

  defp checkout_owner_error(%Session{}, _operation), do: nil

  defp discard_pooled_session?(_operation, _payload, %Error{type: :admission_timeout}), do: false

  defp discard_pooled_session?(_operation, _payload, %Error{type: :invalid_checkout_owner}),
    do: true

  defp discard_pooled_session?(_operation, _payload, %Error{type: :connection_error}), do: true

  defp discard_pooled_session?(operation, _payload, %Error{} = error)
       when operation in [:execute, :materialize, :transaction] do
    not retryable_admission_only?(error)
  end

  defp discard_pooled_session?(_operation, _payload, %Error{} = error) do
    classification = Map.get(error.details || %{}, :classification)
    classification in [:unknown_commit_state, :unknown_outcome_timeout]
  end

  defp retryable_admission_only?(%Error{type: :admission_timeout}), do: true
  defp retryable_admission_only?(_error), do: false

  defp discard_reason(operation, %Error{} = error) do
    %{
      operation: operation,
      type: error.type,
      classification: Map.get(error.details || %{}, :classification)
    }
  end

  defp invalid_connection_error(connection) do
    %Error{
      type: :invalid_config,
      message: "connection not found: #{inspect(connection)}",
      connection: if(is_atom(connection), do: connection, else: nil),
      operation: :connect
    }
  end

  defp invalid_session_error do
    %Error{type: :invalid_config, message: "invalid SQL session", operation: :session}
  end

  defp unsupported_transaction_error(%Session{resolved: %Resolved{name: connection}}) do
    %Error{
      type: :unsupported_capability,
      message: "adapter does not support transactions",
      connection: connection,
      operation: :transaction
    }
  end

  defp unsupported_introspection_error(%Session{resolved: %Resolved{name: connection}}, operation) do
    %Error{
      type: :unsupported_capability,
      message: "adapter does not support #{operation}",
      connection: connection,
      operation: operation
    }
  end

  defp sample_limit(opts) do
    limit = Keyword.get(opts, :limit, 20)

    cond do
      is_integer(limit) and limit >= 0 and limit <= 20 ->
        {:ok, limit}

      is_integer(limit) and limit > 20 ->
        {:ok, 20}

      true ->
        {:error,
         %Error{
           type: :invalid_config,
           message: "sample limit must be a non-negative integer",
           operation: :sample
         }}
    end
  end

  defp normalize_runtime_error(operation, reason) do
    %Error{
      type: :execution_error,
      message: "SQL runtime operation failed",
      operation: operation,
      details: %{reason: inspect(reason)},
      cause: reason
    }
  end

  defp monotonic_ms, do: System.monotonic_time(:millisecond)
end
