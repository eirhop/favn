defmodule Favn.SQL.Client do
  @moduledoc """
  Shared SQL runtime client for named Favn connections.

  DuckDB/ADBC connections use runner-local pooling by default when the adapter is
  poolable. Disable per connection with `pool: [enabled: false]`, or tune with
  `pool: [enabled: true, max_idle_per_key: 1, idle_timeout_ms: 300_000]`.

  The pool is local to one runner BEAM and does not increase catalog/write
  concurrency. Checked-out sessions are exclusive, and reuse requires matching
  connection identity/config, required catalog and resource sets, and adapter
  fingerprint. DuckDB adapter fingerprints include selected script content and
  parameter fingerprints. A
  pooled session is process-affine: only the checkout owner may run operations or
  disconnect it; non-owner use returns `:invalid_checkout_owner` and marks the
  checkout for discard.
  Automatic retries are limited to session creation/bootstrap and read-only
  inspection/query paths; writes, materialization, transactions, and unknown
  outcome failures are not blindly retried. Raw execute/materialize/transaction
  paths discard pooled sessions after mutation unless explicitly marked
  internally as pool-safe.

  Concurrent misses for the same pool key can create fresh sessions in parallel
  up to the selected finite admission/catalog limit. This avoids serializing
  expensive DuckDB/DuckLake bootstrap for independent asset windows while keeping
  arbitrary raw SQL writes on fresh, discarded sessions by default.

  Idle pooled sessions keep their catalog admission leases until reuse or idle
  eviction. When an adapter fingerprint changes for the same connection and
  session requirements, the pool evicts idle sessions from the superseded
  fingerprint and closes active ones on checkin before creating a replacement.
  A miss for a different overlapping catalog set evicts conflicting idle
  sessions before creating a replacement, while incompatible scopes with the
  same catalog set can still compete for finite catalog capacity.

  SQL sessions retain their normalized `:required_catalogs` and
  `:required_resources` scopes. Raw write
  operations use explicit `admission: [...]` operation catalog targets when
  provided and otherwise use that retained session scope for catalog admission;
  arbitrary SQL text is not parsed to infer target catalogs.
  """

  alias Favn.Connection.Loader
  alias Favn.Connection.Registry
  alias Favn.Connection.Resolved
  alias Favn.RelationRef
  alias Favn.SQL.Admission
  alias Favn.SQL.ConcurrencyPolicies
  alias Favn.SQL.ConcurrencyPolicy
  alias Favn.SQL.Deadline
  alias Favn.SQL.Error
  alias Favn.SQL.GenerationActivation
  alias Favn.SQL.GenerationDiscard
  alias Favn.SQL.GenerationMarkerInitialization
  alias Favn.SQL.GenerationReconciliation
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
  @transaction_context_key {__MODULE__, :transaction_context}
  @nested_timeout_exit_tag {__MODULE__, :nested_operation_timeout}

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

  @doc "Returns explicit target-generation capabilities for the session adapter."
  @spec generation_capabilities(Session.t(), keyword()) :: operation_result()
  def generation_capabilities(session, opts \\ [])

  def generation_capabilities(%Session{} = session, opts) when is_list(opts) do
    if generation_adapter?(session.adapter) do
      session.adapter.generation_capabilities(session.resolved, opts)
    else
      {:error, unsupported_generation_error(session, :generation_capabilities)}
    end
  rescue
    error -> {:error, normalize_runtime_error(:generation_capabilities, error)}
  catch
    :exit, reason -> {:error, normalize_runtime_error(:generation_capabilities, reason)}
  end

  def generation_capabilities(_session, _opts), do: {:error, invalid_session_error()}

  @spec query(Session.t(), iodata(), keyword()) :: operation_result()
  def query(%Session{} = session, statement, opts) when is_list(opts) do
    {admission_opts, adapter_opts} = split_operation_opts(opts)

    session
    |> run_session_operation(:query, statement, operation_runtime_opts(opts), fn session ->
      run_with_optional_retry(:query, adapter_opts, fn ->
        Admission.with_permit(session, :query, {statement, admission_opts}, fn ->
          session.adapter.query(session.conn, statement, adapter_opts)
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
    {admission_opts, adapter_opts} = split_operation_opts(opts)

    session
    |> run_session_operation(:execute, statement, operation_runtime_opts(opts), fn session ->
      Admission.with_permit(session, :execute, {statement, admission_opts}, fn ->
        session.adapter.execute(session.conn, statement, adapter_opts)
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
    {_admission_opts, adapter_opts} = split_operation_opts(opts)

    session
    |> run_session_operation(:materialize, write_plan, operation_runtime_opts(opts), fn session ->
      Admission.with_permit(session, :materialize, write_plan, fn ->
        session.adapter.materialize(session.conn, write_plan, adapter_opts)
      end)
    end)
  rescue
    error -> {:error, normalize_runtime_error(:materialize, error)}
  catch
    :exit, reason -> {:error, normalize_runtime_error(:materialize, reason)}
  end

  def materialize(_session, _write_plan, _opts), do: {:error, invalid_session_error()}

  @doc false
  @spec materialize_in_transaction(Session.t(), WritePlan.t(), keyword()) :: operation_result()
  def materialize_in_transaction(%Session{} = session, %WritePlan{} = write_plan, opts)
      when is_list(opts) do
    {_admission_opts, adapter_opts} = split_operation_opts(opts)

    if function_exported?(session.adapter, :materialize_in_transaction, 3) do
      session
      |> run_session_operation(
        :materialize,
        write_plan,
        operation_runtime_opts(opts),
        fn session ->
          Admission.with_permit(session, :materialize, write_plan, fn ->
            session.adapter.materialize_in_transaction(session.conn, write_plan, adapter_opts)
          end)
        end
      )
    else
      {:error, unsupported_transactional_materialization_error(session)}
    end
  rescue
    error -> {:error, normalize_runtime_error(:materialize, error)}
  catch
    :exit, reason -> {:error, normalize_runtime_error(:materialize, reason)}
  end

  def materialize_in_transaction(_session, _write_plan, _opts),
    do: {:error, invalid_session_error()}

  @spec relation(Session.t(), RelationRef.t()) :: operation_result()
  def relation(%Session{} = session, %RelationRef{} = relation_ref) do
    session
    |> run_session_operation(:relation, relation_ref, [], fn session ->
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
    |> run_session_operation(:columns, relation_ref, [], fn session ->
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
      |> run_session_operation(:row_count, relation_ref, [], fn session ->
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
        |> run_session_operation(:sample, relation_ref, [], fn session ->
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
      |> run_session_operation(:table_metadata, relation_ref, [], fn session ->
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

  @doc "Inspects and fingerprints one physical generation relation."
  @spec inspect_generation(Session.t(), RelationRef.t(), keyword()) :: operation_result()
  def inspect_generation(session, relation_ref, opts \\ [])

  def inspect_generation(%Session{} = session, %RelationRef{} = relation_ref, opts)
      when is_list(opts) do
    {_admission_opts, adapter_opts} = split_operation_opts(opts)

    if generation_adapter?(session.adapter) do
      session
      |> run_session_operation(
        :inspect_generation,
        relation_ref,
        operation_runtime_opts(opts),
        fn session ->
          run_with_optional_retry(:inspect_generation, adapter_opts, fn ->
            Admission.with_permit(session, :inspect_generation, relation_ref, fn ->
              session.adapter.inspect_generation(session.conn, relation_ref, adapter_opts)
            end)
          end)
        end
      )
    else
      {:error, unsupported_generation_error(session, :inspect_generation)}
    end
  rescue
    error -> {:error, normalize_runtime_error(:inspect_generation, error)}
  catch
    :exit, reason -> {:error, normalize_runtime_error(:inspect_generation, reason)}
  end

  def inspect_generation(_session, _relation_ref, _opts), do: {:error, invalid_session_error()}

  @doc "Initializes the sidecar marker for an already materialized first generation."
  @spec initialize_generation_marker(Session.t(), GenerationMarkerInitialization.t(), keyword()) ::
          operation_result()
  def initialize_generation_marker(session, request, opts \\ [])

  def initialize_generation_marker(
        %Session{} = session,
        %GenerationMarkerInitialization{} = request,
        opts
      )
      when is_list(opts) do
    {_admission_opts, adapter_opts} = split_operation_opts(opts)

    if generation_adapter?(session.adapter) do
      session
      |> run_session_operation(
        :initialize_generation_marker,
        request,
        operation_runtime_opts(opts),
        fn session ->
          Admission.with_permit(session, :initialize_generation_marker, request, fn ->
            session.adapter.initialize_generation_marker(session.conn, request, adapter_opts)
          end)
        end
      )
    else
      {:error, unsupported_generation_error(session, :initialize_generation_marker)}
    end
  rescue
    error -> {:error, normalize_runtime_error(:initialize_generation_marker, error)}
  catch
    :exit, reason -> {:error, normalize_runtime_error(:initialize_generation_marker, reason)}
  end

  def initialize_generation_marker(_session, _request, _opts),
    do: {:error, invalid_session_error()}

  @doc "Atomically swaps a candidate generation and writes its active marker."
  @spec activate_generation(Session.t(), GenerationActivation.t(), keyword()) ::
          operation_result()
  def activate_generation(session, request, opts \\ [])

  def activate_generation(%Session{} = session, %GenerationActivation{} = request, opts)
      when is_list(opts) do
    {_admission_opts, adapter_opts} = split_operation_opts(opts)

    if generation_adapter?(session.adapter) do
      session
      |> run_session_operation(
        :activate_generation,
        request,
        operation_runtime_opts(opts),
        fn session ->
          Admission.with_permit(session, :activate_generation, request, fn ->
            session.adapter.activate_generation(session.conn, request, adapter_opts)
          end)
        end
      )
    else
      {:error, unsupported_generation_error(session, :activate_generation)}
    end
  rescue
    error -> {:error, normalize_runtime_error(:activate_generation, error)}
  catch
    :exit, reason -> {:error, normalize_runtime_error(:activate_generation, reason)}
  end

  def activate_generation(_session, _request, _opts), do: {:error, invalid_session_error()}

  @doc "Reads the data-plane marker used to reconcile an activation outcome."
  @spec reconcile_generation(Session.t(), GenerationReconciliation.t(), keyword()) ::
          operation_result()
  def reconcile_generation(session, request, opts \\ [])

  def reconcile_generation(
        %Session{} = session,
        %GenerationReconciliation{} = request,
        opts
      )
      when is_list(opts) do
    {_admission_opts, adapter_opts} = split_operation_opts(opts)

    if generation_adapter?(session.adapter) do
      session
      |> run_session_operation(
        :reconcile_generation,
        request,
        operation_runtime_opts(opts),
        fn session ->
          run_with_optional_retry(:reconcile_generation, adapter_opts, fn ->
            Admission.with_permit(session, :reconcile_generation, request, fn ->
              session.adapter.reconcile_generation(session.conn, request, adapter_opts)
            end)
          end)
        end
      )
    else
      {:error, unsupported_generation_error(session, :reconcile_generation)}
    end
  rescue
    error -> {:error, normalize_runtime_error(:reconcile_generation, error)}
  catch
    :exit, reason -> {:error, normalize_runtime_error(:reconcile_generation, reason)}
  end

  def reconcile_generation(_session, _request, _opts), do: {:error, invalid_session_error()}

  @doc "Drops a candidate or retired generation relation idempotently."
  @spec discard_generation(Session.t(), GenerationDiscard.t(), keyword()) :: operation_result()
  def discard_generation(session, request, opts \\ [])

  def discard_generation(%Session{} = session, %GenerationDiscard{} = request, opts)
      when is_list(opts) do
    {_admission_opts, adapter_opts} = split_operation_opts(opts)

    if generation_adapter?(session.adapter) do
      session
      |> run_session_operation(
        :discard_generation,
        request,
        operation_runtime_opts(opts),
        fn session ->
          Admission.with_permit(session, :discard_generation, request, fn ->
            case session.adapter.discard_generation(session.conn, request, adapter_opts) do
              :ok -> {:ok, :discarded}
              {:error, %Error{} = error} -> {:error, error}
            end
          end)
        end
      )
    else
      {:error, unsupported_generation_error(session, :discard_generation)}
    end
  rescue
    error -> {:error, normalize_runtime_error(:discard_generation, error)}
  catch
    :exit, reason -> {:error, normalize_runtime_error(:discard_generation, reason)}
  end

  def discard_generation(_session, _request, _opts), do: {:error, invalid_session_error()}

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

  defp run_transaction(%Session{adapter: adapter} = session, fun, opts) do
    {admission_opts, adapter_opts} = split_operation_opts(opts)
    required_catalogs = effective_required_catalogs(session, admission_opts)

    if function_exported?(adapter, :transaction, 3) do
      session
      |> run_session_operation(:transaction, nil, operation_runtime_opts(opts), fn %Session{} =
                                                                                     session ->
        Admission.with_permit(session, :transaction, admission_opts, fn ->
          adapter.transaction(
            session.conn,
            fn tx_conn ->
              tx_session = %Session{session | conn: tx_conn, required_catalogs: required_catalogs}

              tx_session
              |> put_checkout_owner(self())
              |> with_transaction_context(fun)
            end,
            adapter_opts
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

  defp split_operation_opts(opts) do
    {operation_admission_opts(opts), Keyword.drop(opts, [:admission, :deadline, :timeout_ms])}
  end

  defp operation_admission_opts(opts) do
    case Keyword.get(opts, :admission, []) do
      admission_opts when is_list(admission_opts) ->
        if Keyword.keyword?(admission_opts), do: admission_opts, else: []

      _other ->
        []
    end
  end

  defp operation_runtime_opts(opts) do
    Keyword.take(opts, [:deadline, :timeout_ms, :pool_safe?, :read_only?])
  end

  defp effective_required_catalogs(%Session{required_catalogs: required_catalogs}, admission_opts) do
    case explicit_required_catalogs(admission_opts) do
      [] -> required_catalogs
      catalogs -> catalogs
    end
  end

  defp explicit_required_catalogs(admission_opts) when is_list(admission_opts) do
    cond do
      Keyword.has_key?(admission_opts, :catalog) ->
        admission_opts |> Keyword.get(:catalog) |> List.wrap() |> normalize_catalogs()

      Keyword.has_key?(admission_opts, :target) ->
        admission_opts |> Keyword.get(:target) |> target_catalogs()

      Keyword.has_key?(admission_opts, :required_catalogs) ->
        admission_opts |> Keyword.get(:required_catalogs) |> List.wrap() |> normalize_catalogs()

      true ->
        []
    end
  end

  defp target_catalogs({:catalog, catalog}), do: normalize_catalogs(List.wrap(catalog))
  defp target_catalogs(%{catalog: catalog}), do: normalize_catalogs(List.wrap(catalog))
  defp target_catalogs(_target), do: []

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
    with {:ok, fingerprint, preparation} <-
           prepare_pool(resolved.adapter, resolved, adapter_opts) do
      key = pool_key(resolved, adapter_opts, fingerprint)

      adapter_opts =
        adapter_opts
        |> Keyword.put(:favn_pool_preparation, preparation)
        |> Keyword.put(:favn_pool_fingerprint, fingerprint)

      case checkout_or_create_session(key, resolved, concurrency_policies, adapter_opts) do
        {:ok, %Session{} = session} ->
          {:ok, session}

        :create ->
          create_pooled_session(resolved, concurrency_policies, adapter_opts, pool_config, key)

        {:error, %Error{}} = error ->
          error
      end
    end
  end

  defp checkout_or_create_session(key, resolved, concurrency_policies, adapter_opts) do
    max_creating_per_key = checkout_max_creating_per_key(concurrency_policies, adapter_opts)

    case SessionPool.checkout_or_create(
           key,
           max_creating_per_key: max_creating_per_key,
           checkout_timeout_ms: Keyword.get(adapter_opts, :checkout_timeout_ms),
           connection: resolved.name,
           required_catalogs: normalized_required_catalogs(adapter_opts)
         ) do
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
        session =
          put_session_runtime(session, resolved, concurrency_policies, adapter_opts, lease)

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

  defp checkout_max_creating_per_key(%ConcurrencyPolicies{} = policies, adapter_opts) do
    policies
    |> checkout_creation_policies(adapter_opts)
    |> strictest_finite_limit()
  end

  defp checkout_max_creating_per_key(%ConcurrencyPolicy{limit: limit}, _adapter_opts) do
    finite_limit_or_one(limit)
  end

  defp checkout_max_creating_per_key(_policy, _adapter_opts), do: 1

  defp checkout_creation_policies(%ConcurrencyPolicies{} = policies, adapter_opts) do
    case normalized_required_catalogs(adapter_opts) do
      [] ->
        case Map.values(policies.catalog) do
          [] -> List.wrap(policies.default)
          catalog_policies -> catalog_policies
        end

      catalogs ->
        catalog_policies =
          catalogs
          |> Enum.map(&ConcurrencyPolicies.catalog_policy(policies, &1))
          |> Enum.reject(&is_nil/1)

        case catalog_policies do
          [] -> List.wrap(policies.default)
          policies -> policies
        end
    end
  end

  defp strictest_finite_limit(policies) do
    policies
    |> Enum.map(& &1.limit)
    |> Enum.filter(&finite_limit?/1)
    |> case do
      [] -> 1
      limits -> Enum.min(limits)
    end
  end

  defp finite_limit_or_one(limit) when is_integer(limit) and limit > 0, do: limit
  defp finite_limit_or_one(_limit), do: 1

  defp finite_limit?(limit), do: is_integer(limit) and limit > 0

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

  defp normalized_required_resources(adapter_opts) do
    adapter_opts
    |> Keyword.get(:required_resources, [])
    |> List.wrap()
    |> normalize_names()
  end

  defp normalize_catalogs(catalogs) when is_list(catalogs) do
    catalogs
    |> Enum.map(&to_string/1)
    |> Enum.reject(&(&1 == ""))
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp normalize_names(names) when is_list(names) do
    names
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

  defp pool_key(%Resolved{} = resolved, adapter_opts, adapter_fingerprint) do
    required_catalogs = Keyword.get(adapter_opts, :required_catalogs, []) |> List.wrap()
    required_resources = Keyword.get(adapter_opts, :required_resources, []) |> List.wrap()

    PoolKey.build(
      resolved,
      adapter_opts,
      required_catalogs,
      required_resources,
      adapter_fingerprint
    )
  end

  defp prepare_pool(adapter, resolved, adapter_opts) do
    if function_exported?(adapter, :prepare_pool, 2) do
      case adapter.prepare_pool(resolved, adapter_opts) do
        {:ok, fingerprint, preparation} ->
          {:ok, fingerprint, preparation}

        {:error, %Error{}} = error ->
          error

        _other ->
          {:error, invalid_pool_preparation_error(resolved)}
      end
    else
      {:ok, adapter, nil}
    end
  rescue
    _error -> {:error, invalid_pool_preparation_error(resolved)}
  catch
    _kind, _reason -> {:error, invalid_pool_preparation_error(resolved)}
  end

  defp invalid_pool_preparation_error(%Resolved{} = resolved) do
    %Error{
      type: :execution_error,
      message: "SQL adapter could not prepare a pool identity",
      adapter: resolved.adapter,
      connection: resolved.name,
      operation: :connect,
      retryable?: false,
      details: %{reason: :invalid_pool_preparation}
    }
  end

  defp put_session_runtime(
         %Session{} = session,
         resolved,
         concurrency_policies,
         adapter_opts,
         lease
       ) do
    %Session{
      session
      | resolved: resolved,
        concurrency_policy: singular_policy(concurrency_policies),
        concurrency_policies: policy_container(concurrency_policies),
        required_catalogs: normalized_required_catalogs(adapter_opts),
        required_resources: normalized_required_resources(adapter_opts),
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
           required_resources: normalized_required_resources(adapter_opts),
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
       when is_function(fun, 1) do
    case checkout_owner_error(session, operation) do
      nil ->
        run_owned_session_operation(session, operation, payload, opts, fun)

      %Error{} = error ->
        maybe_mark_pooled_session_discard(session, operation, payload, error)
        {:error, error}
    end
  end

  defp run_owned_session_operation(%Session{} = session, operation, payload, opts, fun)
       when is_function(fun, 1) do
    deadline = Deadline.from_opts(opts, default_operation_timeout_ms())

    result =
      if transaction_context?(session) do
        run_inline_in_transaction(session, operation, deadline, fun)
      else
        run_with_deadline(session, operation, deadline, fun)
      end

    case result do
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

  defp with_transaction_context(%Session{} = session, fun) when is_function(fun, 1) do
    previous = Process.get(@transaction_context_key)
    Process.put(@transaction_context_key, {session.adapter, session.conn})

    try do
      fun.(session)
    after
      if is_nil(previous) do
        Process.delete(@transaction_context_key)
      else
        Process.put(@transaction_context_key, previous)
      end
    end
  end

  defp transaction_context?(%Session{} = session) do
    Process.get(@transaction_context_key) == {session.adapter, session.conn}
  end

  # A transaction already runs inside the deadline worker. Its child operations
  # stay in that process to preserve connection ownership. A shorter child
  # deadline terminates the owning transaction worker so no adapter call can
  # continue after the caller observes a timeout.
  defp run_inline_in_transaction(
         %Session{} = session,
         operation,
         %Deadline{} = deadline,
         fun
       ) do
    if Deadline.expired?(deadline) do
      operation_timeout(session, operation, deadline)
    else
      watchdog = start_nested_deadline_watchdog(session, operation, deadline)

      Observability.emit(
        [:operation, :start],
        %{},
        operation_metadata(session, operation, deadline)
      )

      result =
        try do
          normalize_operation_result(operation, fun.(session))
        rescue
          error -> {:error, normalize_runtime_error(operation, error)}
        catch
          :exit, reason -> {:error, normalize_runtime_error(operation, reason)}
          kind, reason -> {:error, normalize_runtime_error(operation, {kind, reason})}
        after
          stop_nested_deadline_watchdog(watchdog)
        end

      case result do
        {:ok, value} ->
          Observability.emit(
            [:operation, :stop],
            %{duration_ms: elapsed_ms(deadline)},
            operation_metadata(session, operation, deadline)
          )

          value

        {:error, %Error{}} = error ->
          Observability.emit(
            [:operation, :exception],
            %{duration_ms: elapsed_ms(deadline)},
            operation_metadata(session, operation, deadline)
          )

          error
      end
    end
  end

  defp start_nested_deadline_watchdog(%Session{} = session, operation, %Deadline{} = deadline) do
    owner = self()
    ref = make_ref()

    {watchdog, monitor} =
      spawn_monitor(fn ->
        owner_monitor = Process.monitor(owner)

        receive do
          {:cancel, ^ref} ->
            Process.demonitor(owner_monitor, [:flush])

          {:DOWN, ^owner_monitor, :process, ^owner, _reason} ->
            :ok
        after
          Deadline.remaining_ms(deadline) ->
            {:error, error} = operation_timeout(session, operation, deadline)

            Observability.emit(
              [:operation, :timeout],
              %{duration_ms: elapsed_ms(deadline)},
              operation_metadata(session, operation, deadline)
            )

            Process.exit(owner, {@nested_timeout_exit_tag, error})

            receive do
              {:DOWN, ^owner_monitor, :process, ^owner, _reason} -> :ok
            end
        end
      end)

    {watchdog, monitor, ref}
  end

  defp stop_nested_deadline_watchdog({watchdog, monitor, ref}) do
    send(watchdog, {:cancel, ref})

    receive do
      {:DOWN, ^monitor, :process, ^watchdog, _reason} -> :ok
    end
  end

  defp run_with_deadline(%Session{} = session, operation, %Deadline{} = deadline, fun) do
    if Deadline.expired?(deadline) do
      operation_timeout(session, operation, deadline)
    else
      parent = self()
      ref = make_ref()
      admission_permits = Process.get({Admission, :permits}, %{})

      {guard, monitor} =
        spawn_monitor(fn ->
          supervise_deadline_worker(
            parent,
            ref,
            session,
            operation,
            fun,
            admission_permits
          )
        end)

      Observability.emit(
        [:operation, :start],
        %{},
        operation_metadata(session, operation, deadline)
      )

      receive do
        {^ref, {:ok, result}} ->
          receive do
            {:DOWN, ^monitor, :process, ^guard, _reason} -> :ok
          after
            0 -> Process.demonitor(monitor, [:flush])
          end

          Observability.emit(
            [:operation, :stop],
            %{duration_ms: elapsed_ms(deadline)},
            operation_metadata(session, operation, deadline)
          )

          result

        {^ref, {:error, %Error{} = error}} ->
          receive do
            {:DOWN, ^monitor, :process, ^guard, _reason} -> :ok
          after
            0 -> Process.demonitor(monitor, [:flush])
          end

          Observability.emit(
            [:operation, :exception],
            %{duration_ms: elapsed_ms(deadline)},
            operation_metadata(session, operation, deadline)
          )

          {:error, error}

        {:DOWN, ^monitor, :process, ^guard, reason} ->
          error = normalize_runtime_error(operation, reason)

          Observability.emit(
            [:operation, :exception],
            %{duration_ms: elapsed_ms(deadline)},
            operation_metadata(session, operation, deadline)
          )

          {:error, error}
      after
        Deadline.remaining_ms(deadline) ->
          send(guard, {:cancel, ref})

          receive do
            {:DOWN, ^monitor, :process, ^guard, _reason} -> :ok
          end

          Observability.emit(
            [:operation, :timeout],
            %{duration_ms: elapsed_ms(deadline)},
            operation_metadata(session, operation, deadline)
          )

          operation_timeout(session, operation, deadline)
      end
    end
  end

  defp supervise_deadline_worker(
         parent,
         ref,
         %Session{} = session,
         operation,
         fun,
         admission_permits
       ) do
    parent_monitor = Process.monitor(parent)
    guard = self()
    worker_ref = make_ref()

    {worker, worker_monitor} =
      spawn_monitor(fn ->
        Process.put({Admission, :permits}, admission_permits)
        session = put_checkout_owner(session, self())

        result =
          try do
            normalize_operation_result(operation, fun.(session))
          rescue
            error -> {:error, normalize_runtime_error(operation, error)}
          catch
            :exit, reason -> {:error, normalize_runtime_error(operation, reason)}
            kind, reason -> {:error, normalize_runtime_error(operation, {kind, reason})}
          end

        send(guard, {worker_ref, result})
      end)

    receive do
      {^worker_ref, result} ->
        receive do
          {:DOWN, ^worker_monitor, :process, ^worker, _reason} -> :ok
        end

        send(parent, {ref, result})

      {:cancel, ^ref} ->
        stop_deadline_worker(worker, worker_monitor)

      {:DOWN, ^parent_monitor, :process, ^parent, _reason} ->
        stop_deadline_worker(worker, worker_monitor)

      {:DOWN, ^worker_monitor, :process, ^worker, reason} ->
        error =
          case reason do
            {@nested_timeout_exit_tag, %Error{} = error} -> error
            _other -> normalize_runtime_error(operation, reason)
          end

        send(parent, {ref, {:error, error}})
    end
  end

  defp stop_deadline_worker(worker, monitor) do
    Process.exit(worker, :kill)

    receive do
      {:DOWN, ^monitor, :process, ^worker, _reason} -> :ok
    end
  end

  defp operation_timeout(%Session{} = session, operation, %Deadline{} = deadline) do
    {:error,
     %Error{
       type: :operation_timeout,
       message: "SQL operation timed out",
       adapter: session.adapter,
       connection: connection_name(session),
       operation: operation,
       retryable?: nil,
       details: %{
         timeout_ms: deadline.timeout_ms,
         started_at: deadline.started_at,
         deadline_at: deadline.deadline_at,
         unknown_outcome?: true
       }
     }}
  end

  defp normalize_operation_result(_operation, {:error, %Error{} = error}), do: {:error, error}

  defp normalize_operation_result(operation, {:error, reason}),
    do: {:error, normalize_runtime_error(operation, reason)}

  defp normalize_operation_result(_operation, result), do: {:ok, result}

  defp put_checkout_owner(%Session{pool_checkout: %Checkout{} = checkout} = session, owner) do
    %Session{session | pool_checkout: %Checkout{checkout | owner: owner}}
  end

  defp put_checkout_owner(%Session{} = session, _owner), do: session

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
      Keyword.get(opts, :pool_safe?) == true ->
        false

      operation == :query ->
        Keyword.get(opts, :read_only?) != true

      true ->
        operation in [
          :execute,
          :materialize,
          :transaction,
          :initialize_generation_marker,
          :activate_generation,
          :discard_generation
        ]
    end
  end

  defp discard_pooled_session_after_success?(operation, _opts),
    do:
      operation in [
        :execute,
        :materialize,
        :transaction,
        :initialize_generation_marker,
        :activate_generation,
        :discard_generation
      ]

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
    if operation in [
         :relation,
         :columns,
         :row_count,
         :sample,
         :table_metadata,
         :inspect_generation,
         :reconcile_generation
       ] do
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

  defp discard_pooled_session?(_operation, _payload, %Error{type: :operation_timeout}), do: true

  defp discard_pooled_session?(operation, _payload, %Error{})
       when operation in [
              :execute,
              :materialize,
              :transaction,
              :initialize_generation_marker,
              :activate_generation,
              :discard_generation
            ] do
    true
  end

  defp discard_pooled_session?(_operation, _payload, %Error{} = error) do
    classification = Map.get(error.details || %{}, :classification)

    classification in [
      :unknown_commit_state,
      :unknown_outcome_timeout,
      :activation_outcome_unknown
    ]
  end

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

  defp unsupported_transactional_materialization_error(%Session{
         resolved: %Resolved{name: connection}
       }) do
    %Error{
      type: :unsupported_capability,
      message: "adapter cannot materialize inside an active transaction",
      connection: connection,
      operation: :materialize
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

  defp unsupported_generation_error(%Session{resolved: %Resolved{name: connection}}, operation) do
    %Error{
      type: :unsupported_capability,
      message: "SQL adapter does not support target generations",
      retryable?: false,
      operation: operation,
      connection: connection,
      details: %{classification: :unsupported_capability, capability: :target_generations}
    }
  end

  defp generation_adapter?(adapter) do
    function_exported?(adapter, :generation_capabilities, 2) and
      function_exported?(adapter, :inspect_generation, 3) and
      function_exported?(adapter, :initialize_generation_marker, 3) and
      function_exported?(adapter, :activate_generation, 3) and
      function_exported?(adapter, :reconcile_generation, 3) and
      function_exported?(adapter, :discard_generation, 3)
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

  defp operation_metadata(%Session{} = session, operation, %Deadline{} = deadline) do
    %{
      operation: operation,
      connection: connection_name(session),
      adapter: inspect(session.adapter),
      timeout_ms: deadline.timeout_ms
    }
  end

  defp connection_name(%Session{resolved: %Resolved{name: name}}), do: name
  defp connection_name(%Session{}), do: nil

  defp elapsed_ms(%Deadline{} = deadline),
    do: deadline.timeout_ms - Deadline.remaining_ms(deadline)

  defp default_operation_timeout_ms do
    case Application.get_env(:favn_sql_runtime, :sql_operation_timeout_ms, 30_000) do
      value when is_integer(value) and value > 0 -> value
      _other -> 30_000
    end
  end

  defp monotonic_ms, do: System.monotonic_time(:millisecond)
end
