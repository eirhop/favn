defmodule Favn.SQL.Client do
  @moduledoc """
  Shared SQL runtime client for named Favn connections.
  """

  alias Favn.Connection.Loader
  alias Favn.Connection.Registry
  alias Favn.Connection.Resolved
  alias Favn.RelationRef
  alias Favn.SQL.Admission
  alias Favn.SQL.ConcurrencyPolicy
  alias Favn.SQL.Error
  alias Favn.SQL.Session
  alias Favn.SQL.WritePlan

  @resolution_opt_keys [:registry_name]

  @type operation_result :: {:ok, term()} | {:error, term()}

  @spec connect(atom(), keyword()) :: {:ok, Session.t()} | {:error, term()}
  def connect(connection, opts \\ [])

  def connect(connection, opts) when is_atom(connection) and is_list(opts) do
    {resolution_opts, adapter_opts} = split_connect_opts(opts)

    with {:ok, %Resolved{} = resolved} <- fetch_connection(connection, resolution_opts),
         {:ok, concurrency_policy} <- ConcurrencyPolicy.resolve(resolved) do
      connect_with_admission(resolved, concurrency_policy, adapter_opts)
    end
  rescue
    error -> {:error, normalize_runtime_error(:connect, error)}
  catch
    :exit, reason -> {:error, normalize_runtime_error(:connect, reason)}
  end

  def connect(connection, _opts), do: {:error, invalid_connection_error(connection)}

  @spec disconnect(Session.t()) :: :ok
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

  @spec capabilities(Session.t()) :: {:ok, Favn.SQL.Capabilities.t()} | {:error, term()}
  def capabilities(%Session{capabilities: capabilities}), do: {:ok, capabilities}
  def capabilities(_session), do: {:error, invalid_session_error()}

  @spec query(Session.t(), iodata(), keyword()) :: operation_result()
  def query(%Session{} = session, statement, opts) when is_list(opts) do
    Admission.with_permit(session, :query, statement, fn ->
      session.adapter.query(session.conn, statement, opts)
    end)
  rescue
    error -> {:error, normalize_runtime_error(:query, error)}
  catch
    :exit, reason -> {:error, normalize_runtime_error(:query, reason)}
  end

  def query(_session, _statement, _opts), do: {:error, invalid_session_error()}

  @spec execute(Session.t(), iodata(), keyword()) :: operation_result()
  def execute(%Session{} = session, statement, opts) when is_list(opts) do
    Admission.with_permit(session, :execute, statement, fn ->
      session.adapter.execute(session.conn, statement, opts)
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
    Admission.with_permit(session, :materialize, write_plan, fn ->
      session.adapter.materialize(session.conn, write_plan, opts)
    end)
  rescue
    error -> {:error, normalize_runtime_error(:materialize, error)}
  catch
    :exit, reason -> {:error, normalize_runtime_error(:materialize, reason)}
  end

  def materialize(_session, _write_plan, _opts), do: {:error, invalid_session_error()}

  @spec relation(Session.t(), RelationRef.t()) :: operation_result()
  def relation(%Session{} = session, %RelationRef{} = relation_ref) do
    Admission.with_permit(session, :relation, relation_ref, fn ->
      session.adapter.relation(session.conn, relation_ref, [])
    end)
  rescue
    error -> {:error, normalize_runtime_error(:relation, error)}
  catch
    :exit, reason -> {:error, normalize_runtime_error(:relation, reason)}
  end

  def relation(_session, _relation_ref), do: {:error, invalid_session_error()}

  @spec columns(Session.t(), RelationRef.t()) :: operation_result()
  def columns(%Session{} = session, %RelationRef{} = relation_ref) do
    Admission.with_permit(session, :columns, relation_ref, fn ->
      session.adapter.columns(session.conn, relation_ref, [])
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
      Admission.with_permit(session, :row_count, relation_ref, fn ->
        session.adapter.row_count(session.conn, relation_ref, [])
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
        Admission.with_permit(session, :sample, relation_ref, fn ->
          session.adapter.sample(session.conn, relation_ref, limit: limit)
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
      Admission.with_permit(session, :table_metadata, relation_ref, fn ->
        session.adapter.table_metadata(session.conn, relation_ref, [])
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
      Admission.with_permit(session, :transaction, nil, fn ->
        adapter.transaction(conn, fn tx_conn -> fun.(%Session{session | conn: tx_conn}) end, opts)
      end)
    else
      {:error, unsupported_transaction_error(session)}
    end
  end

  defp split_connect_opts(opts) do
    Keyword.split(opts, @resolution_opt_keys)
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

  defp connect_with_admission(%Resolved{} = resolved, concurrency_policy, adapter_opts) do
    lease = Admission.acquire_session(concurrency_policy)

    try do
      connect_and_build_session(resolved, adapter_opts, concurrency_policy, lease)
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

  defp connect_and_build_session(%Resolved{} = resolved, adapter_opts, concurrency_policy, lease) do
    case resolved.adapter.connect(resolved, adapter_opts) do
      {:ok, conn} ->
        bootstrap_and_build_session(resolved, adapter_opts, concurrency_policy, lease, conn)

      {:error, _reason} = error ->
        release_lease_and_return(error, lease)

      other ->
        release_lease_and_return(other, lease)
    end
  end

  defp bootstrap_and_build_session(resolved, adapter_opts, concurrency_policy, lease, conn) do
    case bootstrap_connection_with_cleanup(resolved, conn, adapter_opts) do
      :ok ->
        build_session_with_capabilities(resolved, adapter_opts, concurrency_policy, lease, conn)

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

  defp build_session_with_capabilities(resolved, adapter_opts, concurrency_policy, lease, conn) do
    case resolved.adapter.capabilities(resolved, adapter_opts) do
      {:ok, capabilities} ->
        {:ok,
         %Session{
           adapter: resolved.adapter,
           resolved: resolved,
           conn: conn,
           capabilities: capabilities,
           concurrency_policy: concurrency_policy,
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

  defp disconnect_after_connect_error(adapter, conn, lease, error) do
    _ = adapter.disconnect(conn, [])
    release_lease_and_return(error, lease)
  end

  defp release_lease_and_return(error, lease) do
    Admission.release_session(lease)
    error
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
end
