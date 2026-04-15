defmodule Favn.SQL.RuntimeBridge do
  @moduledoc false

  alias Favn.Connection.Registry
  alias Favn.Connection.Resolved
  alias Favn.RelationRef
  alias Favn.SQL.Error
  alias Favn.SQL.Session
  alias Favn.SQL.WritePlan

  @spec connect(atom(), keyword()) :: {:ok, Session.t()} | {:error, term()}
  def connect(connection, opts \\ [])

  def connect(connection, opts) when is_atom(connection) and is_list(opts) do
    with :ok <- ensure_runtime_started(),
         {:ok, %Resolved{} = resolved} <- fetch_connection(connection),
         {:ok, conn} <- resolved.adapter.connect(resolved, opts),
         {:ok, capabilities} <- resolved.adapter.capabilities(resolved, opts) do
      {:ok,
       %Session{
         adapter: resolved.adapter,
         resolved: resolved,
         conn: conn,
         capabilities: capabilities
       }}
    end
  end

  def connect(connection, _opts), do: {:error, invalid_connection_error(connection)}

  @spec disconnect(Session.t()) :: :ok
  def disconnect(%Session{adapter: adapter, conn: conn}) do
    _ = adapter.disconnect(conn, [])
    :ok
  rescue
    _error -> :ok
  end

  def disconnect(_session), do: :ok

  @spec query(Session.t(), iodata(), keyword()) :: {:ok, term()} | {:error, term()}
  def query(%Session{adapter: adapter, conn: conn}, statement, opts) when is_list(opts) do
    with :ok <- ensure_runtime_started() do
      adapter.query(conn, statement, opts)
    end
  rescue
    error -> {:error, normalize_runtime_error(:query, error)}
  catch
    :exit, _ -> {:error, runtime_not_started_error()}
  end

  def query(_session, _statement, _opts) do
    if runtime_started?(),
      do: {:error, invalid_session_error()},
      else: {:error, runtime_not_started_error()}
  end

  @spec materialize(Session.t(), WritePlan.t(), keyword()) :: {:ok, term()} | {:error, term()}
  def materialize(%Session{adapter: adapter, conn: conn}, %WritePlan{} = write_plan, opts)
      when is_list(opts) do
    with :ok <- ensure_runtime_started() do
      adapter.materialize(conn, write_plan, opts)
    end
  rescue
    error -> {:error, normalize_runtime_error(:materialize, error)}
  catch
    :exit, _ -> {:error, runtime_not_started_error()}
  end

  def materialize(_session, _write_plan, _opts) do
    if runtime_started?(),
      do: {:error, invalid_session_error()},
      else: {:error, runtime_not_started_error()}
  end

  @spec get_relation(Session.t(), RelationRef.t()) :: {:ok, term()} | {:error, term()}
  def get_relation(%Session{adapter: adapter, conn: conn}, %RelationRef{} = relation_ref) do
    with :ok <- ensure_runtime_started() do
      adapter.relation(conn, relation_ref, [])
    end
  rescue
    error -> {:error, normalize_runtime_error(:get_relation, error)}
  catch
    :exit, _ -> {:error, runtime_not_started_error()}
  end

  def get_relation(_session, _relation_ref) do
    if runtime_started?(),
      do: {:error, invalid_session_error()},
      else: {:error, runtime_not_started_error()}
  end

  @spec columns(Session.t(), RelationRef.t()) :: {:ok, [term()]} | {:error, term()}
  def columns(%Session{adapter: adapter, conn: conn}, %RelationRef{} = relation_ref) do
    with :ok <- ensure_runtime_started() do
      adapter.columns(conn, relation_ref, [])
    end
  rescue
    error -> {:error, normalize_runtime_error(:columns, error)}
  catch
    :exit, _ -> {:error, runtime_not_started_error()}
  end

  def columns(_session, _relation_ref) do
    if runtime_started?(),
      do: {:error, invalid_session_error()},
      else: {:error, runtime_not_started_error()}
  end

  defp fetch_connection(connection) do
    case Registry.fetch(connection, registry_name: FavnRunner.ConnectionRegistry) do
      {:ok, %Resolved{} = resolved} -> {:ok, resolved}
      :error -> {:error, invalid_connection_error(connection)}
    end
  catch
    :exit, _ -> {:error, runtime_not_started_error()}
  end

  defp ensure_runtime_started,
    do: if(runtime_started?(), do: :ok, else: {:error, runtime_not_started_error()})

  defp runtime_started?, do: Process.whereis(FavnRunner.ConnectionRegistry) != nil

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

  defp runtime_not_started_error, do: :runtime_not_available

  defp normalize_runtime_error(operation, reason) do
    %Error{
      type: :execution_error,
      message: "SQL runtime bridge operation failed",
      operation: operation,
      details: %{reason: inspect(reason)},
      cause: reason
    }
  end
end
