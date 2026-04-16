defmodule FavnDuckdb.Worker do
  @moduledoc false

  use GenServer

  @type state :: %{
          client: module(),
          handles: %{reference() => term()}
        }

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) when is_list(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @impl true
  def init(opts) do
    client = Keyword.get(opts, :client, Favn.SQL.Adapter.DuckDB.Client.Duckdbex)
    {:ok, %{client: client, handles: %{}}}
  end

  @impl true
  def handle_call({:open, database}, _from, state) do
    case state.client.open(database) do
      {:ok, handle} ->
        {ref, next_state} = store_handle(state, handle)
        {:reply, {:ok, ref}, next_state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:connection, db_ref}, _from, state) do
    with {:ok, db_handle} <- fetch_handle(state, db_ref),
         {:ok, conn_handle} <- state.client.connection(db_handle) do
      {ref, next_state} = store_handle(state, conn_handle)
      {:reply, {:ok, ref}, next_state}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:query, conn_ref, sql, params}, _from, state) do
    with {:ok, conn_handle} <- fetch_handle(state, conn_ref),
         {:ok, result_handle} <- state.client.query(conn_handle, sql, params) do
      {ref, next_state} = store_handle(state, result_handle)
      {:reply, {:ok, ref}, next_state}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:fetch_all, result_ref}, _from, state) do
    case fetch_handle(state, result_ref) do
      {:ok, result_handle} -> {:reply, state.client.fetch_all(result_handle), state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:columns, result_ref}, _from, state) do
    case fetch_handle(state, result_ref) do
      {:ok, result_handle} -> {:reply, state.client.columns(result_handle), state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:begin_transaction, conn_ref}, _from, state) do
    reply =
      with {:ok, conn_handle} <- fetch_handle(state, conn_ref),
           do: state.client.begin_transaction(conn_handle)

    {:reply, normalize_ok_error(reply), state}
  end

  def handle_call({:commit, conn_ref}, _from, state) do
    reply =
      with {:ok, conn_handle} <- fetch_handle(state, conn_ref),
           do: state.client.commit(conn_handle)

    {:reply, normalize_ok_error(reply), state}
  end

  def handle_call({:rollback, conn_ref}, _from, state) do
    reply =
      with {:ok, conn_handle} <- fetch_handle(state, conn_ref),
           do: state.client.rollback(conn_handle)

    {:reply, normalize_ok_error(reply), state}
  end

  def handle_call({:appender, conn_ref, table_name, schema}, _from, state) do
    with {:ok, conn_handle} <- fetch_handle(state, conn_ref),
         {:ok, appender_handle} <- state.client.appender(conn_handle, table_name, schema) do
      {ref, next_state} = store_handle(state, appender_handle)
      {:reply, {:ok, ref}, next_state}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:appender_add_rows, appender_ref, rows}, _from, state) do
    reply =
      with {:ok, appender_handle} <- fetch_handle(state, appender_ref) do
        state.client.appender_add_rows(appender_handle, rows)
      end

    {:reply, normalize_ok_error(reply), state}
  end

  def handle_call({:appender_flush, appender_ref}, _from, state) do
    reply =
      with {:ok, appender_handle} <- fetch_handle(state, appender_ref),
           do: state.client.appender_flush(appender_handle)

    {:reply, normalize_ok_error(reply), state}
  end

  def handle_call({:appender_close, appender_ref}, _from, state) do
    reply =
      with {:ok, appender_handle} <- fetch_handle(state, appender_ref) do
        state.client.appender_close(appender_handle)
      end

    next_state = drop_handle(state, appender_ref)
    {:reply, normalize_ok_error(reply), next_state}
  end

  def handle_call({:release, ref}, _from, state) do
    case Map.fetch(state.handles, ref) do
      {:ok, handle} ->
        reply = state.client.release(handle)
        {:reply, normalize_ok_error(reply), drop_handle(state, ref)}

      :error ->
        {:reply, :ok, state}
    end
  end

  defp store_handle(state, handle) do
    ref = make_ref()
    {ref, %{state | handles: Map.put(state.handles, ref, handle)}}
  end

  defp fetch_handle(state, ref) when is_reference(ref) do
    case Map.fetch(state.handles, ref) do
      {:ok, handle} -> {:ok, handle}
      :error -> {:error, :invalid_handle}
    end
  end

  defp fetch_handle(_state, _ref), do: {:error, :invalid_handle}

  defp drop_handle(state, ref), do: %{state | handles: Map.delete(state.handles, ref)}

  defp normalize_ok_error({:error, _reason} = error), do: error
  defp normalize_ok_error(:ok), do: :ok
  defp normalize_ok_error(other), do: other
end
