defmodule FavnDuckdb.Runtime.SeparateProcess do
  @moduledoc false

  @behaviour Favn.SQL.Adapter.DuckDB.Client

  defp worker_name, do: FavnDuckdb.Runtime.worker_name()

  @impl true
  def open(database), do: call({:open, database})

  @impl true
  def connection(db_ref), do: call({:connection, db_ref})

  @impl true
  def query(conn_ref, sql, params), do: call({:query, conn_ref, sql, params})

  @impl true
  def fetch_all(result_ref), do: call({:fetch_all, result_ref})

  @impl true
  def columns(result_ref), do: call({:columns, result_ref})

  @impl true
  def begin_transaction(conn_ref), do: call({:begin_transaction, conn_ref})

  @impl true
  def commit(conn_ref), do: call({:commit, conn_ref})

  @impl true
  def rollback(conn_ref), do: call({:rollback, conn_ref})

  @impl true
  def appender(conn_ref, table_name, schema), do: call({:appender, conn_ref, table_name, schema})

  @impl true
  def appender_add_rows(appender_ref, rows), do: call({:appender_add_rows, appender_ref, rows})

  @impl true
  def appender_flush(appender_ref), do: call({:appender_flush, appender_ref})

  @impl true
  def appender_close(appender_ref), do: call({:appender_close, appender_ref})

  @impl true
  def release(resource), do: call({:release, resource})

  defp call(message) do
    GenServer.call(worker_name(), message, FavnDuckdb.Runtime.worker_call_timeout())
  catch
    :exit, {:timeout, _detail} -> {:error, :worker_call_timeout}
    :exit, _ -> {:error, :worker_not_available}
  end
end
