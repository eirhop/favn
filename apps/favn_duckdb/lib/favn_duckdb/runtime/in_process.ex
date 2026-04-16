defmodule FavnDuckdb.Runtime.InProcess do
  @moduledoc false

  @behaviour Favn.SQL.Adapter.DuckDB.Client

  alias Favn.SQL.Adapter.DuckDB.Client.Duckdbex, as: DuckdbexClient

  defp client_module do
    Application.get_env(:favn, :duckdb_in_process_client, DuckdbexClient)
  end

  @impl true
  def open(database), do: client_module().open(database)

  @impl true
  def connection(db_ref), do: client_module().connection(db_ref)

  @impl true
  def query(conn_ref, sql, params), do: client_module().query(conn_ref, sql, params)

  @impl true
  def fetch_all(result_ref), do: client_module().fetch_all(result_ref)

  @impl true
  def columns(result_ref), do: client_module().columns(result_ref)

  @impl true
  def begin_transaction(conn_ref), do: client_module().begin_transaction(conn_ref)

  @impl true
  def commit(conn_ref), do: client_module().commit(conn_ref)

  @impl true
  def rollback(conn_ref), do: client_module().rollback(conn_ref)

  @impl true
  def appender(conn_ref, table_name, schema),
    do: client_module().appender(conn_ref, table_name, schema)

  @impl true
  def appender_add_rows(appender_ref, rows),
    do: client_module().appender_add_rows(appender_ref, rows)

  @impl true
  def appender_flush(appender_ref), do: client_module().appender_flush(appender_ref)

  @impl true
  def appender_close(appender_ref), do: client_module().appender_close(appender_ref)

  @impl true
  def release(resource), do: client_module().release(resource)
end
