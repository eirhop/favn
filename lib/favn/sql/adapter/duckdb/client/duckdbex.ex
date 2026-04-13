defmodule Favn.SQL.Adapter.DuckDB.Client.Duckdbex do
  @moduledoc false

  @behaviour Favn.SQL.Adapter.DuckDB.Client

  @impl true
  def open(nil), do: Duckdbex.open()

  @impl true
  def open(":memory:"), do: Duckdbex.open()

  @impl true
  def open(path) when is_binary(path), do: Duckdbex.open(path)

  @impl true
  def open(_), do: {:error, :invalid_database}

  @impl true
  def connection(db_ref), do: Duckdbex.connection(db_ref)

  @impl true
  def query(conn_ref, sql, []), do: Duckdbex.query(conn_ref, sql)

  @impl true
  def query(conn_ref, sql, params), do: Duckdbex.query(conn_ref, sql, params)

  @impl true
  def fetch_all(result_ref), do: Duckdbex.fetch_all(result_ref)

  @impl true
  def columns(result_ref), do: Duckdbex.columns(result_ref)

  @impl true
  def begin_transaction(conn_ref), do: Duckdbex.begin_transaction(conn_ref)

  @impl true
  def commit(conn_ref), do: Duckdbex.commit(conn_ref)

  @impl true
  def rollback(conn_ref), do: Duckdbex.rollback(conn_ref)

  @impl true
  def appender(conn_ref, table_name, nil), do: Duckdbex.appender(conn_ref, table_name)

  @impl true
  def appender(conn_ref, table_name, schema), do: Duckdbex.appender(conn_ref, schema, table_name)

  @impl true
  def appender_add_rows(appender_ref, rows), do: Duckdbex.appender_add_rows(appender_ref, rows)

  @impl true
  def appender_flush(appender_ref), do: Duckdbex.appender_flush(appender_ref)

  @impl true
  def appender_close(appender_ref), do: Duckdbex.appender_close(appender_ref)

  @impl true
  def release(resource), do: Duckdbex.release(resource)
end
