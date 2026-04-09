defmodule Favn.SQL.Adapter.DuckDB.Client do
  @moduledoc false

  @spec open(nil | binary()) :: {:ok, reference()} | {:error, term()}
  def open(nil), do: Duckdbex.open()
  def open(":memory:"), do: Duckdbex.open()
  def open(path) when is_binary(path), do: Duckdbex.open(path)
  def open(_), do: {:error, :invalid_database}

  @spec connection(reference()) :: {:ok, reference()} | {:error, term()}
  def connection(db_ref), do: Duckdbex.connection(db_ref)

  @spec query(reference(), binary(), list()) :: {:ok, reference()} | {:error, term()}
  def query(conn_ref, sql, []), do: Duckdbex.query(conn_ref, sql)
  def query(conn_ref, sql, params), do: Duckdbex.query(conn_ref, sql, params)

  @spec fetch_all(reference()) :: term()
  def fetch_all(result_ref), do: Duckdbex.fetch_all(result_ref)

  @spec columns(reference()) :: term()
  def columns(result_ref), do: Duckdbex.columns(result_ref)

  @spec begin_transaction(reference()) :: :ok | {:error, term()}
  def begin_transaction(conn_ref), do: Duckdbex.begin_transaction(conn_ref)

  @spec commit(reference()) :: :ok | {:error, term()}
  def commit(conn_ref), do: Duckdbex.commit(conn_ref)

  @spec rollback(reference()) :: :ok | {:error, term()}
  def rollback(conn_ref), do: Duckdbex.rollback(conn_ref)

  @spec appender(reference(), binary(), binary() | nil) :: {:ok, reference()} | {:error, term()}
  def appender(conn_ref, table_name, nil), do: Duckdbex.appender(conn_ref, table_name)
  def appender(conn_ref, table_name, schema), do: Duckdbex.appender(conn_ref, schema, table_name)

  @spec appender_add_rows(reference(), list()) :: :ok | {:error, term()}
  def appender_add_rows(appender_ref, rows), do: Duckdbex.appender_add_rows(appender_ref, rows)

  @spec appender_flush(reference()) :: :ok | {:error, term()}
  def appender_flush(appender_ref), do: Duckdbex.appender_flush(appender_ref)

  @spec appender_close(reference()) :: :ok | {:error, term()}
  def appender_close(appender_ref), do: Duckdbex.appender_close(appender_ref)

  @spec release(reference()) :: :ok | {:error, term()}
  def release(resource), do: Duckdbex.release(resource)
end
