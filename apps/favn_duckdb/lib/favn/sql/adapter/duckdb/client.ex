defmodule Favn.SQL.Adapter.DuckDB.Client do
  @moduledoc false

  @type handle :: reference()

  @callback open(nil | binary()) :: {:ok, handle()} | {:error, term()}
  @callback connection(handle()) :: {:ok, handle()} | {:error, term()}
  @callback query(handle(), binary(), list()) :: {:ok, handle()} | {:error, term()}
  @callback fetch_all(handle()) :: term()
  @callback columns(handle()) :: term()
  @callback begin_transaction(handle()) :: :ok | {:error, term()}
  @callback commit(handle()) :: :ok | {:error, term()}
  @callback rollback(handle()) :: :ok | {:error, term()}
  @callback appender(handle(), binary(), binary() | nil) :: {:ok, handle()} | {:error, term()}
  @callback appender_add_rows(handle(), list()) :: :ok | {:error, term()}
  @callback appender_flush(handle()) :: :ok | {:error, term()}
  @callback appender_close(handle()) :: :ok | {:error, term()}
  @callback release(handle()) :: :ok | {:error, term()}

  @spec default() :: module()
  def default, do: FavnDuckdb.Runtime.client_module()
end
