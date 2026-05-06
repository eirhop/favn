defmodule Favn.SQL.Adapter.DuckDB.ADBC.Client do
  @moduledoc false

  @type handle :: term()

  @callback open(nil | binary(), keyword()) :: {:ok, handle()} | {:error, term()}
  @callback connection(handle()) :: {:ok, handle()} | {:error, term()}
  @callback query(handle(), binary(), list()) :: {:ok, handle()} | {:error, term()}
  @callback execute(handle(), binary(), list()) :: {:ok, non_neg_integer() | nil} | {:error, term()}
  @callback fetch_all(handle(), pos_integer()) :: term()
  @callback columns(handle()) :: term()
  @callback bulk_insert(handle(), [map()], keyword()) :: {:ok, non_neg_integer()} | {:error, term()}
  @callback begin_transaction(handle()) :: :ok | {:error, term()}
  @callback commit(handle()) :: :ok | {:error, term()}
  @callback rollback(handle()) :: :ok | {:error, term()}
  @callback release(handle()) :: :ok | {:error, term()}

  @spec default() :: module()
  def default, do: FavnDuckdbADBC.Runtime.client_module()
end
