defmodule FavnDuckdbADBC.Runtime do
  @moduledoc false

  @type execution_mode :: :in_process

  @spec client_module() :: module()
  def client_module, do: Favn.SQL.Adapter.DuckDB.ADBC.Client.ADBC

  @spec execution_mode() :: execution_mode()
  def execution_mode, do: execution_mode(plugin_opts())

  @spec execution_mode(keyword()) :: execution_mode()
  def execution_mode(opts) when is_list(opts) do
    case Keyword.get(opts, :execution_mode, :in_process) do
      :in_process -> :in_process
      _other -> :in_process
    end
  end

  @spec default_row_limit() :: pos_integer()
  def default_row_limit do
    case Keyword.get(plugin_opts(), :default_row_limit, 10_000) do
      limit when is_integer(limit) and limit > 0 -> limit
      _other -> 10_000
    end
  end

  @spec default_result_byte_limit() :: pos_integer()
  def default_result_byte_limit do
    case Keyword.get(plugin_opts(), :default_result_byte_limit, 20_000_000) do
      limit when is_integer(limit) and limit > 0 -> limit
      _other -> 20_000_000
    end
  end

  @spec driver_opts() :: keyword()
  def driver_opts do
    Application.get_env(:favn, :duckdb_adbc, [])
    |> case do
      opts when is_list(opts) -> opts
      _other -> []
    end
  end

  @spec plugin_opts() :: keyword()
  def plugin_opts do
    Application.get_env(:favn, :runner_plugins, [])
    |> Enum.find_value([], fn
      {FavnDuckdbADBC, opts} when is_list(opts) -> opts
      FavnDuckdbADBC -> []
      _other -> nil
    end)
  end
end
