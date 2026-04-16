defmodule FavnDuckdb.Runtime do
  @moduledoc false

  @type execution_mode :: :in_process | :separate_process

  @spec client_module() :: module()
  def client_module do
    case execution_mode() do
      :in_process -> FavnDuckdb.Runtime.InProcess
      :separate_process -> FavnDuckdb.Runtime.SeparateProcess
    end
  end

  @spec execution_mode() :: execution_mode()
  def execution_mode do
    execution_mode(plugin_opts())
  end

  @spec execution_mode(keyword()) :: execution_mode()
  def execution_mode(opts) when is_list(opts) do
    case Keyword.get(opts, :execution_mode, :in_process) do
      :in_process -> :in_process
      :separate_process -> :separate_process
      _other -> :in_process
    end
  end

  @spec worker_name() :: atom()
  def worker_name do
    worker_name(plugin_opts())
  end

  @spec worker_name(keyword()) :: atom()
  def worker_name(opts) when is_list(opts) do
    case Keyword.get(opts, :worker_name, FavnDuckdb.Worker) do
      name when is_atom(name) -> name
      _other -> FavnDuckdb.Worker
    end
  end

  @spec plugin_opts() :: keyword()
  def plugin_opts do
    Application.get_env(:favn, :runner_plugins, [])
    |> Enum.find_value([], fn
      {FavnDuckdb, opts} when is_list(opts) -> opts
      FavnDuckdb -> []
      _other -> nil
    end)
  end
end
