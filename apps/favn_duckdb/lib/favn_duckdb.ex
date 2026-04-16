defmodule FavnDuckdb do
  @moduledoc """
  DuckDB execution plugin for `favn_runner`.
  """

  @behaviour FavnRunner.Plugin

  @impl true
  def child_specs(opts) when is_list(opts) do
    case FavnDuckdb.Runtime.execution_mode(opts) do
      :in_process ->
        []

      :separate_process ->
        [
          {FavnDuckdb.Worker,
           [
             name: FavnDuckdb.Runtime.worker_name(opts),
             client: Favn.SQL.Adapter.DuckDB.Client.Duckdbex
           ]}
        ]
    end
  end
end
