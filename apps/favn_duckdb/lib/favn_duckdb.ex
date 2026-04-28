defmodule FavnDuckdb do
  @moduledoc """
  DuckDB execution plugin for `favn_runner`.

  Use `Favn.SQL.Adapter.DuckDB` as the adapter in `Favn.Connection` definitions.
  That module also exposes `bootstrap_schema_field/0` for DuckDB/DuckLake
  connection bootstrap configuration.
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
