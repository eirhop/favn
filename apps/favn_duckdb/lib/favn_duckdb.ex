defmodule FavnDuckdb do
  @moduledoc """
  DuckDB execution plugin for `favn_runner`.

  Use `Favn.SQL.Adapter.DuckDB` as the adapter in `Favn.Connection` definitions.
  That module also exposes `config_schema_fields/0` for DuckDB/DuckLake runtime
  connection configuration.
  """

  @behaviour Favn.Runner.Plugin

  @impl true
  def applications(_opts), do: {:ok, [:duckdbex]}

  @impl true
  def child_specs(opts) when is_list(opts) do
    case FavnDuckdb.Runtime.execution_mode(opts) do
      :in_process ->
        {:ok, []}

      :separate_process ->
        {:ok,
         [
           {FavnDuckdb.Worker,
            [
              name: FavnDuckdb.Runtime.worker_name(opts),
              client: Favn.SQL.Adapter.DuckDB.Client.Duckdbex
            ]}
         ]}
    end
  end
end
