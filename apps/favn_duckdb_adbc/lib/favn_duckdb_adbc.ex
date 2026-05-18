defmodule FavnDuckdbADBC do
  @moduledoc """
  DuckDB ADBC execution plugin for `favn_runner`.

  Use `Favn.SQL.Adapter.DuckDB.ADBC` as the adapter in `Favn.Connection`
  definitions. That module exposes `config_schema_fields/0` for DuckDB and
  DuckLake runtime connection configuration.
  """

  @behaviour FavnRunner.Plugin

  @impl true
  def child_specs(opts) when is_list(opts) do
    case FavnDuckdbADBC.Runtime.execution_mode(opts) do
      :in_process -> []
    end
  end
end
