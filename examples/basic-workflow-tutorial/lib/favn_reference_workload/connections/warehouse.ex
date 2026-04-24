defmodule FavnReferenceWorkload.Connections.Warehouse do
  @moduledoc """
  DuckDB warehouse connection used by the reference workload.

  This module teaches the basic connection contract in Favn:

  - `@behaviour Favn.Connection` means this module must return a connection
    definition.
  - `name: :warehouse` is the symbolic name assets refer to.
  - `adapter: Favn.SQL.Adapter.DuckDB` chooses DuckDB as execution backend.
  - `config_schema` declares required runtime config (`:database` path).

  Alternative configurations:

  - Keep `name: :warehouse` and point `database` to another `.duckdb` file.
  - Use a different SQL adapter if your project targets another engine.
  - Add extra keys in `config_schema` when your adapter requires more options.
  """

  @behaviour Favn.Connection

  @impl true
  def definition do
    %Favn.Connection.Definition{
      name: :warehouse,
      adapter: Favn.SQL.Adapter.DuckDB,
      doc: "Reference workload DuckDB warehouse",
      metadata: %{scope: :reference_workload},
      config_schema: [
        %{key: :database, required: true, type: :path}
      ]
    }
  end
end
