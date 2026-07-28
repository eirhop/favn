defmodule CrmDemo.Connections.Warehouse do
  @moduledoc """
  The single DuckDB warehouse every SQL asset in this project writes to.

  A connection module names an engine and declares which runtime options it
  needs. The values come from `config :favn, connections: [warehouse: ...]`.
  """

  @behaviour Favn.Connection

  @impl true
  def definition do
    %Favn.Connection.Definition{
      name: :warehouse,
      adapter: Favn.SQL.Adapter.DuckDB.ADBC,
      doc: "CRM demo DuckDB warehouse",
      config_schema: Favn.SQL.Adapter.DuckDB.ADBC.config_schema_fields()
    }
  end
end
