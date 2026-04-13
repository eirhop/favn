defmodule Favn.Connection do
  @moduledoc """
  Public contract for connection definition providers.

  Use `Favn.Connection` when assets or pipelines need a named backend
  connection such as DuckDB now and other SQL engines later.

  Provider modules return static connection metadata from `definition/0`. Host
  applications then supply runtime values through `config :favn, connections: [...]`.

  ## Minimal example

      defmodule MyApp.Connections.Warehouse do
        @behaviour Favn.Connection

        @impl true
        def definition do
          %Favn.Connection.Definition{
            name: :warehouse,
            adapter: MyApp.WarehouseAdapter,
            config_schema: [
              %{key: :database, required: true, type: :path}
            ]
          }
        end
      end

  The adapter module should implement the SQL adapter contract used by your
  runtime.

  ## See also

  - `Favn.AgentGuide`
  - `Favn.SQLAsset`
  - `Favn.SQL`
  """

  alias Favn.Connection.Definition

  @callback definition() :: Definition.t()
end
