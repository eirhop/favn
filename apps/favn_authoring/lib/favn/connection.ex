defmodule Favn.Connection do
  @moduledoc """
  Public contract for connection definition providers.

  Use `Favn.Connection` when assets or pipelines need a named backend
  connection such as DuckDB now and other SQL engines later.

  Provider modules return static connection metadata from `definition/0`. Host
  applications then supply runtime values through `config :favn, connections: [...]`.
  Runtime values may be literals or `Favn.RuntimeConfig.Ref` values that resolve
  from the runner environment before adapter connection.

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

  ## Definition fields

  `definition/0` returns `%Favn.Connection.Definition{}` with these public
  fields:

  - `name`: connection name used by assets and runtime lookup
  - `adapter`: backend adapter module
  - `config_schema`: runtime config schema entries
  - `doc`: optional connection documentation
  - `metadata`: optional descriptive metadata

  Each `config_schema` entry supports:

  - `key`: required config key name
  - `required`: boolean, defaults to optional when omitted
  - `default`: optional default value
  - `secret`: boolean for redaction-sensitive values
  - `type`: one of `:string`, `:atom`, `:boolean`, `:integer`, `:float`, `:path`, `:module`, `{:in, values}`, or `{:custom, fun}`

  Favn also reserves `:write_concurrency` as runtime connection config for SQL
  admission control. Use `write_concurrency: 1` or `:single` for single-writer
  backends, and `write_concurrency: :unlimited` for backends that safely support
  parallel writes.

  ## Runtime Environment Values

      config :favn,
        connection_modules: [MyApp.Connections.Warehouse],
        connections: [
          warehouse: [
            database: Favn.RuntimeConfig.Ref.env!("WAREHOUSE_DB_PATH"),
            password: Favn.RuntimeConfig.Ref.secret_env!("WAREHOUSE_PASSWORD")
          ]
        ]

  The connection loader resolves these refs before calling the adapter. A missing
  required value returns a structured Favn error such as
  `missing_env WAREHOUSE_PASSWORD`. Secret values are passed to the adapter but
  redacted from inspection payloads.

  ## See also

  - `Favn.SQLAsset`
  - `Favn.SQL`
  - `Favn.RuntimeConfig.Ref`
  """

  alias Favn.Connection.Definition

  @callback definition() :: Definition.t()
end
