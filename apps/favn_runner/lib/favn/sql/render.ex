defmodule Favn.SQL.ParamBinding do
  @moduledoc """
  One canonical SQL bind value emitted by the SQL asset renderer.
  """

  alias Favn.SQL.Template.Span

  @enforce_keys [:ordinal, :name, :source, :value]
  defstruct [:ordinal, :name, :source, :value, :span]

  @type source :: :runtime | :query_param

  @type t :: %__MODULE__{
          ordinal: pos_integer(),
          name: atom() | String.t(),
          source: source(),
          value: term(),
          span: Span.t() | nil
        }
end

defmodule Favn.SQL.Params do
  @moduledoc """
  Canonical SQL parameter payload emitted by the SQL asset renderer.
  """

  alias Favn.SQL.ParamBinding

  @enforce_keys [:format, :bindings]
  defstruct [:format, :bindings]

  @type format :: :positional

  @type t :: %__MODULE__{format: format(), bindings: [ParamBinding.t()]}

  @spec to_adapter_params(t()) :: [term()]
  def to_adapter_params(%__MODULE__{format: :positional, bindings: bindings}) do
    Enum.map(bindings, & &1.value)
  end
end

defmodule Favn.SQL.Render do
  @moduledoc """
  Canonical SQL asset render output.

  This payload is backend-neutral and does not require opening a SQL session.
  """

  alias Favn.RelationRef
  alias Favn.SQL.Params
  alias Favn.Window.Runtime

  @enforce_keys [:asset_ref, :connection, :relation, :materialization, :sql, :params]
  defstruct [
    :asset_ref,
    :connection,
    :relation,
    :materialization,
    :sql,
    :params,
    :runtime,
    resolved_asset_refs: [],
    metadata: %{}
  ]

  @type t :: %__MODULE__{
          asset_ref: Favn.asset_ref(),
          connection: atom(),
          relation: RelationRef.t(),
          materialization: Favn.SQLAsset.Materialization.t(),
          sql: String.t(),
          params: Params.t(),
          runtime: Runtime.t() | map() | nil,
          resolved_asset_refs: [map()],
          metadata: map()
        }
end

defmodule Favn.SQL.Preview do
  @moduledoc """
  SQL preview output.

  `statement` is the actual SQL statement executed for preview.
  """

  alias Favn.SQL.{Render, Result}

  @enforce_keys [:render, :limit, :statement, :result]
  defstruct [:render, :limit, :statement, :result]

  @type t :: %__MODULE__{
          render: Render.t(),
          limit: pos_integer(),
          statement: String.t(),
          result: Result.t()
        }
end

defmodule Favn.SQL.Explain do
  @moduledoc """
  SQL explain output.
  """

  alias Favn.SQL.{Render, Result}

  @enforce_keys [:render, :statement, :analyze?, :result]
  defstruct [:render, :statement, :analyze?, :result]

  @type t :: %__MODULE__{
          render: Render.t(),
          statement: String.t(),
          analyze?: boolean(),
          result: Result.t()
        }
end

defmodule Favn.SQL.MaterializationResult do
  @moduledoc """
  SQL materialization output for one SQL asset.
  """

  alias Favn.SQL.{Render, Result, WritePlan}

  @enforce_keys [:render, :write_plan, :result]
  defstruct [:render, :write_plan, :result]

  @type t :: %__MODULE__{render: Render.t(), write_plan: WritePlan.t(), result: Result.t()}
end
