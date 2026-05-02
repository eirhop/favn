defmodule Favn.SQL.Adapter do
  @moduledoc """
  SQL runtime plugin behaviour for backend adapters.

  This is the contract implemented by SQL backend plugins such as DuckDB. It is
  not the end-user SQL asset DSL and should not be treated as an ordinary stable
  authoring API.
  """

  alias Favn.Connection.Resolved
  alias Favn.RelationRef
  alias Favn.SQL.{Capabilities, Column, ConcurrencyPolicy, Error, Relation, Result, WritePlan}

  @type conn :: term()
  @type statement :: iodata()
  @type opts :: keyword()
  @type introspection_kind ::
          :schema_exists | :relation | :list_schemas | :list_relations | :columns

  @callback connect(Resolved.t(), opts()) :: {:ok, conn()} | {:error, Error.t()}
  @callback bootstrap(conn(), Resolved.t(), opts()) :: :ok | {:error, Error.t()}
  @callback disconnect(conn(), opts()) :: :ok | {:error, Error.t()}

  @callback capabilities(Resolved.t(), opts()) :: {:ok, Capabilities.t()} | {:error, Error.t()}

  @callback diagnostics(Resolved.t(), opts()) :: {:ok, map()} | {:error, term()}

  @callback default_concurrency_policy(Resolved.t()) :: ConcurrencyPolicy.t()

  @callback execute(conn(), statement(), opts()) :: {:ok, Result.t()} | {:error, Error.t()}
  @callback query(conn(), statement(), opts()) :: {:ok, Result.t()} | {:error, Error.t()}

  @callback introspection_query(introspection_kind(), term(), opts()) ::
              {:ok, statement()} | {:error, Error.t()}

  @callback materialization_statements(WritePlan.t(), Capabilities.t(), opts()) ::
              {:ok, [statement()]} | {:error, Error.t()}

  @callback ping(conn(), opts()) :: :ok | {:error, Error.t()}

  @callback schema_exists?(conn(), binary(), opts()) :: {:ok, boolean()} | {:error, Error.t()}
  @callback relation(conn(), RelationRef.t(), opts()) ::
              {:ok, Relation.t() | nil} | {:error, Error.t()}
  @callback list_schemas(conn(), opts()) :: {:ok, [binary()]} | {:error, Error.t()}
  @callback list_relations(conn(), binary() | nil, opts()) ::
              {:ok, [Relation.t()]} | {:error, Error.t()}
  @callback columns(conn(), RelationRef.t(), opts()) :: {:ok, [Column.t()]} | {:error, Error.t()}
  @callback row_count(conn(), RelationRef.t(), opts()) ::
              {:ok, non_neg_integer()} | {:error, Error.t()}
  @callback sample(conn(), RelationRef.t(), opts()) :: {:ok, Result.t()} | {:error, Error.t()}
  @callback table_metadata(conn(), RelationRef.t(), opts()) :: {:ok, map()} | {:error, Error.t()}

  @callback transaction(conn(), (conn() -> {:ok, term()} | {:error, Error.t()}), opts()) ::
              {:ok, term()} | {:error, Error.t()}

  @callback materialize(conn(), WritePlan.t(), opts()) :: {:ok, Result.t()} | {:error, Error.t()}

  @optional_callbacks [
    ping: 2,
    bootstrap: 3,
    diagnostics: 2,
    schema_exists?: 3,
    relation: 3,
    list_schemas: 2,
    list_relations: 3,
    columns: 3,
    row_count: 3,
    sample: 3,
    table_metadata: 3,
    default_concurrency_policy: 1,
    transaction: 3,
    materialize: 3
  ]
end
