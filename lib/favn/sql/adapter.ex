defmodule Favn.SQL.Adapter do
  @moduledoc """
  Internal backend behaviour for SQL execution in Favn.

  This is not an end-user SQL asset DSL contract.
  It is the runtime backend boundary used by `Favn.SQL`.
  """

  alias Favn.Connection.Resolved
  alias Favn.SQL.{Capabilities, Column, Error, Relation, RelationRef, Result, WritePlan}

  @type conn :: term()
  @type statement :: iodata()
  @type opts :: keyword()
  @type introspection_kind ::
          :schema_exists | :relation | :list_schemas | :list_relations | :columns

  @callback connect(Resolved.t(), opts()) :: {:ok, conn()} | {:error, Error.t()}
  @callback disconnect(conn(), opts()) :: :ok | {:error, Error.t()}

  @callback capabilities(Resolved.t(), opts()) :: {:ok, Capabilities.t()} | {:error, Error.t()}

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

  @callback transaction(conn(), (conn() -> {:ok, term()} | {:error, Error.t()}), opts()) ::
              {:ok, term()} | {:error, Error.t()}

  @callback materialize(conn(), WritePlan.t(), opts()) :: {:ok, Result.t()} | {:error, Error.t()}

  @optional_callbacks [
    ping: 2,
    schema_exists?: 3,
    relation: 3,
    list_schemas: 2,
    list_relations: 3,
    columns: 3,
    transaction: 3,
    materialize: 3
  ]
end
