defmodule Favn.SQL.Adapter do
  @moduledoc """
  SQL runtime plugin behaviour for backend adapters.

  This is the contract implemented by SQL backend plugins such as DuckDB. It is
  not the end-user SQL asset DSL and should not be treated as an ordinary stable
  authoring API.

  Adapters that implement `poolable?/2`, `pool_fingerprint/2`,
  `validate_session/2`, and `reset_session/3` opt into Favn's runner-local SQL
  session pool. Pooling is default-on for poolable adapters unless the connection
  sets `pool: [enabled: false]`. Adapter lifecycle callbacks must make warm reuse
  safe for read-only paths; write/materialization/raw execution paths are
  discarded by the shared client unless explicitly proven pool-safe internally.
  `classify_error/2` may return `details.classification: :capacity` through
  normalized errors so shared retry logic uses capacity backoff. Unknown outcome
  or commit-state errors must remain non-retryable.
  """

  alias Favn.Connection.Resolved
  alias Favn.RelationRef
  alias Favn.SQL.{Capabilities, Column, ConcurrencyPolicy, Error, Relation, Result, WritePlan}

  @type conn :: term()
  @type statement :: iodata()
  @type opts :: keyword()
  @type error_classification :: %{
          required(:classification) => atom(),
          optional(:retryable?) => boolean(),
          optional(:capacity?) => boolean(),
          optional(:unknown_outcome?) => boolean()
        }
  @type introspection_kind ::
          :schema_exists | :relation | :list_schemas | :list_relations | :columns

  @callback connect(Resolved.t(), opts()) :: {:ok, conn()} | {:error, Error.t()}
  @callback bootstrap(conn(), Resolved.t(), opts()) :: :ok | {:error, Error.t()}
  @callback disconnect(conn(), opts()) :: :ok | {:error, Error.t()}

  @callback poolable?(Resolved.t(), opts()) :: boolean()
  @callback pool_fingerprint(Resolved.t(), opts()) :: term()
  @callback validate_session(conn(), opts()) :: :ok | {:error, Error.t()}
  @callback reset_session(conn(), Resolved.t(), opts()) :: :ok | {:error, Error.t()}
  @callback classify_error(term(), opts()) :: error_classification()

  @callback capabilities(Resolved.t(), opts()) :: {:ok, Capabilities.t()} | {:error, Error.t()}

  @callback diagnostics(Resolved.t(), opts()) :: {:ok, map()} | {:error, term()}

  @callback configured_catalogs(Resolved.t()) :: {:ok, [atom() | String.t()]} | {:error, term()}

  @callback default_catalog(Resolved.t()) :: {:ok, atom() | String.t() | nil} | {:error, term()}

  @callback default_concurrency_policy(Resolved.t()) :: ConcurrencyPolicy.t()

  @callback concurrency_policies(Resolved.t()) ::
              {:ok, [ConcurrencyPolicy.t()]} | {:error, Error.t()}

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
    poolable?: 2,
    pool_fingerprint: 2,
    validate_session: 2,
    reset_session: 3,
    classify_error: 2,
    bootstrap: 3,
    diagnostics: 2,
    configured_catalogs: 1,
    default_catalog: 1,
    schema_exists?: 3,
    relation: 3,
    list_schemas: 2,
    list_relations: 3,
    columns: 3,
    row_count: 3,
    sample: 3,
    table_metadata: 3,
    default_concurrency_policy: 1,
    concurrency_policies: 1,
    transaction: 3,
    materialize: 3
  ]
end
