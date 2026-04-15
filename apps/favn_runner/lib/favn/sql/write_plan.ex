defmodule Favn.SQL.WritePlan do
  @moduledoc """
  Canonical SQL materialization plan consumed by adapter materialization paths.
  """

  alias Favn.SQL.Relation

  @type materialization :: :view | :table | :incremental
  @type strategy :: :append | :replace | :delete_insert | :merge

  @enforce_keys [:materialization, :target, :select_sql]
  defstruct [
    :asset_ref,
    :connection,
    :materialization,
    :strategy,
    :mode,
    :target,
    :query,
    :select_sql,
    :params,
    :replace_existing?,
    :replace?,
    :if_not_exists?,
    :transactional?,
    :window,
    :effective_window,
    :window_column,
    :unique_key,
    :incremental_predicate_sql,
    :bootstrap?,
    pre_statements: [],
    post_statements: [],
    options: %{},
    metadata: %{}
  ]

  @type t :: %__MODULE__{
          asset_ref: Favn.asset_ref() | nil,
          connection: atom() | nil,
          materialization: materialization(),
          strategy: strategy() | nil,
          mode: :bootstrap | :incremental | nil,
          target: Relation.t(),
          query: term() | nil,
          select_sql: iodata(),
          params: term() | nil,
          replace_existing?: boolean() | nil,
          replace?: boolean() | nil,
          if_not_exists?: boolean() | nil,
          transactional?: boolean() | nil,
          window: term(),
          effective_window: term(),
          window_column: String.t() | nil,
          unique_key: [binary()] | nil,
          incremental_predicate_sql: iodata() | nil,
          bootstrap?: boolean() | nil,
          pre_statements: [iodata()],
          post_statements: [iodata()],
          options: map(),
          metadata: map()
        }
end
