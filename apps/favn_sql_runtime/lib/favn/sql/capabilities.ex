defmodule Favn.SQL.Capabilities do
  @moduledoc """
  Normalized SQL backend capability model used by `Favn.SQL`.

  Adapter-specific `extensions.pool_safe_after_success` may list controlled
  operations whose successful completion leaves a pooled session safe to reset
  and reuse. `extensions.pool_safe_when_requested` may list operations that
  additionally require a trusted internal `pool_safe?: true` request. Both
  default to absent, preserving conservative post-write discard behavior.
  """

  @type support :: :supported | :unsupported | :emulated

  defstruct relation_types: [:table, :view],
            replace_view: :unsupported,
            replace_table: :unsupported,
            transactions: :unsupported,
            merge: :unsupported,
            group_replacement: :unsupported,
            physical_partitioning: :unsupported,
            materialized_views: :unsupported,
            relation_comments: :unsupported,
            column_comments: :unsupported,
            metadata_timestamps: :unsupported,
            query_tracking: :unsupported,
            extensions: %{}

  @type t :: %__MODULE__{
          relation_types: [atom()],
          replace_view: support(),
          replace_table: support(),
          transactions: support(),
          merge: support(),
          group_replacement: support(),
          physical_partitioning: support(),
          materialized_views: support(),
          relation_comments: support(),
          column_comments: support(),
          metadata_timestamps: support(),
          query_tracking: support(),
          extensions: map()
        }
end
