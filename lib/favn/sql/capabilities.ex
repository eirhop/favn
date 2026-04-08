defmodule Favn.SQL.Capabilities do
  @moduledoc """
  Normalized SQL backend capability model used by `Favn.SQL`.
  """

  @type support :: :supported | :unsupported | :emulated

  defstruct relation_types: [:table, :view],
            replace_view: :unsupported,
            replace_table: :unsupported,
            transactions: :unsupported,
            merge: :unsupported,
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
          materialized_views: support(),
          relation_comments: support(),
          column_comments: support(),
          metadata_timestamps: support(),
          query_tracking: support(),
          extensions: map()
        }
end
