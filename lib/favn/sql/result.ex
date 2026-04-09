defmodule Favn.SQL.Result do
  @moduledoc """
  Normalized SQL execution result.
  """

  @type kind :: :execute | :query | :materialize

  defstruct [:kind, :command, :rows_affected, rows: [], columns: [], notices: [], metadata: %{}]

  @type t :: %__MODULE__{
          kind: kind() | nil,
          command: binary() | nil,
          rows_affected: non_neg_integer() | nil,
          rows: [map()],
          columns: [binary()],
          notices: [binary()],
          metadata: map()
        }
end
