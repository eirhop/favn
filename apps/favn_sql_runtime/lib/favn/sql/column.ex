defmodule Favn.SQL.Column do
  @moduledoc """
  Normalized column metadata.

  Adapters set `metadata.contract_nullability` to `:reliable` only when
  `nullable?` is authoritative for contract comparison. Missing or
  `:unreliable` metadata means the value is descriptive only.
  """

  @enforce_keys [:name]
  defstruct [:name, :position, :data_type, :nullable?, :default, :comment, metadata: %{}]

  @type t :: %__MODULE__{
          name: binary(),
          position: non_neg_integer() | nil,
          data_type: binary() | nil,
          nullable?: boolean() | nil,
          default: term(),
          comment: binary() | nil,
          metadata: map()
        }
end
