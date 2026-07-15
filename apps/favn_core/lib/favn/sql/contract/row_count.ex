defmodule Favn.SQL.Contract.RowCount do
  @moduledoc "Minimum row-count claim compiled into the normal SQL check engine."

  @enforce_keys [:min, :on_violation]
  defstruct [:min, :when, :on_violation]

  @type t :: %__MODULE__{
          min: non_neg_integer(),
          when: Favn.SQL.Check.condition(),
          on_violation: Favn.SQL.Check.violation_policy()
        }

  @doc "Builds and validates a row-count claim."
  @spec new!(keyword() | map()) :: t()
  def new!(opts) when is_list(opts) or is_map(opts) do
    opts = Map.new(opts)

    %__MODULE__{
      min: Map.get(opts, :min),
      when: Map.get(opts, :when),
      on_violation: Map.get(opts, :on_violation, :fail)
    }
    |> validate!()
  end

  @doc "Validates a compiled or rehydrated row-count claim."
  @spec validate!(t()) :: t()
  def validate!(%__MODULE__{} = row_count) do
    unless is_integer(row_count.min) and row_count.min >= 0,
      do: raise(ArgumentError, "contract row_count min: must be a non-negative integer")

    unless row_count.when in [nil, :target_exists],
      do: raise(ArgumentError, "contract row_count when: must be :target_exists")

    unless row_count.on_violation in [:fail, :warn, :skip_materialization],
      do:
        raise(
          ArgumentError,
          "contract row_count on_violation: must be :fail, :warn, or :skip_materialization"
        )

    if row_count.on_violation == :skip_materialization and row_count.when != :target_exists,
      do:
        raise(
          ArgumentError,
          "contract row_count :skip_materialization requires when: :target_exists"
        )

    row_count
  end
end
