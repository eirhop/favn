defmodule Favn.SQL.Contract.RowCount do
  @moduledoc "Bounded row-count claim compiled into the normal SQL check engine."

  alias Favn.SQL.Contract.Param

  @enforce_keys [:on_violation]
  defstruct [:equals, :min, :max, :when, :on_violation]

  @type t :: %__MODULE__{
          equals: non_neg_integer() | Param.t() | nil,
          min: non_neg_integer() | nil,
          max: non_neg_integer() | nil,
          when: Favn.SQL.Check.condition(),
          on_violation: Favn.SQL.Check.violation_policy()
        }

  @doc "Builds and validates a row-count claim."
  @spec new!(keyword() | map()) :: t()
  def new!(opts) when is_list(opts) or is_map(opts) do
    opts = Map.new(opts)

    %__MODULE__{
      equals: normalize_equals(Map.get(opts, :equals)),
      min: Map.get(opts, :min),
      max: Map.get(opts, :max),
      when: Map.get(opts, :when),
      on_violation: Map.get(opts, :on_violation, :fail)
    }
    |> validate!()
  end

  @doc "Validates a compiled or rehydrated row-count claim."
  @spec validate!(t()) :: t()
  def validate!(%__MODULE__{} = row_count) do
    unless row_count.equals != nil or row_count.min != nil or row_count.max != nil,
      do: raise(ArgumentError, "contract row_count requires equals:, min:, or max:")

    if row_count.equals != nil and (row_count.min != nil or row_count.max != nil),
      do: raise(ArgumentError, "contract row_count equals: cannot be combined with min: or max:")

    validate_equals!(row_count.equals)
    validate_bound!(:min, row_count.min)
    validate_bound!(:max, row_count.max)

    if row_count.min != nil and row_count.max != nil and row_count.min > row_count.max,
      do: raise(ArgumentError, "contract row_count min: must not exceed max:")

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

  defp normalize_equals(%Param{} = param), do: Param.validate!(param)
  defp normalize_equals(value), do: value

  defp validate_equals!(nil), do: :ok
  defp validate_equals!(%Param{} = param), do: Param.validate!(param)

  defp validate_equals!(value) when is_integer(value) and value >= 0,
    do: :ok

  defp validate_equals!(_value),
    do:
      raise(ArgumentError, "contract row_count equals: must be a non-negative integer or param/1")

  defp validate_bound!(_name, nil), do: :ok

  defp validate_bound!(_name, value) when is_integer(value) and value >= 0,
    do: :ok

  defp validate_bound!(name, _value),
    do: raise(ArgumentError, "contract row_count #{name}: must be a non-negative integer")
end
