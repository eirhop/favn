defmodule Favn.SQL.ContractValidation do
  @moduledoc """
  Structured expected-versus-observed candidate-schema evidence.

  A successful value is stored with SQL run metadata. A failed value is placed
  in the SQL asset error details before any target mutation, giving operators
  the same bounded, machine-readable differences in both paths.
  """

  alias Favn.SQL.Contract

  @max_observed_columns Contract.max_columns()

  @enforce_keys [:status, :expected_columns, :observed_columns, :differences]
  defstruct [
    :status,
    :expected_columns,
    :observed_columns,
    :differences,
    :observed_column_count,
    observed_truncated?: false
  ]

  @type difference :: %{
          required(:kind) =>
            :missing | :unexpected | :order | :type | :nullability | :column_limit,
          optional(:column) => String.t(),
          optional(:expected) => term(),
          optional(:observed) => term()
        }

  @type t :: %__MODULE__{
          status: :passed | :failed,
          expected_columns: [map()],
          observed_columns: [map()],
          differences: [difference()],
          observed_column_count: non_neg_integer(),
          observed_truncated?: boolean()
        }

  @doc "Compares ordered observed SQL columns with a typed output contract."
  @spec compare(Contract.t(), [map()]) :: t()
  def compare(%Contract{} = contract, observed_columns) when is_list(observed_columns) do
    contract = Contract.validate!(contract)
    expected = Enum.map(contract.columns, &expected_column/1)
    observed_column_count = length(observed_columns)

    observed =
      observed_columns
      |> Enum.take(@max_observed_columns)
      |> Enum.map(&observed_column/1)

    observed_truncated? = observed_column_count > @max_observed_columns

    differences =
      column_limit_differences(observed_column_count) ++ schema_differences(expected, observed)

    %__MODULE__{
      status: if(differences == [], do: :passed, else: :failed),
      expected_columns: expected,
      observed_columns: observed,
      differences: differences,
      observed_column_count: observed_column_count,
      observed_truncated?: observed_truncated?
    }
  end

  @doc "Returns the maximum observed candidate columns retained as assurance evidence."
  @spec max_observed_columns() :: pos_integer()
  def max_observed_columns, do: @max_observed_columns

  defp expected_column(column) do
    %{name: Atom.to_string(column.name), type: column.type, nullable?: column.nullable?}
  end

  defp observed_column(column) when is_map(column) do
    name = Map.get(column, :name) || Map.get(column, "name")
    native_type = Map.get(column, :data_type) || Map.get(column, "data_type")
    nullable? = Map.get(column, :nullable?, Map.get(column, "nullable?"))
    metadata = Map.get(column, :metadata, Map.get(column, "metadata", %{})) || %{}

    %{
      name: to_string(name),
      type: Contract.normalize_observed_type(native_type),
      native_type: native_type,
      nullable?: nullable?,
      nullability_observed?:
        Map.get(metadata, :contract_nullability, Map.get(metadata, "contract_nullability")) in [
          :reliable,
          "reliable"
        ]
    }
  end

  defp schema_differences(expected, observed) do
    expected_by_name = Map.new(expected, &{&1.name, &1})
    observed_by_name = Map.new(observed, &{&1.name, &1})

    missing =
      for column <- expected, not Map.has_key?(observed_by_name, column.name) do
        %{kind: :missing, column: column.name, expected: column}
      end

    unexpected =
      for column <- observed, not Map.has_key?(expected_by_name, column.name) do
        %{kind: :unexpected, column: column.name, observed: column}
      end

    common =
      Enum.flat_map(expected, fn expected_column ->
        case Map.get(observed_by_name, expected_column.name) do
          nil ->
            []

          observed_column ->
            type_difference(expected_column, observed_column) ++
              nullability_difference(expected_column, observed_column)
        end
      end)

    order =
      if Enum.map(expected, & &1.name) == Enum.map(observed, & &1.name) do
        []
      else
        [
          %{
            kind: :order,
            expected: Enum.map(expected, & &1.name),
            observed: Enum.map(observed, & &1.name)
          }
        ]
      end

    missing ++ unexpected ++ common ++ order
  end

  defp column_limit_differences(observed_count) when observed_count > @max_observed_columns do
    [
      %{
        kind: :column_limit,
        expected: %{maximum: @max_observed_columns},
        observed: %{count: observed_count}
      }
    ]
  end

  defp column_limit_differences(_observed_count), do: []

  defp type_difference(expected, observed) do
    if Contract.compatible_type?(expected.type, observed.native_type) do
      []
    else
      [
        %{
          kind: :type,
          column: expected.name,
          expected: expected.type,
          observed: observed.native_type || :unknown
        }
      ]
    end
  end

  defp nullability_difference(
         expected,
         %{nullable?: nullable?, nullability_observed?: true}
       )
       when expected.nullable? == false and nullable? == true do
    [
      %{
        kind: :nullability,
        column: expected.name,
        expected: expected.nullable?,
        observed: nullable?
      }
    ]
  end

  defp nullability_difference(_expected, _observed), do: []
end
