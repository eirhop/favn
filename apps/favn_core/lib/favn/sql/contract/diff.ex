defmodule Favn.SQL.Contract.Diff do
  @moduledoc """
  Semantic differences between two SQL output contracts.

  The diff uses explicit `renamed_from:` declarations instead of guessing from
  SQL or column similarity. Results are deterministic and suitable for
  manifest review or compatibility policy.
  """

  alias Favn.SQL.Contract
  alias Favn.SQL.Contract.Composition

  @type change :: %{
          required(:kind) =>
            :column_added
            | :column_removed
            | :column_renamed
            | :column_order_changed
            | :type_changed
            | :nullability_changed
            | :grain_changed
            | :unique_keys_changed
            | :row_count_changed,
          optional(:column) => atom(),
          optional(:from) => term(),
          optional(:to) => term()
        }

  @type provenance_change :: %{
          required(:kind) =>
            :fragment_added
            | :fragment_removed
            | :fragment_moved
            | :fragment_columns_changed,
          required(:module) => module(),
          optional(:from) => term(),
          optional(:to) => term()
        }

  @doc "Returns semantic changes from `previous` to `current`."
  @spec between(Contract.t() | nil, Contract.t() | nil) :: [change()]
  def between(nil, nil), do: []

  def between(nil, %Contract{} = current),
    do: Enum.map(current.columns, &%{kind: :column_added, column: &1.name})

  def between(%Contract{} = previous, nil),
    do: Enum.map(previous.columns, &%{kind: :column_removed, column: &1.name})

  def between(%Contract{} = previous, %Contract{} = current) do
    previous = Contract.validate!(previous)
    current = Contract.validate!(current)
    previous_by_name = Map.new(previous.columns, &{&1.name, &1})
    current_by_name = Map.new(current.columns, &{&1.name, &1})
    renames = Map.new(current.columns, &{&1.renamed_from, &1.name}) |> Map.delete(nil)

    removals =
      previous.columns
      |> Enum.reject(fn column ->
        Map.has_key?(current_by_name, column.name) or Map.has_key?(renames, column.name)
      end)
      |> Enum.map(&%{kind: :column_removed, column: &1.name})

    additions =
      current.columns
      |> Enum.reject(fn column ->
        Map.has_key?(previous_by_name, column.name) or
          (column.renamed_from && Map.has_key?(previous_by_name, column.renamed_from))
      end)
      |> Enum.map(&%{kind: :column_added, column: &1.name})

    renamed =
      current.columns
      |> Enum.filter(fn column ->
        column.renamed_from && Map.has_key?(previous_by_name, column.renamed_from)
      end)
      |> Enum.map(&%{kind: :column_renamed, from: &1.renamed_from, to: &1.name})

    column_changes =
      Enum.flat_map(current.columns, fn current_column ->
        previous_name = current_column.renamed_from || current_column.name

        case Map.get(previous_by_name, previous_name) do
          nil -> []
          previous_column -> compare_column(previous_column, current_column)
        end
      end)

    removals ++
      additions ++
      renamed ++
      column_changes ++
      column_order_changes(previous, current, renames) ++
      contract_changes(previous, current)
  end

  @doc "Returns fragment-composition provenance changes without classifying schema compatibility."
  @spec provenance_between(Contract.t() | nil, Contract.t() | nil) :: [provenance_change()]
  def provenance_between(nil, nil), do: []

  def provenance_between(nil, %Contract{} = current) do
    current
    |> Contract.validate!()
    |> Map.fetch!(:compositions)
    |> Enum.map(&%{kind: :fragment_added, module: &1.module, to: composition_value(&1)})
  end

  def provenance_between(%Contract{} = previous, nil) do
    previous
    |> Contract.validate!()
    |> Map.fetch!(:compositions)
    |> Enum.map(&%{kind: :fragment_removed, module: &1.module, from: composition_value(&1)})
  end

  def provenance_between(%Contract{} = previous, %Contract{} = current) do
    previous = Contract.validate!(previous)
    current = Contract.validate!(current)
    previous_by_module = Map.new(previous.compositions, &{&1.module, &1})
    current_by_module = Map.new(current.compositions, &{&1.module, &1})

    removals =
      previous.compositions
      |> Enum.reject(&Map.has_key?(current_by_module, &1.module))
      |> Enum.map(
        &%{
          kind: :fragment_removed,
          module: &1.module,
          from: composition_value(&1)
        }
      )

    additions_and_changes =
      Enum.flat_map(current.compositions, fn composition ->
        case Map.get(previous_by_module, composition.module) do
          nil ->
            [
              %{
                kind: :fragment_added,
                module: composition.module,
                to: composition_value(composition)
              }
            ]

          previous_composition ->
            composition_changes(previous_composition, composition)
        end
      end)

    removals ++ additions_and_changes
  end

  defp column_order_changes(previous, current, renames) do
    previous_names = MapSet.new(previous.columns, & &1.name)
    current_names = MapSet.new(current.columns, & &1.name)

    previous_shared_order =
      previous.columns
      |> Enum.map(&Map.get(renames, &1.name, &1.name))
      |> Enum.filter(&MapSet.member?(current_names, &1))

    current_shared_order =
      current.columns
      |> Enum.filter(fn column ->
        MapSet.member?(previous_names, column.name) or
          (column.renamed_from && MapSet.member?(previous_names, column.renamed_from))
      end)
      |> Enum.map(& &1.name)

    if previous_shared_order == current_shared_order do
      []
    else
      [
        %{
          kind: :column_order_changed,
          from: previous_shared_order,
          to: current_shared_order
        }
      ]
    end
  end

  defp compare_column(previous, current) do
    type =
      if previous.type == current.type,
        do: [],
        else: [
          %{kind: :type_changed, column: current.name, from: previous.type, to: current.type}
        ]

    nullability =
      if previous.nullable? == current.nullable? do
        []
      else
        [
          %{
            kind: :nullability_changed,
            column: current.name,
            from: previous.nullable?,
            to: current.nullable?
          }
        ]
      end

    type ++ nullability
  end

  defp contract_changes(previous, current) do
    maybe_change(:grain_changed, previous.grain, current.grain) ++
      maybe_change(:unique_keys_changed, previous.unique_keys, current.unique_keys) ++
      maybe_change(:row_count_changed, previous.row_counts, current.row_counts)
  end

  defp maybe_change(_kind, value, value), do: []
  defp maybe_change(kind, from, to), do: [%{kind: kind, from: from, to: to}]

  defp composition_changes(%Composition{} = previous, %Composition{} = current) do
    moved =
      if previous.start_index == current.start_index do
        []
      else
        [
          %{
            kind: :fragment_moved,
            module: current.module,
            from: previous.start_index,
            to: current.start_index
          }
        ]
      end

    columns =
      if previous.columns == current.columns do
        []
      else
        [
          %{
            kind: :fragment_columns_changed,
            module: current.module,
            from: previous.columns,
            to: current.columns
          }
        ]
      end

    moved ++ columns
  end

  defp composition_value(%Composition{} = composition),
    do: %{start_index: composition.start_index, columns: composition.columns}
end
