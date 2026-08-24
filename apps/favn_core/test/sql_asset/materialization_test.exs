defmodule Favn.SQLAsset.MaterializationTest do
  use ExUnit.Case, async: true

  alias Favn.SQLAsset.Materialization

  test "enforces group replacement option combinations at the Core boundary" do
    assert {:incremental, strategy: :replace_groups, replacement_key: [:customer_id]} =
             Materialization.normalize!(
               {:incremental, strategy: :replace_groups, replacement_key: [:customer_id]}
             )

    for invalid <- [
          {:incremental,
           strategy: :replace_groups, replacement_key: [:customer_id], unique_key: [:row_id]},
          {:incremental,
           strategy: :replace_groups, replacement_key: [:customer_id], window_column: :day},
          {:incremental, strategy: :append, replacement_key: [:customer_id]}
        ] do
      assert_raise ArgumentError, fn -> Materialization.normalize!(invalid) end
    end
  end
end
