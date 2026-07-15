defmodule Favn.SQLAsset.CheckResultNormalizerTest do
  use ExUnit.Case, async: true

  alias Favn.SQL.{Check, Result, Template}
  alias Favn.SQLAsset.CheckResultNormalizer

  test "accepts one native boolean result and bounded scalar metrics" do
    result = %Result{
      columns: ["passed", "row_count", "label"],
      rows: [%{"passed" => true, "row_count" => 3, "label" => "ready"}]
    }

    assert {:ok, true, %{"row_count" => 3, "label" => "ready"}} =
             CheckResultNormalizer.normalize(result, check(), {:asset, :ref})
  end

  test "rejects multiple rows before they can be truncated silently" do
    result = %Result{
      columns: ["passed"],
      rows: [%{"passed" => true}, %{"passed" => false}]
    }

    assert {:error, %{type: :invalid_check_result, details: %{reason: :invalid_row_count}}} =
             CheckResultNormalizer.normalize(result, check(), {:asset, :ref})
  end

  test "rejects zero rows and duplicate or missing passed columns" do
    assert {:error, %{details: %{reason: :invalid_row_count}}} =
             CheckResultNormalizer.normalize(
               %Result{columns: ["passed"], rows: []},
               check(),
               {:asset, :ref}
             )

    assert {:error, %{details: %{reason: :duplicate_columns}}} =
             CheckResultNormalizer.normalize(
               %Result{columns: ["passed", "passed"], rows: [%{"passed" => true}]},
               check(),
               {:asset, :ref}
             )

    assert {:error, %{details: %{reason: :invalid_passed_column}}} =
             CheckResultNormalizer.normalize(
               %Result{columns: ["metric"], rows: [%{"metric" => 1}]},
               check(),
               {:asset, :ref}
             )
  end

  test "requires exactly one non-null native boolean passed column" do
    result = %Result{columns: ["passed"], rows: [%{"passed" => 1}]}

    assert {:error, %{details: %{reason: :non_boolean_passed}}} =
             CheckResultNormalizer.normalize(result, check(), {:asset, :ref})

    assert {:error, %{details: %{reason: :null_passed}}} =
             CheckResultNormalizer.normalize(
               %Result{columns: ["passed"], rows: [%{"passed" => nil}]},
               check(),
               {:asset, :ref}
             )
  end

  test "rejects unsupported and oversized metric values" do
    nested = %Result{columns: ["passed", "nested"], rows: [%{"passed" => true, "nested" => []}]}

    assert {:error, %{details: %{reason: :unsupported_metric_type}}} =
             CheckResultNormalizer.normalize(nested, check(), {:asset, :ref})

    long_text = String.duplicate("x", 4_097)

    oversized = %Result{
      columns: ["passed", "text"],
      rows: [%{"passed" => true, "text" => long_text}]
    }

    assert {:error, %{details: %{reason: :text_metric_limit_exceeded}}} =
             CheckResultNormalizer.normalize(oversized, check(), {:asset, :ref})
  end

  test "bounds both metric column count and total encoded size" do
    metric_columns = Enum.map(1..33, &"metric_#{&1}")

    assert {:error, %{details: %{reason: :metric_column_limit_exceeded}}} =
             CheckResultNormalizer.normalize(
               %Result{
                 columns: ["passed" | metric_columns],
                 rows: [Map.new([{"passed", true} | Enum.map(metric_columns, &{&1, 1})])]
               },
               check(),
               {:asset, :ref}
             )

    bounded_columns = Enum.map(1..32, &"metric_#{&1}")
    value = String.duplicate("x", 4_096)

    assert {:error, %{details: %{reason: :metrics_byte_limit_exceeded}}} =
             CheckResultNormalizer.normalize(
               %Result{
                 columns: ["passed" | bounded_columns],
                 rows: [
                   Map.new([{"passed", true} | Enum.map(bounded_columns, &{&1, value})])
                 ]
               },
               check(),
               {:asset, :ref}
             )
  end

  test "measures the encoded JSON size including control-character escaping" do
    columns = Enum.map(1..15, &"metric_#{&1}")
    value = String.duplicate(<<0>>, 4_000)

    assert {:error, %{details: %{reason: :metrics_byte_limit_exceeded}}} =
             CheckResultNormalizer.normalize(
               %Result{
                 columns: ["passed" | columns],
                 rows: [Map.new([{"passed", true} | Enum.map(columns, &{&1, value})])]
               },
               check(),
               {:asset, :ref}
             )
  end

  defp check do
    Check.new!(%{
      name: :quality,
      at: :before_materialize,
      on_violation: :fail,
      sql: "select true as passed",
      template:
        Template.compile!("select true as passed",
          file: "test/check_result.sql",
          line: 1,
          module: __MODULE__,
          scope: :query,
          enforce_query_root: true
        ),
      uses_query?: false,
      uses_target?: false
    })
  end
end
