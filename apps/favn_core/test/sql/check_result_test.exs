defmodule Favn.SQL.CheckResultTest do
  use ExUnit.Case, async: true

  alias Favn.SQL.CheckResult

  test "builds a typed check result" do
    assert %CheckResult{name: :valid_rows, outcome: :passed} =
             CheckResult.new(%{
               name: :valid_rows,
               phase: :before_materialize,
               outcome: :passed,
               metrics: %{"row_count" => 1},
               duration_ms: 3
             })
  end

  test "rejects invalid public result fields" do
    assert_raise ArgumentError, ~r/metric names must be strings/, fn ->
      CheckResult.new(%{
        name: :valid_rows,
        phase: :before_materialize,
        outcome: :passed,
        metrics: %{row_count: 1}
      })
    end

    assert_raise ArgumentError, ~r/duration must be a non-negative integer/, fn ->
      CheckResult.new(%{
        name: :valid_rows,
        phase: :before_materialize,
        outcome: :passed,
        duration_ms: -1
      })
    end
  end
end
