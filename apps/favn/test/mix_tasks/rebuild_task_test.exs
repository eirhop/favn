defmodule Mix.Tasks.Favn.RebuildTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureIO

  alias Mix.Tasks.Favn.Rebuild

  test "prints a full-refresh plan without coverage" do
    output =
      capture_io(fn ->
        Rebuild.print_plan(%{
          "plan_id" => "rebuild-plan-1",
          "plan_hash" => String.duplicate("a", 64),
          "payload" => %{
            "root_target_id" => "asset:orders",
            "coverage" => nil,
            "evaluated_range" => %{"start_at" => nil, "end_at" => nil},
            "binding_snapshot" => %{"asset:orders" => %{}},
            "actions" => []
          }
        })
      end)

    assert output =~ "Plan: rebuild-plan-1"
    assert output =~ "Target: asset:orders"
  end

  test "formats safe structured rebuild errors" do
    assert Rebuild.error_message(%{
             operation: :start_rebuild,
             reason: {:http_error, 409, %{error_code: "rebuild_plan_stale"}}
           }) == "rebuild plan is stale; create and review a new plan"

    assert Rebuild.error_message(%{
             operation: :plan_rebuild,
             reason: {:http_error, 500, %{error_code: "internal_error"}}
           }) == "rebuild plan failed: HTTP 500 (internal_error)"
  end

  test "prints terminal item counts as completed rebuild progress" do
    output =
      capture_io(fn ->
        Rebuild.print_operation(%{
          "operation_id" => "rebuild-1",
          "root_target_id" => "asset:orders",
          "state" => "succeeded",
          "phase" => "terminal",
          "progress" => %{"succeeded" => 1, "total" => 1}
        })
      end)

    assert output =~ "Progress: 1/1"
  end
end
