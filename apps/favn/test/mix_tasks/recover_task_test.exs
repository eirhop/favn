defmodule Mix.Tasks.Favn.RecoverTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureIO

  alias Mix.Tasks.Favn.Recover

  test "requires an explicit reason and exact plan hash" do
    assert {:error, "missing required option: --reason"} =
             Recover.parse_args(["plan", "MyApp.Orders"])

    assert {:error, "--plan-hash must be 64 lowercase hexadecimal characters"} =
             Recover.parse_args(["start", "recovery-1", "--plan-hash", "unsafe"])

    assert {:ok, {:start, "recovery-1", opts}} =
             Recover.parse_args([
               "start",
               "recovery-1",
               "--plan-hash",
               String.duplicate("a", 64)
             ])

    assert opts[:plan_hash] == String.duplicate("a", 64)
  end

  test "prints the evidence required before start" do
    hash = String.duplicate("a", 64)

    output =
      capture_io(fn ->
        Recover.print_plan(%{
          "plan_id" => "recovery-1",
          "plan_hash" => hash,
          "payload" => %{
            "target_id" => "asset:orders",
            "target_generation_id" => "generation-1",
            "materialization_id" => "materialization-1",
            "physical_fingerprint" => hash
          }
        })
      end)

    assert output =~ "Target: asset:orders"
    assert output =~ "Generation: generation-1"
    assert output =~ "Materialization: materialization-1"
    assert output =~ "Start only after review: mix favn.recover start recovery-1"
  end
end
