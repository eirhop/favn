defmodule CrmDemo.Warehouse.Source.Crm.InputsTest do
  use ExUnit.Case, async: false

  alias CrmDemo.Landing.Crm.{Daily, Snapshots}
  alias CrmDemo.RunContext
  alias CrmDemo.Support.Landing.Storage
  alias CrmDemo.Warehouse.Source.Crm.Customers.Account
  alias CrmDemo.Warehouse.Source.Crm.Events.Deal
  alias CrmDemo.Warehouse.Source.Crm.Inputs

  setup do
    File.rm_rf!(Storage.root())
    :ok
  end

  test "resolves the completed snapshot into bind values" do
    assert {:ok, landed} = Snapshots.asset(RunContext.new({Snapshots, :accounts}))

    assert {:ok, resolved} = Inputs.resolve(RunContext.new(Account))
    assert resolved.identity == landed.landing_run_id
    assert resolved.params.landing_run_id == landed.landing_run_id
    assert resolved.params.expected_row_count == 4
    assert Jason.decode!(resolved.params.files_json) |> length() == 2
    assert resolved.metadata == %{dataset: "accounts", part_count: 2, rows_landed: 4}
  end

  test "resolves the manifest matching the requested window" do
    for date <- [~D[2026-07-22], ~D[2026-07-23]] do
      ctx = RunContext.new({Daily, :deals}, window: RunContext.day(date), run_id: "run_#{date}")
      assert {:ok, _landed} = Daily.asset(ctx)
    end

    window = RunContext.day(~D[2026-07-22])
    assert {:ok, resolved} = Inputs.resolve(RunContext.new(Deal, window: window))
    assert resolved.params.expected_row_count == 2
  end

  test "refuses to publish when no landing run finished" do
    assert_raise RuntimeError, ~r/no completed landing run for accounts/, fn ->
      Inputs.resolve(RunContext.new(Account))
    end
  end
end
