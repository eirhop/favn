defmodule CrmDemo.Warehouse.MaterializationTest do
  @moduledoc """
  Runs the whole project against a real DuckDB file: land, publish, model,
  report.
  """

  use ExUnit.Case, async: false

  alias CrmDemo.Landing.Crm.{Daily, Snapshots}
  alias CrmDemo.RunContext
  alias CrmDemo.Support.Landing.Storage
  alias CrmDemo.Warehouse.Core.Sales.Customers.Customer
  alias CrmDemo.Warehouse.Core.Sales.Events.{Engagement, Opportunity}
  alias CrmDemo.Warehouse.Mart.Sales.{AccountHealth, ExecutiveOverview, PipelineDaily}
  alias CrmDemo.Warehouse.Source.Crm.Customers.{Account, Contact}
  alias CrmDemo.Warehouse.Source.Crm.Events.{Activity, Deal}
  alias Favn.SQLClient

  @reference [Account, Contact, Customer, AccountHealth]
  @daily [Deal, Activity, Opportunity, Engagement, PipelineDaily, ExecutiveOverview]

  setup do
    File.rm_rf!(Storage.root())
    File.rm(database_path())

    on_exit(fn ->
      File.rm_rf!(Storage.root())
      File.rm(database_path())
    end)

    :ok
  end

  test "publishes marts a report can query" do
    window = RunContext.day(~D[2026-07-23])
    land_reference()
    land_daily(window)

    Enum.each(@reference, &assert_published(&1, nil))
    Enum.each(@daily, &assert_published(&1, window))

    assert query("""
           select customer_id, contact_count, health_status
           from mart.account_health
           order by customer_id
           """) == [
             %{"customer_id" => "acct_001", "contact_count" => 2, "health_status" => "engaged"},
             %{
               "customer_id" => "acct_002",
               "contact_count" => 1,
               "health_status" => "needs_attention"
             },
             %{"customer_id" => "acct_003", "contact_count" => 2, "health_status" => "engaged"},
             %{
               "customer_id" => "acct_004",
               "contact_count" => 0,
               "health_status" => "needs_attention"
             }
           ]

    assert query("""
           select stage, deal_count, cast(pipeline_amount_cents as bigint) as amount
           from mart.pipeline_daily
           order by stage
           """) == [
             %{"stage" => "proposal", "deal_count" => 1, "amount" => 45_000},
             %{"stage" => "qualified", "deal_count" => 1, "amount" => 21_500},
             %{"stage" => "won", "deal_count" => 1, "amount" => 8_000}
           ]

    assert query("""
           select
             cast(deal_count as bigint) as deal_count,
             cast(pipeline_amount_cents as bigint) as amount,
             customer_count,
             engaged_count
           from mart.executive_overview
           """) == [
             %{"deal_count" => 3, "amount" => 74_500, "customer_count" => 4, "engaged_count" => 2}
           ]
  end

  test "each landed row carries its landing run and a row hash" do
    land_reference()
    Enum.each(@reference, &assert_published(&1, nil))

    assert [%{"runs" => 1, "hashes" => 4}] =
             query("""
             select
               count(distinct _landing_run_id) as runs,
               count(distinct _row_hash) as hashes
             from source.account
             """)
  end

  test "a second day replaces only its own window" do
    first = RunContext.day(~D[2026-07-22])
    second = RunContext.day(~D[2026-07-23])

    land_reference()
    Enum.each(@reference, &assert_published(&1, nil))

    Enum.each([first, second], fn window ->
      land_daily(window)
      Enum.each(@daily, &assert_published(&1, window))
    end)

    assert query("select snapshot_date, deal_count from mart.pipeline_daily order by 1, 2") == [
             %{"snapshot_date" => ~D[2026-07-22], "deal_count" => 1},
             %{"snapshot_date" => ~D[2026-07-22], "deal_count" => 1},
             %{"snapshot_date" => ~D[2026-07-23], "deal_count" => 1},
             %{"snapshot_date" => ~D[2026-07-23], "deal_count" => 1},
             %{"snapshot_date" => ~D[2026-07-23], "deal_count" => 1}
           ]
  end

  test "a contract violation fails the asset and leaves the target untouched" do
    land_reference()
    assert_published(Account, nil)

    # Landing wrote four accounts; the contract reconciles against that count, so
    # a manifest claiming a different number must fail before any write.
    manifest = Storage.latest_manifest!("accounts", nil)
    File.write!(manifest_path("accounts", manifest.landing_run_id), tampered(manifest))

    assert {:error, error} = publish(Account, nil)
    assert error.type == :check_failed
    assert error.phase == :before_materialize

    assert query("select count(*) as n from source.account") == [%{"n" => 4}]
  end

  defp land_reference do
    assert {:ok, _} = Snapshots.asset(RunContext.new({Snapshots, :accounts}))
    assert {:ok, _} = Snapshots.asset(RunContext.new({Snapshots, :contacts}))
  end

  defp land_daily(window) do
    run_id = "run_#{DateTime.to_date(window.start_at)}"

    for child <- [:deals, :activities] do
      assert {:ok, _} =
               Daily.asset(RunContext.new({Daily, child}, window: window, run_id: run_id))
    end
  end

  defp assert_published(module, window) do
    assert {:ok, _result} = publish(module, window), "failed to publish #{inspect(module)}"
  end

  defp publish(module, window) do
    Favn.SQLAsset.Runtime.run(module, RunContext.new(module, window: window))
  end

  defp manifest_path(dataset, landing_run_id) do
    Path.join(Storage.run_dir(dataset, landing_run_id), "_manifest.json")
  end

  defp tampered(manifest) do
    CrmDemo.Support.Landing.Manifest.encode!(%{manifest | row_count: 99})
  end

  defp query(sql) do
    assert {:ok, session} = SQLClient.connect(:warehouse)

    try do
      assert {:ok, result} = SQLClient.query(session, sql, [])
      result.rows
    after
      SQLClient.disconnect(session)
    end
  end

  defp database_path do
    :favn
    |> Application.fetch_env!(:connections)
    |> Keyword.fetch!(:warehouse)
    |> Keyword.fetch!(:open)
    |> Keyword.fetch!(:database)
    |> Path.expand()
  end
end
