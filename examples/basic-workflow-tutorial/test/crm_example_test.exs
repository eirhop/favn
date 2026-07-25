defmodule FavnReferenceWorkload.CRMExampleTest do
  use ExUnit.Case, async: false

  alias Favn.Run.Context
  alias Favn.SQLClient
  alias Favn.Window.{Key, Runtime}
  alias FavnReferenceWorkload.{CRMData, LandingFiles}

  setup do
    File.rm_rf!(Path.expand(".data/generic_crm"))
    File.rm(Path.expand("generic_crm.duckdb"))

    on_exit(fn ->
      File.rm_rf!(Path.expand(".data/generic_crm"))
      File.rm(Path.expand("generic_crm.duckdb"))
    end)

    :ok
  end

  test "seed data is deterministic and compact JSON compatible" do
    assert CRMData.seed() == CRMData.seed()

    encoded = Jason.encode!(CRMData.seed())
    refute encoded =~ "\n"
    assert encoded =~ "Northwind Labs"
    assert encoded =~ "deal_003"
  end

  test "landing writer creates the documented fixed-path files" do
    assert :ok = LandingFiles.write_seed!(CRMData.seed())
    assert :ok = LandingFiles.write_entity!(:accounts, CRMData.seed().accounts)

    assert File.exists?(LandingFiles.seed_path())
    assert File.exists?(LandingFiles.entity_path(:accounts))
    assert File.read!(LandingFiles.entity_path(:accounts)) =~ "acct_001"
  end

  test "manifest exposes full-refresh and daily-window CRM assets" do
    assert {:ok, seed} =
             Favn.get_asset({FavnReferenceWorkload.Warehouse.Landing.GenerateSeed, :asset})

    assert seed.type == :elixir
    assert seed.freshness != nil

    assert {:ok, daily} =
             Favn.get_asset({FavnReferenceWorkload.Warehouse.Source.DealsDaily, :asset})

    assert daily.type == :sql

    assert daily.materialization ==
             {:incremental, strategy: :delete_insert, window_column: :occurred_at}

    assert daily.window_spec.kind == :day

    assert {:ok, pipeline} =
             Favn.resolve_pipeline(FavnReferenceWorkload.Pipelines.DailyCrmAnalytics)

    assert pipeline.pipeline.name == :daily_crm_analytics
    assert pipeline.pipeline.window.kind == :day
  end

  test "materializes exact CRM marts and rejects invalid source rows" do
    seed = CRMData.seed()
    assert :ok = LandingFiles.write_seed!(seed)

    Enum.each(seed, fn {entity, rows} ->
      assert :ok = LandingFiles.write_entity!(entity, rows)
    end)

    window = daily_window(~U[2026-07-23 00:00:00Z])

    Enum.each(
      [
        FavnReferenceWorkload.Warehouse.Source.Accounts,
        FavnReferenceWorkload.Warehouse.Source.Contacts,
        FavnReferenceWorkload.Warehouse.Source.DealsDaily,
        FavnReferenceWorkload.Warehouse.Source.ActivitiesDaily,
        FavnReferenceWorkload.Warehouse.Core.Customers,
        FavnReferenceWorkload.Warehouse.Core.OpportunitiesDaily,
        FavnReferenceWorkload.Warehouse.Core.ActivitiesDaily,
        FavnReferenceWorkload.Warehouse.Mart.AccountHealth,
        FavnReferenceWorkload.Warehouse.Mart.PipelineDaily,
        FavnReferenceWorkload.Warehouse.Mart.ExecutiveOverview
      ],
      &assert_materialized(&1, window)
    )

    assert query_rows("""
           select
             customer_count,
             cast(deal_count as bigint) as deal_count,
             cast(pipeline_amount_cents as bigint) as pipeline_amount_cents
           from mart.executive_overview
           """) == [
             %{"customer_count" => 3, "deal_count" => 2, "pipeline_amount_cents" => 53_000}
           ]

    assert query_rows("""
           select
             stage,
             cast(deal_count as bigint) as deal_count,
             cast(pipeline_amount_cents as bigint) as pipeline_amount_cents
           from mart.pipeline_daily
           order by stage
           """) == [
             %{"stage" => "proposal", "deal_count" => 1, "pipeline_amount_cents" => 45_000},
             %{"stage" => "won", "deal_count" => 1, "pipeline_amount_cents" => 8_000}
           ]

    invalid_activity = %{
      activity_id: nil,
      account_id: "acct_002",
      activity_type: "email",
      occurred_at: "2026-07-23T09:30:00Z"
    }

    assert :ok = LandingFiles.write_entity!(:activities, [invalid_activity])

    assert {:error, error} =
             materialize(FavnReferenceWorkload.Warehouse.Source.ActivitiesDaily, window)

    assert error.type == :check_failed
    assert error.phase == :before_materialize
    assert error.message =~ "required_keys_are_present"
  end

  defp assert_materialized(module, window) do
    assert {:ok, _result} = materialize(module, window)
  end

  defp materialize(module, window) do
    with {:ok, asset} <- Favn.get_asset(module) do
      context = %Context{asset: asset, window: window}
      Favn.SQLAsset.Runtime.run(module, context)
    end
  end

  defp query_rows(sql) do
    assert {:ok, session} = SQLClient.connect(:warehouse)

    try do
      assert {:ok, result} = SQLClient.query(session, sql, [])
      result.rows
    after
      SQLClient.disconnect(session)
    end
  end

  defp daily_window(start_at) do
    Runtime.new!(
      :day,
      start_at,
      DateTime.add(start_at, 1, :day),
      Key.new!(:day, start_at, "Etc/UTC")
    )
  end
end
