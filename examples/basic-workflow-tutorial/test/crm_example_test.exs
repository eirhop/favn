defmodule FavnReferenceWorkload.CRMExampleTest do
  use ExUnit.Case, async: false

  alias FavnReferenceWorkload.{CRMData, LandingFiles}

  setup do
    File.rm_rf!(Path.expand(".data/generic_crm"))

    on_exit(fn ->
      File.rm_rf!(Path.expand(".data/generic_crm"))
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
end
