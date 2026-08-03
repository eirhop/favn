defmodule CrmDemo.Warehouse.ManifestTest do
  @moduledoc """
  Compile-time guarantees: what the project publishes, and the declarations that
  make it publishable. None of these tests touch DuckDB.
  """

  use ExUnit.Case, async: true

  alias CrmDemo.Warehouse.Core.Sales.Customers.Customer
  alias CrmDemo.Warehouse.Core.Sales.Events.Opportunity
  alias CrmDemo.Warehouse.Mart.Sales.ExecutiveOverview
  alias CrmDemo.Warehouse.Source.Crm.Customers.Account
  alias CrmDemo.Warehouse.Source.Crm.Events.Deal
  alias Favn.Manifest

  @runner_release_id "rr_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

  test "the project compiles into one manifest" do
    assert {:ok, %Manifest{} = manifest} =
             Favn.generate_manifest(runner_release_id: @runner_release_id)

    assert length(manifest.assets) == 17
    assert length(manifest.pipelines) == 3

    assert Enum.all?(
             Enum.filter(manifest.assets, &(&1.type == :sql)),
             &(&1.execution_pool == :duckdb)
           )
  end

  test "namespaces supply the relation each layer writes to" do
    # Each level comes from a different ancestor: the connection from
    # `CrmDemo.Warehouse`, the catalog from the phase namespace, the schema from
    # the domain namespace, and only the name from the leaf.
    assert {:ok, account} = Favn.get_asset(Account)
    assert account.relation.connection == :warehouse
    assert account.relation.catalog == "source"
    assert account.relation.schema == "crm"
    assert account.relation.name == "account"

    assert {:ok, customer} = Favn.get_asset(Customer)
    assert customer.relation.catalog == "core"
    assert customer.relation.schema == "sales"
  end

  test "source contracts end with the shared metadata fragment" do
    contract = published_contract(Deal)

    assert Enum.map(contract.columns, & &1.name) == [
             :deal_id,
             :account_id,
             :stage,
             :amount_cents,
             :occurred_at,
             :_landing_run_id,
             :_extracted_at,
             :_row_hash,
             :_favn_run_id
           ]

    assert contract.grain.by == [:deal_id]
    refute Enum.any?(contract.columns, & &1.nullable?)
  end

  test "windowed assets inherit the group's window and coverage" do
    assert {:ok, deal} = Favn.get_asset(Deal)
    assert deal.window_spec.kind == :day
    assert deal.coverage_spec.from == ~D[2026-07-22]

    assert deal.materialization ==
             {:incremental, strategy: :delete_insert, window_column: :occurred_at}

    assert {:ok, account} = Favn.get_asset(Account)
    assert account.window_spec == nil
    assert account.materialization == :table
  end

  test "source relations depend on landing, core depends on source" do
    assert {:ok, deal} = Favn.get_asset(Deal)
    assert deal.depends_on == [{CrmDemo.Landing.Crm.Daily, :deals}]

    assert {:ok, opportunity} = Favn.get_asset(Opportunity)
    assert Enum.sort(opportunity.depends_on) == [{Account, :asset}, {Deal, :asset}]
  end

  test "the view overrides the inherited table materialization" do
    assert {:ok, overview} = Favn.get_asset(ExecutiveOverview)
    assert overview.materialization == :view
    assert published_asset(ExecutiveOverview).assurance == nil
  end

  test "pipelines expose their windows and schedules" do
    assert {:ok, reference} = Favn.resolve_pipeline(CrmDemo.Pipelines.CrmReference)
    assert reference.pipeline.name == :crm_reference
    assert reference.pipeline.window == nil

    assert {:ok, daily} = Favn.resolve_pipeline(CrmDemo.Pipelines.CrmDaily)
    assert daily.pipeline.window.kind == :day
    assert match?({:inline, %{cron: "0 2 * * *"}}, daily.pipeline.schedule)
  end

  test "the manifest content hash survives JSON publication" do
    assert {:ok, build} = FavnAuthoring.build_manifest(runner_release_id: @runner_release_id)
    assert {:ok, pinned} = FavnAuthoring.pin_manifest_version(build.manifest)
    assert {:ok, hash} = FavnAuthoring.hash_manifest(build.manifest)
    assert hash == pinned.content_hash
  end

  defp published_asset(module) do
    assert {:ok, %Manifest{} = manifest} =
             Favn.generate_manifest(runner_release_id: @runner_release_id)

    Enum.find(manifest.assets, &(&1.ref == {module, :asset}))
  end

  defp published_contract(module), do: published_asset(module).assurance.contract
end
