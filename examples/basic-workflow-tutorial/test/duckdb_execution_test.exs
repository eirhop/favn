defmodule FavnReferenceWorkload.DuckdbExecutionTest do
  use ExUnit.Case, async: false

  alias Favn.Run.Context
  alias Favn.SQLClient

  @duckdb_path ".favn/data/reference_workload.duckdb"
  @terminal_ref {FavnReferenceWorkload.Warehouse.Ops.ReferenceWorkloadComplete, :asset}

  setup do
    File.mkdir_p!(".favn/data")
    File.rm(@duckdb_path)

    :ok = bootstrap_sources()

    on_exit(fn ->
      File.rm(@duckdb_path)
    end)

    :ok
  end

  test "executes the full reference workload graph" do
    assert {:ok, plan} = Favn.plan_asset_run(@terminal_ref)

    Enum.each(plan.topo_order, fn ref ->
      assert {:ok, asset} = Favn.get_asset(ref)

      case asset.type do
        :source ->
          :ok

        :sql ->
          assert {:ok, _result} = Favn.materialize(asset)

        :elixir ->
          ctx = %Context{asset: %{relation: asset.relation}}
          assert :ok = apply(asset.module, :asset, [ctx])
      end
    end)

    assert {:ok, 1} = relation_count("gold", "executive_overview")
    assert {:ok, 6} = relation_count("raw", "customers")
    assert {:ok, 7} = relation_count("stg", "order_facts")
  end

  defp bootstrap_sources do
    with {:ok, session} <- SQLClient.connect(:warehouse),
         {:ok, _} <- SQLClient.query(session, "create schema if not exists sources", []),
         {:ok, _} <- SQLClient.query(session, "create schema if not exists raw", []),
         {:ok, _} <- SQLClient.query(session, "create schema if not exists stg", []),
         {:ok, _} <- SQLClient.query(session, "create schema if not exists gold", []),
         {:ok, _} <- SQLClient.query(session, country_regions_sql(), []),
         {:ok, _} <- SQLClient.query(session, channel_catalog_sql(), []) do
      SQLClient.disconnect(session)
      :ok
    else
      {:error, reason} ->
        flunk("failed to bootstrap reference workload sources: #{inspect(reason)}")
    end
  end

  defp relation_count(schema, relation) do
    with {:ok, session} <- SQLClient.connect(:warehouse),
         {:ok, result} <- SQLClient.query(session, count_sql(schema, relation), []) do
      SQLClient.disconnect(session)

      count =
        result.rows
        |> List.first()
        |> Map.get("row_count")

      {:ok, count}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  defp country_regions_sql do
    """
    create or replace table sources.country_regions as
    select region_code
    from (
      values ('nordic'), ('dach'), ('uk_ie'), ('southern_eu')
    ) as t(region_code)
    """
  end

  defp channel_catalog_sql do
    """
    create or replace table sources.channel_catalog as
    select channel_code
    from (
      values ('organic_search'), ('paid_social'), ('email'), ('affiliate')
    ) as t(channel_code)
    """
  end

  defp count_sql(schema, relation) do
    "select count(*) as row_count from #{schema}.#{relation}"
  end
end
