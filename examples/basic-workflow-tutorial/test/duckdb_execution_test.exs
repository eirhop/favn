defmodule FavnReferenceWorkload.DuckdbExecutionTest do
  use ExUnit.Case, async: false

  alias Favn.Run.Context
  alias Favn.SQLClient

  @duckdb_path ".favn/data/reference_workload.duckdb"
  @terminal_ref {FavnReferenceWorkload.Warehouse.Ops.ReferenceWorkloadComplete, :asset}

  setup do
    File.mkdir_p!(".favn/data")
    File.rm(@duckdb_path)

    on_exit(fn ->
      File.rm(@duckdb_path)
    end)

    :ok
  end

  test "executes the full reference workload graph" do
    refute relation_exists?("sources", "country_regions")
    refute relation_exists?("gold", "executive_overview")

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
    assert {:ok, 4} = relation_count("sources", "country_regions")
    assert {:ok, 4} = relation_count("sources", "channel_catalog")
    assert {:ok, 6} = relation_count("raw", "customers")
    assert {:ok, 7} = relation_count("stg", "order_facts")
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

  defp relation_exists?(schema, relation) do
    with {:ok, session} <- SQLClient.connect(:warehouse),
         {:ok, result} <-
           SQLClient.query(session, relation_exists_sql(), params: [schema, relation]) do
      SQLClient.disconnect(session)

      result.rows != []
    else
      {:error, reason} -> flunk("failed to inspect relation existence: #{inspect(reason)}")
    end
  end

  defp count_sql(schema, relation) do
    "select count(*) as row_count from #{schema}.#{relation}"
  end

  defp relation_exists_sql do
    """
    select 1
    from information_schema.tables
    where table_schema = ?
      and table_name = ?
    limit 1
    """
  end
end
