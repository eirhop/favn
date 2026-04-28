defmodule FavnReferenceWorkload.DuckdbExecutionTest do
  use ExUnit.Case, async: false

  alias Favn.Run.Context
  alias Favn.SQLClient
  alias Favn.Contracts.RunnerWork

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
          ctx = %Context{asset: %{relation: asset.relation}, config: source_config()}
          assert_successful_asset_return(apply(asset.module, :asset, [ctx]))
      end
    end)

    assert {:ok, 1} = relation_count("gold", "executive_overview")
    assert {:ok, 4} = relation_count("sources", "country_regions")
    assert {:ok, 4} = relation_count("sources", "channel_catalog")
    assert {:ok, 6} = relation_count("raw", "customers")
    assert {:ok, 7} = relation_count("stg", "order_facts")
  end

  test "raw orders source-system asset returns structured landing metadata" do
    with_source_env("northbeam-private-segment", "private-token", fn ->
      assert :ok = prepare_order_prerequisites()
      assert {:ok, result} = run_orders_asset()

      assert result.status == :ok
      assert [%{meta: meta}] = result.asset_results

      assert meta.rows_written == 6
      assert meta.mode == :full_refresh
      assert meta.relation == "raw.orders"
      assert %DateTime{} = meta.loaded_at
      assert meta.source.system == :reference_source
      assert meta.source.segment_id_hash == hash_identity("northbeam-private-segment")
      refute inspect(result) =~ "northbeam-private-segment"
      refute inspect(result) =~ "private-token"

      assert {:ok, 6} = relation_count("raw", "orders")
      refute raw_orders_contains_value?("northbeam-private-segment")
      refute raw_orders_has_column?("source_segment_id")
    end)
  end

  test "raw orders source config fails before source fetch when required env is missing" do
    with_source_env(nil, "private-token", fn ->
      assert {:ok, result} = run_orders_asset()

      assert result.status == :error
      assert [%{error: error}] = result.asset_results
      assert error.type == :missing_env
      assert error.message == "missing_env FAVN_REFERENCE_SOURCE_SEGMENT_ID"
    end)
  end

  test "raw orders source client failures are returned as asset failure diagnostics" do
    with_source_env("source-failure", "private-token", fn ->
      assert :ok = prepare_order_prerequisites()
      assert {:ok, result} = run_orders_asset()

      assert result.status == :error
      assert [%{error: error}] = result.asset_results
      assert error.reason == {:source_unavailable, :orders}
      refute inspect(result) =~ "private-token"
    end)
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

  defp prepare_order_prerequisites do
    for ref <- [
          {FavnReferenceWorkload.Warehouse.Sources.CountryRegions, :asset},
          {FavnReferenceWorkload.Warehouse.Sources.ChannelCatalog, :asset},
          {FavnReferenceWorkload.Warehouse.Raw.Customers, :asset}
        ] do
      assert {:ok, asset} = Favn.get_asset(ref)

      case asset.type do
        :sql ->
          assert {:ok, _result} = Favn.materialize(asset)

        :elixir ->
          ctx = %Context{asset: %{relation: asset.relation}, config: source_config()}
          assert_successful_asset_return(apply(asset.module, :asset, [ctx]))
      end
    end

    :ok
  end

  defp run_orders_asset do
    {:ok, _started} = Application.ensure_all_started(:favn_runner)
    {:ok, manifest} = Favn.generate_manifest()
    {:ok, version} = Favn.pin_manifest_version(manifest)
    :ok = FavnRunner.register_manifest(version)

    FavnRunner.run(
      %RunnerWork{
        run_id: "raw-orders-#{System.unique_integer([:positive])}",
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        asset_ref: {FavnReferenceWorkload.Warehouse.Raw.Orders, :asset}
      },
      timeout: 15_000
    )
  end

  defp raw_orders_contains_value?(value) do
    with {:ok, session} <- SQLClient.connect(:warehouse),
         {:ok, result} <-
           SQLClient.query(
             session,
             """
             select 1
             from raw.orders
             where cast(order_id as varchar) = ?
                or cast(customer_id as varchar) = ?
                or channel_code = ?
                or cast(order_date as varchar) = ?
             limit 1
             """,
             params: List.duplicate(value, 4)
           ) do
      SQLClient.disconnect(session)
      result.rows != []
    else
      {:error, reason} -> flunk("failed to inspect raw orders values: #{inspect(reason)}")
    end
  end

  defp raw_orders_has_column?(column_name) do
    with {:ok, session} <- SQLClient.connect(:warehouse),
         {:ok, result} <-
           SQLClient.query(
             session,
             "select 1 from information_schema.columns where table_schema = ? and table_name = ? and column_name = ? limit 1",
             params: ["raw", "orders", column_name]
           ) do
      SQLClient.disconnect(session)
      result.rows != []
    else
      {:error, reason} -> flunk("failed to inspect raw orders columns: #{inspect(reason)}")
    end
  end

  defp assert_successful_asset_return(:ok), do: :ok
  defp assert_successful_asset_return({:ok, meta}) when is_map(meta), do: :ok

  defp source_config do
    %{source_system: %{segment_id: "northbeam-demo-segment", token: "direct-test-token"}}
  end

  defp with_source_env(segment_id, token, fun) do
    previous_segment = System.get_env("FAVN_REFERENCE_SOURCE_SEGMENT_ID")
    previous_token = System.get_env("FAVN_REFERENCE_SOURCE_TOKEN")

    try do
      put_or_delete_env("FAVN_REFERENCE_SOURCE_SEGMENT_ID", segment_id)
      put_or_delete_env("FAVN_REFERENCE_SOURCE_TOKEN", token)
      fun.()
    after
      put_or_delete_env("FAVN_REFERENCE_SOURCE_SEGMENT_ID", previous_segment)
      put_or_delete_env("FAVN_REFERENCE_SOURCE_TOKEN", previous_token)
    end
  end

  defp put_or_delete_env(key, nil), do: System.delete_env(key)
  defp put_or_delete_env(key, value), do: System.put_env(key, value)

  defp hash_identity(value) do
    :sha256
    |> :crypto.hash(to_string(value))
    |> Base.encode16(case: :lower)
  end
end
