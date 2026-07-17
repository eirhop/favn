defmodule FavnOrchestrator.Operator.LineageTest do
  use ExUnit.Case, async: false

  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.ExecutionPackage
  alias Favn.Manifest.Graph
  alias Favn.Manifest.SQLExecution
  alias Favn.Manifest.Version
  alias Favn.SQL.Template
  alias FavnOrchestrator.Operator.Lineage
  alias FavnOrchestrator.Operator.Lineage.AssetInspector
  alias FavnOrchestrator.Operator.Lineage.EdgeInspector
  alias FavnOrchestrator.Operator.Lineage.GroupInspector
  alias FavnOrchestrator.Storage.Adapter.Memory

  setup do
    put_orchestrator_env(:storage_adapter, Memory)
    put_orchestrator_env(:storage_adapter_opts, [])
    Memory.reset()
    on_exit(&Memory.reset/0)

    version = manifest_version()
    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest(version.manifest_version_id)

    {:ok, version: version}
  end

  test "get_graph returns a bounded grouped left-to-right overview", %{version: version} do
    assert {:ok, graph} = Lineage.get_graph()

    assert graph.manifest_version_id == version.manifest_version_id
    assert graph.layout.direction == :left_to_right
    assert graph.summary.total_assets == 10
    assert graph.summary.visible_groups == 4

    assert github = Enum.find(graph.groups, &(&1.id == "group:raw:github"))
    assert github.label == "Github raw"
    assert github.asset_count == 6
    assert github.hidden_asset_count == 2
    assert github.status_counts.unknown == 6

    assert staging = Enum.find(graph.groups, &(&1.id == "group:staging:github:staging"))
    assert staging.label == "staging.github"

    assert edge = Enum.find(graph.edges, &(&1.from == github.id and &1.to == staging.id))
    assert edge.dependency_count == 2
    assert edge.aggregated?
  end

  test "inspectors are loaded through selected graph facades" do
    assert {:ok, %GroupInspector{} = inspector} = Lineage.get_group("group:raw:github")
    assert inspector.title == "Github raw"
    assert [%{dependency_count: 2, label: "staging.github"}] = inspector.downstream

    assert {:ok, graph} = Lineage.get_graph()

    edge =
      Enum.find(
        graph.edges,
        &(&1.from == "group:raw:github" and &1.to == "group:staging:github:staging")
      )

    assert {:ok, %EdgeInspector{} = edge_inspector} = Lineage.get_edge(edge.id)
    assert edge_inspector.edge.dependency_count == 2
    assert edge_inspector.upstream_label == "Github raw"
    assert edge_inspector.downstream_label == "staging.github"
  end

  test "hidden group assets are pageable, searchable, and inspectable" do
    assert {:ok, page} = Lineage.list_group_assets("group:raw:github", limit: 10)
    assert length(page.items) == 6
    assert Enum.any?(page.items, &(&1.label == "raw_users"))

    hidden_id = target_id(:raw_users)
    assert {:ok, asset_inspector} = Lineage.get_asset(hidden_id)
    assert asset_inspector.asset.label == "raw_users"

    assert {:ok, search_page} = Lineage.search("raw_users", limit: 5)
    assert Enum.any?(search_page.items, &(&1.id == hidden_id))
  end

  test "group and asset inspectors expose bounded adjacent groups" do
    version = fanout_manifest_version()
    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest(version.manifest_version_id)

    assert {:ok, %GroupInspector{} = group_inspector} =
             Lineage.get_group("group:raw:source", limit: [max_inspector_adjacent_groups: 2])

    assert length(group_inspector.downstream) == 2
    assert group_inspector.hidden_downstream_count == 2
    assert group_inspector.hidden_upstream_count == 0

    assert {:ok, %AssetInspector{} = asset_inspector} =
             Lineage.get_asset(target_id(:raw_events), limit: [max_inspector_adjacent_groups: 2])

    assert length(asset_inspector.downstream) == 2
    assert asset_inspector.hidden_downstream_count == 2
  end

  test "schemas on the same connection remain distinct groups" do
    assert {:ok, graph} = Lineage.get_graph()

    assert Enum.find(graph.groups, &(&1.id == "group:core:core"))
    assert Enum.find(graph.groups, &(&1.id == "group:core:finance"))
  end

  test "unsupported graph options fail explicitly" do
    assert {:error, %{code: :invalid_scope}} = Lineage.get_graph(view_mode: :upstream)
    assert {:error, %{code: :invalid_scope}} = Lineage.get_graph(filters: %{status: :failed})

    assert {:error, %{code: :invalid_request}} = Lineage.get_graph(unknown: true)

    assert {:error, %{code: :invalid_request}} =
             Lineage.get_graph(limit: [max_visible_groups: 0])

    assert {:error, %{code: :invalid_request}} = Lineage.search(String.duplicate("x", 513))
    assert {:error, %{code: :invalid_request}} = Lineage.get_group(nil)
  end

  test "asset previews honor the global visible-node budget" do
    assert {:ok, graph} =
             Lineage.get_graph(
               limit: [max_visible_asset_nodes: 2, max_preview_assets_per_group: 2]
             )

    assert Enum.sum(Enum.map(graph.groups, &length(&1.preview_assets))) == 2
    assert graph.summary.visible_assets == 2
    assert graph.summary.truncated?
  end

  test "group identifiers do not collapse punctuation-distinct schemas" do
    assets = [
      asset(:hyphenated, :sql, :warehouse, "core", "foo-bar", []),
      asset(:underscored, :sql, :warehouse, "core", "foo_bar", [])
    ]

    assert {:ok, graph} = Graph.build(assets)
    assert {:ok, version} = Version.new(%Manifest{assets: assets, graph: graph})
    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest(version.manifest_version_id)

    assert {:ok, lineage} = Lineage.get_graph()
    assert Enum.map(lineage.groups, & &1.id) |> Enum.uniq() |> length() == 2
  end

  test "search returns bounded group and asset results" do
    assert {:ok, page} = Lineage.search("orders", limit: 3)

    assert Enum.any?(page.items, &(&1.label == "stg_orders"))
    assert page.limit == 3
  end

  defp manifest_version do
    assets = [
      asset(:raw_comments, :source, :github, "raw", "raw", []),
      asset(:raw_issues, :source, :github, "raw", "raw", []),
      asset(:raw_labels, :source, :github, "raw", "raw", []),
      asset(:raw_pull_requests, :source, :github, "raw", "raw", []),
      asset(:raw_users, :source, :github, "raw", "raw", []),
      asset(:raw_workflows, :source, :github, "raw", "raw", []),
      asset(:stg_issues, :sql, :github, "staging", "staging", [{__MODULE__.Assets, :raw_issues}]),
      asset(:stg_orders, :sql, :github, "staging", "staging", [
        {__MODULE__.Assets, :raw_pull_requests}
      ]),
      asset(:fct_orders, :sql, :warehouse, "core", "core", [{__MODULE__.Assets, :stg_orders}]),
      asset(:fct_revenue, :sql, :warehouse, "core", "finance", [])
    ]

    assert {:ok, graph} = Graph.build(assets)

    manifest = %Manifest{assets: assets, graph: graph}
    assert {:ok, version} = Version.new(manifest, manifest_version_id: "mv_lineage_test")
    version
  end

  defp fanout_manifest_version do
    assets = [
      asset(:raw_events, :source, :source, "raw", "raw", []),
      asset(:domain_a_orders, :sql, :warehouse, "core", "domain_a", [
        {__MODULE__.Assets, :raw_events}
      ]),
      asset(:domain_b_orders, :sql, :warehouse, "core", "domain_b", [
        {__MODULE__.Assets, :raw_events}
      ]),
      asset(:domain_c_orders, :sql, :warehouse, "core", "domain_c", [
        {__MODULE__.Assets, :raw_events}
      ]),
      asset(:domain_d_orders, :sql, :warehouse, "core", "domain_d", [
        {__MODULE__.Assets, :raw_events}
      ])
    ]

    assert {:ok, graph} = Graph.build(assets)

    manifest = %Manifest{assets: assets, graph: graph}
    assert {:ok, version} = Version.new(manifest, manifest_version_id: "mv_lineage_fanout_test")
    version
  end

  defp asset(name, type, connection, catalog, schema, depends_on) do
    asset = %Asset{
      ref: {__MODULE__.Assets, name},
      module: __MODULE__.Assets,
      name: name,
      type: type,
      depends_on: depends_on,
      relation: %{
        connection: connection,
        catalog: catalog,
        schema: schema,
        name: Atom.to_string(name)
      }
    }

    maybe_attach_execution_package(asset)
  end

  defp maybe_attach_execution_package(%Asset{type: :sql, ref: ref} = asset) do
    sql = "SELECT 1 AS id"

    template =
      Template.compile!(sql,
        file: "test/operator_lineage_package.sql",
        line: 1,
        module: __MODULE__,
        scope: :query,
        enforce_query_root: true
      )

    {:ok, package} = ExecutionPackage.new(ref, %SQLExecution{sql: sql, template: template})
    :ok = FavnOrchestrator.register_execution_packages([package])
    %{asset | execution_package_hash: package.content_hash}
  end

  defp maybe_attach_execution_package(%Asset{} = asset), do: asset

  defp target_id(name), do: "asset:#{Atom.to_string(__MODULE__.Assets)}:#{name}"

  defp put_orchestrator_env(key, value) do
    original = Application.get_env(:favn_orchestrator, key, :__missing__)
    Application.put_env(:favn_orchestrator, key, value)

    on_exit(fn ->
      case original do
        :__missing__ -> Application.delete_env(:favn_orchestrator, key)
        value -> Application.put_env(:favn_orchestrator, key, value)
      end
    end)
  end
end
