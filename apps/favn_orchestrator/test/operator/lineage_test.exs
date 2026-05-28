defmodule FavnOrchestrator.Operator.LineageTest do
  use ExUnit.Case, async: false

  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Graph
  alias Favn.Manifest.Version
  alias FavnOrchestrator.Operator.Lineage
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

  test "schemas on the same connection remain distinct groups" do
    assert {:ok, graph} = Lineage.get_graph()

    assert Enum.find(graph.groups, &(&1.id == "group:core:core"))
    assert Enum.find(graph.groups, &(&1.id == "group:core:finance"))
  end

  test "unsupported graph options fail explicitly" do
    assert {:error, %{code: :invalid_scope}} = Lineage.get_graph(view_mode: :upstream)
    assert {:error, %{code: :invalid_scope}} = Lineage.get_graph(filters: %{status: :failed})
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

  defp asset(name, type, connection, catalog, schema, depends_on) do
    %Asset{
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
  end

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
