defmodule FavnOrchestrator.TargetGenerationsTest do
  use ExUnit.Case, async: false

  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Index
  alias Favn.Manifest.Version
  alias Favn.Plan
  alias Favn.Retry.Policy
  alias FavnOrchestrator.Persistence.Results.EvidenceBinding
  alias FavnOrchestrator.Persistence.Runtime, as: PersistenceRuntime
  alias FavnOrchestrator.Persistence.Stores
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.TargetGenerations

  @evidence_generation_id "ag_#{String.duplicate("a", 64)}"

  defmodule FakeStore do
    def get_evidence_bindings(query) do
      send(self(), {:get_evidence_bindings, query.target_ids})
      bindings = Process.get(:evidence_bindings, %{})
      {:ok, Enum.flat_map(query.target_ids, &List.wrap(Map.get(bindings, &1)))}
    end
  end

  setup do
    stores =
      struct(Stores,
        registry: FakeStore,
        runs: FakeStore,
        run_ownership: FakeStore,
        scheduler: FakeStore,
        admission: FakeStore,
        resource_circuits: FakeStore,
        target_generations: FakeStore,
        rebuilds: FakeStore,
        target_operation_locks: FakeStore,
        materialization: FakeStore,
        backfills: FakeStore,
        operator_reads: FakeStore,
        logs: FakeStore,
        identity: FakeStore,
        maintenance: FakeStore
      )

    runtime = %PersistenceRuntime{backend: __MODULE__, options: [], stores: stores}
    start_supervised!({PersistenceRuntime, runtime})
    Process.put(:evidence_bindings, %{})

    :ok
  end

  test "pins output and upstream semantic generations into a normal plan" do
    upstream_ref = {MyApp.Raw, :orders}
    target_ref = {MyApp.Gold, :orders}
    upstream_key = {upstream_ref, nil}
    target_key = {target_ref, nil}

    manifest =
      %Manifest{
        assets: [asset(upstream_ref), asset(target_ref)]
      }
      |> FavnTestSupport.with_manifest_contract()
      |> FavnTestSupport.with_manifest_graph()

    assert {:ok, version} = Version.new(manifest, manifest_version_id: "manifest")
    assert {:ok, index} = Index.build_from_version(version)
    assert {:ok, context} = WorkspaceContext.new("workspace", "test", [:workspace_admin])
    bind_assets(version.manifest.assets, version.manifest_version_id)

    plan = %Plan{
      target_refs: [target_ref],
      target_node_keys: [target_key],
      nodes: %{
        upstream_key => node(upstream_ref, upstream_key, [], [target_key], 0),
        target_key => node(target_ref, target_key, [upstream_key], [], 1)
      },
      topo_order: [upstream_ref, target_ref],
      stages: [[upstream_ref], [target_ref]],
      node_stages: [[upstream_key], [target_key]]
    }

    assert {:ok, pinned} =
             TargetGenerations.pin_plan(context, version, index, plan, ~U[2026-07-22 10:00:00Z])

    upstream = pinned.nodes[upstream_key]
    target = pinned.nodes[target_key]

    assert upstream.target_id == Favn.TargetIdentity.for_asset(upstream_ref)
    assert upstream.target_generation_id == nil
    assert upstream.evidence_generation_id == @evidence_generation_id
    assert upstream.input_generations == []

    assert target.target_id == Favn.TargetIdentity.for_asset(target_ref)
    assert target.target_generation_id == nil
    assert target.evidence_generation_id == @evidence_generation_id

    assert [input] = target.input_generations
    assert input.target_id == upstream.target_id
    assert input.evidence_generation_id == upstream.evidence_generation_id

    required_generation =
      Map.take(target, [:target_id, :evidence_generation_id, :target_generation_id])

    assert {:ok, _plan} =
             TargetGenerations.pin_plan(
               context,
               version,
               index,
               plan,
               ~U[2026-07-22 10:00:00Z],
               required_generation: required_generation
             )

    stale_generation = %{required_generation | evidence_generation_id: "stale-generation"}

    assert {:error, :coverage_selection_stale} =
             TargetGenerations.pin_plan(
               context,
               version,
               index,
               plan,
               ~U[2026-07-22 10:00:00Z],
               required_generation: stale_generation
             )
  end

  test "retains evidence bindings when the manifest runner release changes" do
    ref = {MyApp.Source, :orders}
    key = {ref, nil}
    manifest = %Manifest{assets: [asset(ref)]}

    {:ok, first} =
      manifest
      |> FavnTestSupport.with_manifest_contract(FavnTestSupport.runner_release_id())
      |> FavnTestSupport.with_manifest_graph()
      |> Version.new(manifest_version_id: "manifest-first")

    {:ok, second} =
      manifest
      |> FavnTestSupport.with_manifest_contract(FavnTestSupport.runner_release_id(:alternate))
      |> FavnTestSupport.with_manifest_graph()
      |> Version.new(manifest_version_id: "manifest-second")

    refute hd(first.manifest.assets).semantic_generation_id ==
             hd(second.manifest.assets).semantic_generation_id

    bind_assets(first.manifest.assets, first.manifest_version_id)

    assert {:ok, context} = WorkspaceContext.new("workspace", "test", [:workspace_admin])

    plan = %Plan{
      target_refs: [ref],
      target_node_keys: [key],
      nodes: %{key => node(ref, key, [], [], 0)}
    }

    assert {:ok, first_index} = Index.build_from_version(first)
    assert {:ok, second_index} = Index.build_from_version(second)

    assert {:ok, first_plan} =
             TargetGenerations.pin_plan(
               context,
               first,
               first_index,
               plan,
               ~U[2026-07-22 10:00:00Z]
             )

    assert {:ok, second_plan} =
             TargetGenerations.pin_plan(
               context,
               second,
               second_index,
               plan,
               ~U[2026-07-22 10:01:00Z]
             )

    assert first_plan.nodes[key].evidence_generation_id == @evidence_generation_id
    assert second_plan.nodes[key].evidence_generation_id == @evidence_generation_id
  end

  defp asset({module, name} = ref) do
    %Asset{ref: ref, module: module, name: name, type: :elixir}
  end

  defp bind_assets(assets, manifest_version_id) do
    bindings =
      Map.new(assets, fn asset ->
        target_id = Favn.TargetIdentity.for_asset(asset.ref)

        {target_id,
         %EvidenceBinding{
           workspace_id: "workspace",
           target_id: target_id,
           evidence_generation_id: @evidence_generation_id,
           initial_manifest_id: manifest_version_id,
           created_at: ~U[2026-07-22 09:00:00Z]
         }}
      end)

    Process.put(:evidence_bindings, bindings)
  end

  defp node(ref, key, upstream, downstream, stage) do
    %{
      ref: ref,
      node_key: key,
      window: nil,
      upstream: upstream,
      downstream: downstream,
      stage: stage,
      execution_pool: nil,
      action: :run,
      retry_policy: Policy.default(),
      retry_policy_source: :default
    }
  end
end
