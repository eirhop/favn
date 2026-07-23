defmodule FavnOrchestrator.TargetCompatibilityPlannerTest do
  use ExUnit.Case, async: false

  alias Favn.Contracts.RelationInspectionResult
  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Version
  alias Favn.RelationRef
  alias Favn.TargetCompatibility.PhysicalFingerprint
  alias FavnOrchestrator.Lifecycle
  alias FavnOrchestrator.Persistence.DeploymentPlanner
  alias FavnOrchestrator.Persistence.PlatformContext
  alias FavnOrchestrator.Persistence.Results.TargetBinding
  alias FavnOrchestrator.Persistence.Runtime, as: PersistenceRuntime
  alias FavnOrchestrator.Persistence.Stores
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.TargetCompatibilityPlanner

  @now ~U[2026-07-22 12:00:00Z]

  defmodule RunnerClient do
    def ensure_manifest(version, opts) do
      send(Keyword.fetch!(opts, :test_pid), {:ensure_manifest, version.manifest_version_id})
      Application.get_env(:favn_orchestrator, :compatibility_test_ensure_result, :ok)
    end

    def register_manifest(version, opts) do
      send(Keyword.fetch!(opts, :test_pid), {:register_manifest, version.manifest_version_id})
      :ok
    end

    def inspect_relation(request, opts) do
      send(Keyword.fetch!(opts, :test_pid), {:inspect_relation, request})
      {:ok, Application.fetch_env!(:favn_orchestrator, :compatibility_test_inspection)}
    end
  end

  defmodule Store do
    def get_bindings(_query),
      do: {:ok, Application.get_env(:favn_orchestrator, :compatibility_test_bindings, [])}

    def get_manifest(query) do
      versions = Application.fetch_env!(:favn_orchestrator, :compatibility_test_versions)

      case Map.fetch(versions, query.manifest_version_id) do
        {:ok, version} -> {:ok, version}
        :error -> {:error, :manifest_not_found}
      end
    end
  end

  setup do
    previous =
      for key <- [
            :runner_client,
            :runner_client_opts,
            :compatibility_test_inspection,
            :compatibility_test_bindings,
            :compatibility_test_versions,
            :compatibility_test_ensure_result
          ],
          into: %{},
          do: {key, Application.get_env(:favn_orchestrator, key, :missing)}

    Application.put_env(:favn_orchestrator, :runner_client, RunnerClient)
    Application.put_env(:favn_orchestrator, :runner_client_opts, test_pid: self())

    stores =
      struct(Stores,
        registry: Store,
        runs: Store,
        run_ownership: Store,
        scheduler: Store,
        admission: Store,
        resource_circuits: Store,
        target_generations: Store,
        materialization: Store,
        backfills: Store,
        operator_reads: Store,
        logs: Store,
        identity: Store,
        maintenance: Store
      )

    runtime = %PersistenceRuntime{backend: __MODULE__, options: [], stores: stores}
    start_supervised!({PersistenceRuntime, runtime})

    on_exit(fn ->
      Enum.each(previous, fn
        {key, :missing} -> Application.delete_env(:favn_orchestrator, key)
        {key, value} -> Application.put_env(:favn_orchestrator, key, value)
      end)
    end)

    {:ok, platform_context} =
      PlatformContext.new("planner-test", "planner-grant", [:platform_reader])

    {:ok, workspace_context} =
      WorkspaceContext.new("workspace", "planner-test", [:workspace_admin])

    {:ok, platform_context: platform_context, workspace_context: workspace_context}
  end

  test "classifies a missing unbound relation as uninitialized", contexts do
    {version, asset} = persisted_version("manifest-uninitialized")
    put_versions([version])
    Application.put_env(:favn_orchestrator, :compatibility_test_bindings, [])
    Application.put_env(:favn_orchestrator, :compatibility_test_inspection, inspection(version))

    assert {:ok, [decision]} = plan(version, asset, contexts)
    assert decision.target_id == asset.target_descriptor.target_id
    assert decision.compatibility_status == :uninitialized
    assert decision.reason_code == "no_active_generation"
    assert is_nil(decision.active_physical_fingerprint)

    assert_received {:inspect_relation, request}
    assert request.asset_ref == asset.ref
    assert request.sample_limit == 0
  end

  test "keeps compatibility inspection inside the caller's maintenance admission", contexts do
    {version, asset} = persisted_version("manifest-maintenance-inspection")
    put_versions([version])
    Application.put_env(:favn_orchestrator, :compatibility_test_bindings, [])
    Application.put_env(:favn_orchestrator, :compatibility_test_inspection, inspection(version))

    token = :crypto.strong_rand_bytes(32) |> Base.url_encode64(padding: false)
    assert {:ok, ^token} = Lifecycle.begin_maintenance(:runner_replacement, token)
    assert {:ok, permit} = Lifecycle.acquire_maintenance_admission(token)

    try do
      assert {:ok, [decision]} = plan(version, asset, contexts)
      assert decision.compatibility_status == :uninitialized
      assert_received {:inspect_relation, _request}
    after
      assert :ok = Lifecycle.release_admission(permit)
      assert :ok = Lifecycle.end_maintenance(token)
    end
  end

  test "does not adopt an observed drift fingerprint", contexts do
    {version, asset} = persisted_version("manifest-active")

    {:ok, recorded} =
      PhysicalFingerprint.new(
        adapter: FavnTestSupport.TargetAdapter,
        relation: %{schema: "gold", name: "sales_summary", type: :table},
        columns: [%{name: "id", data_type: "BIGINT", nullable?: false}]
      )

    binding = %TargetBinding{
      workspace_id: contexts.workspace_context.workspace_id,
      target_id: asset.target_descriptor.target_id,
      active_generation_id: Ecto.UUID.generate(),
      active_manifest_id: version.manifest_version_id,
      active_descriptor_hash: asset.target_descriptor.descriptor_hash,
      desired_manifest_id: version.manifest_version_id,
      desired_descriptor_hash: asset.target_descriptor.descriptor_hash,
      compatibility_status: :ready,
      reason_code: "compatible",
      compatibility_diff: %{},
      active_physical_fingerprint: recorded.fingerprint,
      version: 3,
      updated_at: @now
    }

    put_versions([version])
    Application.put_env(:favn_orchestrator, :compatibility_test_bindings, [binding])

    Application.put_env(
      :favn_orchestrator,
      :compatibility_test_inspection,
      inspection(version,
        relation: %{catalog: nil, schema: "gold", name: "sales_summary", type: :table},
        columns: [%{name: "id", data_type: "VARCHAR", nullable?: false}]
      )
    )

    assert {:ok, [decision]} = plan(version, asset, contexts)
    assert decision.compatibility_status == :unexpected_drift
    assert decision.reason_code == "physical_fingerprint_mismatch"
    assert decision.active_physical_fingerprint == recorded.fingerprint
    assert decision.expected_binding_version == 3
    assert decision.expected_active_generation_id == binding.active_generation_id
  end

  test "inspects the active relation when the desired relation changes", contexts do
    {active_version, active_asset} =
      persisted_version("manifest-before-relation-change", "sales_summary")

    {desired_version, desired_asset} =
      persisted_version("manifest-after-relation-change", "sales_summary_v2")

    observed =
      inspection(active_version,
        relation: %{catalog: nil, schema: "gold", name: "sales_summary", type: :table}
      )

    {:ok, recorded} = PhysicalFingerprint.from_inspection(observed)

    binding = %TargetBinding{
      workspace_id: contexts.workspace_context.workspace_id,
      target_id: active_asset.target_descriptor.target_id,
      active_generation_id: Ecto.UUID.generate(),
      active_manifest_id: active_version.manifest_version_id,
      active_descriptor_hash: active_asset.target_descriptor.descriptor_hash,
      desired_manifest_id: active_version.manifest_version_id,
      desired_descriptor_hash: active_asset.target_descriptor.descriptor_hash,
      compatibility_status: :ready,
      reason_code: "compatible",
      compatibility_diff: %{},
      active_physical_fingerprint: recorded.fingerprint,
      version: 4,
      updated_at: @now
    }

    put_versions([active_version, desired_version])
    Application.put_env(:favn_orchestrator, :compatibility_test_bindings, [binding])
    Application.put_env(:favn_orchestrator, :compatibility_test_inspection, observed)
    Application.put_env(:favn_orchestrator, :compatibility_test_ensure_result, :missing)

    assert {:ok, [decision]} = plan(desired_version, desired_asset, contexts)
    assert decision.compatibility_status == :rebuild_required
    assert decision.reason_code == "incompatible_descriptor"

    assert_received {:ensure_manifest, active_manifest_id}
    assert active_manifest_id == active_version.manifest_version_id
    assert_received {:register_manifest, ^active_manifest_id}
    assert_received {:inspect_relation, request}
    assert request.manifest_version_id == active_version.manifest_version_id
    assert request.asset_ref == active_asset.ref
  end

  test "runner release changes inspect compatible targets through the desired manifest",
       contexts do
    {active_version, active_asset} = persisted_version("manifest-before-runner-change")

    {desired_version, desired_asset} =
      persisted_version(
        "manifest-after-runner-change",
        "sales_summary",
        FavnTestSupport.runner_release_id(:alternate)
      )

    observed =
      inspection(desired_version,
        relation: %{catalog: nil, schema: "gold", name: "sales_summary", type: :table}
      )

    {:ok, recorded} = PhysicalFingerprint.from_inspection(observed)

    binding = %TargetBinding{
      workspace_id: contexts.workspace_context.workspace_id,
      target_id: active_asset.target_descriptor.target_id,
      active_generation_id: Ecto.UUID.generate(),
      active_manifest_id: active_version.manifest_version_id,
      active_descriptor_hash: active_asset.target_descriptor.descriptor_hash,
      desired_manifest_id: active_version.manifest_version_id,
      desired_descriptor_hash: active_asset.target_descriptor.descriptor_hash,
      compatibility_status: :ready,
      reason_code: "compatible",
      compatibility_diff: %{},
      active_physical_relation: Map.from_struct(active_asset.relation),
      active_physical_fingerprint: recorded.fingerprint,
      version: 5,
      updated_at: @now
    }

    put_versions([active_version, desired_version])
    Application.put_env(:favn_orchestrator, :compatibility_test_bindings, [binding])
    Application.put_env(:favn_orchestrator, :compatibility_test_inspection, observed)

    assert {:ok, [decision]} = plan(desired_version, desired_asset, contexts)
    assert decision.compatibility_status == :ready
    assert decision.reason_code == "compatible"
    refute_received {:ensure_manifest, _active_manifest_id}
    assert_received {:inspect_relation, request}
    assert request.manifest_version_id == desired_version.manifest_version_id
    assert request.required_runner_release_id == desired_version.required_runner_release_id
    assert request.asset_ref == nil
    assert request.relation == active_asset.relation
  end

  test "runner release changes inspect structural changes without loading old code", contexts do
    {active_version, active_asset} =
      persisted_version("manifest-before-cross-release-relation-change", "sales_summary")

    {desired_version, desired_asset} =
      persisted_version(
        "manifest-after-cross-release-relation-change",
        "sales_summary_v2",
        FavnTestSupport.runner_release_id(:alternate)
      )

    observed =
      inspection(desired_version,
        relation: %{catalog: nil, schema: "gold", name: "sales_summary", type: :table}
      )

    {:ok, recorded} = PhysicalFingerprint.from_inspection(observed)

    binding = %TargetBinding{
      workspace_id: contexts.workspace_context.workspace_id,
      target_id: active_asset.target_descriptor.target_id,
      active_generation_id: Ecto.UUID.generate(),
      active_manifest_id: active_version.manifest_version_id,
      active_descriptor_hash: active_asset.target_descriptor.descriptor_hash,
      desired_manifest_id: active_version.manifest_version_id,
      desired_descriptor_hash: active_asset.target_descriptor.descriptor_hash,
      compatibility_status: :ready,
      reason_code: "compatible",
      compatibility_diff: %{},
      active_physical_relation: Map.from_struct(active_asset.relation),
      active_physical_fingerprint: recorded.fingerprint,
      version: 6,
      updated_at: @now
    }

    put_versions([active_version, desired_version])
    Application.put_env(:favn_orchestrator, :compatibility_test_bindings, [binding])

    Application.put_env(
      :favn_orchestrator,
      :compatibility_test_inspection,
      observed
    )

    assert {:ok, [decision]} = plan(desired_version, desired_asset, contexts)
    assert decision.compatibility_status == :rebuild_required
    assert decision.reason_code == "incompatible_descriptor"
    assert Enum.any?(decision.compatibility_diff.descriptor, &(&1.field == :relation))
    refute_received {:ensure_manifest, _active_manifest_id}
    assert_received {:inspect_relation, request}
    assert request.asset_ref == nil
    assert request.relation == active_asset.relation
  end

  test "runner release changes preserve missing-relation drift over descriptor changes",
       contexts do
    {active_version, active_asset} =
      persisted_version("manifest-before-cross-release-missing-relation", "sales_summary")

    {desired_version, desired_asset} =
      persisted_version(
        "manifest-after-cross-release-missing-relation",
        "sales_summary_v2",
        FavnTestSupport.runner_release_id(:alternate)
      )

    binding = %TargetBinding{
      workspace_id: contexts.workspace_context.workspace_id,
      target_id: active_asset.target_descriptor.target_id,
      active_generation_id: Ecto.UUID.generate(),
      active_manifest_id: active_version.manifest_version_id,
      active_descriptor_hash: active_asset.target_descriptor.descriptor_hash,
      desired_manifest_id: active_version.manifest_version_id,
      desired_descriptor_hash: active_asset.target_descriptor.descriptor_hash,
      compatibility_status: :ready,
      reason_code: "compatible",
      compatibility_diff: %{},
      active_physical_relation: Map.from_struct(active_asset.relation),
      active_physical_fingerprint: String.duplicate("f", 64),
      version: 7,
      updated_at: @now
    }

    put_versions([active_version, desired_version])
    Application.put_env(:favn_orchestrator, :compatibility_test_bindings, [binding])

    Application.put_env(
      :favn_orchestrator,
      :compatibility_test_inspection,
      inspection(desired_version)
    )

    assert {:ok, [decision]} = plan(desired_version, desired_asset, contexts)
    assert decision.compatibility_status == :unexpected_drift
    assert decision.reason_code == "physical_relation_missing"
    assert_received {:inspect_relation, request}
    assert request.relation == active_asset.relation
  end

  defp plan(version, asset, contexts) do
    selection = %DeploymentPlanner{
      common_assets: [asset.ref],
      common_pipelines: [],
      workspace_assets: [],
      workspace_pipelines: []
    }

    TargetCompatibilityPlanner.plan(
      contexts.platform_context,
      contexts.workspace_context,
      version,
      selection
    )
  end

  defp persisted_version(
         manifest_id,
         relation_name \\ "sales_summary",
         runner_release_id \\ FavnTestSupport.runner_release_id()
       ) do
    ref = {MyApp.SalesSummary, :asset}

    asset =
      FavnTestSupport.with_target_descriptor(%Asset{
        ref: ref,
        module: elem(ref, 0),
        name: elem(ref, 1),
        type: :sql,
        relation: RelationRef.new!(connection: :warehouse, schema: "gold", name: relation_name),
        materialization: :table,
        execution_package_hash: String.duplicate("a", 64)
      })

    manifest =
      %Manifest{assets: [asset]}
      |> FavnTestSupport.with_manifest_graph()
      |> FavnTestSupport.with_manifest_contract(runner_release_id)

    {:ok, version} = Version.new(manifest, manifest_version_id: manifest_id)
    {version, hd(version.manifest.assets)}
  end

  defp inspection(version, opts \\ []) do
    %RelationInspectionResult{
      asset_ref: {MyApp.SalesSummary, :asset},
      required_runner_release_id: version.required_runner_release_id,
      relation_ref:
        RelationRef.new!(connection: :warehouse, schema: "gold", name: "sales_summary"),
      relation: Keyword.get(opts, :relation),
      columns: Keyword.get(opts, :columns, []),
      table_metadata: %{},
      adapter: FavnTestSupport.TargetAdapter,
      inspected_at: @now
    }
  end

  defp put_versions(versions) do
    Application.put_env(
      :favn_orchestrator,
      :compatibility_test_versions,
      Map.new(versions, &{&1.manifest_version_id, &1})
    )
  end
end
