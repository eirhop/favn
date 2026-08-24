defmodule FavnOrchestrator.TargetCompatibilityPlannerTest do
  use ExUnit.Case, async: false

  alias Favn.Contracts.RelationInspectionResult
  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.TargetDescriptor
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

  defmodule RunnerExecutor do
    def inspect_relation(request, opts) do
      send(Keyword.fetch!(opts, :test_pid), {:inspect_relation, request})

      inspection = Application.fetch_env!(:favn_orchestrator, :compatibility_test_inspection)
      {:ok, if(is_function(inspection, 1), do: inspection.(request), else: inspection)}
    end
  end

  defmodule Store do
    def get_bindings(_query),
      do: {:ok, Application.get_env(:favn_orchestrator, :compatibility_test_bindings, [])}

    def get_manifest(query) do
      versions = Application.fetch_env!(:favn_orchestrator, :compatibility_test_versions)
      errors = Application.get_env(:favn_orchestrator, :compatibility_test_manifest_errors, %{})

      case {Map.fetch(errors, query.manifest_version_id),
            Map.fetch(versions, query.manifest_version_id)} do
        {{:ok, error}, _version} -> {:error, error}
        {:error, {:ok, version}} -> {:ok, version}
        {:error, :error} -> {:error, :manifest_not_found}
      end
    end

    def get_manifest_target_descriptors(query) do
      descriptors =
        Application.get_env(
          :favn_orchestrator,
          :compatibility_test_historical_descriptors,
          %{}
        )

      {:ok, Map.get(descriptors, query.manifest_version_id, [])}
    end
  end

  setup do
    previous =
      for key <- [
            :test_runner_executor,
            :test_runner_executor_opts,
            :compatibility_test_inspection,
            :compatibility_test_bindings,
            :compatibility_test_versions,
            :compatibility_test_manifest_errors,
            :compatibility_test_historical_descriptors
          ],
          into: %{},
          do: {key, Application.get_env(:favn_orchestrator, key, :missing)}

    Application.put_env(:favn_orchestrator, :test_runner_executor, RunnerExecutor)
    Application.put_env(:favn_orchestrator, :test_runner_executor_opts, test_pid: self())

    stores =
      struct(Stores,
        registry: Store,
        runs: Store,
        runner_tasks: FavnOrchestrator.TestRunnerTaskStore,
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

  test "reports deterministic bounded inspection progress", contexts do
    {version, asset} = persisted_version("manifest-progress")
    put_versions([version])
    Application.put_env(:favn_orchestrator, :compatibility_test_bindings, [])
    Application.put_env(:favn_orchestrator, :compatibility_test_inspection, inspection(version))

    selection = %DeploymentPlanner{
      common_assets: [asset.ref],
      common_pipelines: [],
      workspace_assets: [],
      workspace_pipelines: []
    }

    test_pid = self()

    assert {:ok, [_decision]} =
             TargetCompatibilityPlanner.plan(
               contexts.platform_context,
               contexts.workspace_context,
               version,
               selection,
               progress: fn completed, total -> send(test_pid, {:progress, completed, total}) end
             )

    assert_received {:progress, 0, 1}
    assert_received {:progress, 1, 1}
  end

  @tag :slow
  test "attempts one thousand targets exactly once with bounded fair concurrency", contexts do
    {version, assets} = large_persisted_version(1_000)
    put_versions([version])
    Application.put_env(:favn_orchestrator, :compatibility_test_bindings, [])
    tracker = start_supervised!({Agent, fn -> %{active: 0, maximum: 0, attempts: %{}} end})

    Application.put_env(
      :favn_orchestrator,
      :compatibility_test_inspection,
      fn request ->
        Agent.update(tracker, fn state ->
          active = state.active + 1

          %{
            state
            | active: active,
              maximum: max(state.maximum, active),
              attempts: Map.update(state.attempts, request.asset_ref, 1, &(&1 + 1))
          }
        end)

        Process.sleep(2)
        Agent.update(tracker, &%{&1 | active: &1.active - 1})

        %RelationInspectionResult{
          asset_ref: request.asset_ref,
          required_runner_release_id: request.required_runner_release_id,
          relation_ref: nil,
          relation: nil,
          columns: [],
          table_metadata: %{},
          adapter: FavnTestSupport.TargetAdapter,
          inspected_at: @now
        }
      end
    )

    selection = %DeploymentPlanner{
      common_assets: Enum.map(assets, & &1.ref),
      common_pipelines: [],
      workspace_assets: [],
      workspace_pipelines: []
    }

    test_pid = self()

    assert {:ok, decisions} =
             TargetCompatibilityPlanner.plan(
               contexts.platform_context,
               contexts.workspace_context,
               version,
               selection,
               progress: fn completed, total ->
                 send(test_pid, {:scale_progress, completed, total})
               end
             )

    tracker_state = Agent.get(tracker, & &1)
    assert length(decisions) == 1_000
    assert Enum.map(decisions, & &1.target_id) == Enum.sort(Enum.map(decisions, & &1.target_id))
    assert map_size(tracker_state.attempts) == 1_000
    assert Enum.all?(tracker_state.attempts, fn {_asset_ref, attempts} -> attempts == 1 end)
    assert tracker_state.maximum in 2..32
    assert_received {:scale_progress, 0, 1_000}
    assert_received {:scale_progress, 1_000, 1_000}
  end

  test "keeps compatibility inspection inside the caller's maintenance admission", contexts do
    {version, asset} = persisted_version("manifest-maintenance-inspection")
    put_versions([version])
    Application.put_env(:favn_orchestrator, :compatibility_test_bindings, [])
    Application.put_env(:favn_orchestrator, :compatibility_test_inspection, inspection(version))

    token = :crypto.strong_rand_bytes(32) |> Base.url_encode64(padding: false)
    assert {:ok, ^token} = Lifecycle.begin_maintenance(:deployment_maintenance, token)
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

    assert {:ok, [decision]} = plan(desired_version, desired_asset, contexts)
    assert decision.compatibility_status == :rebuild_required
    assert decision.reason_code == "incompatible_descriptor"

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
    assert request.required_runner_release_id == release_id(desired_version)
    assert request.asset_ref == nil
    assert request.relation == active_asset.relation
  end

  test "unavailable full manifests use persisted descriptor semantics instead of hash equality",
       contexts do
    {historical_version, historical_asset} = persisted_version("historical-manifest")
    {desired_version, desired_asset} = persisted_version("current-manifest")

    historical_descriptor =
      descriptor_with_manifest_schema(
        historical_asset,
        historical_asset.target_descriptor.manifest_schema_version + 1
      )

    refute historical_descriptor.descriptor_hash ==
             desired_asset.target_descriptor.descriptor_hash

    observed =
      inspection(desired_version,
        relation: %{catalog: nil, schema: "gold", name: "sales_summary", type: :table}
      )

    {:ok, recorded} = PhysicalFingerprint.from_inspection(observed)

    binding = %TargetBinding{
      workspace_id: contexts.workspace_context.workspace_id,
      target_id: historical_asset.target_descriptor.target_id,
      active_generation_id: Ecto.UUID.generate(),
      active_manifest_id: historical_version.manifest_version_id,
      active_descriptor_hash: historical_descriptor.descriptor_hash,
      desired_manifest_id: historical_version.manifest_version_id,
      desired_descriptor_hash: historical_descriptor.descriptor_hash,
      compatibility_status: :ready,
      reason_code: "compatible",
      compatibility_diff: %{},
      active_physical_relation: Map.from_struct(historical_asset.relation),
      active_physical_fingerprint: recorded.fingerprint,
      version: 8,
      updated_at: @now
    }

    put_versions([desired_version])

    Application.put_env(:favn_orchestrator, :compatibility_test_manifest_errors, %{
      historical_version.manifest_version_id => historical_manifest_error()
    })

    put_historical_descriptors(historical_version, [historical_descriptor])
    Application.put_env(:favn_orchestrator, :compatibility_test_bindings, [binding])
    Application.put_env(:favn_orchestrator, :compatibility_test_inspection, observed)

    assert {:ok, [decision]} = plan(desired_version, desired_asset, contexts)
    assert decision.compatibility_status == :ready
    assert decision.reason_code == "compatible"
    refute_received {:ensure_manifest, _historical_manifest_id}
    assert_received {:inspect_relation, request}
    assert request.manifest_version_id == desired_version.manifest_version_id
    assert request.asset_ref == nil
    assert request.relation == historical_asset.relation
  end

  test "unavailable full manifests still classify structural descriptor changes", contexts do
    {historical_version, historical_asset} =
      persisted_version("historical-structural-manifest", "sales_summary")

    {desired_version, desired_asset} =
      persisted_version("current-structural-manifest", "sales_summary_v2")

    historical_descriptor = historical_asset.target_descriptor

    observed =
      inspection(desired_version,
        relation: %{catalog: nil, schema: "gold", name: "sales_summary", type: :table}
      )

    {:ok, recorded} = PhysicalFingerprint.from_inspection(observed)

    binding = %TargetBinding{
      workspace_id: contexts.workspace_context.workspace_id,
      target_id: historical_descriptor.target_id,
      active_generation_id: Ecto.UUID.generate(),
      active_manifest_id: historical_version.manifest_version_id,
      active_descriptor_hash: historical_descriptor.descriptor_hash,
      desired_manifest_id: historical_version.manifest_version_id,
      desired_descriptor_hash: historical_descriptor.descriptor_hash,
      compatibility_status: :ready,
      reason_code: "compatible",
      compatibility_diff: %{},
      active_physical_relation: Map.from_struct(historical_asset.relation),
      active_physical_fingerprint: recorded.fingerprint,
      version: 10,
      updated_at: @now
    }

    put_versions([desired_version])

    Application.put_env(:favn_orchestrator, :compatibility_test_manifest_errors, %{
      historical_version.manifest_version_id => historical_manifest_error()
    })

    put_historical_descriptors(historical_version, [historical_descriptor])
    Application.put_env(:favn_orchestrator, :compatibility_test_bindings, [binding])
    Application.put_env(:favn_orchestrator, :compatibility_test_inspection, observed)

    assert {:ok, [decision]} = plan(desired_version, desired_asset, contexts)
    assert decision.compatibility_status == :rebuild_required
    assert decision.reason_code == "incompatible_descriptor"
    assert Enum.any?(decision.compatibility_diff.descriptor, &(&1.field == :relation))
    refute_received {:ensure_manifest, _historical_manifest_id}
    assert_received {:inspect_relation, request}
    assert request.asset_ref == nil
    assert request.relation == historical_asset.relation
  end

  test "missing historical descriptor evidence remains an operator decision", contexts do
    {historical_version, historical_asset} = persisted_version("historical-missing-descriptor")
    {desired_version, desired_asset} = persisted_version("current-missing-descriptor")

    observed =
      inspection(desired_version,
        relation: %{catalog: nil, schema: "gold", name: "sales_summary", type: :table}
      )

    {:ok, recorded} = PhysicalFingerprint.from_inspection(observed)

    binding = %TargetBinding{
      workspace_id: contexts.workspace_context.workspace_id,
      target_id: historical_asset.target_descriptor.target_id,
      active_generation_id: Ecto.UUID.generate(),
      active_manifest_id: historical_version.manifest_version_id,
      active_descriptor_hash: historical_asset.target_descriptor.descriptor_hash,
      desired_manifest_id: historical_version.manifest_version_id,
      desired_descriptor_hash: historical_asset.target_descriptor.descriptor_hash,
      compatibility_status: :ready,
      reason_code: "compatible",
      compatibility_diff: %{},
      active_physical_relation: Map.from_struct(historical_asset.relation),
      active_physical_fingerprint: recorded.fingerprint,
      version: 11,
      updated_at: @now
    }

    put_versions([desired_version])

    Application.put_env(:favn_orchestrator, :compatibility_test_manifest_errors, %{
      historical_version.manifest_version_id => historical_manifest_error()
    })

    put_historical_descriptors(historical_version, [])
    Application.put_env(:favn_orchestrator, :compatibility_test_bindings, [binding])
    Application.put_env(:favn_orchestrator, :compatibility_test_inspection, observed)

    assert {:ok, [decision]} = plan(desired_version, desired_asset, contexts)
    assert decision.compatibility_status == :operator_decision
    assert decision.reason_code == "inconsistent_generation_state"
    assert_received {:inspect_relation, request}
    assert request.asset_ref == desired_asset.ref
  end

  test "does not use the historical fallback for a loaded manifest descriptor mismatch",
       contexts do
    {active_version, active_asset} =
      persisted_version("loaded-active-manifest", "sales_summary_v1")

    {desired_version, desired_asset} =
      persisted_version("desired-manifest", "sales_summary_v2")

    binding = %TargetBinding{
      workspace_id: contexts.workspace_context.workspace_id,
      target_id: active_asset.target_descriptor.target_id,
      active_generation_id: Ecto.UUID.generate(),
      active_manifest_id: active_version.manifest_version_id,
      active_descriptor_hash: desired_asset.target_descriptor.descriptor_hash,
      desired_manifest_id: active_version.manifest_version_id,
      desired_descriptor_hash: desired_asset.target_descriptor.descriptor_hash,
      compatibility_status: :ready,
      reason_code: "compatible",
      compatibility_diff: %{},
      active_physical_relation: Map.from_struct(active_asset.relation),
      version: 9,
      updated_at: @now
    }

    put_versions([active_version, desired_version])
    Application.put_env(:favn_orchestrator, :compatibility_test_bindings, [binding])

    Application.put_env(
      :favn_orchestrator,
      :compatibility_test_inspection,
      inspection(desired_version,
        relation: %{catalog: nil, schema: "gold", name: "sales_summary_v2", type: :table}
      )
    )

    assert {:ok, [decision]} = plan(desired_version, desired_asset, contexts)
    assert decision.compatibility_status == :operator_decision
    assert_received {:inspect_relation, request}
    assert request.manifest_version_id == desired_version.manifest_version_id
    assert request.asset_ref == desired_asset.ref
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

  defp large_persisted_version(count) do
    assets =
      Enum.map(1..count, fn index ->
        module = Module.concat(__MODULE__, "ScaleAsset#{index}")
        ref = {module, :asset}

        FavnTestSupport.with_target_descriptor(%Asset{
          ref: ref,
          module: module,
          name: :asset,
          type: :sql,
          relation:
            RelationRef.new!(
              connection: :warehouse,
              schema: "scale",
              name: "asset_#{index}"
            ),
          materialization: :table,
          execution_package_hash:
            :sha256
            |> :crypto.hash("scale-package-#{index}")
            |> Base.encode16(case: :lower)
        })
      end)

    manifest =
      %Manifest{assets: assets}
      |> FavnTestSupport.with_manifest_graph()
      |> FavnTestSupport.with_manifest_contract()

    {:ok, version} = Version.new(manifest, manifest_version_id: "manifest-scale-#{count}")
    {version, version.manifest.assets}
  end

  defp inspection(version, opts \\ []) do
    %RelationInspectionResult{
      asset_ref: {MyApp.SalesSummary, :asset},
      required_runner_release_id: release_id(version),
      relation_ref:
        RelationRef.new!(connection: :warehouse, schema: "gold", name: "sales_summary"),
      relation: Keyword.get(opts, :relation),
      columns: Keyword.get(opts, :columns, []),
      table_metadata: %{},
      adapter: FavnTestSupport.TargetAdapter,
      inspected_at: @now
    }
  end

  defp release_id(version) do
    {:ok, release_id} = Version.release_for_pool(version, :default)
    release_id
  end

  defp put_versions(versions) do
    Application.put_env(
      :favn_orchestrator,
      :compatibility_test_versions,
      Map.new(versions, &{&1.manifest_version_id, &1})
    )
  end

  defp put_historical_descriptors(version, descriptors) do
    Application.put_env(:favn_orchestrator, :compatibility_test_historical_descriptors, %{
      version.manifest_version_id => descriptors
    })
  end

  defp descriptor_with_manifest_schema(asset, manifest_schema_version) do
    TargetDescriptor.from_asset(
      asset,
      connection_definitions: %{
        asset.relation.connection => %{adapter: FavnTestSupport.TargetAdapter, module: nil}
      },
      manifest_schema_version: manifest_schema_version,
      runner_contract_version: asset.target_descriptor.runner_contract_version
    )
  end

  defp historical_manifest_error do
    %FavnOrchestrator.Persistence.Error{
      kind: :invalid,
      message: "historical manifest cannot be used as a current release",
      retryable?: false,
      details: %{reason: :historical_manifest_not_activatable}
    }
  end
end
