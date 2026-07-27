defmodule FavnOrchestrator.RunServer.Execution.PlanPreflightTest do
  use ExUnit.Case, async: false

  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Graph
  alias Favn.Manifest.Version
  alias Favn.Plan
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Runtime, as: PersistenceRuntime
  alias FavnOrchestrator.Persistence.Stores
  alias FavnOrchestrator.RunServer.Execution
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage.{ManifestCodec, RunSnapshotCodec}

  defmodule FakeStore do
    def get_freshness_many(_command), do: {:ok, []}

    def get_evidence_bindings(query) do
      {:ok, Enum.map(query.target_ids, &evidence_binding/1)}
    end

    defp evidence_binding(target_id) do
      digest = target_id |> then(&:crypto.hash(:sha256, &1)) |> Base.encode16(case: :lower)
      %{target_id: target_id, evidence_generation_id: "ag_" <> digest}
    end
  end

  defmodule RunnerClient do
    @behaviour Favn.Contracts.RunnerClient

    def register_manifest(_version, _opts), do: :ok
    def ensure_manifest(_version, _opts), do: :ok

    def acquire_manifest(_version, lease_id, _expires_at, planned_asset_refs, opts) do
      send(
        Keyword.fetch!(opts, :test_pid),
        {:manifest_acquired, lease_id, planned_asset_refs, opts}
      )

      :ok
    end

    def renew_manifest(_lease_id, _expires_at, _opts), do: :ok

    def release_manifest(lease_id, opts) do
      send(Keyword.fetch!(opts, :test_pid), {:manifest_released, lease_id})
      :ok
    end

    def submit_work(_work, _opts), do: {:error, :not_executed}
    def await_result(_execution_id, _timeout, _opts), do: {:error, :not_executed}
    def cancel_work(_execution_id, _reason, _opts), do: {:error, :not_executed}
    def inspect_relation(_request, _opts), do: {:error, :not_supported}

    def diagnostics(opts) do
      {:ok,
       %{
         available?: true,
         ready?: true,
         status: :ready,
         runner_release_id: Keyword.fetch!(opts, :runner_release_id),
         favn_version: Favn.RunnerRelease.current_favn_version(),
         runner_contract_version: Favn.Manifest.Compatibility.current_runner_contract_version(),
         identity_source: :operator,
         node_name: "runner@runner.internal"
       }}
    end
  end

  setup do
    previous_client = Application.get_env(:favn_orchestrator, :runner_client)
    previous_opts = Application.get_env(:favn_orchestrator, :runner_client_opts)

    Application.put_env(:favn_orchestrator, :runner_client, RunnerClient)

    Application.put_env(:favn_orchestrator, :runner_client_opts,
      test_pid: self(),
      runner_release_id: FavnTestSupport.runner_release_id()
    )

    stores = %Stores{
      registry: FakeStore,
      runs: FakeStore,
      run_submissions: FakeStore,
      runner_tasks: FavnOrchestrator.TestRunnerTaskStore,
      run_ownership: FakeStore,
      scheduler: FakeStore,
      admission: FakeStore,
      resource_circuits: FakeStore,
      target_generations: FakeStore,
      target_recovery: FakeStore,
      rebuilds: FakeStore,
      target_operation_locks: FakeStore,
      materialization: FakeStore,
      backfills: FakeStore,
      operator_reads: FakeStore,
      logs: FakeStore,
      identity: FakeStore,
      maintenance: FakeStore
    }

    runtime = %PersistenceRuntime{backend: __MODULE__, options: [], stores: stores}
    assert {:ok, persistence_pid} = PersistenceRuntime.start_link(runtime)

    on_exit(fn ->
      restore_env(:runner_client, previous_client)
      restore_env(:runner_client_opts, previous_opts)
      if Process.alive?(persistence_pid), do: GenServer.stop(persistence_pid)
    end)

    :ok
  end

  test "does not require a live runner or manifest lease for wide-plan activation" do
    ref = {__MODULE__.Source, :asset}
    wide_refs = List.duplicate(ref, 10_000)
    node_key = {ref, nil}

    asset = %Asset{
      ref: ref,
      module: elem(ref, 0),
      name: elem(ref, 1),
      type: :source,
      relation: %{name: "wide_source"}
    }

    manifest =
      FavnTestSupport.with_manifest_contract(%Manifest{
        assets: [asset],
        graph: %Graph{nodes: [ref], topo_order: [ref]}
      })

    {:ok, version} = Version.new(manifest, manifest_version_id: "mv-wide-plan-preflight")

    plan = %Plan{
      target_refs: [ref],
      target_node_keys: [node_key],
      topo_order: wide_refs,
      nodes: %{
        node_key => %{
          ref: ref,
          node_key: node_key,
          window: nil,
          upstream: [],
          downstream: [],
          stage: 0,
          execution_pool: :default,
          action: :observe,
          retry_policy: nil,
          retry_policy_source: :default
        }
      }
    }

    run =
      RunState.new(
        id: "run-wide-plan-preflight",
        workspace_id: "workspace-wide-plan-preflight",
        deployment_id: "deployment-wide-plan-preflight",
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        required_runner_release_id: version.required_runner_release_id,
        asset_ref: ref,
        target_refs: [ref],
        plan: plan
      )

    assert {:ok, _execution_state} = Execution.start_state(run, version)

    refute_receive {:manifest_acquired, _lease_id, _refs, _opts}

    assert :ok = Execution.release_manifest_lease(run)
    refute_receive {:manifest_released, _lease_id}
  end

  test "restored manual dependency runs use pipeline stages in upstream order" do
    upstream_ref = {__MODULE__.Upstream, :asset}
    target_ref = {__MODULE__.Target, :asset}
    upstream_node = {upstream_ref, nil}
    target_node = {target_ref, nil}

    assets = [
      manifest_asset(upstream_ref, "upstream"),
      manifest_asset(target_ref, "target", [upstream_ref])
    ]

    assert {:ok, graph} = Graph.build(assets)

    manifest =
      FavnTestSupport.with_manifest_contract(%Manifest{
        assets: assets,
        graph: graph
      })

    {:ok, version} =
      Version.new(manifest, manifest_version_id: "mv-restored-manual-dependencies")

    plan = %Plan{
      dependencies: :all,
      target_refs: [target_ref],
      target_node_keys: [target_node],
      topo_order: [upstream_ref, target_ref],
      stages: [[upstream_ref], [target_ref]],
      node_stages: [[upstream_node], [target_node]],
      nodes: %{
        upstream_node => plan_node(upstream_ref, upstream_node, [], [target_node], 0),
        target_node => plan_node(target_ref, target_node, [upstream_node], [], 1)
      }
    }

    run =
      RunState.new(
        id: "run-restored-manual-dependencies",
        workspace_id: "workspace-restored-manual-dependencies",
        deployment_id: "deployment-restored-manual-dependencies",
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        required_runner_release_id: version.required_runner_release_id,
        asset_ref: target_ref,
        target_refs: [target_ref],
        plan: plan,
        submit_kind: :manual
      )

    assert {:ok, payload} = RunSnapshotCodec.encode_run(run)
    assert {:ok, manifest_record} = ManifestCodec.to_record(version)

    assert {:ok, restored_run} =
             RunSnapshotCodec.decode_run(
               %{run_blob: payload, manifest_version_id: version.manifest_version_id},
               manifest_record
             )

    assert {:ok, state} = Execution.start_state(restored_run, version)
    assert state.mode == :pipeline
    assert state.stage_groups == [{0, [upstream_node]}, {1, [target_node]}]

    refute_receive {:manifest_acquired, _lease_id, [^upstream_ref, ^target_ref], _opts}
    assert :ok = Execution.release_manifest_lease(restored_run)
    refute_receive {:manifest_released, _lease_id}
  end

  test "rejects an oversized planned asset step before acquiring or dispatching" do
    ref = {__MODULE__.Source, :asset}
    node_key = String.duplicate("s", 256)

    asset = %Asset{
      ref: ref,
      module: elem(ref, 0),
      name: elem(ref, 1),
      type: :source,
      relation: %{name: "source"}
    }

    manifest =
      FavnTestSupport.with_manifest_contract(%Manifest{
        assets: [asset],
        graph: %Graph{nodes: [ref], topo_order: [ref]}
      })

    {:ok, version} = Version.new(manifest, manifest_version_id: "mv-identity-preflight")

    plan = %Plan{
      target_refs: [ref],
      target_node_keys: [node_key],
      topo_order: [ref],
      nodes: %{
        node_key => %{
          ref: ref,
          node_key: node_key,
          window: nil,
          upstream: [],
          downstream: [],
          stage: 0,
          execution_pool: :default,
          action: :observe,
          retry_policy: nil,
          retry_policy_source: :default
        }
      }
    }

    run =
      RunState.new(
        id: "run-identity-preflight",
        workspace_id: "workspace-identity-preflight",
        deployment_id: "deployment-identity-preflight",
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        required_runner_release_id: version.required_runner_release_id,
        asset_ref: ref,
        target_refs: [ref],
        plan: plan
      )

    assert {:terminal, %RunState{error: %Error{} = error}} = Execution.start_state(run, version)
    assert error.details == %{field: :asset_step_id, actual_bytes: 256, max_bytes: 255}
    refute_receive {:manifest_acquired, _lease_id, _refs, _opts}
  end

  defp manifest_asset(ref, relation_name, depends_on \\ []) do
    %Asset{
      ref: ref,
      module: elem(ref, 0),
      name: elem(ref, 1),
      type: :source,
      depends_on: depends_on,
      relation: %{name: relation_name}
    }
  end

  defp plan_node(ref, node_key, upstream, downstream, stage) do
    %{
      ref: ref,
      node_key: node_key,
      window: nil,
      upstream: upstream,
      downstream: downstream,
      stage: stage,
      execution_pool: :default,
      action: :run,
      retry_policy: Favn.Retry.Policy.default(),
      retry_policy_source: :default
    }
  end

  test "rejects a forged run release before acquiring the manifest" do
    ref = {__MODULE__.Source, :asset}

    asset = %Asset{
      ref: ref,
      module: elem(ref, 0),
      name: elem(ref, 1),
      type: :source,
      relation: %{name: "source"}
    }

    manifest =
      FavnTestSupport.with_manifest_contract(%Manifest{
        assets: [asset],
        graph: %Graph{nodes: [ref], topo_order: [ref]}
      })

    {:ok, version} = Version.new(manifest, manifest_version_id: "mv-forged-release")
    alternate = FavnTestSupport.runner_release_id(:alternate)

    run =
      RunState.new(
        id: "run-forged-release",
        workspace_id: "workspace-forged-release",
        deployment_id: "deployment-forged-release",
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        required_runner_release_id: alternate,
        asset_ref: ref,
        target_refs: [ref]
      )

    assert {:terminal,
            %RunState{
              status: :error,
              error: {:runner_release_mismatch, actual, required}
            }} = Execution.start_state(run, version)

    assert actual == %{"default" => alternate}
    assert required == version.runner_releases
    refute_receive {:manifest_acquired, _lease_id, _refs, _opts}
  end

  defp restore_env(key, nil), do: Application.delete_env(:favn_orchestrator, key)
  defp restore_env(key, value), do: Application.put_env(:favn_orchestrator, key, value)
end

defmodule FavnOrchestrator.RunServer.Execution.PlanPreflightTest.Source do
end
