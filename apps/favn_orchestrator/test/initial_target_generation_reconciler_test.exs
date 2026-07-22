defmodule FavnOrchestrator.InitialTargetGenerationReconcilerTest do
  use ExUnit.Case, async: false

  alias Favn.Contracts.RelationInspectionResult
  alias Favn.Contracts.GenerationMarker
  alias Favn.Contracts.GenerationMarkerInitializationResult
  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Version
  alias Favn.RelationRef
  alias Favn.TargetCompatibility.PhysicalFingerprint
  alias FavnOrchestrator.InitialTargetGenerationReconciler
  alias FavnOrchestrator.Persistence.Runtime, as: PersistenceRuntime
  alias FavnOrchestrator.Persistence.Stores

  @generation_id "018f47a0-7b0d-4b1a-8d8b-e18a9a987654"
  @ref {__MODULE__.MonthlyOrders, :asset}

  defmodule FakeStore do
    def get_binding(query) do
      send(Process.get(:test_pid), {:get_binding, query})
      {:ok, Process.get(:target_binding)}
    end

    def reconcile_initial(command) do
      send(Process.get(:test_pid), {:reconcile_initial, command})
      Process.get(:reconcile_result, {:ok, :reconciled})
    end
  end

  defmodule RunnerClient do
    @behaviour Favn.Contracts.RunnerClient

    def inspect_relation(request, opts) do
      send(Keyword.fetch!(opts, :test_pid), {:inspect_relation, request})
      Process.get(:inspection_result)
    end

    def generation_capabilities(_version, _asset_ref, _opts) do
      {:ok,
       %{
         transactional_ddl: :supported,
         physical_inspection: :supported,
         marker_reconciliation: :supported
       }}
    end

    def initialize_generation_marker(request, opts) do
      send(Keyword.fetch!(opts, :test_pid), {:initialize_generation_marker, request})
      Process.put(:last_initialization_request, request)

      case Process.get(:initialization_mode) do
        :unknown ->
          {:error, Favn.Contracts.RunnerError.new(outcome: :unknown)}

        _success ->
          {:ok,
           %GenerationMarkerInitializationResult{
             required_runner_release_id: request.required_runner_release_id,
             target_id: request.target_id,
             target_generation_id: request.target_generation_id,
             initialization_token: request.initialization_token,
             outcome: :succeeded,
             observed_marker: marker(request),
             physical_fingerprint: request.expected_physical_fingerprint,
             completed_at: ~U[2026-07-22 12:01:00Z]
           }}
      end
    end

    def generation_marker(_version, asset_ref, opts) do
      send(Keyword.fetch!(opts, :test_pid), {:generation_marker, asset_ref})

      case Process.get(:last_initialization_request) do
        nil -> {:ok, nil}
        request -> {:ok, marker(request)}
      end
    end

    defp marker(request) do
      %GenerationMarker{
        target_id: request.target_id,
        active_relation: request.active_relation,
        active_generation_id: request.target_generation_id,
        activation_operation_id: request.initialization_operation_id,
        activation_token: request.initialization_token,
        activated_at: ~U[2026-07-22 12:01:00Z]
      }
    end

    def register_manifest(_version, _opts), do: :ok
    def ensure_manifest(_version, _opts), do: :ok
    def acquire_manifest(_version, _lease_id, _expires_at, _refs, _opts), do: :ok
    def renew_manifest(_lease_id, _expires_at, _opts), do: :ok
    def release_manifest(_lease_id, _opts), do: :ok
    def submit_work(_work, _opts), do: {:error, :not_used}
    def resolve_runtime_inputs(_work, _opts), do: {:error, :not_used}
    def await_result(_execution_id, _timeout, _opts), do: {:error, :not_used}
    def cancel_work(_execution_id, _cancellation, _opts), do: {:error, :not_used}
    def subscribe_execution_logs(_execution_id, _pid, _opts), do: {:error, :not_used}
    def unsubscribe_execution_logs(_execution_id, _pid, _opts), do: :ok
    def diagnostics(_opts), do: {:error, :not_used}
  end

  setup do
    previous_client = Application.get_env(:favn_orchestrator, :runner_client)
    previous_opts = Application.get_env(:favn_orchestrator, :runner_client_opts)

    Application.put_env(:favn_orchestrator, :runner_client, RunnerClient)
    Application.put_env(:favn_orchestrator, :runner_client_opts, test_pid: self())

    stores = %Stores{
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
    }

    runtime = %PersistenceRuntime{backend: __MODULE__, options: [], stores: stores}
    assert {:ok, pid} = PersistenceRuntime.start_link(runtime)

    Process.put(:test_pid, self())

    on_exit(fn ->
      if Process.alive?(pid), do: GenServer.stop(pid)
      restore_env(:runner_client, previous_client)
      restore_env(:runner_client_opts, previous_opts)
    end)

    version = version()
    {:ok, version: version, entry: entry(version)}
  end

  test "inspects and reconciles the first successful persisted materialization", %{
    version: version,
    entry: entry
  } do
    target_id = Favn.TargetIdentity.for_asset(@ref)

    Process.put(:target_binding, %{
      active_generation_id: nil,
      compatibility_status: :uninitialized,
      desired_manifest_id: version.manifest_version_id
    })

    inspection = inspection(version)
    Process.put(:inspection_result, {:ok, inspection})
    assert {:ok, physical} = PhysicalFingerprint.from_inspection(inspection)
    manifest_id = version.manifest_version_id

    assert :ok = InitialTargetGenerationReconciler.reconcile(entry)

    assert_receive {:get_binding, %{target_id: ^target_id}}

    assert_receive {:inspect_relation,
                    %{
                      asset_ref: @ref,
                      manifest_version_id: ^manifest_id,
                      include: [:relation, :columns, :table_metadata],
                      sample_limit: 0
                    }}

    assert_receive {:reconcile_initial, command}
    assert command.target_id == target_id
    assert command.target_generation_id == @generation_id
    assert command.manifest_version_id == version.manifest_version_id
    assert command.materialization_id == "mat:claim-1"
    assert command.physical_schema_fingerprint == physical.fingerprint
    assert_receive {:initialize_generation_marker, marker_request}
    assert marker_request.target_generation_id == @generation_id
    assert marker_request.expected_physical_fingerprint == physical.fingerprint
    assert command.data_plane_marker.active_generation_id == @generation_id
    assert command.data_plane_marker.activation_token == marker_request.initialization_token
    assert String.starts_with?(command.command_id, "target-generation:reconcile-initial:")
  end

  test "is a no-op when the pinned generation is already active", %{entry: entry} do
    Process.put(:target_binding, %{
      active_generation_id: @generation_id,
      compatibility_status: :rebuild_available
    })

    assert :ok = InitialTargetGenerationReconciler.reconcile(entry)
    assert_receive {:get_binding, _query}
    refute_receive {:inspect_relation, _request}
    refute_receive {:reconcile_initial, _command}
  end

  test "reconciles an unknown marker initialization reply before binding", %{
    version: version,
    entry: entry
  } do
    Process.put(:target_binding, %{
      active_generation_id: nil,
      compatibility_status: :uninitialized,
      desired_manifest_id: version.manifest_version_id
    })

    Process.put(:inspection_result, {:ok, inspection(version)})
    Process.put(:initialization_mode, :unknown)

    assert :ok = InitialTargetGenerationReconciler.reconcile(entry)
    assert_receive {:initialize_generation_marker, _request}
    assert_receive {:generation_marker, @ref}
    assert_receive {:reconcile_initial, command}
    assert command.data_plane_marker.active_generation_id == @generation_id
  end

  test "does not activate when physical inspection cannot prove the relation", %{
    version: version,
    entry: entry
  } do
    Process.put(:target_binding, %{
      active_generation_id: nil,
      compatibility_status: :uninitialized,
      desired_manifest_id: version.manifest_version_id
    })

    Process.put(:inspection_result, {:ok, %{inspection(version) | relation: nil}})

    assert {:error,
            {:initial_target_generation_reconciliation_failed, :materialized_relation_not_found}} =
             InitialTargetGenerationReconciler.reconcile(entry)

    refute_receive {:reconcile_initial, _command}
  end

  test "does not bind a materialization to the wrong physical relation", %{
    version: version,
    entry: entry
  } do
    Process.put(:target_binding, %{
      active_generation_id: nil,
      compatibility_status: :uninitialized,
      desired_manifest_id: version.manifest_version_id
    })

    Process.put(
      :inspection_result,
      {:ok, %{inspection(version) | relation: %{name: "other_table", type: :table}}}
    )

    assert {:error,
            {:initial_target_generation_reconciliation_failed,
             {:physical_identity_mismatch, [%{field: :relation}]}}} =
             InitialTargetGenerationReconciler.reconcile(entry)

    refute_receive {:reconcile_initial, _command}
  end

  test "does nothing for semantic generations", %{entry: entry} do
    semantic_entry = put_in(entry, [:materialization_claim, :target_generation_id], nil)

    assert :ok = InitialTargetGenerationReconciler.reconcile(semantic_entry)
    refute_receive {:get_binding, _query}
  end

  defp version do
    manifest =
      %Manifest{
        assets: [
          FavnTestSupport.with_target_descriptor(%Asset{
            ref: @ref,
            module: elem(@ref, 0),
            name: elem(@ref, 1),
            type: :sql,
            relation:
              RelationRef.new!(
                connection: :warehouse,
                schema: "analytics",
                name: "monthly_orders"
              ),
            materialization: :table,
            execution_package_hash: String.duplicate("a", 64)
          })
        ]
      }
      |> FavnTestSupport.with_manifest_contract()
      |> FavnTestSupport.with_manifest_graph()

    {:ok, version} = Version.new(manifest, manifest_version_id: "manifest-initial-generation")
    version
  end

  defp entry(version) do
    %{
      asset_ref: @ref,
      version: version,
      materialization_claim: %{
        claim_key: "claim-1",
        workspace_id: "workspace-1",
        target_generation_id: @generation_id
      }
    }
  end

  defp inspection(version) do
    %RelationInspectionResult{
      asset_ref: @ref,
      required_runner_release_id: version.required_runner_release_id,
      relation: %{catalog: nil, schema: "analytics", name: "monthly_orders", type: :table},
      columns: [%{name: "id", data_type: "BIGINT", nullable?: false}],
      table_metadata: %{},
      adapter: FavnTestSupport.TargetAdapter,
      inspected_at: ~U[2026-07-22 12:00:00Z]
    }
  end

  defp restore_env(key, nil), do: Application.delete_env(:favn_orchestrator, key)
  defp restore_env(key, value), do: Application.put_env(:favn_orchestrator, key, value)
end
