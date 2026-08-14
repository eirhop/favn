defmodule FavnRunner.GenerationOperationsTest do
  use ExUnit.Case, async: false

  alias Favn.Connection.Registry
  alias Favn.Connection.Resolved
  alias Favn.Contracts.GenerationCapabilitiesRequest
  alias Favn.Contracts.GenerationCapabilitiesResult
  alias Favn.Contracts.GenerationActivationRequest
  alias Favn.Contracts.GenerationActivationResult
  alias Favn.Contracts.GenerationDiscardRequest
  alias Favn.Contracts.GenerationDiscardResult
  alias Favn.Contracts.GenerationMarkerReadRequest
  alias Favn.Contracts.GenerationMarkerReadResult
  alias Favn.Contracts.GenerationMarkerInitializationRequest
  alias Favn.Contracts.GenerationMarkerInitializationResult
  alias Favn.Contracts.GenerationReconciliationRequest
  alias Favn.Contracts.GenerationReconciliationResult
  alias Favn.Contracts.RunnerTask.Assignment
  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Graph
  alias Favn.Manifest.TargetDescriptor
  alias Favn.Manifest.Version
  alias Favn.RelationRef
  alias Favn.SQL.GenerationRelation
  alias FavnRunner.TaskExecutor
  alias FavnRunner.TaskExecutor.Result

  @previous_generation_id "018f47a0-7b0d-4b1a-8d8b-e18a9a987654"
  @candidate_generation_id "018f47a0-7b0d-4b1a-8d8b-e18a9a987655"
  @candidate_fingerprint String.duplicate("c", 64)
  @active_fingerprint String.duplicate("d", 64)

  defmodule Adapter do
    @behaviour Favn.SQL.Adapter
    @behaviour Favn.SQL.GenerationAdapter

    alias Favn.SQL.GenerationActivationResult
    alias Favn.SQL.GenerationCapabilities
    alias Favn.SQL.GenerationInspection
    alias Favn.SQL.GenerationMarker
    alias Favn.SQL.GenerationMarkerInitializationResult

    @impl true
    def connect(%Resolved{}, _opts), do: {:ok, :generation_conn}

    @impl true
    def disconnect(:generation_conn, _opts), do: :ok

    @impl true
    def capabilities(%Resolved{}, _opts), do: {:ok, %Favn.SQL.Capabilities{}}

    @impl true
    def execute(:generation_conn, _statement, _opts),
      do: {:ok, %Favn.SQL.Result{kind: :execute}}

    @impl true
    def query(:generation_conn, _statement, _opts),
      do: {:ok, %Favn.SQL.Result{kind: :query}}

    @impl true
    def introspection_query(_kind, _payload, _opts), do: {:ok, "SELECT 1"}

    @impl true
    def materialization_statements(_plan, _capabilities, _opts), do: {:ok, []}

    @impl Favn.SQL.GenerationAdapter
    def generation_capabilities(%Resolved{}, _opts) do
      notify_operation(:generation_capabilities)

      {:ok,
       %GenerationCapabilities{
         transactional_ddl: :supported,
         isolated_candidates: :supported,
         physical_inspection: :supported,
         atomic_swap: :supported,
         marker_reconciliation: :supported,
         idempotent_discard: :supported,
         max_identifier_bytes: 128
       }}
    end

    @impl Favn.SQL.GenerationAdapter
    def bind_relation_instance(:generation_conn, _relation, _instance_id, _opts), do: :ok

    @impl Favn.SQL.GenerationAdapter
    def initialize_generation_marker(:generation_conn, request, _opts) do
      notify_operation(:generation_marker_initialize)

      marker = %GenerationMarker{
        logical_target_id: request.logical_target_id,
        active_relation: request.stable_relation,
        active_generation_id: request.active_generation_id,
        activation_operation_id: request.initialization_operation_id,
        activation_token: request.initialization_token,
        activated_at: request.initialized_at
      }

      Application.put_env(:favn_runner, :generation_operations_test_marker, marker)

      {:ok,
       %GenerationMarkerInitializationResult{
         marker: marker,
         physical_fingerprint: request.expected_physical_fingerprint,
         inspection: inspection(request.stable_relation)
       }}
    end

    defp notify_operation(kind) do
      case Application.get_env(:favn_runner, :generation_operations_test_observer) do
        {owner, :block} when is_pid(owner) ->
          send(owner, {:generation_operation_started, kind, self()})

          receive do
            {:release_generation_operation, ^kind} -> :ok
          end

        owner when is_pid(owner) ->
          send(owner, {:generation_operation_started, kind, self()})

        _other ->
          :ok
      end
    end

    @impl Favn.SQL.GenerationAdapter
    def activate_generation(:generation_conn, request, _opts) do
      case Application.get_env(:favn_runner, :generation_operations_test_activation_error) do
        nil ->
          marker = %GenerationMarker{
            logical_target_id: request.logical_target_id,
            active_relation: request.stable_relation,
            active_generation_id: request.candidate_generation_id,
            activation_operation_id: request.activation_operation_id,
            activation_token: request.activation_token,
            activated_at: request.activated_at
          }

          Application.put_env(:favn_runner, :generation_operations_test_marker, marker)

          {:ok,
           %GenerationActivationResult{
             marker: marker,
             candidate_fingerprint: request.expected_candidate_fingerprint,
             physical_fingerprint: String.duplicate("d", 64),
             inspection: inspection(request.stable_relation)
           }}

        %Favn.SQL.Error{} = error ->
          {:error, error}
      end
    end

    @impl Favn.SQL.GenerationAdapter
    def reconcile_generation(:generation_conn, request, _opts) do
      Application.put_env(
        :favn_runner,
        :generation_operations_test_reconciliation_request,
        request
      )

      {:ok, Application.get_env(:favn_runner, :generation_operations_test_marker)}
    end

    @impl Favn.SQL.GenerationAdapter
    def inspect_generation(:generation_conn, relation, _opts) do
      marker = Application.get_env(:favn_runner, :generation_operations_test_marker)

      if marker && relation.name != marker.active_relation.name,
        do: {:ok, :not_found},
        else: {:ok, inspection(relation)}
    end

    @impl Favn.SQL.GenerationAdapter
    def discard_generation(:generation_conn, _request, _opts) do
      case Application.get_env(:favn_runner, :generation_operations_test_discard_error) do
        nil -> :ok
        %Favn.SQL.Error{} = error -> {:error, error}
      end
    end

    defp inspection(relation) do
      %GenerationInspection{
        relation_ref: relation,
        relation: %Favn.SQL.Relation{
          catalog: relation.catalog,
          schema: relation.schema,
          name: relation.name,
          type: :table
        },
        columns: [],
        physical_fingerprint: %Favn.TargetCompatibility.PhysicalFingerprint{
          adapter: Atom.to_string(__MODULE__),
          relation: %{
            catalog: relation.catalog,
            schema: relation.schema,
            name: relation.name,
            kind: "table"
          },
          columns: [],
          fingerprint: String.duplicate("d", 64)
        }
      }
    end
  end

  setup do
    previous = Registry.list(registry_name: FavnRunner.ConnectionRegistry)
    Application.delete_env(:favn_runner, :generation_operations_test_marker)
    Application.delete_env(:favn_runner, :generation_operations_test_discard_error)
    Application.delete_env(:favn_runner, :generation_operations_test_reconciliation_request)
    Application.delete_env(:favn_runner, :generation_operations_test_activation_error)

    Registry.reload(
      %{
        generation_warehouse: %Resolved{
          name: :generation_warehouse,
          adapter: Adapter,
          module: __MODULE__,
          config: %{}
        }
      },
      registry_name: FavnRunner.ConnectionRegistry
    )

    on_exit(fn ->
      Application.delete_env(:favn_runner, :generation_operations_test_marker)
      Application.delete_env(:favn_runner, :generation_operations_test_discard_error)
      Application.delete_env(:favn_runner, :generation_operations_test_reconciliation_request)
      Application.delete_env(:favn_runner, :generation_operations_test_activation_error)

      Registry.reload(Map.new(previous, &{&1.name, &1}),
        registry_name: FavnRunner.ConnectionRegistry
      )
    end)

    :ok
  end

  test "runner maps activation and reconciliation through exact boundary contracts" do
    {version, asset} = registered_target()
    manifest_identity = %{version | manifest: nil}

    assert {:ok, capabilities} =
             FavnRunner.generation_capabilities(manifest_identity, asset.ref)

    assert capabilities.atomic_swap == :supported
    assert capabilities.marker_reconciliation == :supported

    initialization = initialization_request(version, asset)

    assert {:ok, %GenerationMarkerInitializationResult{outcome: :succeeded} = initialized} =
             FavnRunner.initialize_generation_marker(initialization)

    assert initialized.physical_fingerprint == @active_fingerprint

    assert {:ok, initialized.observed_marker} ==
             FavnRunner.generation_marker(manifest_identity, asset.ref)

    assert Application.fetch_env!(
             :favn_runner,
             :generation_operations_test_reconciliation_request
           ).require_relation_instance?

    assert {:ok, initialized.observed_marker} ==
             FavnRunner.generation_marker(manifest_identity, asset.ref,
               require_relation_instance?: false
             )

    refute Application.fetch_env!(
             :favn_runner,
             :generation_operations_test_reconciliation_request
           ).require_relation_instance?

    request = activation_request(version, asset, initialized.observed_marker)

    assert {:ok, %GenerationActivationResult{outcome: :succeeded} = result} =
             FavnRunner.activate_generation(request)

    assert result.candidate_fingerprint == @candidate_fingerprint
    assert result.physical_fingerprint == @active_fingerprint
    assert result.observed_marker.activation_operation_id == request.rebuild_operation_id
    assert :ok = GenerationActivationResult.validate(result, request)

    reconciliation = %GenerationReconciliationRequest{activation: request}

    assert {:ok, %GenerationReconciliationResult{disposition: :candidate_active} = observed} =
             FavnRunner.reconcile_generation(reconciliation)

    assert Application.fetch_env!(
             :favn_runner,
             :generation_operations_test_reconciliation_request
           ).require_relation_instance?

    assert observed.physical_fingerprint == @active_fingerprint
    assert :ok = GenerationReconciliationResult.validate(observed, reconciliation)

    retired_discard = %GenerationDiscardRequest{
      manifest_version_id: version.manifest_version_id,
      manifest_content_hash: version.content_hash,
      required_runner_release_id: Map.fetch!(version.runner_releases, "default"),
      rebuild_operation_id: request.rebuild_operation_id,
      rebuild_action_id: request.rebuild_action_id,
      target_id: request.target_id,
      candidate_generation_id: @previous_generation_id,
      active_relation: asset.relation,
      candidate_relation:
        GenerationRelation.retired(asset.relation, @previous_generation_id, 128),
      discard_token: "cleanup-retired-generation-operations",
      relation_kind: :retired
    }

    assert {:ok, %GenerationDiscardResult{outcome: :already_absent} = retired_result} =
             FavnRunner.discard_generation(retired_discard)

    assert :ok = GenerationDiscardResult.validate(retired_result, retired_discard)

    discard = %GenerationDiscardRequest{
      manifest_version_id: version.manifest_version_id,
      manifest_content_hash: version.content_hash,
      required_runner_release_id: Map.fetch!(version.runner_releases, "default"),
      rebuild_operation_id: request.rebuild_operation_id,
      rebuild_action_id: request.rebuild_action_id,
      target_id: request.target_id,
      candidate_generation_id: request.candidate_generation_id,
      active_relation: %{asset.relation | name: "another_target"},
      candidate_relation: request.candidate_relation,
      discard_token: "discard-generation-operations"
    }

    assert {:error, :generation_relation_mismatch} = FavnRunner.discard_generation(discard)

    Application.put_env(
      :favn_runner,
      :generation_operations_test_discard_error,
      %Favn.SQL.Error{
        type: :introspection_mismatch,
        message: "candidate is active",
        retryable?: false,
        details: %{classification: :active_generation_discard_forbidden}
      }
    )

    active_discard = %{discard | active_relation: asset.relation}

    assert {:ok, %GenerationDiscardResult{outcome: :outcome_unknown} = discard_result} =
             FavnRunner.discard_generation(active_discard)

    assert discard_result.observed_marker.active_generation_id == @candidate_generation_id
    assert discard_result.candidate_present == nil
    assert :ok = GenerationDiscardResult.validate(discard_result, active_discard)
  end

  test "full manifest generation reads do not depend on manifest cache residency" do
    {version, asset} = registered_target()
    {:ok, empty_store} = start_supervised({FavnRunner.ManifestStore, name: nil})

    assert {:ok, capabilities} =
             FavnRunner.generation_capabilities(version, asset.ref, manifest_store: empty_store)

    assert capabilities.atomic_swap == :supported
  end

  test "durable task executor runs generation reads through the registered manifest" do
    {version, asset} = registered_target()

    payload = %GenerationCapabilitiesRequest{
      manifest: %{version | manifest: nil},
      asset_ref: asset.ref
    }

    assignment = %Assignment{
      command_id: "claim-generation-capabilities",
      workspace_id: "workspace-generation-capabilities",
      task_id: "rt_generation_capabilities",
      task_kind: :generation_capabilities,
      runner_instance_id: "runner-generation-capabilities",
      runner_session_generation: 1,
      assignment_generation: 1,
      runner_pool: "default",
      required_runner_release_id: Map.fetch!(version.runner_releases, "default"),
      assigned_at: DateTime.utc_now(),
      lease_expires_at: DateTime.add(DateTime.utc_now(), 30, :second),
      retry_class: :safe_to_retry,
      payload: payload
    }

    assert {:ok, executor} =
             TaskExecutor.start_link(assignment: assignment, payload: payload, owner: self())

    assert_receive {:runner_task_finished, ^executor,
                    %Result{
                      outcome: :succeeded,
                      retry_class: :terminal,
                      result: %GenerationCapabilitiesResult{capabilities: capabilities}
                    }}

    assert capabilities.atomic_swap == :supported
  end

  test "durable marker reads preserve the managed-rebuild relation policy" do
    {version, asset} = registered_target()
    initialization = initialization_request(version, asset)
    assert {:ok, _initialized} = FavnRunner.initialize_generation_marker(initialization)

    payload = %GenerationMarkerReadRequest{
      manifest: %{version | manifest: nil},
      asset_ref: asset.ref,
      require_relation_instance?: false
    }

    assignment = %{
      generation_assignment(version, payload)
      | task_id: "rt_generation_marker_read",
        task_kind: :generation_marker_read,
        payload: payload
    }

    assert {:ok, executor} =
             TaskExecutor.start_link(assignment: assignment, payload: payload, owner: self())

    assert_receive {:runner_task_finished, ^executor,
                    %Result{
                      outcome: :succeeded,
                      result: %GenerationMarkerReadResult{}
                    }}

    refute Application.fetch_env!(
             :favn_runner,
             :generation_operations_test_reconciliation_request
           ).require_relation_instance?
  end

  test "a temporary executor child performs one external operation and is not restarted" do
    {version, asset} = registered_target()
    Application.put_env(:favn_runner, :generation_operations_test_observer, self())

    on_exit(fn ->
      Application.delete_env(:favn_runner, :generation_operations_test_observer)
    end)

    payload = %GenerationCapabilitiesRequest{
      manifest: %{version | manifest: nil},
      asset_ref: asset.ref
    }

    assignment = generation_assignment(version, payload)

    {:ok, supervisor} = DynamicSupervisor.start_link(strategy: :one_for_one)

    assert {:ok, executor} =
             DynamicSupervisor.start_child(
               supervisor,
               {TaskExecutor, assignment: assignment, payload: payload, owner: self()}
             )

    monitor = Process.monitor(executor)
    assert_receive {:generation_operation_started, :generation_capabilities, _worker}
    assert_receive {:runner_task_finished, ^executor, %Result{outcome: :succeeded}}
    assert_receive {:DOWN, ^monitor, :process, ^executor, :normal}
    refute_receive {:generation_operation_started, :generation_capabilities, _worker}, 100
  end

  test "killing an executor also terminates its owned operation process" do
    {version, asset} = registered_target()
    Application.put_env(:favn_runner, :generation_operations_test_observer, {self(), :block})

    on_exit(fn ->
      Application.delete_env(:favn_runner, :generation_operations_test_observer)
    end)

    payload = %GenerationCapabilitiesRequest{
      manifest: %{version | manifest: nil},
      asset_ref: asset.ref
    }

    assignment = generation_assignment(version, payload)
    {:ok, supervisor} = DynamicSupervisor.start_link(strategy: :one_for_one)

    assert {:ok, executor} =
             DynamicSupervisor.start_child(
               supervisor,
               {TaskExecutor, assignment: assignment, payload: payload, owner: self()}
             )

    assert_receive {:generation_operation_started, :generation_capabilities, worker}
    executor_monitor = Process.monitor(executor)
    worker_monitor = Process.monitor(worker)

    Process.exit(executor, :kill)

    assert_receive {:DOWN, ^executor_monitor, :process, ^executor, :killed}
    assert_receive {:DOWN, ^worker_monitor, :process, ^worker, reason}
    assert reason in [:killed, :kill, :noproc]
    refute Process.alive?(worker)
  end

  test "an assigned non-default pool uses its exact release and ignores the default release" do
    {version, asset} = registered_target_for_exact_pool()

    payload = %GenerationCapabilitiesRequest{
      manifest: %{version | manifest: nil},
      asset_ref: asset.ref
    }

    assignment = %{
      generation_assignment(version, payload)
      | runner_pool: "duckdb_image",
        required_runner_release_id: FavnTestSupport.runner_release_id()
    }

    assert {:ok, executor} =
             TaskExecutor.start_link(assignment: assignment, payload: payload, owner: self())

    assert_receive {:runner_task_finished, ^executor,
                    %Result{
                      outcome: :succeeded,
                      result: %GenerationCapabilitiesResult{}
                    }}
  end

  test "operation interruption is safe to retry for reads and unknown for writes" do
    {version, asset} = registered_target()
    Application.put_env(:favn_runner, :generation_operations_test_observer, {self(), :block})

    on_exit(fn ->
      Application.delete_env(:favn_runner, :generation_operations_test_observer)
    end)

    read_payload = %GenerationCapabilitiesRequest{
      manifest: %{version | manifest: nil},
      asset_ref: asset.ref
    }

    read_assignment = generation_assignment(version, read_payload)

    assert {:ok, read_executor} =
             TaskExecutor.start_link(
               assignment: read_assignment,
               payload: read_payload,
               owner: self()
             )

    assert_receive {:generation_operation_started, :generation_capabilities, read_worker}
    Process.exit(read_worker, :kill)

    assert_receive {:runner_task_finished, ^read_executor,
                    %Result{outcome: :failed, retry_class: :safe_to_retry}}

    write_payload = initialization_request(version, asset)

    write_assignment = %{
      generation_assignment(version, write_payload)
      | task_id: "rt_generation_initialize",
        task_kind: :generation_marker_initialize,
        retry_class: :reconcile_before_retry,
        payload: write_payload
    }

    assert {:ok, write_executor} =
             TaskExecutor.start_link(
               assignment: write_assignment,
               payload: write_payload,
               owner: self()
             )

    assert_receive {:generation_operation_started, :generation_marker_initialize, _adapter_worker}
    Process.exit(:sys.get_state(write_executor).worker, :kill)

    assert_receive {:runner_task_finished, ^write_executor,
                    %Result{outcome: :unknown, retry_class: :reconcile_before_retry}}
  end

  test "adapter-proven safe generation failures enter the durable retry lifecycle" do
    {version, asset} = registered_target()

    initialization = initialization_request(version, asset)
    assert {:ok, initialized} = FavnRunner.initialize_generation_marker(initialization)

    payload = activation_request(version, asset, initialized.observed_marker)

    Application.put_env(
      :favn_runner,
      :generation_operations_test_activation_error,
      %Favn.SQL.Error{
        type: :execution_error,
        message: "activation failed before commit",
        retryable?: false,
        details: %{transaction_stage: :prepare}
      }
    )

    assignment = %{
      generation_assignment(version, payload)
      | task_id: "rt_generation_activation_safe_failure",
        task_kind: :generation_activate,
        retry_class: :reconcile_before_retry,
        payload: payload
    }

    assert {:ok, executor} =
             TaskExecutor.start_link(assignment: assignment, payload: payload, owner: self())

    assert_receive {:runner_task_finished, ^executor,
                    %Result{
                      outcome: :failed,
                      retry_class: :safe_to_retry,
                      result: nil,
                      error: %{outcome: :safe_failure, retryable?: true}
                    }}
  end

  test "cancelling an operation remains responsive and conservatively classifies writes" do
    {version, asset} = registered_target()
    Application.put_env(:favn_runner, :generation_operations_test_observer, {self(), :block})

    on_exit(fn ->
      Application.delete_env(:favn_runner, :generation_operations_test_observer)
    end)

    payload = initialization_request(version, asset)

    assignment = %{
      generation_assignment(version, payload)
      | task_id: "rt_generation_initialize_cancel",
        task_kind: :generation_marker_initialize,
        retry_class: :reconcile_before_retry,
        payload: payload
    }

    assert {:ok, executor} =
             TaskExecutor.start_link(assignment: assignment, payload: payload, owner: self())

    assert_receive {:generation_operation_started, :generation_marker_initialize, _worker}
    assert :ok = TaskExecutor.cancel(executor, :operator_request)

    assert_receive {:runner_task_finished, ^executor,
                    %Result{outcome: :unknown, retry_class: :reconcile_before_retry}}
  end

  defp registered_target do
    ref = {MyApp.GenerationTarget, :asset}
    relation = RelationRef.new!(connection: :generation_warehouse, schema: "main", name: "target")

    asset = %Asset{
      ref: ref,
      module: elem(ref, 0),
      name: elem(ref, 1),
      type: :sql,
      relation: relation,
      materialization: :table,
      execution_package_hash: String.duplicate("a", 64)
    }

    descriptor =
      TargetDescriptor.from_asset(Map.from_struct(asset),
        connection_definitions: %{
          generation_warehouse: %{adapter: Adapter, module: __MODULE__}
        },
        manifest_schema_version: 15,
        runner_contract_version: 13
      )

    asset = %{asset | target_descriptor: descriptor}

    manifest =
      %Manifest{
        schema_version: 15,
        runner_contract_version: 13,
        runner_releases: %{"default" => FavnTestSupport.runner_release_id()},
        assets: [asset],
        graph: %Graph{nodes: [ref], topo_order: [ref]}
      }

    {:ok, version} =
      Version.new(manifest,
        manifest_version_id:
          "mv_generation_operations_" <>
            Base.encode16(:crypto.strong_rand_bytes(6), case: :lower)
      )

    :ok = FavnRunner.register_manifest(version)
    {version, hd(version.manifest.assets)}
  end

  defp registered_target_for_exact_pool do
    ref = {MyApp.ExactPoolGenerationTarget, :asset}
    relation = RelationRef.new!(connection: :generation_warehouse, schema: "main", name: "target")

    asset = %Asset{
      ref: ref,
      module: elem(ref, 0),
      name: elem(ref, 1),
      type: :sql,
      relation: relation,
      materialization: :table,
      runner_pool: :duckdb_image,
      execution_package_hash: String.duplicate("a", 64)
    }

    descriptor =
      TargetDescriptor.from_asset(Map.from_struct(asset),
        connection_definitions: %{
          generation_warehouse: %{adapter: Adapter, module: __MODULE__}
        },
        manifest_schema_version: 15,
        runner_contract_version: 13
      )

    asset = %{asset | target_descriptor: descriptor}

    default_ref = {MyApp.DefaultPoolGenerationTarget, :asset}

    default_asset = %Asset{
      asset
      | ref: default_ref,
        module: elem(default_ref, 0),
        name: elem(default_ref, 1),
        runner_pool: nil,
        execution_package_hash: String.duplicate("b", 64),
        relation: %{relation | name: "default_target"}
    }

    default_asset = %{
      default_asset
      | target_descriptor:
          TargetDescriptor.from_asset(Map.from_struct(default_asset),
            connection_definitions: %{
              generation_warehouse: %{adapter: Adapter, module: __MODULE__}
            },
            manifest_schema_version: 15,
            runner_contract_version: 13
          )
    }

    manifest = %Manifest{
      schema_version: 15,
      runner_contract_version: 13,
      runner_releases: %{
        "default" => FavnTestSupport.runner_release_id(:alternate),
        "duckdb_image" => FavnTestSupport.runner_release_id()
      },
      assets: [asset, default_asset],
      graph: %Graph{nodes: [ref, default_ref], topo_order: [ref, default_ref]}
    }

    {:ok, version} =
      Version.new(manifest,
        manifest_version_id:
          "mv_exact_pool_generation_" <>
            Base.encode16(:crypto.strong_rand_bytes(6), case: :lower)
      )

    :ok =
      FavnRunner.ManifestStore.register_for_release(
        version,
        FavnTestSupport.runner_release_id()
      )

    {version, hd(version.manifest.assets)}
  end

  defp generation_assignment(version, payload) do
    %Assignment{
      command_id: "claim-generation-capabilities",
      workspace_id: "workspace-generation-capabilities",
      task_id: "rt_generation_capabilities_" <> Base.encode16(:crypto.strong_rand_bytes(4)),
      task_kind: :generation_capabilities,
      runner_instance_id: "runner-generation-capabilities",
      runner_session_generation: 1,
      assignment_generation: 1,
      runner_pool: "default",
      required_runner_release_id: Map.fetch!(version.runner_releases, "default"),
      assigned_at: DateTime.utc_now(),
      lease_expires_at: DateTime.add(DateTime.utc_now(), 30, :second),
      retry_class: :safe_to_retry,
      payload: payload
    }
  end

  defp initialization_request(version, asset) do
    %GenerationMarkerInitializationRequest{
      manifest_version_id: version.manifest_version_id,
      manifest_content_hash: version.content_hash,
      required_runner_release_id: Map.fetch!(version.runner_releases, "default"),
      target_id: asset.target_descriptor.target_id,
      target_generation_id: @previous_generation_id,
      active_relation: asset.relation,
      expected_physical_fingerprint: @active_fingerprint,
      initialization_operation_id: "initialization-generation-operations",
      initialization_token: "initialization-token-generation-operations"
    }
  end

  defp activation_request(version, asset, expected_marker) do
    max_identifier_bytes = 128

    %GenerationActivationRequest{
      manifest_version_id: version.manifest_version_id,
      manifest_content_hash: version.content_hash,
      required_runner_release_id: Map.fetch!(version.runner_releases, "default"),
      rebuild_operation_id: "rebuild-generation-operations",
      rebuild_action_id: "action-generation-operations",
      target_id: asset.target_descriptor.target_id,
      previous_generation_id: @previous_generation_id,
      candidate_generation_id: @candidate_generation_id,
      expected_candidate_fingerprint: @candidate_fingerprint,
      active_relation: asset.relation,
      candidate_relation:
        GenerationRelation.candidate(
          asset.relation,
          @candidate_generation_id,
          max_identifier_bytes
        ),
      retired_relation:
        GenerationRelation.retired(
          asset.relation,
          @previous_generation_id,
          max_identifier_bytes
        ),
      activation_token: "activation-generation-operations",
      expected_marker: expected_marker
    }
  end
end
