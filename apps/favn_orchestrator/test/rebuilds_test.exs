defmodule FavnOrchestrator.RebuildsTest do
  use ExUnit.Case, async: false

  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.ExecutionPackage
  alias Favn.Manifest.SQLExecution
  alias Favn.Manifest.Version
  alias Favn.Contracts.GenerationMarker
  alias Favn.Contracts.RelationInspectionResult
  alias Favn.RelationRef
  alias Favn.RuntimeInputResolver.Ref, as: RuntimeInputResolverRef
  alias Favn.SQL.Template
  alias Favn.TargetCompatibility.PhysicalFingerprint
  alias FavnOrchestrator.Persistence.Commands.DeploymentTarget
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Queries.GetRebuild
  alias FavnOrchestrator.Persistence.Results.RebuildOperation
  alias FavnOrchestrator.Persistence.Results.RuntimeState
  alias FavnOrchestrator.Persistence.Results.TargetBinding
  alias FavnOrchestrator.Persistence.Runtime, as: PersistenceRuntime
  alias FavnOrchestrator.Persistence.Stores
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.Rebuilds

  defmodule Inputs do
  end

  defmodule Store do
    alias Favn.Manifest.Serializer
    alias FavnOrchestrator.Persistence.Error
    alias FavnOrchestrator.Persistence.Queries.GetRebuild
    alias FavnOrchestrator.Persistence.Results.RebuildOperation

    def get(%GetRebuild{operation_id: operation_id}) do
      case Process.get({:rebuild_operation, operation_id}) do
        nil -> {:error, Error.new(:not_found, "rebuild not found")}
        operation -> {:ok, operation}
      end
    end

    def get_runtime_state(_query), do: {:ok, Process.get(:rebuild_runtime)}
    def get_deployment_targets(_query), do: {:ok, Process.get(:rebuild_grants)}
    def get_deployment_manifest(_query), do: {:ok, Process.get(:rebuild_version)}

    def get_execution_package(query) do
      {:ok, Process.get(:rebuild_packages) |> Map.fetch!(query.content_hash)}
    end

    def get_bindings(query) do
      bindings = Process.get(:rebuild_bindings, [])
      {:ok, Enum.filter(bindings, &(&1.target_id in query.target_ids))}
    end

    def create_plan(command) do
      send(Process.get(:rebuild_test_pid), {:create_rebuild_plan, command})

      payload =
        command.plan_payload
        |> Serializer.encode_canonical!()
        |> Jason.decode!()

      operation = %RebuildOperation{
        workspace_id: command.workspace_context.workspace_id,
        operation_id: command.operation_id,
        root_target_id: command.root_target_id,
        manifest_version_id: command.manifest_version_id,
        active_generation_id: command.active_generation_id,
        candidate_generation_id: command.candidate_generation_id,
        plan_hash: command.plan_hash,
        plan_version: command.plan_version,
        plan_payload: payload,
        actor_id: command.actor_id,
        session_id: command.session_id,
        reason: command.reason,
        idempotency_key: command.idempotency_key,
        evaluated_at: command.evaluated_at,
        action_count: length(command.actions),
        window_count: length(command.items),
        state: :planned,
        phase: :planned,
        cleanup_state: :not_started,
        cancel_requested: false,
        dispatcher_fencing_token: 0,
        version: 1,
        inserted_at: command.occurred_at,
        updated_at: command.occurred_at
      }

      Process.put({:rebuild_operation, command.operation_id}, operation)
      {:ok, operation}
    end

    def acquire_many(command) do
      send(Process.get(:rebuild_test_pid), {:acquire_rebuild_locks, command})
      {:ok, []}
    end
  end

  defmodule RunnerClient do
    alias Favn.RuntimeInput.Resolution

    def generation_capabilities(_version, _asset_ref, _opts) do
      {:ok,
       %{
         transactional_ddl: :supported,
         isolated_candidates: :supported,
         physical_inspection: :supported,
         atomic_swap: :supported,
         marker_reconciliation: :supported,
         idempotent_discard: :supported,
         snapshots: :unsupported,
         max_identifier_bytes: 128
       }}
    end

    def generation_marker(_version, asset_ref, _opts),
      do: {:ok, Process.get(:rebuild_markers) |> Map.fetch!(asset_ref)}

    def inspect_relation(request, _opts),
      do: {:ok, Process.get(:rebuild_inspections) |> Map.fetch!(request.asset_ref)}

    def resolve_runtime_inputs(work, _opts) do
      send(Process.get(:rebuild_test_pid), {:resolved_rebuild_runtime_inputs, work.run_id})

      Resolution.new(
        resolver: FavnOrchestrator.RebuildsTest.Inputs,
        params: %{run_id: work.run_id},
        input_identity: work.run_id
      )
    end
  end

  setup do
    previous_client = Application.get_env(:favn_orchestrator, :runner_client)
    previous_opts = Application.get_env(:favn_orchestrator, :runner_client_opts)
    Application.put_env(:favn_orchestrator, :runner_client, RunnerClient)
    Application.put_env(:favn_orchestrator, :runner_client_opts, [])

    stores = %Stores{
      registry: Store,
      runs: Store,
      run_ownership: Store,
      scheduler: Store,
      admission: Store,
      resource_circuits: Store,
      target_generations: Store,
      rebuilds: Store,
      target_operation_locks: Store,
      materialization: Store,
      backfills: Store,
      operator_reads: Store,
      logs: Store,
      identity: Store,
      maintenance: Store
    }

    assert {:ok, pid} =
             PersistenceRuntime.start_link(%PersistenceRuntime{
               backend: __MODULE__,
               options: [],
               stores: stores
             })

    Process.put(:rebuild_test_pid, self())
    {version, root, downstream, packages} = version()
    runtime = runtime(version)
    bindings = [binding(root, version, 1), binding(downstream, version, 1)]

    Process.put(:rebuild_version, version)
    Process.put(:rebuild_runtime, runtime)
    Process.put(:rebuild_bindings, bindings)
    Process.put(:rebuild_packages, Map.new(packages, &{&1.content_hash, &1}))

    Process.put(
      :rebuild_markers,
      Map.new(bindings, &{asset_ref(version, &1.target_id), marker(&1)})
    )

    Process.put(
      :rebuild_inspections,
      Map.new([root, downstream], &{&1.ref, inspection(&1, version)})
    )

    Process.put(:rebuild_grants, [
      %DeploymentTarget{
        target_kind: :asset,
        target_id: root.target_descriptor.target_id,
        selection_source: :explicit,
        customer_visible: true,
        descriptor: %{}
      }
    ])

    on_exit(fn ->
      if Process.alive?(pid), do: GenServer.stop(pid)
      restore_env(:runner_client, previous_client)
      restore_env(:runner_client_opts, previous_opts)
    end)

    {:ok, version: version, root: root, downstream: downstream, bindings: bindings}
  end

  test "freezes an exact downstream rebuild plan and replays its idempotency key", fixture do
    {:ok, context} =
      WorkspaceContext.new("workspace-rebuild", "operator", [:customer_operator])

    now = DateTime.utc_now()

    assert {:ok, first} =
             Rebuilds.plan(context, fixture.root.target_descriptor.target_id, "schema changed",
               idempotency_key: "schema-change-1",
               evaluated_at: now,
               occurred_at: now
             )

    assert_received {:create_rebuild_plan, command}
    assert Enum.map(command.actions, & &1.action) == [:rebuild, :rebuild]
    assert command.plan_payload.item_count == 2
    assert byte_size(command.plan_payload.items_digest) == 64
    refute Map.has_key?(command.plan_payload, :items)
    assert command.plan_payload.coverage == nil
    assert command.plan_payload.evaluated_range == %{start_at: nil, end_at: nil}

    root_binding_snapshot =
      command.plan_payload.binding_snapshot[fixture.root.target_descriptor.target_id]

    assert root_binding_snapshot["reason_code"] == "incompatible_descriptor"
    assert root_binding_snapshot["compatibility_diff"] == %{"columns" => "changed"}

    assert command.plan_payload.assurance_expectations == %{
             fixture.root.target_descriptor.target_id => %{
               contract_required: false,
               checks: []
             },
             fixture.downstream.target_descriptor.target_id => %{
               contract_required: false,
               checks: []
             }
           }

    Enum.each(command.items, fn item ->
      expected_run_id =
        deterministic_command_id(
          "run-rebuild",
          command.operation_id <> ":" <> item.item_id
        )

      assert item.runtime_input_expectation.input_identity == expected_run_id
      assert_received {:resolved_rebuild_runtime_inputs, ^expected_run_id}
    end)

    [root_action, downstream_action] = command.actions
    assert Ecto.UUID.cast(root_action.candidate_generation.target_generation_id) != :error
    assert Ecto.UUID.cast(downstream_action.candidate_generation.target_generation_id) != :error

    assert [%{target_id: pinned_target, data_plane_marker: marker}] =
             downstream_action.pinned_input_generation_ids

    assert pinned_target == fixture.root.target_descriptor.target_id
    assert marker["active_generation_id"] || marker[:active_generation_id]

    assert hd(downstream_action.pinned_input_generation_ids).target_generation_id ==
             root_action.candidate_generation.target_generation_id

    assert {:ok, replayed} =
             Rebuilds.plan(context, fixture.root.target_descriptor.target_id, "schema changed",
               idempotency_key: "schema-change-1"
             )

    refute first.idempotency_replay?
    assert replayed.idempotency_replay?
    assert %{replayed | idempotency_replay?: false} == first
    refute_received {:create_rebuild_plan, _command}
  end

  test "rejects approval when a pinned target binding changes", fixture do
    {:ok, context} =
      WorkspaceContext.new("workspace-rebuild", "admin", [:workspace_admin])

    now = DateTime.utc_now()

    assert {:ok, plan} =
             Rebuilds.plan(context, fixture.root.target_descriptor.target_id, "schema changed",
               operation_id: "rebuild-stale-plan",
               evaluated_at: now,
               occurred_at: now
             )

    Process.put(
      :rebuild_bindings,
      Enum.map(fixture.bindings, fn binding ->
        if binding.target_id == fixture.downstream.target_descriptor.target_id,
          do: %{binding | version: binding.version + 1},
          else: binding
      end)
    )

    assert {:error, %Error{kind: :conflict, details: %{reason_code: "rebuild_plan_stale"}}} =
             Rebuilds.start(context, plan.plan_id, plan.plan_hash)

    refute_received {:acquire_rebuild_locks, _command}
  end

  test "rejects a plan when an affected persisted target has no active generation", fixture do
    {:ok, context} =
      WorkspaceContext.new("workspace-rebuild", "operator", [:customer_operator])

    Process.put(:rebuild_bindings, [hd(fixture.bindings)])

    assert {:error, %Error{kind: :conflict}} =
             Rebuilds.plan(context, fixture.root.target_descriptor.target_id, "schema changed",
               operation_id: "rebuild-missing-downstream-generation"
             )

    refute_received {:create_rebuild_plan, _command}
  end

  test "reports stable conflicts for drift and unresolved operator decisions", fixture do
    {:ok, context} =
      WorkspaceContext.new("workspace-rebuild", "operator", [:customer_operator])

    for {status, expected_code} <- [
          unexpected_drift: "target_drift",
          operator_decision: "operator_decision_required"
        ] do
      bindings =
        Enum.map(fixture.bindings, fn
          %{target_id: target_id} = binding
          when target_id == fixture.root.target_descriptor.target_id ->
            %{binding | compatibility_status: status}

          binding ->
            binding
        end)

      Process.put(:rebuild_bindings, bindings)

      assert {:error, %Error{kind: :conflict, details: %{reason_code: ^expected_code}}} =
               Rebuilds.plan(context, fixture.root.target_descriptor.target_id, "schema changed",
                 operation_id: "rebuild-conflict-#{status}"
               )
    end
  end

  defp version do
    root_ref = {__MODULE__.Root, :asset}
    downstream_ref = {__MODULE__.Downstream, :asset}

    {root, root_package} = persisted_asset(root_ref, "root", [])

    {downstream, downstream_package} =
      persisted_asset(downstream_ref, "downstream", [root_ref])

    manifest =
      %Manifest{assets: [root, downstream]}
      |> FavnTestSupport.with_manifest_graph()
      |> FavnTestSupport.with_manifest_contract()

    {:ok, version} = Version.new(manifest, manifest_version_id: "manifest-rebuild-test")
    [root, downstream] = version.manifest.assets
    {version, root, downstream, [root_package, downstream_package]}
  end

  defp persisted_asset(ref, relation_name, depends_on) do
    {:ok, package} =
      ExecutionPackage.new(ref, %SQLExecution{
        sql: "SELECT 1 AS id",
        template: Template.compile!("SELECT 1 AS id", file: "rebuild_test.sql", line: 1),
        runtime_inputs: %RuntimeInputResolverRef{module: Inputs}
      })

    asset =
      FavnTestSupport.with_target_descriptor(%Asset{
        ref: ref,
        module: elem(ref, 0),
        name: elem(ref, 1),
        type: :sql,
        relation:
          RelationRef.new!(connection: :warehouse, schema: "analytics", name: relation_name),
        materialization: :table,
        depends_on: depends_on,
        execution_package_hash: package.content_hash
      })

    {asset, package}
  end

  defp runtime(version) do
    %RuntimeState{
      workspace_id: "workspace-rebuild",
      deployment_id: "deployment-rebuild",
      manifest_version_id: version.manifest_version_id,
      revision: 1
    }
  end

  defp binding(asset, version, binding_version) do
    generation_id = Ecto.UUID.generate()
    relation = Map.from_struct(asset.relation)

    %TargetBinding{
      workspace_id: "workspace-rebuild",
      target_id: asset.target_descriptor.target_id,
      active_generation_id: generation_id,
      active_manifest_id: version.manifest_version_id,
      active_descriptor_hash: asset.target_descriptor.descriptor_hash,
      active_physical_relation: relation,
      active_data_plane_marker: %{
        active_generation_id: generation_id,
        activation_operation_id: "initial-generation",
        activation_token: "initial-token",
        activated_at: DateTime.utc_now(),
        target_id: asset.target_descriptor.target_id,
        active_relation: relation
      },
      desired_manifest_id: version.manifest_version_id,
      desired_descriptor_hash: asset.target_descriptor.descriptor_hash,
      compatibility_status: :rebuild_required,
      reason_code: "incompatible_descriptor",
      compatibility_diff: %{"columns" => "changed"},
      active_physical_fingerprint: physical_fingerprint(asset, version),
      version: binding_version,
      updated_at: DateTime.utc_now()
    }
  end

  defp inspection(asset, version) do
    %RelationInspectionResult{
      asset_ref: asset.ref,
      required_runner_release_id: version.required_runner_release_id,
      relation_ref: asset.relation,
      relation: %{
        catalog: asset.relation.catalog,
        schema: asset.relation.schema,
        name: asset.relation.name,
        type: "table"
      },
      columns: [],
      adapter: :test,
      inspected_at: DateTime.utc_now()
    }
  end

  defp physical_fingerprint(asset, version) do
    {:ok, fingerprint} = asset |> inspection(version) |> PhysicalFingerprint.from_inspection()
    fingerprint.fingerprint
  end

  defp marker(binding) do
    value = binding.active_data_plane_marker

    %GenerationMarker{
      target_id: value.target_id,
      active_relation: RelationRef.new!(value.active_relation),
      active_generation_id: value.active_generation_id,
      activation_operation_id: value.activation_operation_id,
      activation_token: value.activation_token,
      activated_at: value.activated_at
    }
  end

  defp asset_ref(version, target_id) do
    version.manifest.assets
    |> Enum.find(&(&1.target_descriptor.target_id == target_id))
    |> Map.fetch!(:ref)
  end

  defp restore_env(key, nil), do: Application.delete_env(:favn_orchestrator, key)
  defp restore_env(key, value), do: Application.put_env(:favn_orchestrator, key, value)

  defp deterministic_command_id(prefix, identity) do
    digest = :crypto.hash(:sha256, identity) |> Base.url_encode64(padding: false)
    prefix <> ":" <> String.slice(digest, 0, 40)
  end
end
