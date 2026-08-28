defmodule FavnOrchestrator.RebuildDispatcherCleanupTest do
  use ExUnit.Case, async: false

  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Version
  alias Favn.RelationRef
  alias FavnOrchestrator.Persistence.Results.RebuildAction
  alias FavnOrchestrator.Persistence.Results.RebuildLease
  alias FavnOrchestrator.Persistence.Results.RebuildOperation
  alias FavnOrchestrator.Persistence.Runtime
  alias FavnOrchestrator.Persistence.Stores
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.RebuildDispatcher

  defmodule RebuildStore do
    def transition_operation(command) do
      Agent.get_and_update(agent(), fn state ->
        operation = state.operation

        updated = %{
          operation
          | state: command.state,
            phase: command.phase,
            terminal_error: command.terminal_error,
            cleanup_state: command.cleanup_state,
            version: operation.version + 1
        }

        {{:ok, updated}, %{state | operation: updated}}
      end)
    end

    def transition_generation(command) do
      Agent.update(agent(), fn state ->
        Map.update!(state, :generation_transitions, &[command.status | &1])
      end)

      :ok
    end

    def get(_query), do: {:ok, Agent.get(agent(), & &1.operation)}

    defp agent,
      do: Application.fetch_env!(:favn_orchestrator, :rebuild_dispatcher_cleanup_test_agent)
  end

  defmodule RegistryStore do
    def get_runtime_state(_query) do
      state = Agent.get(agent(), & &1)

      {:ok,
       %{
         manifest_version_id: state.version.manifest_version_id,
         deployment_id: "deployment"
       }}
    end

    def get_deployment_manifest(_query), do: {:ok, Agent.get(agent(), & &1.version)}
    def get_manifest_size(_selector), do: {:ok, 1_024}

    defp agent,
      do: Application.fetch_env!(:favn_orchestrator, :rebuild_dispatcher_cleanup_test_agent)
  end

  defmodule RunnerTaskStore do
    alias Favn.Contracts.GenerationDiscardResult
    alias Favn.Contracts.RunnerTask.PersistenceCodec
    alias FavnOrchestrator.Persistence.Results.RunnerTask

    def enqueue(command) do
      {:ok, request} = PersistenceCodec.decode_payload(:generation_discard, command.payload)

      result = %GenerationDiscardResult{
        required_runner_release_id: request.required_runner_release_id,
        target_id: request.target_id,
        candidate_generation_id: request.candidate_generation_id,
        discard_token: request.discard_token,
        outcome: :discarded,
        candidate_present: false,
        completed_at: DateTime.utc_now()
      }

      :ok = GenerationDiscardResult.validate(result, request)

      task = %RunnerTask{
        workspace_id: command.workspace_context.workspace_id,
        task_id: command.task_id,
        task_kind: command.task_kind,
        runner_pool: command.runner_pool,
        required_runner_release_id: command.required_runner_release_id,
        retry_class: :terminal,
        status: :succeeded,
        result: result,
        terminal_at: DateTime.utc_now()
      }

      Agent.update(agent(), fn state ->
        state
        |> Map.put(:runner_task, task)
        |> Map.update!(:discard_requests, &[request | &1])
      end)

      {:ok, task}
    end

    def get(_query), do: {:ok, Agent.get(agent(), & &1.runner_task)}

    defp agent,
      do: Application.fetch_env!(:favn_orchestrator, :rebuild_dispatcher_cleanup_test_agent)
  end

  defmodule LockStore do
    def release_many(command) do
      Agent.update(agent(), &Map.put(&1, :released_locks, command.locks))
      :ok
    end

    defp agent,
      do: Application.fetch_env!(:favn_orchestrator, :rebuild_dispatcher_cleanup_test_agent)
  end

  setup do
    {version, asset} = version()
    target_id = asset.target_descriptor.target_id
    release_id = version.runner_releases["duckdb_image"]
    candidate_generation_id = Ecto.UUID.generate()

    action = %RebuildAction{
      operation_id: "combined-append-cleanup",
      target_id: target_id,
      action: :rebuild,
      runner_pool: "duckdb_image",
      required_runner_release_id: release_id,
      candidate_generation_id: candidate_generation_id,
      status: :failed,
      terminal_error: %{reason: "combined_append_claim_absent_and_candidate_requires_cleanup"},
      version: 2
    }

    operation = %RebuildOperation{
      workspace_id: "workspace",
      operation_id: action.operation_id,
      root_target_id: action.target_id,
      manifest_version_id: version.manifest_version_id,
      plan_hash: String.duplicate("a", 64),
      plan_version: 1,
      plan_payload: %{
        combined_append: true,
        capabilities: %{target_id => %{max_identifier_bytes: 255}}
      },
      actor_id: "operator",
      reason: "test cleanup",
      idempotency_key: "combined-append-cleanup",
      evaluated_at: DateTime.utc_now(),
      action_count: 1,
      window_count: 1,
      state: :building,
      phase: :building,
      cleanup_state: :not_started,
      version: 3,
      dispatcher: %RebuildLease{owner: "dispatcher", fencing_token: 7},
      actions: [action]
    }

    {:ok, agent} =
      Agent.start_link(fn ->
        %{
          operation: operation,
          version: version,
          runner_task: nil,
          discard_requests: [],
          generation_transitions: [],
          released_locks: nil
        }
      end)

    Application.put_env(:favn_orchestrator, :rebuild_dispatcher_cleanup_test_agent, agent)

    stores =
      struct(Stores,
        rebuilds: RebuildStore,
        registry: RegistryStore,
        runner_tasks: RunnerTaskStore,
        target_operation_locks: LockStore
      )

    start_supervised!({Runtime, %Runtime{backend: __MODULE__, options: [], stores: stores}})

    on_exit(fn ->
      Application.delete_env(:favn_orchestrator, :rebuild_dispatcher_cleanup_test_agent)
    end)

    {:ok, context} = WorkspaceContext.new("workspace", "operator", [:workspace_admin])

    %{
      agent: agent,
      context: context,
      operation: operation,
      candidate_generation_id: candidate_generation_id
    }
  end

  test "an unknown combined append failure discards its candidate", fixture do
    assert :ok =
             RebuildDispatcher.process_claimed_operation(
               fixture.context,
               fixture.operation,
               [],
               %{owner_id: "dispatcher"}
             )

    failed = Agent.get(fixture.agent, & &1.operation)
    assert failed.state == :failed
    assert failed.phase == :terminal
    assert failed.cleanup_state == :pending

    assert :ok =
             RebuildDispatcher.process_claimed_operation(
               fixture.context,
               failed,
               [],
               %{owner_id: "dispatcher"}
             )

    state = Agent.get(fixture.agent, & &1)
    assert state.operation.cleanup_state == :complete
    assert state.generation_transitions == [:discarded]

    assert [%{candidate_generation_id: candidate_generation_id}] = state.discard_requests
    assert candidate_generation_id == fixture.candidate_generation_id
    assert state.released_locks == []
  end

  defp version do
    release = FavnTestSupport.runner_release_id()
    ref = {MyApp.CombinedAppendCleanup, :asset}

    asset =
      FavnTestSupport.with_target_descriptor(%Asset{
        ref: ref,
        module: elem(ref, 0),
        name: elem(ref, 1),
        type: :sql,
        runner_pool: :duckdb_image,
        relation: RelationRef.new!(connection: :warehouse, schema: "gold", name: "orders"),
        materialization: :table,
        execution_package_hash: String.duplicate("b", 64)
      })

    manifest =
      %Manifest{assets: [asset]}
      |> FavnTestSupport.with_manifest_graph()
      |> FavnTestSupport.with_manifest_contract(%{"duckdb_image" => release})

    {:ok, version} =
      Version.new(manifest, manifest_version_id: "manifest-combined-append-cleanup")

    {version, hd(version.manifest.assets)}
  end
end
