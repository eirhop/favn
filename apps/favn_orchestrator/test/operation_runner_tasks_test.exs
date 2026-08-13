defmodule FavnOrchestrator.OperationRunnerTasksTest do
  use ExUnit.Case, async: false

  alias Favn.Contracts.RelationInspectionRequest
  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Version
  alias Favn.RelationRef
  alias FavnOrchestrator.OperationRunnerTasks
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Results.RunnerTask
  alias FavnOrchestrator.Persistence.Runtime
  alias FavnOrchestrator.Persistence.Stores
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.RunnerTaskResultRouter

  defmodule Store do
    @behaviour FavnOrchestrator.Persistence.RunnerTaskStore

    def enqueue(command) do
      Agent.get_and_update(agent(), fn state ->
        case Map.fetch(state.enqueue_receipts, command.task_id) do
          {:ok, receipt} ->
            {{:ok, receipt}, %{state | commands: [command | state.commands]}}

          :error ->
            task = %RunnerTask{
              workspace_id: command.workspace_context.workspace_id,
              task_id: command.task_id,
              domain_identity: command.domain_identity,
              task_kind: command.task_kind,
              runner_pool: command.runner_pool,
              required_runner_release_id: command.required_runner_release_id,
              required_capability: command.required_capability,
              retry_class: command.retry_class,
              status: :queued,
              payload: command.payload,
              payload_hash: command.payload_hash,
              orchestration_context: command.orchestration_context,
              assignment_generation: 0,
              enqueued_at: command.occurred_at,
              inserted_at: command.occurred_at
            }

            next = %{
              state
              | tasks: Map.put(state.tasks, task.task_id, task),
                enqueue_receipts: Map.put(state.enqueue_receipts, task.task_id, task),
                commands: [command | state.commands]
            }

            {{:ok, task}, next}
        end
      end)
    end

    def get(query) do
      Agent.get_and_update(agent(), fn state ->
        reply =
          case Map.fetch(state.tasks, query.task_id) do
            {:ok, task} -> {:ok, task}
            :error -> unavailable()
          end

        {reply, %{state | reads: state.reads + 1}}
      end)
    end

    def claim(_command), do: unavailable()
    def transition(_command), do: unavailable()
    def persist_runtime_inputs(_command), do: unavailable()
    def append_log_batch(_command), do: unavailable()
    def complete(_command), do: unavailable()

    def request_cancellation(command) do
      Agent.get_and_update(agent(), fn state ->
        task = Map.fetch!(state.tasks, command.task_id)
        cancelled = %{task | status: :cancelled, terminal_at: command.occurred_at}

        {{:ok, cancelled},
         %{
           state
           | tasks: Map.put(state.tasks, command.task_id, cancelled),
             cancellations: [command | state.cancellations]
         }}
      end)
    end

    def acknowledge_cancellation(_command), do: unavailable()
    def release(_command), do: unavailable()

    def retry(command) do
      Agent.get_and_update(agent(), fn state ->
        task = Map.fetch!(state.tasks, command.task_id)

        retried = %{
          task
          | status: :queued,
            result_version: nil,
            result: nil,
            error: nil,
            terminal_at: nil
        }

        {{:ok, retried},
         %{
           state
           | tasks: Map.put(state.tasks, command.task_id, retried),
             retries: [command | state.retries]
         }}
      end)
    end

    def recover_expired(_command), do: unavailable()
    def reconcile_demand(_command), do: unavailable()
    def ensure_demand(_command), do: unavailable()
    def page_run(_query), do: unavailable()
    def page_workspace(_query), do: unavailable()
    def demand(_query), do: unavailable()
    def list_demands(_query), do: unavailable()
    def release_drain(_query), do: unavailable()
    def capacity_health(_query), do: unavailable()
    def list_release_drains(_query), do: unavailable()

    defp agent,
      do: Application.fetch_env!(:favn_orchestrator, :operation_runner_tasks_test_agent)

    defp unavailable,
      do: {:error, Error.new(:unavailable, "operation runner task test store")}
  end

  defmodule RebuildStore do
    def get(query) do
      agent =
        Application.fetch_env!(:favn_orchestrator, :operation_runner_tasks_test_agent)

      case Agent.get(agent, &Map.get(&1.rebuilds, query.operation_id)) do
        nil -> {:error, Error.new(:not_found, "rebuild not found")}
        operation -> {:ok, operation}
      end
    end
  end

  setup do
    {:ok, agent} =
      Agent.start_link(fn ->
        %{
          commands: [],
          retries: [],
          cancellations: [],
          tasks: %{},
          enqueue_receipts: %{},
          rebuilds: %{},
          reads: 0
        }
      end)

    Application.put_env(:favn_orchestrator, :operation_runner_tasks_test_agent, agent)

    stores = struct(Stores, runner_tasks: Store, rebuilds: RebuildStore)
    start_supervised!({Runtime, %Runtime{backend: __MODULE__, options: [], stores: stores}})
    start_supervised!({Task.Supervisor, name: FavnOrchestrator.RunnerClaimSupervisor})
    start_supervised!({Task.Supervisor, name: FavnOrchestrator.RunnerTaskWaitSupervisor})
    start_supervised!({RunnerTaskResultRouter, []})

    on_exit(fn ->
      Application.delete_env(:favn_orchestrator, :operation_runner_tasks_test_agent)
    end)

    {:ok, context} =
      WorkspaceContext.new("workspace-operation-task", "operator", [:workspace_admin])

    {version, asset} = version()
    %{agent: agent, asset: asset, context: context, version: version}
  end

  test "freezes an arbitrary logical pool and release into an idempotent task", fixture do
    request = inspection_request(fixture)
    identity = {:target_inspection, "target-sales"}

    assert {:ok, first} =
             OperationRunnerTasks.ensure(
               fixture.context,
               fixture.version,
               fixture.asset.ref,
               :relation_inspection,
               request,
               identity
             )

    assert {:ok, replay} =
             OperationRunnerTasks.ensure(
               fixture.context,
               fixture.version,
               fixture.asset.ref,
               :relation_inspection,
               request,
               identity
             )

    assert replay.task_id == first.task_id
    assert first.runner_pool == "duckdb_image"
    assert first.required_runner_release_id == fixture.version.runner_releases["duckdb_image"]

    commands = Agent.get(fixture.agent, & &1.commands)
    assert length(commands) == 2
    assert Enum.uniq_by(commands, & &1.task_id) |> length() == 1
    assert Enum.all?(commands, &(&1.runner_pool == "duckdb_image"))
  end

  test "a later caller resumes from the durable terminal result", fixture do
    assert {:ok, task} =
             OperationRunnerTasks.ensure(
               fixture.context,
               fixture.version,
               fixture.asset.ref,
               :relation_inspection,
               inspection_request(fixture),
               {:target_inspection, "target-resume"}
             )

    abandoned =
      Task.async(fn ->
        OperationRunnerTasks.await(fixture.context, task.task_id, timeout: 5_000)
      end)

    assert_eventually(fn -> Agent.get(fixture.agent, & &1.reads) >= 2 end)
    Task.shutdown(abandoned, :brutal_kill)

    terminal =
      Agent.get_and_update(fixture.agent, fn state ->
        succeeded =
          state.tasks
          |> Map.fetch!(task.task_id)
          |> Map.merge(%{
            status: :succeeded,
            retry_class: :terminal,
            result: :inspection_complete,
            terminal_at: DateTime.utc_now()
          })

        {succeeded, %{state | tasks: Map.put(state.tasks, task.task_id, succeeded)}}
      end)

    RunnerTaskResultRouter.notify(terminal)

    assert {:ok, :inspection_complete} =
             OperationRunnerTasks.await(fixture.context, task.task_id, timeout: 1_000)
  end

  test "a later caller durably requeues a terminal safe failure", fixture do
    identity = {:target_inspection, "target-safe-retry"}

    assert {:ok, task} =
             OperationRunnerTasks.ensure(
               fixture.context,
               fixture.version,
               fixture.asset.ref,
               :relation_inspection,
               inspection_request(fixture),
               identity
             )

    Agent.update(fixture.agent, fn state ->
      failed = %{
        task
        | status: :failed,
          retry_class: :safe_to_retry,
          assignment_generation: 1,
          result_version: 1,
          error: %{outcome: :safe_failure},
          terminal_at: DateTime.utc_now()
      }

      %{state | tasks: Map.put(state.tasks, task.task_id, failed)}
    end)

    assert {:ok, retried} =
             OperationRunnerTasks.ensure(
               fixture.context,
               fixture.version,
               fixture.asset.ref,
               :relation_inspection,
               inspection_request(fixture),
               identity
             )

    assert retried.status == :queued
    assert retried.assignment_generation == 1
    assert Agent.get(fixture.agent, & &1.enqueue_receipts[task.task_id].status) == :queued
    assert [retry] = Agent.get(fixture.agent, & &1.retries)
    assert retry.expected_assignment_generation == 1
    assert retry.expected_result_version == 1
  end

  test "a planning cancellation cancels the currently awaited runner task", fixture do
    assert {:ok, task} =
             OperationRunnerTasks.ensure(
               fixture.context,
               fixture.version,
               fixture.asset.ref,
               :relation_inspection,
               inspection_request(fixture),
               {:target_inspection, "target-cancel"}
             )

    operation_id = "rebuild-operation-cancel"

    Agent.update(fixture.agent, fn state ->
      %{state | rebuilds: %{operation_id => %{state: :planning}}}
    end)

    awaiting =
      Task.async(fn ->
        OperationRunnerTasks.await(fixture.context, task.task_id,
          rebuild_operation_id: operation_id,
          timeout: 5_000
        )
      end)

    assert_eventually(fn -> Agent.get(fixture.agent, & &1.reads) >= 2 end)

    Agent.update(fixture.agent, fn state ->
      %{state | rebuilds: %{operation_id => %{state: :cancelled}}}
    end)

    assert {:error, :rebuild_operation_cancelled} = Task.await(awaiting, 2_000)
    assert [cancellation] = Agent.get(fixture.agent, & &1.cancellations)
    assert cancellation.task_id == task.task_id
  end

  defp version do
    release = FavnTestSupport.runner_release_id()
    ref = {MyApp.OperationSales, :asset}

    asset =
      FavnTestSupport.with_target_descriptor(%Asset{
        ref: ref,
        module: elem(ref, 0),
        name: elem(ref, 1),
        type: :sql,
        runner_pool: :duckdb_image,
        relation: RelationRef.new!(connection: :warehouse, schema: "gold", name: "sales"),
        materialization: :table,
        execution_package_hash: String.duplicate("a", 64)
      })

    manifest =
      %Manifest{assets: [asset]}
      |> FavnTestSupport.with_manifest_graph()
      |> FavnTestSupport.with_manifest_contract(%{"duckdb_image" => release})

    {:ok, version} = Version.new(manifest, manifest_version_id: "manifest-operation-task")
    {version, hd(version.manifest.assets)}
  end

  defp inspection_request(fixture) do
    %RelationInspectionRequest{
      manifest_version_id: fixture.version.manifest_version_id,
      manifest_content_hash: fixture.version.content_hash,
      required_runner_release_id: fixture.version.runner_releases["duckdb_image"],
      asset_ref: fixture.asset.ref,
      relation: fixture.asset.relation,
      include: [:relation],
      sample_limit: 0
    }
  end

  defp assert_eventually(fun, attempts \\ 100)
  defp assert_eventually(_fun, 0), do: flunk("condition did not become true")

  defp assert_eventually(fun, attempts) do
    if fun.() do
      :ok
    else
      Process.sleep(10)
      assert_eventually(fun, attempts - 1)
    end
  end
end
