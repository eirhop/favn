defmodule FavnOrchestrator.RunServer.Execution.SequentialTest do
  use ExUnit.Case, async: false

  alias Favn.Manifest.Version
  alias Favn.Plan
  alias FavnOrchestrator.Persistence.Runtime, as: PersistenceRuntime
  alias FavnOrchestrator.Persistence.Stores
  alias FavnOrchestrator.RunServer.Execution.RunExecutionState
  alias FavnOrchestrator.RunServer.Execution.Sequential
  alias FavnOrchestrator.RunServer.PersistenceRetry
  alias FavnOrchestrator.RunState

  defmodule FakeStore do
    def commit_transition(command) do
      send(self(), {:commit_transition, command})
      {:error, :forced_failure}
    end
  end

  setup do
    stores = %Stores{
      registry: FakeStore,
      runs: FakeStore,
      run_ownership: FakeStore,
      scheduler: FakeStore,
      admission: FakeStore,
      resource_circuits: FakeStore,
      target_generations: FakeStore,
      materialization: FakeStore,
      backfills: FakeStore,
      operator_reads: FakeStore,
      logs: FakeStore,
      identity: FakeStore,
      maintenance: FakeStore
    }

    runtime = %PersistenceRuntime{backend: __MODULE__, options: [], stores: stores}
    assert {:ok, pid} = PersistenceRuntime.start_link(runtime)

    on_exit(fn -> Process.exit(pid, :shutdown) end)

    :ok
  end

  test "pre-submit failures preserve the planned effective window" do
    ref = {__MODULE__.Asset, :orders}
    node_key = {ref, "window:2026-07-20"}

    window = %{
      key: "window:2026-07-20",
      kind: :day,
      start_at: ~U[2026-07-20 00:00:00Z],
      end_at: ~U[2026-07-21 00:00:00Z],
      timezone: "Etc/UTC"
    }

    plan = %Plan{
      target_refs: [ref],
      target_node_keys: [node_key],
      nodes: %{
        node_key => %{
          ref: ref,
          node_key: node_key,
          window: window,
          upstream: [],
          downstream: [],
          stage: 0,
          execution_pool: nil,
          action: :run,
          retry_policy: Favn.Retry.Policy.default(),
          retry_policy_source: :default
        }
      },
      topo_order: [ref],
      stages: [[ref]],
      node_stages: [[node_key]]
    }

    run =
      RunState.new(
        id: "run-pre-submit-window",
        workspace_id: "workspace-pre-submit-window",
        deployment_id: "deployment-pre-submit-window",
        manifest_version_id: "manifest-pre-submit-window",
        manifest_content_hash: "sha256:pre-submit-window",
        required_runner_release_id: FavnTestSupport.runner_release_id(),
        asset_ref: ref,
        target_refs: [ref],
        plan: plan
      )

    version = %Version{
      manifest_version_id: run.manifest_version_id,
      content_hash: run.manifest_content_hash
    }

    state = %RunExecutionState{
      run: run,
      version: version,
      runner_client: nil,
      sequential_refs: [{ref, node_key, 0}]
    }

    assert {:persist_retry, ^state, %PersistenceRetry{event_type: :step_failed, data: data},
            _reason} =
             Sequential.continue(state)

    assert data.window == window
    assert data.node_key == node_key
    assert_receive {:commit_transition, command}
    assert command.event.event_type == :step_failed
    assert command.event.data.window == window
  end
end

defmodule FavnOrchestrator.RunServer.Execution.SequentialTest.Asset do
end
