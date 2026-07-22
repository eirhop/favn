defmodule FavnOrchestrator.Freshness.StateLoaderTest do
  use ExUnit.Case, async: false

  alias Favn.Freshness.Policy
  alias Favn.Plan
  alias FavnOrchestrator.AssetStepIdentity
  alias FavnOrchestrator.Freshness.StateLoader
  alias FavnOrchestrator.Persistence.Runtime, as: PersistenceRuntime
  alias FavnOrchestrator.Persistence.Results.FreshnessState
  alias FavnOrchestrator.Persistence.Stores
  alias FavnOrchestrator.Persistence.WorkspaceContext

  @now ~U[2026-07-20 10:00:00Z]
  @upstream_ref {MyApp.Raw, :orders}
  @target_ref {MyApp.Gold, :orders}
  @upstream_key {@upstream_ref, "window:upstream"}
  @target_key {@target_ref, "window:target"}

  defmodule FakeStore do
    def get_freshness_many(%{identities: identities}) do
      send(self(), {:freshness_batch, length(identities)})
      {:ok, []}
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

    on_exit(fn ->
      if Process.alive?(pid), do: GenServer.stop(pid)
    end)

    :ok
  end

  test "restores planned node identities and consumed upstream versions" do
    plan = plan()
    target_id = "asset:gold"
    freshness_key = "window:target"

    persisted = %FreshnessState{
      workspace_id: "workspace",
      evidence_generation_id: "ag_test",
      deployment_id: "deployment",
      manifest_version_id: "manifest",
      target_id: target_id,
      freshness_key: freshness_key,
      status: :fresh,
      payload: %{
        "freshness_version" => "gold-v2",
        "run_id" => "run-2",
        "node_key_fingerprint" => AssetStepIdentity.node_fingerprint(@target_key),
        "input_fingerprint" => "inputs-v2",
        "input_versions" => [
          %{
            "node_key_fingerprint" => AssetStepIdentity.node_fingerprint(@upstream_key),
            "freshness_version" => "raw-v4",
            "success_run_id" => "run-1"
          }
        ]
      },
      updated_at: @now
    }

    requested = %{{target_id, freshness_key} => {MyApp.Gold, :orders, freshness_key}}

    assert {:ok, [state]} = StateLoader.decode_many([persisted], requested, plan)
    assert state.latest_success_node_key == @target_key
    assert state.freshness_version == "gold-v2"

    assert [
             %{
               upstream_node_key: @upstream_key,
               upstream_ref: @upstream_ref,
               freshness_version: "raw-v4",
               success_run_id: "run-1"
             }
           ] = state.input_versions

    assert %{@target_key => ^state} = StateLoader.index([state])
  end

  test "rejects rows outside the exact requested identity set" do
    persisted = %FreshnessState{
      workspace_id: "workspace",
      evidence_generation_id: "ag_test",
      deployment_id: "deployment",
      manifest_version_id: "manifest",
      target_id: "unexpected",
      freshness_key: "unexpected",
      status: :fresh,
      payload: %{},
      updated_at: @now
    }

    assert {:error, :unexpected_freshness_identity} =
             StateLoader.decode_many([persisted], %{}, plan())
  end

  test "chunks freshness reads for plans larger than the persistence batch limit" do
    ref = {MyApp.Windowed, :orders}

    nodes =
      Map.new(1..501, fn index ->
        window_key = Favn.Window.Key.new!(:day, DateTime.add(@now, index, :day), "Etc/UTC")
        node_key = {ref, window_key}

        {node_key,
         node(ref, node_key, [], [], 0)
         |> Map.put(:window, %{key: window_key})
         |> Map.put(:evidence_generation_id, "ag_windowed")}
      end)

    plan = %Plan{nodes: nodes}
    assets = %{ref => %{freshness: Policy.from_value!(window_success: true)}}
    assert {:ok, context} = WorkspaceContext.new("workspace", "test", [:workspace_admin])

    assert {:ok, %{states: [], indexed: %{}}} =
             StateLoader.load(context, "deployment", plan, assets, now: @now)

    assert_receive {:freshness_batch, 500}
    assert_receive {:freshness_batch, 1}
    refute_receive {:freshness_batch, _size}
  end

  defp plan do
    nodes = %{
      @upstream_key => node(@upstream_ref, @upstream_key, [], [@target_key], 0),
      @target_key => node(@target_ref, @target_key, [@upstream_key], [], 1)
    }

    %Plan{
      target_refs: [@target_ref],
      target_node_keys: [@target_key],
      nodes: nodes,
      topo_order: [@upstream_ref, @target_ref],
      stages: [[@upstream_ref], [@target_ref]],
      node_stages: [[@upstream_key], [@target_key]]
    }
  end

  defp node(ref, node_key, upstream, downstream, stage) do
    %{
      ref: ref,
      node_key: node_key,
      window: nil,
      upstream: upstream,
      downstream: downstream,
      stage: stage,
      action: :run
    }
  end
end
