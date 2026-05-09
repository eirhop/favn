defmodule FavnOrchestrator.RunServerTest do
  use ExUnit.Case, async: false

  alias Favn.Contracts.RunnerResult
  alias Favn.Freshness.{Key, Policy}
  alias Favn.Manifest
  alias Favn.Manifest.Version
  alias Favn.Plan
  alias Favn.Window.Runtime
  alias FavnOrchestrator.RunServer
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.AssetFreshnessState
  alias FavnOrchestrator.Storage
  alias FavnOrchestrator.Storage.Adapter.Memory

  defmodule RunnerClientCancelBeforeStepStartedStub do
    @behaviour Favn.Contracts.RunnerClient

    @impl true
    def register_manifest(_version, _opts), do: :ok

    @impl true
    def submit_work(work, _opts) do
      {:ok, running} = Storage.get_run(work.run_id)

      cancelled =
        running
        |> RunState.transition(
          status: :cancelled,
          error: {:cancelled, %{reason: :submit_race}},
          runner_execution_id: nil,
          metadata: Map.put(running.metadata, :cancelled, true)
        )

      :ok = Storage.put_run(cancelled)
      {:ok, "exec_#{work.run_id}"}
    end

    @impl true
    def await_result(_execution_id, _timeout, _opts) do
      raise "await_result/3 should not be called after external cancel wins step_started"
    end

    @impl true
    def cancel_work(_execution_id, _reason, _opts), do: :ok

    @impl true
    def inspect_relation(_request, _opts), do: {:error, :not_supported}
  end

  defmodule RunnerClientRecordingStub do
    @behaviour Favn.Contracts.RunnerClient

    @impl true
    def register_manifest(_version, _opts), do: :ok

    @impl true
    def submit_work(work, opts) do
      submit_log = Keyword.fetch!(opts, :submit_log)
      Agent.update(submit_log, &[work | &1])
      {:ok, execution_id(work)}
    end

    @impl true
    def await_result(execution_id, _timeout, opts) do
      result_by_ref = Keyword.get(opts, :result_by_ref, %{})
      result_by_node_key = Keyword.get(opts, :result_by_node_key, %{})
      ref = execution_ref(execution_id)
      node_key = execution_node_key(execution_id)
      status = Map.get(result_by_node_key, node_key, Map.get(result_by_ref, ref, :ok))

      {:ok,
       %RunnerResult{
         status: status,
         error: if(status == :ok, do: nil, else: :runner_failed),
         asset_results: [asset_result(ref, status)],
         metadata: %{}
       }}
    end

    @impl true
    def cancel_work(_execution_id, _reason, _opts), do: :ok

    @impl true
    def inspect_relation(_request, _opts), do: {:error, :not_supported}

    defp execution_id(work) do
      {module, name} = work.asset_ref

      encoded_node_key =
        work.metadata.node_key |> :erlang.term_to_binary() |> Base.encode16(case: :lower)

      "exec:#{work.run_id}:#{Atom.to_string(module)}:#{Atom.to_string(name)}:#{encoded_node_key}"
    end

    defp execution_ref(execution_id) do
      [_prefix, _run_id, module, name | _rest] = String.split(execution_id, ":")
      {String.to_existing_atom(module), String.to_existing_atom(name)}
    end

    defp execution_node_key(execution_id) do
      execution_id
      |> String.split(":")
      |> List.last()
      |> Base.decode16!(case: :lower)
      |> :erlang.binary_to_term()
    end

    defp asset_result(ref, status) do
      %Favn.Run.AssetResult{
        ref: ref,
        stage: 0,
        status: status,
        started_at: DateTime.utc_now(),
        finished_at: DateTime.utc_now(),
        duration_ms: 0,
        meta: %{},
        error: if(status == :ok, do: nil, else: :runner_failed),
        attempt_count: 1,
        max_attempts: 1,
        attempts: []
      }
    end
  end

  setup do
    previous_client = Application.get_env(:favn_orchestrator, :runner_client)
    previous_opts = Application.get_env(:favn_orchestrator, :runner_client_opts)

    Application.put_env(:favn_orchestrator, :runner_client, nil)
    Application.put_env(:favn_orchestrator, :runner_client_opts, [])

    Memory.reset()

    on_exit(fn ->
      Application.put_env(:favn_orchestrator, :runner_client, previous_client)
      Application.put_env(:favn_orchestrator, :runner_client_opts, previous_opts)
      Memory.reset()
    end)

    :ok
  end

  test "marks run as failed when runner client is unavailable" do
    version = manifest_version("mv_run_server")

    run_state =
      RunState.new(
        id: "run_server_1",
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        asset_ref: {MyApp.Assets.Gold, :asset},
        target_refs: [{MyApp.Assets.Gold, :asset}]
      )

    assert :ok = Storage.put_run(run_state)

    assert {:ok, pid} =
             RunServer.start_link(%{
               run_state: run_state,
               version: version
             })

    ref = Process.monitor(pid)
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 1_000

    assert {:ok, run} = Storage.get_run("run_server_1")
    assert run.status == :error
    assert run.error == :runner_client_not_available

    assert {:ok, events} = Storage.list_run_events("run_server_1")
    assert Enum.map(events, & &1.event_type) == [:run_started, :step_failed, :run_failed]
  end

  test "does not crash when run_started persist loses to external cancel" do
    version = manifest_version("mv_run_server_cancelled_before_start")

    run_state =
      RunState.new(
        id: "run_server_cancelled_before_start",
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        asset_ref: {MyApp.Assets.Gold, :asset},
        target_refs: [{MyApp.Assets.Gold, :asset}]
      )

    cancelled =
      run_state
      |> RunState.transition(
        status: :cancelled,
        error: {:cancelled, %{reason: :pre_start_cancel}},
        metadata: %{cancelled: true}
      )

    assert :ok = Storage.put_run(cancelled)

    assert {:ok, pid} = RunServer.start_link(%{run_state: run_state, version: version})

    ref = Process.monitor(pid)
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 1_000

    assert {:ok, stored} = Storage.get_run(run_state.id)
    assert stored.status == :cancelled
    assert stored.error == {:cancelled, %{reason: :pre_start_cancel}}

    assert {:ok, events} = Storage.list_run_events(run_state.id)
    assert events == []
  end

  test "does not crash when step_started persist loses to external cancel" do
    previous_client = Application.get_env(:favn_orchestrator, :runner_client)
    previous_opts = Application.get_env(:favn_orchestrator, :runner_client_opts)

    on_exit(fn ->
      Application.put_env(:favn_orchestrator, :runner_client, previous_client)
      Application.put_env(:favn_orchestrator, :runner_client_opts, previous_opts)
    end)

    Application.put_env(
      :favn_orchestrator,
      :runner_client,
      RunnerClientCancelBeforeStepStartedStub
    )

    Application.put_env(:favn_orchestrator, :runner_client_opts, [])

    version = manifest_version("mv_run_server_cancelled_before_step_started")

    run_state =
      RunState.new(
        id: "run_server_cancelled_before_step_started",
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        asset_ref: {MyApp.Assets.Gold, :asset},
        target_refs: [{MyApp.Assets.Gold, :asset}]
      )

    assert :ok = Storage.put_run(run_state)

    assert {:ok, pid} = RunServer.start_link(%{run_state: run_state, version: version})

    ref = Process.monitor(pid)
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 1_000

    assert {:ok, stored} = Storage.get_run(run_state.id)
    assert stored.status == :cancelled
    assert stored.error == {:cancelled, %{reason: :submit_race}}

    assert {:ok, events} = Storage.list_run_events(run_state.id)
    assert Enum.map(events, & &1.event_type) == [:run_started]
  end

  test "pipeline skips fresh windowed node without calling runner" do
    {:ok, submit_log} = Agent.start_link(fn -> [] end)
    Application.put_env(:favn_orchestrator, :runner_client, RunnerClientRecordingStub)
    Application.put_env(:favn_orchestrator, :runner_client_opts, submit_log: submit_log)

    window = runtime_window()

    version =
      manifest_version("mv_pipeline_fresh_skip",
        freshness: Policy.from_value!(%{mode: :window_success})
      )

    plan = single_node_plan({MyApp.Assets.Gold, :asset}, window: window)
    freshness_key = Key.window!(window.key)

    assert :ok =
             freshness_state(
               {MyApp.Assets.Gold, :asset},
               {{MyApp.Assets.Gold, :asset}, nil},
               freshness_key
             )
             |> Storage.put_asset_freshness_state()

    run_state =
      pipeline_run_state("run_pipeline_fresh_skip", version, plan, [{MyApp.Assets.Gold, :asset}])

    assert :ok = Storage.put_run(run_state)

    assert {:ok, pid} = RunServer.start_link(%{run_state: run_state, version: version})
    ref = Process.monitor(pid)
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 1_000

    assert Agent.get(submit_log, & &1) == []
    assert {:ok, stored} = Storage.get_run(run_state.id)
    assert stored.status == :ok

    assert [%Favn.Run.NodeResult{status: :skipped_fresh, freshness_key: ^freshness_key}] =
             stored.result.node_results

    assert {:ok, events} = Storage.list_run_events(run_state.id)
    assert Enum.map(events, & &1.event_type) == [:run_started, :step_skipped_fresh, :run_finished]
  end

  test "pipeline blocks downstream after upstream failure" do
    {:ok, submit_log} = Agent.start_link(fn -> [] end)
    Application.put_env(:favn_orchestrator, :runner_client, RunnerClientRecordingStub)

    Application.put_env(:favn_orchestrator, :runner_client_opts,
      submit_log: submit_log,
      result_by_ref: %{{MyApp.Assets.Raw, :asset} => :error}
    )

    version = manifest_version("mv_pipeline_blocked_downstream")
    plan = raw_to_gold_plan()

    run_state =
      pipeline_run_state("run_pipeline_blocked_downstream", version, plan, [
        {MyApp.Assets.Gold, :asset}
      ])

    assert :ok = Storage.put_run(run_state)

    assert {:ok, pid} = RunServer.start_link(%{run_state: run_state, version: version})
    ref = Process.monitor(pid)
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 1_000

    submitted_refs = submit_log |> Agent.get(& &1) |> Enum.map(& &1.asset_ref)
    assert submitted_refs == [{MyApp.Assets.Raw, :asset}]

    assert {:ok, stored} = Storage.get_run(run_state.id)
    assert stored.status == :error

    statuses = Map.new(stored.result.node_results, &{&1.node_key, &1.status})
    assert statuses[{{MyApp.Assets.Raw, :asset}, nil}] == :error
    assert statuses[{{MyApp.Assets.Gold, :asset}, nil}] == :blocked

    assert {:ok, events} = Storage.list_run_events(run_state.id)
    assert :step_blocked in Enum.map(events, & &1.event_type)
  end

  test "actual upstream success refreshes downstream in same pipeline" do
    {:ok, submit_log} = Agent.start_link(fn -> [] end)
    Application.put_env(:favn_orchestrator, :runner_client, RunnerClientRecordingStub)
    Application.put_env(:favn_orchestrator, :runner_client_opts, submit_log: submit_log)

    version =
      manifest_version("mv_pipeline_success_dirties_downstream",
        freshness: Policy.from_value!(%{mode: :window_success})
      )

    plan = raw_to_gold_plan()
    raw_key = {{MyApp.Assets.Raw, :asset}, nil}
    gold_key = {{MyApp.Assets.Gold, :asset}, nil}

    assert :ok =
             freshness_state({MyApp.Assets.Gold, :asset}, gold_key, Key.latest(),
               input_versions: [%{upstream_node_key: raw_key, freshness_version: "old"}]
             )
             |> Storage.put_asset_freshness_state()

    run_state =
      pipeline_run_state("run_pipeline_success_dirties_downstream", version, plan, [
        {MyApp.Assets.Gold, :asset}
      ])

    assert :ok = Storage.put_run(run_state)

    assert {:ok, pid} = RunServer.start_link(%{run_state: run_state, version: version})
    ref = Process.monitor(pid)
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 1_000

    submitted_refs = submit_log |> Agent.get(& &1) |> Enum.map(& &1.asset_ref) |> Enum.reverse()
    assert submitted_refs == [{MyApp.Assets.Raw, :asset}, {MyApp.Assets.Gold, :asset}]

    assert {:ok, raw_state} =
             Storage.get_asset_freshness_state(MyApp.Assets.Raw, :asset, Key.latest())

    assert raw_state.latest_success_node_key == raw_key

    assert {:ok, gold_state} =
             Storage.get_asset_freshness_state(MyApp.Assets.Gold, :asset, Key.latest())

    assert gold_state.latest_success_node_key == gold_key

    assert [%{upstream_node_key: ^raw_key, freshness_version: raw_version}] =
             gold_state.input_versions

    assert raw_version == raw_state.freshness_version
  end

  test "same-ref stage records freshness only for the node that actually succeeded" do
    {:ok, submit_log} = Agent.start_link(fn -> [] end)
    Application.put_env(:favn_orchestrator, :runner_client, RunnerClientRecordingStub)

    asset_ref = {MyApp.Assets.Raw, :asset}
    window_one = runtime_window_at(~U[2026-05-08 00:00:00Z])
    window_two = runtime_window_at(~U[2026-05-09 00:00:00Z])
    node_one = {asset_ref, window_one.key}
    node_two = {asset_ref, window_two.key}

    Application.put_env(:favn_orchestrator, :runner_client_opts,
      submit_log: submit_log,
      result_by_node_key: %{node_two => :error}
    )

    version =
      manifest_version("mv_pipeline_same_ref_windows",
        freshness: Policy.from_value!(%{mode: :window_success})
      )

    plan = %Plan{
      target_refs: [asset_ref],
      target_node_keys: [node_one, node_two],
      nodes: %{
        node_one => plan_node(asset_ref, node_one, window: window_one, stage: 0),
        node_two => plan_node(asset_ref, node_two, window: window_two, stage: 0)
      },
      topo_order: [asset_ref],
      stages: [[asset_ref]],
      node_stages: [[node_one, node_two]]
    }

    run_state = pipeline_run_state("run_pipeline_same_ref_windows", version, plan, [asset_ref])
    assert :ok = Storage.put_run(run_state)

    assert {:ok, pid} = RunServer.start_link(%{run_state: run_state, version: version})
    ref = Process.monitor(pid)
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 1_000

    assert {:ok, stored} = Storage.get_run(run_state.id)
    assert stored.status == :error

    assert {:ok, state_one} =
             Storage.get_asset_freshness_state(
               MyApp.Assets.Raw,
               :asset,
               Key.window!(window_one.key)
             )

    assert state_one.latest_success_node_key == node_one

    assert {:error, :not_found} =
             Storage.get_asset_freshness_state(
               MyApp.Assets.Raw,
               :asset,
               Key.window!(window_two.key)
             )

    statuses = Map.new(stored.result.node_results, &{&1.node_key, &1.status})
    assert statuses[node_one] == :ok
    assert statuses[node_two] == :error
  end

  defp manifest_version(manifest_version_id, opts \\ []) do
    manifest =
      %Manifest{
        assets: [
          %Favn.Manifest.Asset{
            ref: {MyApp.Assets.Gold, :asset},
            module: MyApp.Assets.Gold,
            name: :asset,
            freshness: Keyword.get(opts, :freshness)
          },
          %Favn.Manifest.Asset{
            ref: {MyApp.Assets.Raw, :asset},
            module: MyApp.Assets.Raw,
            name: :asset,
            freshness: Keyword.get(opts, :freshness)
          }
        ]
      }

    {:ok, version} = Version.new(manifest, manifest_version_id: manifest_version_id)
    version
  end

  defp single_node_plan(ref, opts) do
    node_key = {ref, nil}

    %Plan{
      target_refs: [ref],
      target_node_keys: [node_key],
      nodes: %{node_key => plan_node(ref, node_key, Keyword.put(opts, :stage, 0))},
      topo_order: [ref],
      stages: [[ref]],
      node_stages: [[node_key]]
    }
  end

  defp raw_to_gold_plan do
    raw_ref = {MyApp.Assets.Raw, :asset}
    gold_ref = {MyApp.Assets.Gold, :asset}
    raw_key = {raw_ref, nil}
    gold_key = {gold_ref, nil}

    %Plan{
      target_refs: [gold_ref],
      target_node_keys: [gold_key],
      nodes: %{
        raw_key => plan_node(raw_ref, raw_key, downstream: [gold_key], stage: 0),
        gold_key => plan_node(gold_ref, gold_key, upstream: [raw_key], stage: 1)
      },
      topo_order: [raw_ref, gold_ref],
      stages: [[raw_ref], [gold_ref]],
      node_stages: [[raw_key], [gold_key]]
    }
  end

  defp plan_node(ref, node_key, opts) do
    %{
      ref: ref,
      node_key: node_key,
      window: Keyword.get(opts, :window),
      upstream: Keyword.get(opts, :upstream, []),
      downstream: Keyword.get(opts, :downstream, []),
      stage: Keyword.get(opts, :stage, 0),
      action: :run
    }
  end

  defp pipeline_run_state(id, version, plan, target_refs) do
    RunState.new(
      id: id,
      manifest_version_id: version.manifest_version_id,
      manifest_content_hash: version.content_hash,
      asset_ref: List.first(target_refs),
      target_refs: target_refs,
      plan: plan,
      submit_kind: :pipeline
    )
  end

  defp runtime_window do
    runtime_window_at(~U[2026-05-08 00:00:00Z])
  end

  defp runtime_window_at(start_at) do
    end_at = DateTime.add(start_at, 1, :day)
    anchor_key = Favn.Window.Key.new!(:day, start_at, "Etc/UTC")

    Runtime.new!(:day, start_at, end_at, anchor_key)
  end

  defp freshness_state(ref, node_key, freshness_key, opts \\ []) do
    {module, name} = ref
    now = DateTime.utc_now()

    {:ok, state} =
      AssetFreshnessState.new(%{
        asset_ref_module: module,
        asset_ref_name: name,
        freshness_key: freshness_key,
        status: :ok,
        freshness_version: Keyword.get(opts, :freshness_version, "existing"),
        latest_success_run_id: "previous_run",
        latest_success_node_key: node_key,
        latest_success_at: now,
        latest_attempt_run_id: "previous_run",
        latest_attempt_status: :ok,
        latest_attempt_at: now,
        input_versions: Keyword.get(opts, :input_versions, []),
        updated_at: now
      })

    state
  end
end
