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
  alias FavnOrchestrator.MaterializationClaim.Identity, as: MaterializationClaimIdentity
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

    import ExUnit.Assertions

    @impl true
    def register_manifest(_version, _opts), do: :ok

    @impl true
    def submit_work(work, opts) do
      assert_required_freshness(work, opts)
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

    defp assert_required_freshness(work, opts) do
      opts
      |> Keyword.get(:assert_freshness_before_submit, %{})
      |> Map.get(work.asset_ref)
      |> case do
        nil ->
          :ok

        {module, name} ->
          assert {:ok, %FavnOrchestrator.AssetFreshnessState{latest_success_run_id: run_id}} =
                   Storage.get_asset_freshness_state(module, name, Key.latest())

          assert run_id == work.run_id
      end
    end

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

  defmodule RunnerClientBlockingStub do
    @behaviour Favn.Contracts.RunnerClient

    @impl true
    def register_manifest(_version, _opts), do: :ok

    @impl true
    def submit_work(work, opts) do
      parent = Keyword.fetch!(opts, :parent)
      execution_id = execution_id(work)
      send(parent, {:submitted, work.asset_ref, execution_id})
      {:ok, execution_id}
    end

    @impl true
    def await_result(execution_id, _timeout, opts) do
      parent = Keyword.fetch!(opts, :parent)
      asset_ref = execution_ref(execution_id)
      send(parent, {:awaiting, execution_id, asset_ref, self()})

      receive do
        {:release_runner_result, ^execution_id, status} ->
          {:ok,
           %RunnerResult{
             status: status,
             error: if(status == :ok, do: nil, else: :runner_failed),
             asset_results: [asset_result(execution_id, status)],
             metadata: %{}
           }}
      end
    end

    @impl true
    def cancel_work(_execution_id, _reason, _opts), do: :ok

    @impl true
    def inspect_relation(_request, _opts), do: {:error, :not_supported}

    defp execution_id(work) do
      {module, name} = work.asset_ref

      encoded_node_key =
        work.metadata.node_key |> :erlang.term_to_binary() |> Base.encode16(case: :lower)

      Enum.join(
        [
          "blocking_exec",
          work.run_id,
          Atom.to_string(module),
          Atom.to_string(name),
          encoded_node_key
        ],
        ":"
      )
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

    defp asset_result(execution_id, status) do
      ref = execution_ref(execution_id)
      node_key = execution_node_key(execution_id)

      %Favn.Run.AssetResult{
        ref: ref,
        stage: 0,
        status: status,
        started_at: DateTime.utc_now(),
        finished_at: DateTime.utc_now(),
        duration_ms: 0,
        meta: %{node_key: node_key},
        error: if(status == :ok, do: nil, else: :runner_failed),
        attempt_count: 1,
        max_attempts: 1,
        attempts: []
      }
    end
  end

  defmodule RunnerClientSubmitFailureStub do
    @behaviour Favn.Contracts.RunnerClient

    @impl true
    def register_manifest(_version, _opts), do: :ok

    @impl true
    def submit_work(work, opts) do
      parent = Keyword.fetch!(opts, :parent)
      execution_id = execution_id(work)
      send(parent, {:submitted, work.asset_ref, execution_id})

      if work.asset_ref == Keyword.fetch!(opts, :fail_ref) do
        {:error, :submit_failed}
      else
        {:ok, execution_id}
      end
    end

    @impl true
    def await_result(_execution_id, _timeout, _opts) do
      raise "await_result/3 should not be called after submit failure cancels admitted work"
    end

    @impl true
    def cancel_work(execution_id, reason, opts) do
      send(Keyword.fetch!(opts, :parent), {:cancelled, execution_id, reason})
      :ok
    end

    @impl true
    def inspect_relation(_request, _opts), do: {:error, :not_supported}

    defp execution_id(work) do
      encoded_ref = work.asset_ref |> :erlang.term_to_binary() |> Base.encode16(case: :lower)
      "submit_failure_exec:#{work.run_id}:#{encoded_ref}"
    end
  end

  setup do
    previous_client = Application.get_env(:favn_orchestrator, :runner_client)
    previous_opts = Application.get_env(:favn_orchestrator, :runner_client_opts)

    Application.put_env(:favn_orchestrator, :runner_client, nil)
    Application.put_env(:favn_orchestrator, :runner_client_opts, [])

    start_memory_if_needed()
    Memory.reset()

    on_exit(fn ->
      Application.put_env(:favn_orchestrator, :runner_client, previous_client)
      Application.put_env(:favn_orchestrator, :runner_client_opts, previous_opts)

      if Process.whereis(Memory) do
        Memory.reset()
      end
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

    assert {:ok, state} =
             Storage.get_asset_freshness_state(MyApp.Assets.Gold, :asset, freshness_key)

    assert state.status == :skipped_fresh
    assert state.freshness_version == "existing"
    assert state.latest_success_run_id == "previous_run"
    assert state.latest_attempt_run_id == run_state.id
    assert state.latest_attempt_status == :skipped_fresh

    assert {:ok, events} = Storage.list_run_events(run_state.id)
    assert Enum.map(events, & &1.event_type) == [:run_started, :step_skipped_fresh, :run_finished]
  end

  test "failed attempt updates latest attempt without replacing prior freshness success" do
    {:ok, submit_log} = Agent.start_link(fn -> [] end)
    Application.put_env(:favn_orchestrator, :runner_client, RunnerClientRecordingStub)

    Application.put_env(:favn_orchestrator, :runner_client_opts,
      submit_log: submit_log,
      result_by_ref: %{{MyApp.Assets.Gold, :asset} => :error}
    )

    version = manifest_version("mv_pipeline_failed_attempt_preserves_success")
    plan = single_node_plan({MyApp.Assets.Gold, :asset}, window: nil)

    assert :ok =
             freshness_state(
               {MyApp.Assets.Gold, :asset},
               {{MyApp.Assets.Gold, :asset}, nil},
               Key.latest(),
               freshness_version: "gold:v1"
             )
             |> Storage.put_asset_freshness_state()

    run_state =
      pipeline_run_state("run_pipeline_failed_attempt_preserves_success", version, plan, [
        {MyApp.Assets.Gold, :asset}
      ])

    assert :ok = Storage.put_run(run_state)

    assert {:ok, pid} = RunServer.start_link(%{run_state: run_state, version: version})
    ref = Process.monitor(pid)
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 1_000

    assert {:ok, state} =
             Storage.get_asset_freshness_state(MyApp.Assets.Gold, :asset, Key.latest())

    assert state.status == :error
    assert state.freshness_version == "gold:v1"
    assert state.latest_success_run_id == "previous_run"
    assert state.latest_attempt_run_id == run_state.id
    assert state.latest_attempt_status == :error
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

  test "unrelated downstream branch still runs after another branch fails" do
    {:ok, submit_log} = Agent.start_link(fn -> [] end)
    Application.put_env(:favn_orchestrator, :runner_client, RunnerClientRecordingStub)

    raw_a = {MyApp.Assets.RawA, :asset}
    silver_a = {MyApp.Assets.SilverA, :asset}
    raw_b = {MyApp.Assets.RawB, :asset}
    silver_b = {MyApp.Assets.SilverB, :asset}

    Application.put_env(:favn_orchestrator, :runner_client_opts,
      submit_log: submit_log,
      result_by_ref: %{raw_a => :error}
    )

    refs = [raw_a, silver_a, raw_b, silver_b]
    version = manifest_version_for_refs("mv_pipeline_independent_branch_failure", refs)
    plan = independent_branch_plan(raw_a, silver_a, raw_b, silver_b)
    run_state = pipeline_run_state("run_pipeline_independent_branch_failure", version, plan, refs)

    assert :ok = Storage.put_run(run_state)

    assert {:ok, pid} = RunServer.start_link(%{run_state: run_state, version: version})
    ref = Process.monitor(pid)
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 1_000

    submitted_refs = submit_log |> Agent.get(& &1) |> Enum.map(& &1.asset_ref) |> MapSet.new()
    assert MapSet.member?(submitted_refs, raw_a)
    assert MapSet.member?(submitted_refs, raw_b)
    assert MapSet.member?(submitted_refs, silver_b)
    refute MapSet.member?(submitted_refs, silver_a)

    assert {:ok, stored} = Storage.get_run(run_state.id)
    assert stored.status == :error

    statuses = Map.new(stored.result.node_results, &{&1.node_key, &1.status})
    assert statuses[{raw_a, nil}] == :error
    assert statuses[{raw_b, nil}] == :ok
    assert statuses[{silver_a, nil}] == :blocked
    assert statuses[{silver_b, nil}] == :ok
  end

  test "pipeline max concurrency refills same-stage work as executions complete" do
    Application.put_env(:favn_orchestrator, :runner_client, RunnerClientBlockingStub)
    Application.put_env(:favn_orchestrator, :runner_client_opts, parent: self())

    refs =
      Enum.map(1..6, fn index ->
        {Module.concat([MyApp.Assets.PipelineRefill, "Asset#{index}"]), :asset}
      end)

    plan = same_stage_plan(refs)
    version = manifest_version_for_refs("mv_pipeline_sliding_window_refill", refs)

    run_state =
      "run_pipeline_sliding_window_refill"
      |> pipeline_run_state(version, plan, refs)
      |> RunState.transition(metadata: %{pipeline_execution_policy: %{max_concurrency: 5}})

    assert :ok = Storage.put_run(run_state)

    assert {:ok, pid} = RunServer.start_link(%{run_state: run_state, version: version})
    ref = Process.monitor(pid)

    initial_submissions = receive_submissions(5)
    refute_received {:submitted, _, _}

    awaiters = receive_awaiters(initial_submissions)

    {first_execution_id, first_awaiter} =
      awaiter_for_submission(hd(initial_submissions), awaiters)

    send(first_awaiter, {:release_runner_result, first_execution_id, :ok})

    assert_receive {:submitted, sixth_ref, sixth_execution_id}, 1_000
    assert sixth_ref == List.last(refs)

    awaiters = Map.merge(awaiters, receive_awaiters([{sixth_ref, sixth_execution_id}]))

    initial_submissions
    |> tl()
    |> Enum.each(fn submission -> release_submission(submission, awaiters) end)

    release_submission({sixth_ref, sixth_execution_id}, awaiters)

    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 1_000
    assert {:ok, stored} = Storage.get_run(run_state.id)
    assert stored.status == :ok
  end

  test "pipeline max concurrency does not refill same-stage work after terminal failure" do
    Application.put_env(:favn_orchestrator, :runner_client, RunnerClientBlockingStub)
    Application.put_env(:favn_orchestrator, :runner_client_opts, parent: self())

    refs =
      Enum.map(1..3, fn index ->
        {Module.concat([MyApp.Assets.PipelineFailureDrain, "Asset#{index}"]), :asset}
      end)

    plan = same_stage_plan(refs)
    version = manifest_version_for_refs("mv_pipeline_failure_does_not_refill", refs)

    run_state =
      "run_pipeline_failure_does_not_refill"
      |> pipeline_run_state(version, plan, refs)
      |> RunState.transition(metadata: %{pipeline_execution_policy: %{max_concurrency: 2}})

    assert :ok = Storage.put_run(run_state)

    assert {:ok, pid} = RunServer.start_link(%{run_state: run_state, version: version})
    ref = Process.monitor(pid)

    initial_submissions = receive_submissions(2)
    refute_received {:submitted, _, _}

    awaiters = receive_awaiters(initial_submissions)
    release_submission(hd(initial_submissions), awaiters, :error)

    last_ref = List.last(refs)
    assert {:ok, _event} = wait_for_run_event(run_state.id, :stage_draining_after_failure)
    refute_received {:submitted, ^last_ref, _execution_id}

    initial_submissions
    |> tl()
    |> Enum.each(fn submission -> release_submission(submission, awaiters) end)

    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 1_000
    assert {:ok, stored} = Storage.get_run(run_state.id)
    assert stored.status == :error

    assert {:ok, events} = Storage.list_run_events(run_state.id)

    started_refs =
      events |> Enum.filter(&(&1.event_type == :step_started)) |> Enum.map(& &1.asset_ref)

    refute last_ref in started_refs
  end

  test "pipeline submit failure cancels admitted same-stage work with wrapped reason" do
    [first_ref, fail_ref] =
      Enum.map(1..2, fn index ->
        {Module.concat([MyApp.Assets.PipelineSubmitFailure, "Asset#{index}"]), :asset}
      end)

    Application.put_env(:favn_orchestrator, :runner_client, RunnerClientSubmitFailureStub)

    Application.put_env(:favn_orchestrator, :runner_client_opts,
      parent: self(),
      fail_ref: fail_ref
    )

    refs = [first_ref, fail_ref]
    plan = same_stage_plan(refs)
    version = manifest_version_for_refs("mv_pipeline_submit_failure_cancel_reason", refs)

    run_state =
      pipeline_run_state("run_pipeline_submit_failure_cancel_reason", version, plan, refs)

    assert :ok = Storage.put_run(run_state)

    assert {:ok, pid} = RunServer.start_link(%{run_state: run_state, version: version})
    ref = Process.monitor(pid)

    assert_receive {:submitted, ^first_ref, first_execution_id}, 1_000
    assert_receive {:submitted, ^fail_ref, _failed_execution_id}, 1_000

    assert_receive {:cancelled, ^first_execution_id, cancellation_reason}, 1_000
    assert cancellation_reason.run_id == run_state.id
    assert %DateTime{} = cancellation_reason.requested_at

    assert %{
             kind: :submit_failure,
             asset_ref: ^fail_ref,
             error: :submit_failed
           } = cancellation_reason.reason

    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 1_000

    assert {:ok, stored} = Storage.get_run(run_state.id)
    assert stored.status == :error
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

  test "successful upstream step writes freshness before downstream submit" do
    {:ok, submit_log} = Agent.start_link(fn -> [] end)
    Application.put_env(:favn_orchestrator, :runner_client, RunnerClientRecordingStub)

    Application.put_env(:favn_orchestrator, :runner_client_opts,
      submit_log: submit_log,
      assert_freshness_before_submit: %{
        {MyApp.Assets.Gold, :asset} => {MyApp.Assets.Raw, :asset}
      }
    )

    version = manifest_version("mv_pipeline_success_writes_before_downstream_submit")
    plan = raw_to_gold_plan()

    run_state =
      pipeline_run_state("run_pipeline_success_writes_before_downstream_submit", version, plan, [
        {MyApp.Assets.Gold, :asset}
      ])

    assert :ok = Storage.put_run(run_state)

    assert {:ok, pid} = RunServer.start_link(%{run_state: run_state, version: version})
    ref = Process.monitor(pid)
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 1_000

    assert {:ok, raw_state} =
             Storage.get_asset_freshness_state(MyApp.Assets.Raw, :asset, Key.latest())

    assert raw_state.latest_success_run_id == run_state.id
  end

  test "already succeeded non-reusable materialization claim does not skip runner submission" do
    {:ok, submit_log} = Agent.start_link(fn -> [] end)
    Application.put_env(:favn_orchestrator, :runner_client, RunnerClientRecordingStub)
    Application.put_env(:favn_orchestrator, :runner_client_opts, submit_log: submit_log)

    version = manifest_version("mv_pipeline_already_succeeded_claim")
    plan = single_node_plan({MyApp.Assets.Gold, :asset}, window: nil)

    run_state =
      pipeline_run_state("run_pipeline_already_succeeded_claim", version, plan, [
        {MyApp.Assets.Gold, :asset}
      ])

    node_key = {{MyApp.Assets.Gold, :asset}, nil}
    previous_run = %{run_state | id: "previous_run_pipeline_already_succeeded_claim"}

    assert {:ok, claim} =
             materialization_claim(previous_run, version, node_key, Key.latest())
             |> Storage.try_acquire_materialization_claim()

    assert {:ok, _claim} =
             Storage.complete_materialization_claim(claim.claim_key, %{
               finished_at: DateTime.utc_now(),
               metadata: %{test: :already_succeeded}
             })

    assert :ok = Storage.put_run(run_state)

    assert {:ok, pid} = RunServer.start_link(%{run_state: run_state, version: version})
    ref = Process.monitor(pid)
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 1_000

    assert [_submitted] = Agent.get(submit_log, & &1)

    assert {:ok, stored} = Storage.get_run(run_state.id)

    assert [
             %Favn.Run.NodeResult{
               status: :ok,
               reason: nil
             }
           ] =
             stored.result.node_results
  end

  test "always freshness executes despite an existing succeeded claim" do
    {:ok, submit_log} = Agent.start_link(fn -> [] end)
    Application.put_env(:favn_orchestrator, :runner_client, RunnerClientRecordingStub)
    Application.put_env(:favn_orchestrator, :runner_client_opts, submit_log: submit_log)

    version =
      manifest_version("mv_pipeline_always_claim",
        freshness: Policy.from_value!(%{mode: :always})
      )

    plan = single_node_plan({MyApp.Assets.Gold, :asset}, window: nil)

    run_state =
      pipeline_run_state("run_pipeline_always_claim", version, plan, [
        {MyApp.Assets.Gold, :asset}
      ])

    node_key = {{MyApp.Assets.Gold, :asset}, nil}
    previous_run = %{run_state | id: "previous_run_pipeline_always_claim"}

    assert {:ok, claim} =
             materialization_claim(previous_run, version, node_key, Key.latest())
             |> Storage.try_acquire_materialization_claim()

    assert {:ok, _claim} =
             Storage.complete_materialization_claim(claim.claim_key, %{
               finished_at: DateTime.utc_now()
             })

    assert :ok = Storage.put_run(run_state)

    assert {:ok, pid} = RunServer.start_link(%{run_state: run_state, version: version})
    ref = Process.monitor(pid)
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 1_000

    assert [_submitted] = Agent.get(submit_log, & &1)
  end

  test "upstream refreshed downstream always asset reuses matching concurrent claim" do
    {:ok, submit_log} = Agent.start_link(fn -> [] end)
    Application.put_env(:favn_orchestrator, :runner_client, RunnerClientRecordingStub)
    Application.put_env(:favn_orchestrator, :runner_client_opts, submit_log: submit_log)

    version =
      manifest_version("mv_pipeline_downstream_always_reuses_upstream_refreshed_claim",
        freshness: Policy.from_value!(%{mode: :always})
      )

    plan = raw_to_gold_plan()
    raw_ref = {MyApp.Assets.Raw, :asset}
    gold_ref = {MyApp.Assets.Gold, :asset}
    raw_key = {raw_ref, nil}
    gold_key = {gold_ref, nil}

    run_state =
      pipeline_run_state(
        "run_pipeline_downstream_always_reuses_upstream_refreshed_claim",
        version,
        plan,
        [gold_ref]
      )

    input_versions = [
      %{
        upstream_ref: raw_ref,
        upstream_node_key: raw_key,
        freshness_version: freshness_version(run_state, raw_key),
        success_run_id: run_state.id
      }
    ]

    assert {:ok, claim} =
             materialization_claim(run_state, version, gold_key, Key.latest(),
               input_versions: input_versions,
               reusable: true
             )
             |> Storage.try_acquire_materialization_claim()

    assert {:ok, _claim} =
             Storage.complete_materialization_claim(claim.claim_key, %{
               finished_at: DateTime.utc_now()
             })

    assert :ok = Storage.put_run(run_state)

    assert {:ok, pid} = RunServer.start_link(%{run_state: run_state, version: version})
    ref = Process.monitor(pid)
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 1_000

    assert [%{asset_ref: ^raw_ref}] = Agent.get(submit_log, & &1)

    assert {:ok, stored} = Storage.get_run(run_state.id)
    statuses = Map.new(stored.result.node_results, &{&1.node_key, {&1.status, &1.reason}})

    assert statuses[raw_key] == {:ok, nil}
    assert statuses[gold_key] == {:skipped_fresh, :concurrent_materialization_succeeded}
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

    assert {:ok, state_two} =
             Storage.get_asset_freshness_state(
               MyApp.Assets.Raw,
               :asset,
               Key.window!(window_two.key)
             )

    assert state_two.status == :error
    assert state_two.freshness_version == nil
    assert state_two.latest_success_node_key == nil

    statuses = Map.new(stored.result.node_results, &{&1.node_key, &1.status})
    assert statuses[node_one] == :ok
    assert statuses[node_two] == :error
  end

  defp receive_submissions(count) do
    Enum.map(1..count, fn _index ->
      assert_receive {:submitted, asset_ref, execution_id}, 1_000
      {asset_ref, execution_id}
    end)
  end

  defp receive_awaiters(submissions) do
    submissions
    |> Map.new(fn {asset_ref, execution_id} ->
      assert_receive {:awaiting, ^execution_id, ^asset_ref, awaiter}, 1_000
      {execution_id, awaiter}
    end)
  end

  defp awaiter_for_submission({_asset_ref, execution_id}, awaiters) do
    {execution_id, Map.fetch!(awaiters, execution_id)}
  end

  defp release_submission(submission, awaiters) do
    release_submission(submission, awaiters, :ok)
  end

  defp release_submission({_asset_ref, execution_id} = submission, awaiters, status) do
    {_execution_id, awaiter} = awaiter_for_submission(submission, awaiters)
    send(awaiter, {:release_runner_result, execution_id, status})
  end

  defp wait_for_run_event(run_id, event_type, timeout_ms \\ 1_000) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    wait_for_run_event_until(run_id, event_type, deadline)
  end

  defp wait_for_run_event_until(run_id, event_type, deadline) do
    {:ok, events} = Storage.list_run_events(run_id)

    case Enum.find(events, &(&1.event_type == event_type)) do
      nil ->
        if System.monotonic_time(:millisecond) >= deadline do
          {:error, :timeout}
        else
          Process.sleep(10)
          wait_for_run_event_until(run_id, event_type, deadline)
        end

      event ->
        {:ok, event}
    end
  end

  defp same_stage_plan(refs) do
    node_keys = Enum.map(refs, &{&1, nil})

    nodes =
      Map.new(Enum.zip(refs, node_keys), fn {ref, node_key} ->
        {node_key, plan_node(ref, node_key, stage: 0)}
      end)

    %Plan{
      target_refs: refs,
      target_node_keys: node_keys,
      nodes: nodes,
      topo_order: refs,
      stages: [refs],
      node_stages: [node_keys]
    }
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

  defp manifest_version_for_refs(manifest_version_id, refs) do
    assets =
      Enum.map(refs, fn {module, name} = ref ->
        %Favn.Manifest.Asset{ref: ref, module: module, name: name}
      end)

    {:ok, version} =
      Version.new(%Manifest{assets: assets}, manifest_version_id: manifest_version_id)

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

  defp independent_branch_plan(raw_a, silver_a, raw_b, silver_b) do
    raw_a_key = {raw_a, nil}
    silver_a_key = {silver_a, nil}
    raw_b_key = {raw_b, nil}
    silver_b_key = {silver_b, nil}

    %Plan{
      target_refs: [silver_a, silver_b],
      target_node_keys: [silver_a_key, silver_b_key],
      nodes: %{
        raw_a_key => plan_node(raw_a, raw_a_key, downstream: [silver_a_key], stage: 0),
        raw_b_key => plan_node(raw_b, raw_b_key, downstream: [silver_b_key], stage: 0),
        silver_a_key => plan_node(silver_a, silver_a_key, upstream: [raw_a_key], stage: 1),
        silver_b_key => plan_node(silver_b, silver_b_key, upstream: [raw_b_key], stage: 1)
      },
      topo_order: [raw_a, raw_b, silver_a, silver_b],
      stages: [[raw_a, raw_b], [silver_a, silver_b]],
      node_stages: [[raw_a_key, raw_b_key], [silver_a_key, silver_b_key]]
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

  defp materialization_claim(
         %RunState{} = run_state,
         %Version{} = version,
         node_key,
         freshness_key,
         opts \\ []
       ) do
    node = Map.fetch!(run_state.plan.nodes, node_key)
    {module, name} = node.ref
    now = DateTime.utc_now()
    input_versions = Keyword.get(opts, :input_versions, [])
    input_fingerprint = MaterializationClaimIdentity.input_fingerprint(input_versions)

    producer_identity =
      if Keyword.get(opts, :reusable, false) do
        version.content_hash
      else
        materialization_producer_identity(run_state, version, node_key)
      end

    %{
      claim_key:
        MaterializationClaimIdentity.claim_key(
          node.ref,
          freshness_key,
          input_fingerprint,
          producer_identity
        ),
      run_id: "previous_claim_run",
      asset_step_id: "previous_claim_step",
      node_key: node_key,
      asset_ref_module: module,
      asset_ref_name: name,
      freshness_key: freshness_key,
      input_fingerprint: input_fingerprint,
      input_versions: input_versions,
      manifest_version_id: version.manifest_version_id,
      manifest_content_hash: version.content_hash,
      status: :claimed,
      claimed_at: now,
      heartbeat_at: now,
      expires_at: DateTime.add(now, 60_000, :millisecond)
    }
  end

  defp materialization_producer_identity(%RunState{} = run_state, %Version{} = version, node_key) do
    node_token = node_key |> :erlang.term_to_binary() |> Base.encode16(case: :lower)
    Enum.join([version.content_hash, run_state.id, node_token], ":")
  end

  defp freshness_version(%RunState{} = run_state, node_key) do
    encoded_node_key = node_key |> :erlang.term_to_binary() |> Base.encode16(case: :lower)
    "#{run_state.id}:#{encoded_node_key}"
  end

  defp start_memory_if_needed do
    case Process.whereis(Memory) do
      nil ->
        start_supervised!(%{
          id: Memory,
          start: {Memory, :start_link, [[name: Memory]]}
        })

      _pid ->
        :ok
    end
  end
end
