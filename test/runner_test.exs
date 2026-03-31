defmodule Favn.RunnerTest do
  use ExUnit.Case

  alias Favn.Test.Fixtures.Assets.Runner.RunnerAssets
  alias Favn.Test.Fixtures.Assets.Runner.TerminalFailingStore

  defmodule InitialFailingStore do
    @behaviour Favn.Storage.Adapter

    @impl true
    def child_spec(_opts), do: :none

    @impl true
    def put_run(_run, _opts), do: {:error, :initial_write_failed}

    @impl true
    def get_run(_run_id, _opts), do: {:error, :not_found}

    @impl true
    def list_runs(_opts, _adapter_opts), do: {:ok, []}
  end

  defmodule ProtocolTestExecutor do
    @behaviour Favn.Runtime.Executor

    alias Favn.Asset
    alias Favn.Asset.Output
    alias Favn.Run.Context

    @impl true
    def start_step(%Asset{} = asset, %Context{} = ctx, deps, reply_to, step_ref)
        when is_map(deps) and is_pid(reply_to) do
      exec_ref = make_ref()

      {pid, monitor_ref} =
        spawn_monitor(fn ->
          mode = ctx.params[:executor_mode]
          result = invoke(asset, ctx, deps)

          case mode do
            :mismatch_ref ->
              send(
                reply_to,
                {:executor_step_result, exec_ref, {asset.module, :not_the_step}, result}
              )

            :duplicate_result ->
              send(reply_to, {:executor_step_result, exec_ref, step_ref, result})
              send(reply_to, {:executor_step_result, exec_ref, step_ref, result})

            :late_after_down ->
              sender =
                spawn(fn ->
                  Process.sleep(10)
                  send(reply_to, {:executor_step_result, exec_ref, step_ref, result})
                end)

              Process.unlink(sender)
              exit(:executor_down_before_result)

            :down_normal_before_result ->
              sender =
                spawn(fn ->
                  Process.sleep(10)
                  send(reply_to, {:executor_step_result, exec_ref, step_ref, result})
                end)

              Process.unlink(sender)
              :ok

            _ ->
              send(reply_to, {:executor_step_result, exec_ref, step_ref, result})
          end
        end)

      {:ok, %{exec_ref: exec_ref, monitor_ref: monitor_ref, pid: pid}}
    end

    @impl true
    def cancel_step(%{pid: pid}, _reason) do
      Process.exit(pid, :kill)
      :ok
    end

    defp invoke(asset, %Context{} = ctx, deps) do
      try do
        case apply(asset.module, asset.name, [ctx, deps]) do
          {:ok, %Output{} = asset_output} ->
            {:ok, %{output: asset_output.output, meta: asset_output.meta}}

          {:error, reason} ->
            {:error, %{kind: :error, reason: reason, stacktrace: []}}

          other ->
            {:error,
             %{
               kind: :error,
               reason:
                 {:invalid_return_shape, other,
                  expected: "{:ok, %Favn.Asset.Output{}} | {:error, reason}"},
               stacktrace: []
             }}
        end
      rescue
        error ->
          {:error,
           %{
             kind: :error,
             reason: error,
             stacktrace: __STACKTRACE__,
             message: Exception.message(error)
           }}
      catch
        :throw, reason -> {:error, %{kind: :throw, reason: reason, stacktrace: __STACKTRACE__}}
        :exit, reason -> {:error, %{kind: :exit, reason: reason, stacktrace: __STACKTRACE__}}
      end
    end
  end

  setup do
    state = Favn.TestSetup.capture_state()
    previous_runtime_executor = Application.get_env(:favn, :runtime_executor)

    :ok = Favn.TestSetup.setup_asset_modules([RunnerAssets], reload_graph?: true)
    :ok = Favn.TestSetup.configure_storage_adapter(Favn.Storage.Adapter.Memory, [])
    :ok = Favn.TestSetup.clear_memory_storage_adapter()

    on_exit(fn ->
      Favn.TestSetup.restore_state(state, reload_graph?: true, clear_storage_adapter_env?: true)

      if is_nil(previous_runtime_executor) do
        Application.delete_env(:favn, :runtime_executor)
      else
        Application.put_env(:favn, :runtime_executor, previous_runtime_executor)
      end
    end)

    :ok
  end

  test "runs deterministic stage-by-stage execution with context and deps map" do
    assert {:ok, run_id} =
             Favn.run({RunnerAssets, :final},
               dependencies: :all,
               params: %{partition: "2026-03-25"}
             )

    assert {:ok, run} = Favn.await_run(run_id)

    assert run.status == :ok
    assert is_binary(run.id)
    assert %DateTime{} = run.started_at
    assert %DateTime{} = run.finished_at

    assert run.outputs[{RunnerAssets, :base}] == {:base, "2026-03-25"}
    assert run.outputs[{RunnerAssets, :transform}] == {:transform, {:base, "2026-03-25"}}

    assert run.target_outputs == %{
             {RunnerAssets, :final} => {:final, {:transform, {:base, "2026-03-25"}}}
           }

    assert run.asset_results[{RunnerAssets, :base}].duration_ms >= 0
    assert run.asset_results[{RunnerAssets, :final}].status == :ok
    assert run.asset_results[{RunnerAssets, :base}].stage == 0
    assert run.asset_results[{RunnerAssets, :transform}].stage == 1
    assert run.asset_results[{RunnerAssets, :final}].stage == 2

    assert Enum.sort(Map.keys(run.asset_results)) == [
             {RunnerAssets, :base},
             {RunnerAssets, :final},
             {RunnerAssets, :transform}
           ]

    assert run.event_seq == 12
  end

  test "supports dependencies: :none target-only runs" do
    assert {:ok, run_id} = Favn.run({RunnerAssets, :target_only}, dependencies: :none)
    assert {:ok, run} = Favn.await_run(run_id)

    assert run.status == :ok
    assert Map.keys(run.outputs) == [{RunnerAssets, :target_only}]
    assert run.target_outputs == %{{RunnerAssets, :target_only} => 0}
  end

  test "captures invalid return shape as a structured run failure" do
    assert {:ok, run_id} = Favn.run({RunnerAssets, :invalid_return})
    assert {:error, run} = Favn.await_run(run_id)

    assert run.status == :error
    assert %{ref: {RunnerAssets, :invalid_return}} = run.error

    assert run.asset_results[{RunnerAssets, :invalid_return}].error.reason ==
             {:invalid_return_shape, {:ok, :bad_shape},
              expected: "{:ok, %Favn.Asset.Output{}} | {:error, reason}"}

    assert run.event_seq == 9
  end

  test "captures raised exceptions with stacktrace details" do
    assert {:ok, run_id} = Favn.run({RunnerAssets, :crashes})
    assert {:error, run} = Favn.await_run(run_id)

    assert run.status == :error

    error = run.asset_results[{RunnerAssets, :crashes}].error
    assert error.kind == :error
    assert is_list(error.stacktrace)
    assert error.message == "boom"
  end

  test "normalizes explicit asset error tuples into canonical run error payloads" do
    assert {:ok, run_id} = Favn.run({RunnerAssets, :returns_error})
    assert {:error, run} = Favn.await_run(run_id)

    ref = {RunnerAssets, :returns_error}

    assert run.status == :error
    assert run.error == %{ref: ref, stage: 1, reason: :domain_failure}
    assert run.target_outputs == %{}

    assert %{kind: :error, reason: :domain_failure, stacktrace: []} =
             run.asset_results[ref].error
  end

  test "preserves asset metadata in asset_results while keeping outputs as business values" do
    assert {:ok, run_id} = Favn.run({RunnerAssets, :with_meta})
    assert {:ok, run} = Favn.await_run(run_id)

    ref = {RunnerAssets, :with_meta}
    output = {:rows, [1, 2, 3]}
    meta = %{row_count: 123, source: :test}

    assert run.outputs[ref] == output
    assert run.asset_results[ref].output == output
    assert run.asset_results[ref].meta == meta
  end

  test "persists run records for get_run/1" do
    assert {:ok, run_id} = Favn.run({RunnerAssets, :final})
    assert {:ok, run} = Favn.await_run(run_id)

    assert {:ok, fetched} = Favn.get_run(run.id)
    assert fetched.id == run.id
    assert fetched.status == :ok
    assert fetched.target_refs == run.target_refs
  end

  test "lists runs with status filter and limit in newest-first order" do
    assert {:ok, ok_run_id} = Favn.run({RunnerAssets, :final})
    assert {:ok, ok_run} = Favn.await_run(ok_run_id)

    assert {:ok, error_run_id} = Favn.run({RunnerAssets, :crashes})
    assert {:error, error_run} = Favn.await_run(error_run_id)

    assert {:ok, all_runs} = Favn.list_runs()
    assert Enum.map(all_runs, & &1.id) == [error_run.id, ok_run.id]

    assert {:ok, running_runs} = Favn.list_runs(status: :running)
    assert running_runs == []

    assert {:ok, failed_runs} = Favn.list_runs(status: :error)
    assert Enum.map(failed_runs, & &1.id) == [error_run.id]

    assert {:ok, limited_runs} = Favn.list_runs(limit: 1)
    assert Enum.map(limited_runs, & &1.id) == [error_run.id]
  end

  test "returns :not_found for missing runs" do
    assert {:error, :not_found} = Favn.get_run("missing-run-id")
  end

  test "await_run/2 returns :not_found immediately for unknown run ids" do
    assert {:error, :not_found} = Favn.await_run("missing-run-id")
  end

  test "returns invalid run params as canonical error payload from run/2" do
    assert {:error, :invalid_run_params} = Favn.run({RunnerAssets, :final}, params: :not_a_map)
  end

  test "accepts submission when failure happens at a later terminal persistence point" do
    :ok = Favn.TestSetup.configure_storage_adapter(TerminalFailingStore, [])
    TerminalFailingStore.reset!()

    assert {:ok, run_id} = Favn.run({RunnerAssets, :final})
    assert is_binary(run_id)
  end

  test "fails immediately when first persisted snapshot cannot be written" do
    :ok = Favn.TestSetup.configure_storage_adapter(InitialFailingStore, [])

    assert {:error, {:storage_persist_failed, {:store_error, :initial_write_failed}}} =
             Favn.run({RunnerAssets, :final})
  end

  test "long-running assets are not capped by a hardcoded sync timeout" do
    assert {:ok, run_id} = Favn.run({RunnerAssets, :slow_asset})
    assert {:ok, run} = Favn.await_run(run_id)
    assert run.status == :ok
    assert run.outputs[{RunnerAssets, :slow_asset}] == :slow_ok
  end

  test "emits step_ready and step_started/step_finished events with ref + stage" do
    assert {:ok, run_id} =
             Favn.run({RunnerAssets, :announce_target}, params: %{notify_pid: self()})

    :ok = Favn.subscribe_run(run_id)
    assert {:ok, _run} = Favn.await_run(run_id)
    events = collect_run_events_until_terminal([])

    step_events =
      Enum.filter(events, &(&1.event_type in [:step_ready, :step_started, :step_finished]))

    assert step_events != []
    assert Enum.all?(step_events, &Map.has_key?(&1, :ref))
    assert Enum.all?(step_events, &Map.has_key?(&1, :stage))
  end

  test "emits stable event schema envelope for run and step events" do
    assert {:ok, run_id} =
             Favn.run({RunnerAssets, :announce_target}, params: %{notify_pid: self()})

    :ok = Favn.subscribe_run(run_id)
    assert {:ok, _run} = Favn.await_run(run_id)
    events = collect_run_events_until_terminal([])

    assert events != []

    assert Enum.all?(events, fn event ->
             is_integer(event.schema_version) and event.schema_version >= 1 and
               is_atom(event.event_type) and
               event.run_id == run_id and
               is_integer(event.sequence) and
               match?(%DateTime{}, event.emitted_at) and
               is_atom(event.status) and
               is_map(event.data)
           end)

    assert Enum.any?(events, &(&1.entity == :run))
    assert Enum.any?(events, &(&1.entity == :step))
  end

  test "projected asset_results omit skipped steps that never executed" do
    assert {:ok, run_id} = Favn.run({RunnerAssets, :after_error})
    assert {:error, run} = Favn.await_run(run_id)

    assert run.status == :error
    assert Map.has_key?(run.asset_results, {RunnerAssets, :returns_error})
    refute Map.has_key?(run.asset_results, {RunnerAssets, :after_error})
  end

  test "rejects unsupported list_runs status filter values" do
    assert {:error, :invalid_opts} = Favn.list_runs(status: :pending)
  end

  test "executes independent ready steps in parallel with bounded concurrency" do
    counter = :atomics.new(2, signed: false)

    assert {:ok, run_id} =
             Favn.run({RunnerAssets, :parallel_join},
               max_concurrency: 2,
               params: %{counter: counter}
             )

    assert {:ok, run} = Favn.await_run(run_id)

    assert run.status == :ok
    assert run.outputs[{RunnerAssets, :parallel_join}] == [:parallel_a, :parallel_b, :parallel_c]
    assert :atomics.get(counter, 2) <= 2

    join_started = run.asset_results[{RunnerAssets, :parallel_join}].started_at

    latest_upstream_finish =
      [:parallel_a, :parallel_b, :parallel_c]
      |> Enum.map(fn name -> run.asset_results[{RunnerAssets, name}].finished_at end)
      |> Enum.max(DateTime)

    assert DateTime.compare(join_started, latest_upstream_finish) in [:eq, :gt]
  end

  test "admits ready steps deterministically while allowing non-deterministic completion" do
    parent = self()

    assert {:ok, run_id} =
             Favn.run({RunnerAssets, :parallel_join},
               max_concurrency: 2,
               params: %{notify_pid: parent}
             )

    assert {:ok, _run} = Favn.await_run(run_id)

    started_order =
      collect_parallel_started_order([])

    assert started_order == [
             {RunnerAssets, :parallel_a},
             {RunnerAssets, :parallel_b},
             {RunnerAssets, :parallel_c}
           ]
  end

  test "first failure closes admission and unresolved work is skipped after inflight drains" do
    counter = :atomics.new(2, signed: false)

    assert {:ok, run_id} =
             Favn.run({RunnerAssets, :parallel_terminal},
               max_concurrency: 2,
               params: %{counter: counter}
             )

    assert {:error, run} = Favn.await_run(run_id)

    assert run.status == :error

    assert run.error == %{
             ref: {RunnerAssets, :parallel_fail},
             stage: 1,
             reason: :parallel_failure
           }

    case Map.get(run.asset_results, {RunnerAssets, :parallel_slow}) do
      nil -> :ok
      result -> assert result.status == :ok
    end

    refute Map.has_key?(run.asset_results, {RunnerAssets, :parallel_after_slow})
    refute Map.has_key?(run.asset_results, {RunnerAssets, :parallel_terminal})
    assert :atomics.get(counter, 2) <= 2
  end

  test "normalizes hard executor crashes into failed step results" do
    assert {:ok, run_id} = Favn.run({RunnerAssets, :hard_crash}, max_concurrency: 1)
    assert {:error, run} = Favn.await_run(run_id)

    assert run.status == :error
    assert run.error == %{ref: {RunnerAssets, :hard_crash}, stage: 1, reason: :killed}
    assert run.asset_results[{RunnerAssets, :hard_crash}].error.kind == :exit
  end

  test "coordinator ignores mismatched ref in executor result and uses tracked ref" do
    Application.put_env(:favn, :runtime_executor, ProtocolTestExecutor)

    assert {:ok, run_id} =
             Favn.run({RunnerAssets, :slow_asset},
               max_concurrency: 1,
               params: %{executor_mode: :mismatch_ref}
             )

    assert {:ok, run} = Favn.await_run(run_id)
    assert run.status == :ok
    assert run.outputs[{RunnerAssets, :slow_asset}] == :slow_ok
  end

  test "duplicate executor result for same exec_ref is ignored and does not crash coordinator" do
    Application.put_env(:favn, :runtime_executor, ProtocolTestExecutor)

    assert {:ok, run_id} =
             Favn.run({RunnerAssets, :slow_asset},
               max_concurrency: 1,
               params: %{executor_mode: :duplicate_result}
             )

    assert {:ok, run} = Favn.await_run(run_id)
    assert run.status == :ok
  end

  test "late executor result after DOWN handling is ignored and does not crash coordinator" do
    Application.put_env(:favn, :runtime_executor, ProtocolTestExecutor)

    assert {:ok, run_id} =
             Favn.run({RunnerAssets, :slow_asset},
               max_concurrency: 1,
               params: %{executor_mode: :late_after_down}
             )

    assert {:error, run} = Favn.await_run(run_id)
    assert run.status == :error

    assert run.error == %{
             ref: {RunnerAssets, :slow_asset},
             stage: 0,
             reason: :executor_down_before_result
           }
  end

  test "DOWN :normal before delayed success result does not synthesize step failure" do
    Application.put_env(:favn, :runtime_executor, ProtocolTestExecutor)

    assert {:ok, run_id} =
             Favn.run({RunnerAssets, :slow_asset},
               max_concurrency: 1,
               params: %{executor_mode: :down_normal_before_result}
             )

    assert {:ok, run} = Favn.await_run(run_id)
    assert run.status == :ok
    assert run.outputs[{RunnerAssets, :slow_asset}] == :slow_ok
  end

  test "run/2 returns immediately with a run id while execution continues" do
    assert {:ok, run_id} = Favn.run({RunnerAssets, :slow_asset})
    assert is_binary(run_id)

    assert {:ok, running} = Favn.get_run(run_id)
    assert running.status == :running

    assert {:ok, done} = Favn.await_run(run_id)
    assert done.status == :ok
  end

  test "cancel_run/1 cancels a running run and await_run returns cancelled as error terminal" do
    assert {:ok, run_id} = Favn.run({RunnerAssets, :slow_asset})
    assert {:ok, :cancelling} = Favn.cancel_run(run_id)
    assert {:error, run} = Favn.await_run(run_id)
    assert run.status == :cancelled
    assert run.terminal_reason[:kind] == :cancelled
  end

  test "cancel_run/1 returns already_terminal for completed run" do
    assert {:ok, run_id} = Favn.run({RunnerAssets, :slow_asset})
    assert {:ok, _run} = Favn.await_run(run_id)
    assert {:ok, :already_terminal} = Favn.cancel_run(run_id)
  end

  test "cancel_run/1 returns not_found for unknown run id" do
    assert {:error, :not_found} = Favn.cancel_run("missing-run-id")
  end

  test "cancel_run/1 returns coordinator_unavailable when run is persisted as running but manager has no pid" do
    run = %Favn.Run{
      id: "orphan-running-run",
      status: :running,
      started_at: DateTime.utc_now(),
      target_refs: [],
      plan: %Favn.Plan{}
    }

    assert :ok = Favn.Storage.put_run(run)
    assert {:error, :coordinator_unavailable} = Favn.cancel_run(run.id)
  end

  test "cancel_run/1 returns timeout_in_progress when timeout handling has already started" do
    run = %Favn.Run{
      id: "timing-out-run",
      status: :running,
      started_at: DateTime.utc_now(),
      target_refs: [],
      plan: %Favn.Plan{},
      terminal_reason: %{kind: :timed_out, triggered_at: DateTime.utc_now()}
    }

    assert :ok = Favn.Storage.put_run(run)
    assert {:error, :timeout_in_progress} = Favn.cancel_run(run.id)
  end

  test "run timeout marks run as timed_out" do
    assert {:ok, run_id} = Favn.run({RunnerAssets, :slow_asset}, timeout_ms: 10)
    assert {:error, run} = Favn.await_run(run_id)
    assert run.status == :timed_out
    assert run.terminal_reason[:kind] == :timed_out
  end

  test "list_runs supports cancelled and timed_out status filters" do
    assert {:ok, cancelled_run_id} = Favn.run({RunnerAssets, :slow_asset})
    assert {:ok, :cancelling} = Favn.cancel_run(cancelled_run_id)
    assert {:error, cancelled_run} = Favn.await_run(cancelled_run_id)

    assert {:ok, timed_out_run_id} = Favn.run({RunnerAssets, :slow_asset}, timeout_ms: 10)
    assert {:error, timed_out_run} = Favn.await_run(timed_out_run_id)

    assert {:ok, cancelled_runs} = Favn.list_runs(status: :cancelled)
    assert Enum.map(cancelled_runs, & &1.id) == [cancelled_run.id]

    assert {:ok, timed_out_runs} = Favn.list_runs(status: :timed_out)
    assert Enum.map(timed_out_runs, & &1.id) == [timed_out_run.id]
  end

  test "retries raised exception and succeeds on a later attempt" do
    assert {:ok, run_id} =
             Favn.run({RunnerAssets, :transient_then_ok},
               retry: [max_attempts: 2]
             )

    assert {:ok, run} = Favn.await_run(run_id)
    ref = {RunnerAssets, :transient_then_ok}

    assert run.status == :ok
    assert run.outputs[ref] == :recovered
    assert run.asset_results[ref].attempt_count == 2
    assert length(run.asset_results[ref].attempts) == 2
  end

  test "retryable step_failed event reflects retrying status in stable envelope" do
    assert {:ok, run_id} =
             Favn.run({RunnerAssets, :transient_then_ok},
               retry: [max_attempts: 2]
             )

    :ok = Favn.subscribe_run(run_id)

    assert {:ok, _run} = Favn.await_run(run_id)

    assert_receive {:favn_run_event,
                    %{
                      event_type: :step_failed,
                      status: :retrying,
                      entity: :step,
                      data: %{retryable: true, final: false, exhausted: false}
                    }},
                   1_000
  end

  test "retries exits and succeeds on a later attempt" do
    assert {:ok, run_id} =
             Favn.run({RunnerAssets, :exits_then_ok},
               retry: [max_attempts: 2]
             )

    assert {:ok, run} = Favn.await_run(run_id)
    ref = {RunnerAssets, :exits_then_ok}

    assert run.status == :ok
    assert run.asset_results[ref].attempt_count == 2
  end

  test "explicit error returns are not retried by default" do
    assert {:ok, run_id} =
             Favn.run({RunnerAssets, :returns_error},
               retry: [max_attempts: 3]
             )

    assert {:error, run} = Favn.await_run(run_id)
    ref = {RunnerAssets, :returns_error}

    assert run.status == :error
    assert run.asset_results[ref].attempt_count == 1
  end

  test "explicit error returns can be retried when configured" do
    assert {:ok, run_id} =
             Favn.run({RunnerAssets, :returns_error},
               retry: [max_attempts: 2, retry_on: [:error_return]]
             )

    assert {:error, run} = Favn.await_run(run_id)
    ref = {RunnerAssets, :returns_error}

    assert run.status == :error
    assert run.asset_results[ref].attempt_count == 2
  end

  test "explicit {:error, :timeout} stays classified as error_return by default" do
    assert {:ok, run_id} =
             Favn.run({RunnerAssets, :returns_timeout_error},
               retry: [max_attempts: 3]
             )

    assert {:error, run} = Favn.await_run(run_id)
    ref = {RunnerAssets, :returns_timeout_error}

    assert run.status == :error
    assert run.asset_results[ref].attempt_count == 1
  end

  test "explicit {:error, :timeout} retries when retry_on includes :error_return" do
    assert {:ok, run_id} =
             Favn.run({RunnerAssets, :returns_timeout_error},
               retry: [max_attempts: 2, retry_on: [:error_return]]
             )

    assert {:error, run} = Favn.await_run(run_id)
    ref = {RunnerAssets, :returns_timeout_error}

    assert run.status == :error
    assert run.asset_results[ref].attempt_count == 2
  end

  test "runtime timeout path remains timed_out terminal" do
    assert {:ok, run_id} =
             Favn.run({RunnerAssets, :slow_asset},
               timeout_ms: 10,
               retry: [max_attempts: 3, retry_on: [:timeout]]
             )

    assert {:error, run} = Favn.await_run(run_id)
    assert run.status == :timed_out
    assert run.terminal_reason[:kind] == :timed_out
  end

  test "run validates retry options" do
    assert {:error, :invalid_retry_policy} = Favn.run({RunnerAssets, :slow_asset}, retry: :bad)
    assert {:error, :invalid_retry_policy} = Favn.run({RunnerAssets, :slow_asset}, retry: [:bad])
    assert {:error, :invalid_retry_policy} = Favn.run({RunnerAssets, :slow_asset}, retry: [123])

    assert {:error, :invalid_retry_policy} =
             Favn.run({RunnerAssets, :slow_asset}, retry: [123, max_attempts: 2])

    assert {:error, :invalid_retry_max_attempts} =
             Favn.run({RunnerAssets, :slow_asset}, retry: [max_attempts: 0])

    assert {:error, :invalid_retry_delay_ms} =
             Favn.run({RunnerAssets, :slow_asset}, retry: [delay_ms: -1])

    assert {:error, :invalid_retry_retry_on} =
             Favn.run({RunnerAssets, :slow_asset}, retry: [retry_on: [:not_valid]])

    assert {:ok, run_id} = Favn.run({RunnerAssets, :slow_asset})
    assert {:ok, run} = Favn.await_run(run_id)
    assert run.status == :ok
  end

  defp collect_parallel_started_order(acc) do
    receive do
      {:asset_started, name, _at} when name in [:parallel_a, :parallel_b, :parallel_c] ->
        started = acc ++ [{RunnerAssets, name}]

        if length(started) == 3 do
          started
        else
          collect_parallel_started_order(started)
        end

      {:asset_started, _name, _at} ->
        collect_parallel_started_order(acc)
    after
      2_000 ->
        flunk("did not receive all parallel asset start notifications")
    end
  end

  defp collect_run_events_until_terminal(acc) do
    receive do
      {:favn_run_event, event} ->
        next = acc ++ [event]

        if event.event_type in [:run_finished, :run_failed, :run_cancelled, :run_timed_out] do
          next
        else
          collect_run_events_until_terminal(next)
        end
    after
      2_000 ->
        flunk("did not receive terminal run event")
    end
  end
end
