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

  setup do
    state = Favn.TestSetup.capture_state()

    :ok = Favn.TestSetup.setup_asset_modules([RunnerAssets], reload_graph?: true)
    :ok = Favn.TestSetup.configure_storage_adapter(Favn.Storage.Adapter.Memory, [])
    :ok = Favn.TestSetup.clear_memory_storage_adapter()

    on_exit(fn ->
      Favn.TestSetup.restore_state(state, reload_graph?: true, clear_storage_adapter_env?: true)
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

    assert run.asset_results[ref].error == %{
             kind: :error,
             reason: :domain_failure,
             stacktrace: []
           }
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
    assert {:ok, error_run_id} = Favn.run({RunnerAssets, :crashes})

    assert {:ok, ok_run} = Favn.await_run(ok_run_id)
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
    parent = self()

    spawn(fn ->
      assert {:ok, run_id} =
               Favn.run({RunnerAssets, :announce_target}, params: %{notify_pid: parent})

      assert {:ok, _run} = Favn.await_run(run_id)
      send(parent, {:run_id, run_id})
    end)

    run_id =
      receive do
        {:announced_run_id, run_id} -> run_id
      after
        1_000 -> flunk("did not receive announced run_id from asset context")
      end

    :ok = Favn.subscribe_run(run_id)

    receive do
      {:run_id, run_id} -> run_id
    after
      2_000 -> flunk("run did not complete")
    end

    events =
      Stream.repeatedly(fn ->
        receive do
          {:favn_run_event, event} -> event
        after
          250 -> :done
        end
      end)
      |> Enum.take_while(&(&1 != :done))

    step_events = Enum.filter(events, &(&1.event in [:step_ready, :step_started, :step_finished]))

    assert step_events != []
    assert Enum.all?(step_events, &Map.has_key?(&1, :ref))
    assert Enum.all?(step_events, &Map.has_key?(&1, :stage))
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

  test "run/2 returns immediately with a run id while execution continues" do
    assert {:ok, run_id} = Favn.run({RunnerAssets, :slow_asset})
    assert is_binary(run_id)

    assert {:ok, running} = Favn.get_run(run_id)
    assert running.status == :running

    assert {:ok, done} = Favn.await_run(run_id)
    assert done.status == :ok
  end
end
