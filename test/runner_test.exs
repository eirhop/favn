defmodule Flux.RunnerTest do
  use ExUnit.Case

  alias Flux.Test.Fixtures.Assets.Runner.RunnerAssets
  alias Flux.Test.Fixtures.Assets.Runner.TerminalFailingStore

  setup do
    state = Flux.TestSetup.capture_state()

    :ok = Flux.TestSetup.setup_asset_modules([RunnerAssets], reload_graph?: true)
    :ok = Flux.TestSetup.configure_storage_adapter(Flux.Storage.Adapter.Memory, [])
    :ok = Flux.TestSetup.clear_memory_storage_adapter()

    on_exit(fn ->
      Flux.TestSetup.restore_state(state, reload_graph?: true, clear_storage_adapter_env?: true)
    end)

    :ok
  end

  test "runs deterministic stage-by-stage execution with context and deps map" do
    assert {:ok, run} =
             Flux.run({RunnerAssets, :final},
               dependencies: :all,
               params: %{partition: "2026-03-25"}
             )

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
    assert run.event_seq == 8
  end

  test "supports dependencies: :none target-only runs" do
    assert {:ok, run} = Flux.run({RunnerAssets, :target_only}, dependencies: :none)

    assert run.status == :ok
    assert Map.keys(run.outputs) == [{RunnerAssets, :target_only}]
    assert run.target_outputs == %{{RunnerAssets, :target_only} => 0}
  end

  test "captures invalid return shape as a structured run failure" do
    assert {:error, run} = Flux.run({RunnerAssets, :invalid_return})

    assert run.status == :error
    assert %{ref: {RunnerAssets, :invalid_return}} = run.error

    assert run.asset_results[{RunnerAssets, :invalid_return}].error.reason ==
             {:invalid_return_shape, {:ok, :bad_shape},
              expected: "{:ok, %Flux.Asset.Output{}} | {:error, reason}"}

    assert run.event_seq == 6
  end

  test "captures raised exceptions with stacktrace details" do
    assert {:error, run} = Flux.run({RunnerAssets, :crashes})

    assert run.status == :error

    error = run.asset_results[{RunnerAssets, :crashes}].error
    assert error.kind == :error
    assert is_list(error.stacktrace)
    assert error.message == "boom"
  end

  test "preserves asset metadata in asset_results while keeping outputs as business values" do
    assert {:ok, run} = Flux.run({RunnerAssets, :with_meta})

    ref = {RunnerAssets, :with_meta}
    output = {:rows, [1, 2, 3]}
    meta = %{row_count: 123, source: :test}

    assert run.outputs[ref] == output
    assert run.asset_results[ref].output == output
    assert run.asset_results[ref].meta == meta
  end

  test "persists run records for get_run/1" do
    assert {:ok, run} = Flux.run({RunnerAssets, :final})

    assert {:ok, fetched} = Flux.get_run(run.id)
    assert fetched.id == run.id
    assert fetched.status == :ok
    assert fetched.target_refs == run.target_refs
  end

  test "lists runs with status filter and limit in newest-first order" do
    assert {:ok, ok_run} = Flux.run({RunnerAssets, :final})
    assert {:error, error_run} = Flux.run({RunnerAssets, :crashes})

    assert {:ok, all_runs} = Flux.list_runs()
    assert Enum.map(all_runs, & &1.id) == [error_run.id, ok_run.id]

    assert {:ok, running_runs} = Flux.list_runs(status: :running)
    assert running_runs == []

    assert {:ok, failed_runs} = Flux.list_runs(status: :error)
    assert Enum.map(failed_runs, & &1.id) == [error_run.id]

    assert {:ok, limited_runs} = Flux.list_runs(limit: 1)
    assert Enum.map(limited_runs, & &1.id) == [error_run.id]
  end

  test "returns :not_found for missing runs" do
    assert {:error, :not_found} = Flux.get_run("missing-run-id")
  end

  test "returns execution result even when terminal persistence fails" do
    :ok = Flux.TestSetup.configure_storage_adapter(TerminalFailingStore, [])
    TerminalFailingStore.reset!()

    assert {:ok, run} = Flux.run({RunnerAssets, :final})
    assert run.status == :ok
  end

  test "rejects unsupported list_runs status filter values" do
    assert {:error, :invalid_opts} = Flux.list_runs(status: :pending)
  end
end
