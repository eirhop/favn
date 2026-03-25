defmodule Flux.RunnerTest do
  use ExUnit.Case

  defmodule RunnerAssets do
    use Flux.Assets

    @asset true
    def base(ctx, _deps) do
      {:ok, %Flux.Asset.Output{output: {:base, ctx.params[:partition]}}}
    end

    @asset depends_on: [:base]
    def transform(_ctx, deps) do
      {:ok, %Flux.Asset.Output{output: {:transform, Map.fetch!(deps, {__MODULE__, :base})}}}
    end

    @asset depends_on: [:base]
    def invalid_return(_ctx, _deps) do
      {:ok, :bad_shape}
    end

    @asset depends_on: [:transform]
    def final(_ctx, deps) do
      {:ok, %Flux.Asset.Output{output: {:final, Map.fetch!(deps, {__MODULE__, :transform})}}}
    end

    @asset depends_on: [:transform]
    def target_only(_ctx, deps) do
      {:ok, %Flux.Asset.Output{output: map_size(deps)}}
    end

    @asset depends_on: [:base]
    def crashes(_ctx, _deps) do
      raise "boom"
    end

    @asset true
    def with_meta(_ctx, _deps) do
      {:ok,
       %Flux.Asset.Output{
         output: {:rows, [1, 2, 3]},
         meta: %{row_count: 123, source: :test}
       }}
    end
  end

  defmodule TerminalFailingStore do
    @behaviour Flux.RunStore

    @counter_key {__MODULE__, :put_count}

    @impl true
    def child_spec(_opts), do: :none

    @impl true
    def put_run(_run, _opts) do
      count = :persistent_term.get(@counter_key, 0)
      :persistent_term.put(@counter_key, count + 1)

      if count == 0 do
        :ok
      else
        {:error, :terminal_write_failed}
      end
    end

    @impl true
    def get_run(_run_id, _opts), do: {:error, :not_found}

    @impl true
    def list_runs(_opts, _adapter_opts), do: {:ok, []}

    def reset!, do: :persistent_term.erase(@counter_key)
  end

  setup do
    previous_modules = Application.get_env(:flux, :asset_modules)
    previous_catalog = Flux.Registry.build_catalog(previous_modules || [])

    Application.put_env(:flux, :asset_modules, [RunnerAssets])
    Application.put_env(:flux, :run_store, Flux.RunStore.Memory)
    Application.put_env(:flux, :run_store_opts, [])
    clear_memory_run_store()
    assert :ok = Flux.Registry.reload()
    assert :ok = Flux.GraphIndex.reload()

    on_exit(fn ->
      if is_nil(previous_modules) do
        Application.delete_env(:flux, :asset_modules)
      else
        Application.put_env(:flux, :asset_modules, previous_modules)
      end

      Application.delete_env(:flux, :run_store)
      Application.delete_env(:flux, :run_store_opts)
      restore_registry(previous_catalog)
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
    Application.put_env(:flux, :run_store, TerminalFailingStore)
    TerminalFailingStore.reset!()

    assert {:ok, run} = Flux.run({RunnerAssets, :final})
    assert run.status == :ok
  end

  test "rejects unsupported list_runs status filter values" do
    assert {:error, :invalid_opts} = Flux.list_runs(status: :pending)
  end

  defp clear_memory_run_store do
    table = Flux.RunStore.Memory.Table

    if :ets.whereis(table) != :undefined do
      :ets.delete_all_objects(table)
    end

    :ok
  end

  defp restore_registry({:ok, _catalog}) do
    :ok = Flux.Registry.reload()
    :ok = Flux.GraphIndex.reload()
  end

  defp restore_registry({:error, _reason}), do: :ok
end
