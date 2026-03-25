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

  setup do
    previous_modules = Application.get_env(:flux, :asset_modules)
    previous_catalog = Flux.Registry.build_catalog(previous_modules || [])

    Application.put_env(:flux, :asset_modules, [RunnerAssets])
    assert :ok = Flux.Registry.reload()
    assert :ok = Flux.GraphIndex.reload()

    on_exit(fn ->
      if is_nil(previous_modules) do
        Application.delete_env(:flux, :asset_modules)
      else
        Application.put_env(:flux, :asset_modules, previous_modules)
      end

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

  defp restore_registry({:ok, _catalog}) do
    :ok = Flux.Registry.reload()
    :ok = Flux.GraphIndex.reload()
  end

  defp restore_registry({:error, _reason}), do: :ok
end
