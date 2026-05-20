defmodule Favn.PublicPipelineParityTest do
  use ExUnit.Case, async: false

  alias Favn.Test.Fixtures.Assets.Pipeline.ReportingAssets
  alias Favn.Test.Fixtures.Assets.Pipeline.SalesAssets
  alias Favn.Test.Fixtures.Pipelines.AssetsShorthandPipeline
  alias Favn.Test.Fixtures.Pipelines.SelectPipeline
  alias Favn.Test.Fixtures.Pipelines.SimplePipeline
  alias Favn.Test.Fixtures.Triggers.Schedules
  alias FavnTestSupport.Fixtures

  setup_all do
    Fixtures.compile_fixture!(:pipeline_assets)
    :ok
  end

  setup do
    previous_asset_modules = Application.get_env(:favn, :asset_modules)

    Application.put_env(:favn, :asset_modules, [SalesAssets, ReportingAssets])

    on_exit(fn ->
      if is_nil(previous_asset_modules) do
        Application.delete_env(:favn, :asset_modules)
      else
        Application.put_env(:favn, :asset_modules, previous_asset_modules)
      end
    end)

    :ok
  end

  test "resolve_pipeline/2 resolves shorthand target and pipeline context" do
    assert {:ok, resolution} = Favn.resolve_pipeline(SimplePipeline)

    assert resolution.target_refs == [{SalesAssets, :sales_daily}]
    assert resolution.dependencies == :all
    assert resolution.pipeline_ctx.schedule.ref == {Schedules, :daily_default}
    assert resolution.pipeline_ctx.window.kind == :day
    assert resolution.pipeline_ctx.outputs == [:warehouse_gold]
  end

  test "resolve_pipeline/2 resolves select selectors additively" do
    assert {:ok, resolution} = Favn.resolve_pipeline(SelectPipeline)

    assert resolution.dependencies == :none

    assert resolution.target_refs == [
             {ReportingAssets, :marketing_snapshot},
             {SalesAssets, :extract_orders},
             {SalesAssets, :inventory_daily},
             {SalesAssets, :normalize_orders},
             {SalesAssets, :sales_daily}
           ]
  end

  test "resolve_pipeline/2 resolves assets/1 shorthand" do
    assert {:ok, resolution} = Favn.resolve_pipeline(AssetsShorthandPipeline)

    assert resolution.dependencies == :none

    assert resolution.target_refs == [
             {SalesAssets, :inventory_daily},
             {SalesAssets, :sales_daily}
           ]
  end

  test "resolve_pipeline/2 exposes pipeline execution policy" do
    module = Module.concat(__MODULE__, "ExecutionPolicyPipeline#{unique_suffix()}")

    compile_loadable_module!(
      module,
      """
      defmodule #{inspect(module)} do
        use Favn.Pipeline

        pipeline :execution_policy do
          asset {#{inspect(SalesAssets)}, :sales_daily}
          max_concurrency 2
          execution_pool :github_api
        end
      end
      """
    )

    assert {:ok, resolution} = Favn.resolve_pipeline(module)
    assert resolution.pipeline.max_concurrency == 2
    assert resolution.pipeline.execution_pool == :github_api
    assert resolution.pipeline_ctx.max_concurrency == 2
    assert resolution.pipeline_ctx.execution_pool == :github_api
  end

  test "resolve_pipeline/2 resolves single-asset module shorthand selector" do
    asset_module = Module.concat(__MODULE__, "SingleAsset#{unique_suffix()}")

    pipeline_module =
      Module.concat(__MODULE__, "SingleAssetPipeline#{unique_suffix()}")

    compile_loadable_module!(
      asset_module,
      """
      defmodule #{inspect(asset_module)} do
        use Favn.Asset

        def asset(_ctx), do: :ok
      end
      """
    )

    compile_loadable_module!(
      pipeline_module,
      """
      defmodule #{inspect(pipeline_module)} do
        use Favn.Pipeline

        pipeline :single_asset_shorthand do
          asset #{inspect(asset_module)}
          deps :none
        end
      end
      """
    )

    previous_asset_modules = Application.get_env(:favn, :asset_modules)
    Application.put_env(:favn, :asset_modules, [asset_module, SalesAssets, ReportingAssets])

    on_exit(fn ->
      if is_nil(previous_asset_modules) do
        Application.delete_env(:favn, :asset_modules)
      else
        Application.put_env(:favn, :asset_modules, previous_asset_modules)
      end
    end)

    assert {:ok, resolution} = Favn.resolve_pipeline(pipeline_module)
    assert resolution.target_refs == [{asset_module, :asset}]
    assert resolution.pipeline.selectors == [{:asset, {asset_module, :asset}}]
  end

  test "resolve_pipeline/2 returns not_asset_module for asset module shorthand on multi-asset module" do
    module_name =
      Module.concat(__MODULE__, "InvalidAssetShorthand#{unique_suffix()}")

    Code.compile_string(
      """
      defmodule #{inspect(module_name)} do
        use Favn.Pipeline

        pipeline :invalid_asset_shorthand do
          asset(#{inspect(SalesAssets)})
        end
      end
      """,
      "test/public_pipeline_parity_test.exs"
    )

    assert {:error, :not_asset_module} = Favn.resolve_pipeline(module_name)
  end

  test "resolve_pipeline/2 returns not_asset_module for invalid module selector" do
    module_name = Module.concat(__MODULE__, "InvalidModule#{unique_suffix()}")

    Code.compile_string(
      """
      defmodule #{inspect(module_name)} do
        use Favn.Pipeline

        pipeline :invalid_module do
          select do
            module(Enum)
          end
        end
      end
      """,
      "test/public_pipeline_parity_test.exs"
    )

    assert {:error, :not_asset_module} = Favn.resolve_pipeline(module_name)
  end

  test "resolve_pipeline/2 returns invalid_selector for invalid tag/category selector values" do
    module_name =
      Module.concat(__MODULE__, "InvalidSelectorValue#{unique_suffix()}")

    Code.compile_string(
      """
      defmodule #{inspect(module_name)} do
        use Favn.Pipeline

        pipeline :invalid_selector_value do
          select do
            tag(%{bad: :value})
            category([:bad])
          end
        end
      end
      """,
      "test/public_pipeline_parity_test.exs"
    )

    assert {:error, :invalid_selector} = Favn.resolve_pipeline(module_name)
  end

  test "resolve_pipeline/2 preserves compiler errors for invalid asset module shorthand" do
    bad_asset_module =
      Module.concat(__MODULE__, "BadAssetModule#{unique_suffix()}")

    pipeline_module =
      Module.concat(__MODULE__, "BadAssetPipeline#{unique_suffix()}")

    Code.compile_string(
      """
      defmodule #{inspect(bad_asset_module)} do
        def __favn_assets__, do: :invalid_shape
      end
      """,
      "test/public_pipeline_parity_test.exs"
    )

    Code.compile_string(
      """
      defmodule #{inspect(pipeline_module)} do
        use Favn.Pipeline

        pipeline :bad_asset_module_selector do
          asset #{inspect(bad_asset_module)}
        end
      end
      """,
      "test/public_pipeline_parity_test.exs"
    )

    assert {:error, {:invalid_asset_module, ^bad_asset_module}} =
             Favn.resolve_pipeline(pipeline_module)
  end

  test "pipeline DSL rejects invalid selection and clause usage" do
    assert_raise ArgumentError, ~r/cannot mix shorthand selection/, fn ->
      Code.compile_string("""
      defmodule MixedPipeline do
        use Favn.Pipeline

        pipeline :mixed do
          asset {#{inspect(SalesAssets)}, :sales_daily}

          select do
            tag :daily
          end
        end
      end
      """)
    end

    assert_raise ArgumentError, ~r/deps must be :all or :none/, fn ->
      Code.compile_string("""
      defmodule InvalidDepsPipeline do
        use Favn.Pipeline

        pipeline :invalid_deps do
          asset {#{inspect(SalesAssets)}, :sales_daily}
          deps :invalid
        end
      end
      """)
    end

    assert_raise ArgumentError, ~r/clause `schedule` can only be declared once/, fn ->
      Code.compile_string("""
      defmodule DuplicateSchedulePipeline do
        use Favn.Pipeline

        pipeline :dup_schedule do
          asset {#{inspect(SalesAssets)}, :sales_daily}
          schedule {#{inspect(Schedules)}, :daily_default}
          schedule cron: "0 3 * * *", timezone: "UTC"
        end
      end
      """)
    end

    assert_raise ArgumentError, ~r/must define one `pipeline ... do` block/, fn ->
      Code.compile_string("""
      defmodule MissingPipelineBlock do
        use Favn.Pipeline
      end
      """)
    end
  end

  test "pipeline DSL validates schedule/window/source/outputs clauses" do
    assert_raise ArgumentError, ~r/`schedule` must be `{Module, :name}` or keyword options/, fn ->
      Code.compile_string("""
      defmodule InvalidSchedulePipeline do
        use Favn.Pipeline

        pipeline :invalid_schedule do
          asset {#{inspect(SalesAssets)}, :sales_daily}
          schedule :daily
        end
      end
      """)
    end

    assert_raise ArgumentError,
                 ~r/pipeline clause `schedule` is invalid: \{:invalid_schedule_cron/,
                 fn ->
                   Code.compile_string("""
                   defmodule InvalidScheduleCronPipeline do
                     use Favn.Pipeline

                     pipeline :invalid_schedule_cron do
                       asset {#{inspect(SalesAssets)}, :sales_daily}
                       schedule cron: ""
                     end
                   end
                   """)
                 end

    assert_raise ArgumentError, ~r/pipeline clause `window` is invalid/, fn ->
      Code.compile_string("""
      defmodule InvalidWindowPipeline do
        use Favn.Pipeline

        pipeline :invalid_window do
          asset {#{inspect(SalesAssets)}, :sales_daily}
          window "day"
        end
      end
      """)
    end

    assert_raise ArgumentError, ~r/pipeline clause `source` must be an atom/, fn ->
      Code.compile_string("""
      defmodule InvalidSourcePipeline do
        use Favn.Pipeline

        pipeline :invalid_source do
          asset {#{inspect(SalesAssets)}, :sales_daily}
          source "snowflake"
        end
      end
      """)
    end

    assert_raise ArgumentError, ~r/pipeline clause `outputs` must be a list of atoms/, fn ->
      Code.compile_string("""
      defmodule InvalidOutputsPipeline do
        use Favn.Pipeline

        pipeline :invalid_outputs do
          asset {#{inspect(SalesAssets)}, :sales_daily}
          outputs [:warehouse_gold, "backup"]
        end
      end
      """)
    end

    assert_raise ArgumentError,
                 ~r/pipeline clause `max_concurrency` must be a positive integer/,
                 fn ->
                   Code.compile_string("""
                   defmodule InvalidMaxConcurrencyPipeline do
                     use Favn.Pipeline

                     pipeline :invalid_max_concurrency do
                       asset {#{inspect(SalesAssets)}, :sales_daily}
                       max_concurrency 0
                     end
                   end
                   """)
                 end

    assert_raise ArgumentError, ~r/pipeline clause `execution_pool` must be a non-nil atom/, fn ->
      Code.compile_string("""
      defmodule InvalidExecutionPoolPipeline do
        use Favn.Pipeline

        pipeline :invalid_execution_pool do
          asset {#{inspect(SalesAssets)}, :sales_daily}
          execution_pool "github_api"
        end
      end
      """)
    end
  end

  test "get_pipeline/1 loads valid pipeline modules before export checks" do
    module = Module.concat(__MODULE__, "LoadablePipeline#{unique_suffix()}")

    compile_loadable_module!(
      module,
      """
      defmodule #{inspect(module)} do
        use Favn.Pipeline

        pipeline :loaded_pipeline do
          asset {#{inspect(SalesAssets)}, :sales_daily}
          deps :all
        end
      end
      """
    )

    with_unloaded_module(module, fn ->
      assert {:ok, definition} = Favn.get_pipeline(module)
      assert definition.module == module
      assert definition.name == :loaded_pipeline
    end)
  end

  test "generate_manifest/1 compiles unloaded pipeline and schedule modules" do
    pipeline_module =
      Module.concat(__MODULE__, "ManifestPipeline#{unique_suffix()}")

    schedule_module =
      Module.concat(__MODULE__, "ManifestSchedules#{unique_suffix()}")

    compile_loadable_module!(
      schedule_module,
      """
      defmodule #{inspect(schedule_module)} do
        use Favn.Triggers.Schedules

        schedule :nightly, cron: "0 2 * * *", timezone: "Etc/UTC"
      end
      """
    )

    compile_loadable_module!(
      pipeline_module,
      """
      defmodule #{inspect(pipeline_module)} do
        use Favn.Pipeline

        pipeline :nightly_sales do
          asset {#{inspect(SalesAssets)}, :sales_daily}
          schedule {#{inspect(schedule_module)}, :nightly}
        end
      end
      """
    )

    with_unloaded_modules([pipeline_module, schedule_module], fn ->
      assert {:ok, manifest} =
               Favn.generate_manifest(
                 asset_modules: [SalesAssets, ReportingAssets],
                 pipeline_modules: [pipeline_module],
                 schedule_modules: [schedule_module]
               )

      assert Enum.any?(
               manifest.pipelines,
               &(&1.module == pipeline_module and &1.name == :nightly_sales)
             )

      assert Enum.any?(
               manifest.schedules,
               &(&1.module == schedule_module and &1.name == :nightly)
             )
    end)
  end

  defp with_unloaded_module(module, fun) when is_atom(module) and is_function(fun, 0) do
    with_unloaded_modules([module], fun)
  end

  defp with_unloaded_modules(modules, fun) when is_list(modules) and is_function(fun, 0) do
    Enum.each(modules, fn module ->
      assert {:module, ^module} = Code.ensure_loaded(module)
      :code.purge(module)
      :code.delete(module)
    end)

    try do
      fun.()
    after
      Enum.each(modules, fn module ->
        assert {:module, ^module} = Code.ensure_loaded(module)
      end)
    end
  end

  defp compile_loadable_module!(module, source) when is_atom(module) and is_binary(source) do
    dir =
      Path.join(
        System.tmp_dir!(),
        "favn_pipeline_loadable_modules_#{unique_suffix()}"
      )

    File.mkdir_p!(dir)

    file_path = Path.join(dir, "#{Macro.underscore(Atom.to_string(module))}.ex")
    File.mkdir_p!(Path.dirname(file_path))
    File.write!(file_path, source)

    Code.prepend_path(dir)

    assert {:ok, modules, _diagnostics} =
             Kernel.ParallelCompiler.compile_to_path([file_path], dir, return_diagnostics: true)

    assert module in modules
  end

  defp unique_suffix do
    "#{System.system_time(:nanosecond)}#{System.unique_integer([:positive, :monotonic])}"
  end
end
