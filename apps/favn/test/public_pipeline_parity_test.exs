defmodule Favn.PublicPipelineParityTest do
  use ExUnit.Case, async: true

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
    assert resolution.pipeline_ctx.window == :calendar_day
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

  test "resolve_pipeline/2 returns not_asset_module for invalid module selector" do
    module_name = Module.concat(__MODULE__, "InvalidModule#{System.unique_integer([:positive])}")

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

    assert_raise ArgumentError, ~r/pipeline clause `window` must be an atom/, fn ->
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
  end
end
