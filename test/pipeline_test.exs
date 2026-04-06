defmodule Favn.PipelineTest do
  use ExUnit.Case

  alias Favn.Test.Fixtures.Assets.Pipeline.CtxRecorder
  alias Favn.Test.Fixtures.Assets.Pipeline.ReportingAssets
  alias Favn.Test.Fixtures.Assets.Pipeline.SalesAssets
  alias Favn.Test.Fixtures.Pipelines.AssetsShorthandPipeline
  alias Favn.Test.Fixtures.Pipelines.SelectPipeline
  alias Favn.Test.Fixtures.Pipelines.SimplePipeline

  setup_all do
    start_supervised!(CtxRecorder)
    :ok
  end

  setup do
    state = Favn.TestSetup.capture_state()

    :ok =
      Favn.TestSetup.setup_asset_modules([SalesAssets, ReportingAssets],
        reload_graph?: true
      )

    :ok = Favn.TestSetup.clear_memory_storage_adapter()
    CtxRecorder.reset()

    on_exit(fn ->
      Favn.TestSetup.restore_state(state, reload_graph?: true)
    end)

    :ok
  end

  test "plan_pipeline/2 resolves shorthand asset selection and upstream dependencies" do
    assert {:ok, plan} = Favn.plan_pipeline(SimplePipeline)

    assert plan.target_refs == [{SalesAssets, :sales_daily}]

    assert plan.topo_order == [
             {SalesAssets, :extract_orders},
             {SalesAssets, :normalize_orders},
             {SalesAssets, :sales_daily}
           ]
  end

  test "plan_pipeline/2 resolves select block selectors additively (union semantics)" do
    assert {:ok, plan} = Favn.plan_pipeline(SelectPipeline)

    assert plan.dependencies == :none

    assert plan.target_refs == [
             {ReportingAssets, :marketing_snapshot},
             {SalesAssets, :extract_orders},
             {SalesAssets, :inventory_daily},
             {SalesAssets, :normalize_orders},
             {SalesAssets, :sales_daily}
           ]

    assert plan.topo_order == [
             {ReportingAssets, :marketing_snapshot},
             {SalesAssets, :extract_orders},
             {SalesAssets, :normalize_orders},
             {SalesAssets, :inventory_daily},
             {SalesAssets, :sales_daily}
           ]
  end

  test "plan_pipeline/2 resolves assets/1 shorthand" do
    assert {:ok, plan} = Favn.plan_pipeline(AssetsShorthandPipeline)

    assert plan.dependencies == :none

    assert plan.target_refs == [
             {SalesAssets, :inventory_daily},
             {SalesAssets, :sales_daily}
           ]
  end

  test "run_pipeline/2 persists pipeline provenance in public run state" do
    assert {:ok, run_id} =
             Favn.run_pipeline(SimplePipeline,
               params: %{requested_by: "operator"},
               trigger: %{kind: :manual, requested_by: :user}
             )

    assert {:ok, _run} = Favn.await_run(run_id, timeout: 5_000)
    assert {:ok, stored_run} = Favn.get_run(run_id)

    assert stored_run.pipeline.id == :daily_sales
    assert stored_run.pipeline.name == :daily_sales
    assert stored_run.pipeline.trigger == %{kind: :manual, requested_by: :user}
    assert stored_run.pipeline.schedule == :daily_default
    assert stored_run.pipeline.partition == :calendar_day
    assert stored_run.pipeline.source == :snowflake_primary
    assert stored_run.pipeline.outputs == [:warehouse_gold]
  end

  test "plain run_asset/2 supports explicit public pipeline_context injection" do
    assert {:ok, run_id} =
             Favn.run_asset({SalesAssets, :sales_daily},
               dependencies: :none,
               pipeline_context: %{
                 id: :manual_context,
                 name: :manual_context,
                 trigger: %{kind: :manual, requested_by: :api},
                 schedule: :manual_schedule,
                 partition: :manual_partition,
                 source: :manual_source,
                 outputs: [:manual_output]
               }
             )

    assert {:ok, run} = Favn.await_run(run_id, timeout: 5_000)
    assert run.status == :ok
    assert run.pipeline.id == :manual_context
    assert run.pipeline.trigger == %{kind: :manual, requested_by: :api}
    assert run.pipeline.schedule == :manual_schedule
    assert run.pipeline.partition == :manual_partition
    assert run.pipeline.source == :manual_source
    assert run.pipeline.outputs == [:manual_output]

    [ctx | _] =
      CtxRecorder.all()
      |> Enum.filter(&(&1.current_ref == {SalesAssets, :sales_daily}))

    assert ctx.pipeline.id == :manual_context
    assert ctx.pipeline.trigger == %{kind: :manual, requested_by: :api}
  end

  test "run_pipeline/2 injects pipeline context into asset ctx" do
    assert {:ok, run_id} =
             Favn.run_pipeline(SimplePipeline,
               params: %{requested_by: "operator"},
               trigger: %{kind: :manual, requested_by: :user}
             )

    assert {:ok, run} = Favn.await_run(run_id, timeout: 5_000)
    assert run.status == :ok

    [ctx | _] =
      CtxRecorder.all()
      |> Enum.filter(&(&1.current_ref == {SalesAssets, :sales_daily}))

    assert ctx.pipeline.id == :daily_sales
    assert ctx.pipeline.config == %{timezone: "UTC", notify: false}
    assert ctx.pipeline.meta == %{owner: "data-platform", domain: :sales}
    assert ctx.pipeline.trigger == %{kind: :manual, requested_by: :user}
    assert ctx.pipeline.params == %{requested_by: "operator"}
    assert ctx.pipeline.runtime_window == nil
    assert ctx.pipeline.schedule == :daily_default
    assert ctx.pipeline.partition == :calendar_day
    assert ctx.pipeline.source == :snowflake_primary
    assert ctx.pipeline.outputs == [:warehouse_gold]
  end

  test "rerun_run/2 preserves pipeline context for pipeline-origin runs" do
    assert {:ok, run_id} =
             Favn.run_pipeline(SimplePipeline,
               params: %{requested_by: "operator"},
               trigger: %{kind: :manual, requested_by: :user}
             )

    assert {:ok, source_run} = Favn.await_run(run_id, timeout: 5_000)
    assert {:ok, rerun_id} = Favn.rerun_run(run_id)
    assert {:ok, rerun_run} = Favn.await_run(rerun_id, timeout: 5_000)

    assert rerun_run.submit_kind == :rerun
    assert rerun_run.replay_mode == :resume_from_failure
    assert rerun_run.rerun_of_run_id == run_id
    assert rerun_run.root_run_id == run_id
    assert rerun_run.pipeline == source_run.pipeline
    assert rerun_run.pipeline_context.trigger == %{kind: :manual, requested_by: :user}
    assert rerun_run.pipeline_context.params == %{requested_by: "operator"}
    assert rerun_run.pipeline_context.config == %{timezone: "UTC", notify: false}
    assert rerun_run.pipeline_context.meta == %{owner: "data-platform", domain: :sales}
  end

  test "pipeline DSL rejects mixing shorthand and select modes" do
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
  end

  test "pipeline DSL supports no-parens authoring style" do
    Code.compile_string("""
    defmodule NoParensPipeline do
      use Favn.Pipeline

      pipeline :no_parens do
        asset {#{inspect(SalesAssets)}, :sales_daily}
        deps :none
        schedule :daily_default
        partition :calendar_day
        source :snowflake_primary
        outputs [:warehouse_gold]
      end
    end
    """)

    assert {:ok, plan} = Favn.plan_pipeline(NoParensPipeline)
    assert plan.target_refs == [{SalesAssets, :sales_daily}]
  end

  test "plan_pipeline/2 errors for invalid module selector" do
    defmodule InvalidModulePipeline do
      use Favn.Pipeline

      pipeline :bad_module do
        select do
          module(Enum)
        end
      end
    end

    assert {:error, :not_asset_module} = Favn.plan_pipeline(InvalidModulePipeline)
  end

  test "pipeline DSL rejects invalid deps clause values" do
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
  end

  test "pipeline DSL rejects duplicate singleton clauses" do
    assert_raise ArgumentError, ~r/clause `schedule` can only be declared once/, fn ->
      Code.compile_string("""
      defmodule DuplicateSchedulePipeline do
        use Favn.Pipeline

        pipeline :dup_schedule do
          asset {#{inspect(SalesAssets)}, :sales_daily}
          schedule :daily
          schedule :hourly
        end
      end
      """)
    end

    assert_raise ArgumentError, ~r/clause `config` can only be declared once/, fn ->
      Code.compile_string("""
      defmodule DuplicateConfigPipeline do
        use Favn.Pipeline

        pipeline :dup_config do
          asset {#{inspect(SalesAssets)}, :sales_daily}
          config timezone: "UTC"
          config notify: true
        end
      end
      """)
    end

    assert_raise ArgumentError, ~r/clause `outputs` can only be declared once/, fn ->
      Code.compile_string("""
      defmodule DuplicateOutputsPipeline do
        use Favn.Pipeline

        pipeline :dup_outputs do
          asset {#{inspect(SalesAssets)}, :sales_daily}
          outputs [:warehouse_gold]
          outputs [:warehouse_backup]
        end
      end
      """)
    end
  end

  test "pipeline DSL rejects duplicate pipeline blocks in one module" do
    assert_raise ArgumentError, ~r/pipeline can only be declared once per module/, fn ->
      Code.compile_string("""
      defmodule DuplicatePipelineBlocks do
        use Favn.Pipeline

        pipeline :first do
          asset {#{inspect(SalesAssets)}, :sales_daily}
        end

        pipeline :second do
          asset {#{inspect(SalesAssets)}, :inventory_daily}
        end
      end
      """)
    end
  end

  test "pipeline DSL rejects modules with no pipeline block" do
    assert_raise ArgumentError, ~r/must define one `pipeline ... do` block/, fn ->
      Code.compile_string("""
      defmodule MissingPipelineBlock do
        use Favn.Pipeline
      end
      """)
    end
  end

  test "pipeline clauses must be declared inside pipeline block" do
    assert_raise ArgumentError, ~r/must be declared inside `pipeline ... do`/, fn ->
      Code.compile_string("""
      defmodule ClauseOutsidePipeline do
        use Favn.Pipeline

        asset {#{inspect(SalesAssets)}, :sales_daily}

        pipeline :ok do
          deps :none
        end
      end
      """)
    end
  end

  test "pipeline DSL rejects invalid schedule, partition, source, and outputs" do
    assert_raise ArgumentError, ~r/`schedule` must be an atom/, fn ->
      Code.compile_string("""
      defmodule InvalidSchedulePipeline do
        use Favn.Pipeline

        pipeline :invalid_schedule do
          asset {#{inspect(SalesAssets)}, :sales_daily}
          schedule "daily"
        end
      end
      """)
    end

    assert_raise ArgumentError, ~r/`partition` must be an atom/, fn ->
      Code.compile_string("""
      defmodule InvalidPartitionPipeline do
        use Favn.Pipeline

        pipeline :invalid_partition do
          asset {#{inspect(SalesAssets)}, :sales_daily}
          partition "calendar_day"
        end
      end
      """)
    end

    assert_raise ArgumentError, ~r/`source` must be an atom/, fn ->
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

    assert_raise ArgumentError, ~r/`outputs` must be a list of atoms/, fn ->
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
