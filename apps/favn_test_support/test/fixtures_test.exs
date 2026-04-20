defmodule FavnTestSupport.FixturesTest do
  use ExUnit.Case, async: false

  alias FavnTestSupport.Fixtures

  @fixture_names [:basic_assets, :graph_assets, :pipeline_assets, :runner_assets]

  setup_all do
    created_stubs =
      []
      |> maybe_define_stub(Favn.Assets, """
      defmodule Favn.Assets do
        defmacro __using__(_opts), do: quote(do: nil)
      end
      """)

    on_exit(fn ->
      purge_modules(Enum.reverse(created_stubs))
    end)

    :ok
  end

  setup do
    purge_modules(all_fixture_modules())

    on_exit(fn ->
      purge_modules(all_fixture_modules())
    end)

    :ok
  end

  test "fixture_path!/1 resolves fixture files" do
    path = Fixtures.fixture_path!(:basic_assets)

    assert String.ends_with?(path, "priv/fixtures/assets/basic_assets.ex")
    assert File.regular?(path)
  end

  test "modules!/1 returns deterministic module mapping" do
    assert Fixtures.modules!(:basic_assets) == [
             Favn.Test.Fixtures.Assets.Basic.SampleAssets,
             Favn.Test.Fixtures.Assets.Basic.CrossModuleAssets,
             Favn.Test.Fixtures.Assets.Basic.SpoofedAssets,
             Favn.Test.Fixtures.Assets.Basic.AdditionalAssets
           ]

    assert Fixtures.modules!(:graph_assets) == [
             Favn.Test.Fixtures.Assets.Graph.SourceAssets,
             Favn.Test.Fixtures.Assets.Graph.WarehouseAssets,
             Favn.Test.Fixtures.Assets.Graph.ReportingAssets,
             Favn.Test.Fixtures.Assets.Graph.BronzeAssets,
             Favn.Test.Fixtures.Assets.Graph.SilverAssets,
             Favn.Test.Fixtures.Assets.Graph.GoldAssets
           ]

    assert Fixtures.modules!(:pipeline_assets) == [
             Favn.Test.Fixtures.Assets.Pipeline.CtxRecorder,
             Favn.Test.Fixtures.Assets.Pipeline.SalesAssets,
             Favn.Test.Fixtures.Assets.Pipeline.ReportingAssets,
             Favn.Test.Fixtures.Pipelines.SimplePipeline,
             Favn.Test.Fixtures.Triggers.Schedules,
             Favn.Test.Fixtures.Pipelines.SelectPipeline,
             Favn.Test.Fixtures.Pipelines.AssetsShorthandPipeline,
             Favn.Test.Fixtures.Pipelines.RunnerFailingPipeline,
             Favn.Test.Fixtures.Pipelines.RunnerRetryPipeline,
             Favn.Test.Fixtures.Pipelines.RunnerSlowPipeline,
             Favn.Test.Fixtures.Pipelines.SchedulerDailyPipeline,
             Favn.Test.Fixtures.Pipelines.SchedulerInactivePipeline,
             Favn.Test.Fixtures.Pipelines.SchedulerMissedSkipPipeline,
             Favn.Test.Fixtures.Pipelines.SchedulerMissedOnePipeline,
             Favn.Test.Fixtures.Pipelines.SchedulerMissedAllPipeline
           ]

    assert Fixtures.modules!(:runner_assets) == [
             Favn.Test.Fixtures.Assets.Runner.RunnerAssets,
             Favn.Test.Fixtures.Assets.Runner.TerminalFailingStore
           ]
  end

  test "compile_fixture!/1 compiles a single fixture" do
    assert :ok = Fixtures.compile_fixture!(:basic_assets)

    assert Enum.all?(Fixtures.modules!(:basic_assets), &Code.ensure_loaded?/1)
  end

  test "compile_fixtures!/1 compiles fixtures in caller-provided deterministic order" do
    assert :ok = Fixtures.compile_fixtures!([:graph_assets, :basic_assets, :graph_assets])

    assert Enum.all?(Fixtures.modules!(:graph_assets), &Code.ensure_loaded?/1)
    assert Enum.all?(Fixtures.modules!(:basic_assets), &Code.ensure_loaded?/1)
  end

  test "fixture metadata access does not require runtime app startup" do
    before_apps = Application.started_applications() |> Enum.map(&elem(&1, 0)) |> MapSet.new()

    _ = Fixtures.fixture_path!(:runner_assets)
    _ = Fixtures.modules!(:runner_assets)

    after_apps = Application.started_applications() |> Enum.map(&elem(&1, 0)) |> MapSet.new()

    assert before_apps == after_apps
  end

  test "compiled fixture modules are consumable by callers" do
    assert :ok = Fixtures.compile_fixture!(:basic_assets)

    sample_assets = Module.concat(Favn.Test.Fixtures.Assets.Basic, SampleAssets)
    spoofed_assets = Module.concat(Favn.Test.Fixtures.Assets.Basic, SpoofedAssets)

    assert :ok = sample_assets.extract_orders(%{})
    assert :oops = spoofed_assets.__favn_assets__()
  end

  defp maybe_define_stub(modules, module, definition) do
    if Code.ensure_loaded?(module) do
      modules
    else
      Code.compile_string(definition)
      [module | modules]
    end
  end

  defp all_fixture_modules do
    @fixture_names
    |> Enum.flat_map(&Fixtures.modules!/1)
    |> Enum.uniq()
  end

  defp purge_modules(modules) do
    Enum.each(modules, fn module ->
      :code.purge(module)
      :code.delete(module)
    end)
  end
end
