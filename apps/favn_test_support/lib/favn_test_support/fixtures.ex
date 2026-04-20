defmodule FavnTestSupport.Fixtures do
  @moduledoc """
  Shared fixture metadata and file-compilation helpers.

  Fixture groups are intentionally symbolic (`:basic_assets`, `:graph_assets`,
  `:pipeline_assets`, `:runner_assets`) so owner apps can consume shared fixture
  source files without hard-coding file paths.
  """

  @type fixture_name :: :basic_assets | :graph_assets | :pipeline_assets | :runner_assets

  @fixtures %{
    basic_assets: %{
      path: "fixtures/assets/basic_assets.ex",
      modules: [
        Favn.Test.Fixtures.Assets.Basic.SampleAssets,
        Favn.Test.Fixtures.Assets.Basic.CrossModuleAssets,
        Favn.Test.Fixtures.Assets.Basic.SpoofedAssets,
        Favn.Test.Fixtures.Assets.Basic.AdditionalAssets
      ]
    },
    graph_assets: %{
      path: "fixtures/assets/graph_assets.ex",
      modules: [
        Favn.Test.Fixtures.Assets.Graph.SourceAssets,
        Favn.Test.Fixtures.Assets.Graph.WarehouseAssets,
        Favn.Test.Fixtures.Assets.Graph.ReportingAssets,
        Favn.Test.Fixtures.Assets.Graph.BronzeAssets,
        Favn.Test.Fixtures.Assets.Graph.SilverAssets,
        Favn.Test.Fixtures.Assets.Graph.GoldAssets
      ]
    },
    pipeline_assets: %{
      path: "fixtures/assets/pipeline_assets.ex",
      modules: [
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
    },
    runner_assets: %{
      path: "fixtures/assets/runner_assets.ex",
      modules: [
        Favn.Test.Fixtures.Assets.Runner.RunnerAssets,
        Favn.Test.Fixtures.Assets.Runner.TerminalFailingStore
      ]
    }
  }

  @spec fixture_path!(fixture_name()) :: String.t()
  def fixture_path!(name) do
    fixture = fetch_fixture!(name)
    priv_dir = :code.priv_dir(:favn_test_support)

    case priv_dir do
      dir when is_list(dir) -> Path.join(List.to_string(dir), fixture.path)
      _ -> raise "cannot resolve priv dir for :favn_test_support"
    end
  end

  @spec compile_fixture!(fixture_name()) :: :ok
  def compile_fixture!(name) do
    modules = modules!(name)

    case fixture_loaded_state(modules) do
      :loaded ->
        :ok

      :unloaded ->
        _ = Code.compile_file(fixture_path!(name))
        :ok

      :partial ->
        raise "fixture #{inspect(name)} is partially loaded; expected all modules loaded or none"
    end
  end

  @spec compile_fixtures!([fixture_name()]) :: :ok
  def compile_fixtures!(names) when is_list(names) do
    names
    |> Enum.uniq()
    |> Enum.each(&compile_fixture!/1)

    :ok
  end

  @spec modules!(fixture_name()) :: [module()]
  def modules!(name) do
    name
    |> fetch_fixture!()
    |> Map.fetch!(:modules)
  end

  defp fixture_loaded_state(modules) do
    loaded = Enum.map(modules, &module_loaded?/1)

    cond do
      Enum.all?(loaded) -> :loaded
      Enum.any?(loaded) -> :partial
      true -> :unloaded
    end
  end

  defp module_loaded?(module) do
    match?({:file, _path}, :code.is_loaded(module))
  end

  defp fetch_fixture!(name) do
    case Map.fetch(@fixtures, name) do
      {:ok, fixture} -> fixture
      :error -> raise ArgumentError, invalid_fixture_message(name)
    end
  end

  defp invalid_fixture_message(name) do
    available = @fixtures |> Map.keys() |> Enum.sort()
    "unknown fixture #{inspect(name)}; expected one of #{inspect(available)}"
  end
end
