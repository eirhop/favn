defmodule FavnTestSupport.FixturesTest do
  use ExUnit.Case, async: false

  alias FavnTestSupport.Fixtures

  @fixture_names [:basic_assets, :graph_assets, :pipeline_assets, :runner_assets]

  setup_all do
    created_stubs =
      []
      |> maybe_define_stub(Favn.Assets, """
      defmodule Favn.Assets do
        defmacro __using__(_opts) do
          quote do
            Module.register_attribute(__MODULE__, :asset, persist: false)
            Module.register_attribute(__MODULE__, :depends, accumulate: true)
            Module.register_attribute(__MODULE__, :freshness, accumulate: true)
            Module.register_attribute(__MODULE__, :meta, persist: false)
            Module.register_attribute(__MODULE__, :relation, accumulate: true)
            Module.register_attribute(__MODULE__, :window, accumulate: true)

            @on_definition Favn.Assets
          end
        end

        def __on_definition__(env, _kind, _name, _args, _guards, _body) do
          Module.delete_attribute(env.module, :asset)
          Module.delete_attribute(env.module, :depends)
          Module.delete_attribute(env.module, :freshness)
          Module.delete_attribute(env.module, :meta)
          Module.delete_attribute(env.module, :relation)
          Module.delete_attribute(env.module, :window)
        end
      end
      """)
      |> maybe_define_stub(Favn.Storage.Adapter, """
      defmodule Favn.Storage.Adapter do
        @callback child_spec(keyword()) :: any()
        @callback readiness(keyword()) :: any()
        @callback diagnostics(keyword()) :: any()
        @callback put_manifest_version(term(), keyword()) :: :ok | {:error, term()}
        @callback get_manifest_version(term(), keyword()) :: {:ok, term()} | {:error, term()}
        @callback get_manifest_version_by_content_hash(term(), keyword()) ::
                    {:ok, term()} | {:error, term()}
        @callback list_manifest_versions(keyword()) :: {:ok, list()} | {:error, term()}
        @callback set_active_manifest_version(term(), keyword()) :: :ok | {:error, term()}
        @callback get_active_manifest_version(keyword()) :: {:ok, term()} | {:error, term()}
        @callback put_run(term(), keyword()) :: :ok | {:error, term()}
        @callback get_run(term(), keyword()) :: {:ok, term()} | {:error, term()}
        @callback list_runs(keyword(), keyword()) :: {:ok, list()} | {:error, term()}
        @callback persist_run_transition(term(), term(), keyword()) :: :ok | {:error, term()}
        @callback append_run_event(term(), term(), keyword()) :: :ok | {:error, term()}
        @callback list_run_events(term(), keyword()) :: {:ok, list()} | {:error, term()}
        @callback list_global_run_events(keyword(), keyword()) :: {:ok, list()} | {:error, term()}
        @callback persist_log_entries(list(), keyword()) :: {:ok, list()} | {:error, term()}
        @callback list_logs(term(), keyword(), keyword()) :: {:ok, term()} | {:error, term()}
        @callback replay_logs_after(term(), term(), keyword(), keyword()) ::
                    {:ok, list()} | {:error, term()}
        @callback put_scheduler_state(term(), term(), keyword()) :: :ok | {:error, term()}
        @callback get_scheduler_state(term(), keyword()) :: {:ok, term()} | {:error, term()}
        @callback put_coverage_baseline(term(), keyword()) :: :ok | {:error, term()}
        @callback get_coverage_baseline(term(), keyword()) :: {:ok, term()} | {:error, term()}
        @callback list_coverage_baselines(keyword(), keyword()) :: {:ok, term()} | {:error, term()}
        @callback put_backfill_window(term(), keyword()) :: :ok | {:error, term()}
        @callback get_backfill_window(term(), term(), term(), keyword()) ::
                    {:ok, term()} | {:error, term()}
        @callback list_backfill_windows(keyword(), keyword()) :: {:ok, term()} | {:error, term()}
        @callback put_asset_window_state(term(), keyword()) :: :ok | {:error, term()}
        @callback get_asset_window_state(term(), term(), term(), keyword()) ::
                    {:ok, term()} | {:error, term()}
        @callback list_asset_window_states(keyword(), keyword()) :: {:ok, term()} | {:error, term()}
        @callback replace_backfill_read_models(term(), list(), list(), list(), keyword()) ::
                    :ok | {:error, term()}

        @optional_callbacks readiness: 1, diagnostics: 1
      end
      """)
      |> maybe_define_stub(FavnOrchestrator.Page, """
      defmodule FavnOrchestrator.Page do
        def from_fetched(items, opts) do
          %{
            items: items,
            pagination: %{
              limit: Keyword.fetch!(opts, :limit),
              offset: Keyword.fetch!(opts, :offset),
              total: length(items)
            }
          }
        end
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

  test "runner fixture group compiles and exposes callable behaviors" do
    assert :ok = Fixtures.compile_fixture!(:runner_assets)

    runner_assets = Module.concat(Favn.Test.Fixtures.Assets.Runner, RunnerAssets)
    terminal_failing_store = Module.concat(Favn.Test.Fixtures.Assets.Runner, TerminalFailingStore)

    assert {:ok, %{partition: "2026-01-01"}} =
             runner_assets.base(%{params: %{partition: "2026-01-01"}})

    assert :ok = runner_assets.transient_then_ok(%{attempt: 2})

    assert :ok =
             runner_assets.announce_source(%{params: %{notify_pid: self()}, run_id: "run-123"})

    assert_receive {:announced_run_id, "run-123"}

    assert :ok = terminal_failing_store.reset!()

    Enum.each(1..7, fn _step ->
      assert :ok = terminal_failing_store.put_run(%{}, [])
    end)

    assert {:error, :terminal_write_failed} = terminal_failing_store.put_run(%{}, [])
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
