defmodule Favn.RuntimeFacadeTest do
  use ExUnit.Case, async: false

  defmodule TestSchedules do
    use Favn.Triggers.Schedules

    schedule(:daily, cron: "0 2 * * *", timezone: "Etc/UTC")
  end

  defmodule RawAsset do
    use Favn.Asset

    def asset(_ctx), do: :ok
  end

  defmodule GoldAsset do
    use Favn.Asset

    @depends RawAsset
    def asset(_ctx), do: :ok
  end

  defmodule DailyPipeline do
    use Favn.Pipeline

    pipeline :daily do
      select do
        module(GoldAsset)
      end

      deps(:all)
      schedule({TestSchedules, :daily})
    end
  end

  setup do
    previous_assets = Application.get_env(:favn, :asset_modules)
    Application.put_env(:favn, :asset_modules, [RawAsset, GoldAsset])

    on_exit(fn ->
      if is_nil(previous_assets) do
        Application.delete_env(:favn, :asset_modules)
      else
        Application.put_env(:favn, :asset_modules, previous_assets)
      end
    end)

    :ok
  end

  test "runtime helpers return runtime_not_available when orchestrator runtime is not started" do
    started_before? = started?(:favn_orchestrator)

    _ = Application.stop(:favn_orchestrator)

    on_exit(fn ->
      if started_before? do
        _ = Application.ensure_all_started(:favn_orchestrator)
      end
    end)

    assert {:error, :runtime_not_available} = Favn.get_run("run_missing")
    assert {:error, :runtime_not_available} = Favn.list_runs()
    assert {:error, :runtime_not_available} = Favn.rerun("run_missing")
    assert {:error, :runtime_not_available} = Favn.cancel_run("run_missing")
    assert {:error, :runtime_not_available} = Favn.run_pipeline(DailyPipeline)
    assert {:error, :runtime_not_available} = Favn.reload_scheduler()
    assert {:error, :runtime_not_available} = Favn.tick_scheduler()
    assert {:error, :runtime_not_available} = Favn.list_scheduled_pipelines()
  end

  test "runtime helpers delegate to orchestrator when runtime is started" do
    started_before? = started?(:favn_orchestrator)

    case Application.ensure_all_started(:favn_orchestrator) do
      {:ok, _apps} ->
        :ok = reset_memory_storage!()

        on_exit(fn ->
          :ok = reset_memory_storage!()

          if not started_before? do
            _ = Application.stop(:favn_orchestrator)
          end
        end)

        assert {:ok, []} = Favn.list_runs()
        assert {:error, :not_found} = Favn.get_run("run_missing")
        assert {:error, :not_found} = Favn.rerun("run_missing")
        assert {:error, :not_found} = Favn.cancel_run("run_missing")
        assert {:error, :active_manifest_not_set} = Favn.run_pipeline(DailyPipeline)
        assert {:error, :runtime_not_available} = Favn.reload_scheduler()
        assert {:error, :runtime_not_available} = Favn.tick_scheduler()
        assert {:error, :runtime_not_available} = Favn.list_scheduled_pipelines()

      {:error, {:favn_orchestrator, _reason}} ->
        assert {:error, :runtime_not_available} = Favn.get_run("run_missing")
        assert {:error, :runtime_not_available} = Favn.list_runs()
        assert {:error, :runtime_not_available} = Favn.rerun("run_missing")
        assert {:error, :runtime_not_available} = Favn.cancel_run("run_missing")
    end
  end

  defp started?(app) do
    Enum.any?(Application.started_applications(), fn {started_app, _desc, _version} ->
      started_app == app
    end)
  end

  defp reset_memory_storage! do
    memory_module = Module.concat([FavnOrchestrator, Storage, Adapter, Memory])

    if function_exported?(memory_module, :reset, 0) do
      memory_module.reset()
    else
      :ok
    end
  end
end
