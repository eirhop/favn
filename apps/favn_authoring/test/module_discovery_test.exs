defmodule FavnAuthoring.ModuleDiscoveryTest do
  use ExUnit.Case, async: false

  alias Favn.ModuleDiscovery

  defmodule DiscoveryAsset do
    use Favn.Asset

    def asset(_ctx), do: :ok
  end

  defmodule ExplicitAsset do
    use Favn.Asset

    def asset(_ctx), do: :ok
  end

  defmodule DiscoveryPipeline do
    use Favn.Pipeline

    pipeline :daily do
      asset(DiscoveryAsset)
      deps(:all)
    end
  end

  defmodule DiscoverySchedules do
    use Favn.Triggers.Schedules

    schedule(:daily,
      cron: "0 2 * * *",
      timezone: "Etc/UTC"
    )
  end

  defmodule DiscoveryConnection do
    @behaviour Favn.Connection

    @impl true
    def definition do
      %Favn.Connection.Definition{
        name: :warehouse,
        adapter: __MODULE__.Adapter,
        config_schema: []
      }
    end
  end

  defmodule PlainModule do
  end

  defmodule InvalidAssetModule do
    def __favn_assets__, do: :invalid
  end

  setup do
    keys = [:asset_modules, :pipeline_modules, :schedule_modules, :discovery]
    previous = Map.new(keys, &{&1, Application.get_env(:favn, &1, :__missing__)})

    on_exit(fn ->
      Enum.each(previous, fn
        {key, :__missing__} -> Application.delete_env(:favn, key)
        {key, value} -> Application.put_env(:favn, key, value)
      end)
    end)
  end

  test "discovers assets, pipelines, schedules, and connections from an OTP app" do
    app =
      load_test_app!([DiscoveryAsset, DiscoveryPipeline, DiscoverySchedules, DiscoveryConnection])

    config = [apps: [app]]

    assert {:ok, [DiscoveryAsset]} = ModuleDiscovery.discover(:assets, config)
    assert {:ok, [DiscoveryPipeline]} = ModuleDiscovery.discover(:pipelines, config)
    assert {:ok, [DiscoverySchedules]} = ModuleDiscovery.discover(:schedules, config)
    assert {:ok, [DiscoveryConnection]} = ModuleDiscovery.discover(:connections, config)
  end

  test "invalid authored asset modules fail discovery instead of being skipped" do
    app = load_test_app!([InvalidAssetModule, PlainModule])

    assert {:error, {:asset_discovery_failed, InvalidAssetModule, _reason}} =
             ModuleDiscovery.discover(:assets, apps: [app])
  end

  test "discovers Mix app modules from BEAM artifacts when .app metadata is missing" do
    unique = System.unique_integer([:positive])
    app = String.to_atom("favn_discovery_missing_app_#{unique}")
    module = Module.concat([FavnDiscoveryMissingApp, "Connection#{unique}"])
    ebin_path = Path.join([Mix.Project.build_path(), "lib", Atom.to_string(app), "ebin"])

    File.rm_rf!(ebin_path)
    File.mkdir_p!(ebin_path)

    on_exit(fn ->
      Code.delete_path(ebin_path)
      File.rm_rf!(ebin_path)
    end)

    [{^module, beam}] =
      Code.compile_string("""
      defmodule #{inspect(module)} do
        @behaviour Favn.Connection

        @impl true
        def definition do
          %Favn.Connection.Definition{
            name: :fallback_warehouse,
            adapter: __MODULE__.Adapter,
            config_schema: []
          }
        end
      end
      """)

    File.write!(Path.join(ebin_path, "#{module}.beam"), beam)

    :code.purge(module)
    :code.delete(module)
    refute Code.loaded?(module)

    assert {:ok, [^module]} = ModuleDiscovery.discover(:connections, apps: [app])
    assert Code.loaded?(module)
  end

  test "explicit manifest module inputs override discovery" do
    app = load_test_app!([DiscoveryAsset, DiscoveryPipeline])
    Application.put_env(:favn, :discovery, apps: [app], assets: :all, pipelines: :all)

    assert {:ok, manifest} =
             FavnAuthoring.generate_manifest(
               asset_modules: [ExplicitAsset],
               pipeline_modules: [],
               runner_release: FavnTestSupport.runner_release()
             )

    assert Enum.map(manifest.assets, & &1.ref) == [{ExplicitAsset, :asset}]
    assert manifest.pipelines == []
  end

  test "schedules are included when schedules discovery is enabled" do
    app = load_test_app!([DiscoveryAsset, DiscoveryPipeline, DiscoverySchedules])

    Application.put_env(:favn, :discovery,
      apps: [app],
      assets: :all,
      pipelines: :all,
      schedules: :all
    )

    assert {:ok, manifest} =
             FavnAuthoring.generate_manifest(runner_release: FavnTestSupport.runner_release())

    assert Enum.map(manifest.schedules, &{&1.module, &1.name}) == [{DiscoverySchedules, :daily}]
  end

  defp load_test_app!(modules) do
    app = String.to_atom("favn_discovery_test_#{System.unique_integer([:positive])}")

    :ok =
      :application.load(
        {:application, app,
         [
           description: ~c"Favn discovery test app",
           vsn: ~c"1",
           modules: modules,
           registered: [],
           applications: [:kernel, :stdlib]
         ]}
      )

    on_exit(fn -> :application.unload(app) end)

    app
  end
end
