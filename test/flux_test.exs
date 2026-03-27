defmodule FluxTest do
  use ExUnit.Case

  doctest Flux

  defmodule SampleAssets do
    use Flux.Assets

    @doc "Extract raw orders"
    @asset true
    def extract_orders(_ctx, _deps), do: {:ok, %Flux.Asset.Output{output: [%{id: 1}]}}

    @doc "Normalize extracted orders"
    @asset depends_on: [:extract_orders], tags: [:sales]
    def normalize_orders(_ctx, deps),
      do: {:ok, %Flux.Asset.Output{output: Map.fetch!(deps, {__MODULE__, :extract_orders})}}
  end

  defmodule CrossModuleAssets do
    use Flux.Assets

    alias FluxTest.SampleAssets

    @doc "Publish normalized orders"
    @asset depends_on: [{SampleAssets, :normalize_orders}], tags: [:reporting]
    def publish_orders(_ctx, deps),
      do: {:ok, %Flux.Asset.Output{output: Map.fetch!(deps, {SampleAssets, :normalize_orders})}}
  end

  defmodule SpoofedAssets do
    def __flux_assets__, do: :oops
  end

  defmodule AdditionalAssets do
    use Flux.Assets

    @doc "Archive published orders"
    @asset depends_on: [{CrossModuleAssets, :publish_orders}]
    def archive_orders(_ctx, deps),
      do:
        {:ok, %Flux.Asset.Output{output: Map.fetch!(deps, {CrossModuleAssets, :publish_orders})}}
  end

  defmodule FacadeRawErrorStore do
    @behaviour Flux.Storage.Adapter

    @impl true
    def child_spec(_opts), do: :none

    @impl true
    def put_run(_run, _opts), do: {:error, :write_failed}

    @impl true
    def get_run(_run_id, _opts), do: {:error, :read_failed}

    @impl true
    def list_runs(_opts, _adapter_opts), do: {:error, :list_failed}
  end

  require Logger

  alias Flux.Test.Fixtures.Assets.Basic.AdditionalAssets
  alias Flux.Test.Fixtures.Assets.Basic.CrossModuleAssets
  alias Flux.Test.Fixtures.Assets.Basic.SampleAssets
  alias Flux.Test.Fixtures.Assets.Basic.SpoofedAssets

  setup do
    state = Flux.TestSetup.capture_state()

    on_exit(fn ->
      Flux.TestSetup.restore_state(state)
    end)

    :ok
  end

  test "lists globally configured assets through the registry-backed facade" do
    :ok = Flux.TestSetup.setup_asset_modules([SampleAssets, CrossModuleAssets])

    assert {:ok, assets} = Flux.list_assets()

    assert Enum.map(assets, & &1.ref) == [
             {CrossModuleAssets, :publish_orders},
             {SampleAssets, :extract_orders},
             {SampleAssets, :normalize_orders}
           ]
  end

  test "list_assets/0 sorts globally discovered assets by canonical ref" do
    :ok = Flux.TestSetup.setup_asset_modules([SampleAssets, AdditionalAssets, CrossModuleAssets])

    assert {:ok, assets} = Flux.list_assets()

    assert Enum.map(assets, & &1.ref) == [
             {AdditionalAssets, :archive_orders},
             {CrossModuleAssets, :publish_orders},
             {SampleAssets, :extract_orders},
             {SampleAssets, :normalize_orders}
           ]
  end

  test "build_catalog preserves deterministic asset order across module merges" do
    assert {:ok, catalog} =
             Flux.Registry.build_catalog([SampleAssets, CrossModuleAssets, AdditionalAssets])

    assert Enum.map(catalog.assets, & &1.ref) == [
             {SampleAssets, :extract_orders},
             {SampleAssets, :normalize_orders},
             {CrossModuleAssets, :publish_orders},
             {AdditionalAssets, :archive_orders}
           ]

    :ok = Flux.TestSetup.setup_asset_modules([SampleAssets, CrossModuleAssets, AdditionalAssets])

    assert {:ok, listed_assets} = Flux.list_assets()

    assert Enum.map(listed_assets, & &1.ref) == [
             {AdditionalAssets, :archive_orders},
             {CrossModuleAssets, :publish_orders},
             {SampleAssets, :extract_orders},
             {SampleAssets, :normalize_orders}
           ]
  end

  test "lists assets for a module through the public facade" do
    assert {:ok, assets} = Flux.list_assets(SampleAssets)

    assert Enum.map(assets, & &1.name) == [:extract_orders, :normalize_orders]
  end

  test "fetches an asset through the public facade" do
    :ok = Flux.TestSetup.setup_asset_modules([SampleAssets, CrossModuleAssets])

    assert {:ok, asset} = Flux.get_asset({SampleAssets, :normalize_orders})
    assert {:ok, cross_module_asset} = Flux.get_asset({CrossModuleAssets, :publish_orders})

    assert asset.depends_on == [{SampleAssets, :extract_orders}]
    assert cross_module_asset.depends_on == [{SampleAssets, :normalize_orders}]

    assert {:error, :asset_not_found} = Flux.get_asset({SampleAssets, :missing})
    assert {:error, :not_asset_module} = Flux.get_asset({Enum, :map})
  end

  test "inspects dependency queries through the public facade" do
    :ok =
      Flux.TestSetup.setup_asset_modules([SampleAssets, CrossModuleAssets], reload_graph?: true)

    assert {:ok, upstream_assets} = Flux.upstream_assets({CrossModuleAssets, :publish_orders})

    assert Enum.map(upstream_assets, & &1.ref) == [
             {SampleAssets, :extract_orders},
             {SampleAssets, :normalize_orders}
           ]

    assert {:ok, downstream_assets} =
             Flux.downstream_assets({SampleAssets, :extract_orders}, transitive: false)

    assert Enum.map(downstream_assets, & &1.ref) == [{SampleAssets, :normalize_orders}]

    assert {:ok, dependency_graph} =
             Flux.dependency_graph({CrossModuleAssets, :publish_orders}, tags: [:sales])

    assert Map.keys(dependency_graph.assets_by_ref) |> Enum.sort() == [
             {CrossModuleAssets, :publish_orders},
             {SampleAssets, :normalize_orders}
           ]
  end

  test "reports whether a module exposes Flux asset metadata" do
    assert Flux.asset_module?(SampleAssets)
    refute Flux.asset_module?(Enum)
    refute Flux.asset_module?(SpoofedAssets)
  end

  test "rejects modules that only spoof __flux_assets__/0" do
    assert {:error, :not_asset_module} = Flux.list_assets(SpoofedAssets)
    Application.put_env(:flux, :asset_modules, [SpoofedAssets])

    assert {:error, {:invalid_asset_module, SpoofedAssets}} = Flux.Registry.reload()
    assert {:error, :not_asset_module} = Flux.get_asset({SpoofedAssets, :normalize_orders})
  end

  test "reports invalid globally configured modules" do
    Application.put_env(:flux, :asset_modules, [SampleAssets, Enum])

    assert {:error, {:invalid_asset_module, Enum}} = Flux.Registry.reload()
  end

  test "reads from the startup-loaded cache until the registry is reloaded" do
    :ok = Flux.TestSetup.setup_asset_modules([SampleAssets])
    assert {:ok, assets} = Flux.list_assets()

    assert Enum.map(assets, & &1.ref) == [
             {SampleAssets, :extract_orders},
             {SampleAssets, :normalize_orders}
           ]

    Application.put_env(:flux, :asset_modules, [SampleAssets, CrossModuleAssets])

    assert {:ok, cached_assets} = Flux.list_assets()

    assert Enum.map(cached_assets, & &1.ref) == [
             {SampleAssets, :extract_orders},
             {SampleAssets, :normalize_orders}
           ]

    assert :ok = Flux.Registry.reload()
    assert {:ok, reloaded_assets} = Flux.list_assets()

    assert Enum.map(reloaded_assets, & &1.ref) == [
             {CrossModuleAssets, :publish_orders},
             {SampleAssets, :extract_orders},
             {SampleAssets, :normalize_orders}
           ]
  end

  test "public run facade returns canonical storage error contract" do
    Application.put_env(:flux, :storage_adapter, FacadeRawErrorStore)

    assert {:error, {:store_error, :read_failed}} = Flux.get_run("run-1")
    assert {:error, {:store_error, :list_failed}} = Flux.list_runs()
    assert {:error, :invalid_opts} = Flux.list_runs(status: :pending)
  end
end
