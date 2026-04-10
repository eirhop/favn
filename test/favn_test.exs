defmodule FavnTest do
  use ExUnit.Case

  doctest Favn

  defmodule SampleAssets do
    use Favn.Assets

    @doc "Extract raw orders"
    @asset true
    def extract_orders(_ctx), do: :ok

    @doc "Normalize extracted orders"
    @asset true
    @depends :extract_orders
    @meta tags: [:sales]
    def normalize_orders(_ctx), do: :ok
  end

  defmodule CrossModuleAssets do
    use Favn.Assets

    alias FavnTest.SampleAssets

    @doc "Publish normalized orders"
    @asset true
    @depends {SampleAssets, :normalize_orders}
    @meta tags: [:reporting]
    def publish_orders(_ctx), do: :ok
  end

  defmodule SpoofedAssets do
    def __favn_assets__, do: :oops
  end

  defmodule AdditionalAssets do
    use Favn.Assets

    @doc "Archive published orders"
    @asset true
    @depends {CrossModuleAssets, :publish_orders}
    def archive_orders(_ctx), do: :ok
  end

  defmodule FacadeRawErrorStore do
    @behaviour Favn.Storage.Adapter

    @impl true
    def child_spec(_opts), do: :none

    @impl true
    def put_run(_run, _opts), do: {:error, :write_failed}

    @impl true
    def get_run(_run_id, _opts), do: {:error, :read_failed}

    @impl true
    def list_runs(_opts, _adapter_opts), do: {:error, :list_failed}

    @impl true
    def scheduler_child_spec(_opts), do: :none

    @impl true
    def put_scheduler_state(_state, _opts), do: :ok

    @impl true
    def get_scheduler_state(_pipeline_module, _opts), do: {:ok, nil}
  end

  require Logger

  alias Favn.Test.Fixtures.Assets.Basic.AdditionalAssets
  alias Favn.Test.Fixtures.Assets.Basic.CrossModuleAssets
  alias Favn.Test.Fixtures.Assets.Basic.SampleAssets
  alias Favn.Test.Fixtures.Assets.Basic.SpoofedAssets

  setup do
    state = Favn.TestSetup.capture_state()

    on_exit(fn ->
      Favn.TestSetup.restore_state(state)
    end)

    :ok
  end

  test "lists globally configured assets through the registry-backed facade" do
    :ok = Favn.TestSetup.setup_asset_modules([SampleAssets, CrossModuleAssets])

    assert {:ok, assets} = Favn.list_assets()

    assert Enum.map(assets, & &1.ref) == [
             {CrossModuleAssets, :publish_orders},
             {SampleAssets, :extract_orders},
             {SampleAssets, :normalize_orders}
           ]
  end

  test "list_assets/0 sorts globally discovered assets by canonical ref" do
    :ok = Favn.TestSetup.setup_asset_modules([SampleAssets, AdditionalAssets, CrossModuleAssets])

    assert {:ok, assets} = Favn.list_assets()

    assert Enum.map(assets, & &1.ref) == [
             {AdditionalAssets, :archive_orders},
             {CrossModuleAssets, :publish_orders},
             {SampleAssets, :extract_orders},
             {SampleAssets, :normalize_orders}
           ]
  end

  test "build_catalog preserves deterministic asset order across module merges" do
    assert {:ok, catalog} =
             Favn.Assets.Registry.build_catalog([
               SampleAssets,
               CrossModuleAssets,
               AdditionalAssets
             ])

    assert Enum.map(catalog.assets, & &1.ref) == [
             {SampleAssets, :extract_orders},
             {SampleAssets, :normalize_orders},
             {CrossModuleAssets, :publish_orders},
             {AdditionalAssets, :archive_orders}
           ]

    :ok = Favn.TestSetup.setup_asset_modules([SampleAssets, CrossModuleAssets, AdditionalAssets])

    assert {:ok, listed_assets} = Favn.list_assets()

    assert Enum.map(listed_assets, & &1.ref) == [
             {AdditionalAssets, :archive_orders},
             {CrossModuleAssets, :publish_orders},
             {SampleAssets, :extract_orders},
             {SampleAssets, :normalize_orders}
           ]
  end

  test "build_catalog tracks relation ownership and rejects duplicate owners" do
    owner_a = Module.concat(__MODULE__, "OwnerA#{System.unique_integer([:positive])}")
    owner_b = Module.concat(__MODULE__, "OwnerB#{System.unique_integer([:positive])}")

    Code.compile_string(
      """
      defmodule #{inspect(owner_a)} do
        use Favn.Namespace, connection: :warehouse, catalog: :raw, schema: :sales
        use Favn.Assets

        @asset true
        @produces true
        def orders(_ctx), do: :ok
      end
      """,
      "test/dynamic_assets_test.exs"
    )

    Code.compile_string(
      """
      defmodule #{inspect(owner_b)} do
        use Favn.Namespace, connection: :warehouse, catalog: :raw, schema: :sales
        use Favn.Assets

        @asset true
        @produces name: :orders
        def duplicate_orders(_ctx), do: :ok
      end
      """,
      "test/dynamic_assets_test.exs"
    )

    assert {:ok, catalog} = Favn.Assets.Registry.build_catalog([owner_a])

    assert catalog.relation_owners == %{
             %Favn.RelationRef{
               connection: :warehouse,
               catalog: "raw",
               schema: "sales",
               name: "orders"
             } => {owner_a, :orders}
           }

    assert {:error,
            {:duplicate_relation_owner,
             %Favn.RelationRef{
               name: "orders",
               schema: "sales",
               catalog: "raw",
               connection: :warehouse
             }, {^owner_a, :orders}, {^owner_b, :duplicate_orders}}} =
             Favn.Assets.Registry.build_catalog([owner_a, owner_b])
  end

  test "lists assets for a module through the public facade" do
    assert {:ok, assets} = Favn.list_assets(SampleAssets)

    assert Enum.map(assets, & &1.name) == [:extract_orders, :normalize_orders]
  end

  test "fetches an asset through the public facade" do
    :ok = Favn.TestSetup.setup_asset_modules([SampleAssets, CrossModuleAssets])

    assert {:ok, asset} = Favn.get_asset({SampleAssets, :normalize_orders})
    assert {:ok, cross_module_asset} = Favn.get_asset({CrossModuleAssets, :publish_orders})

    assert asset.depends_on == [{SampleAssets, :extract_orders}]
    assert cross_module_asset.depends_on == [{SampleAssets, :normalize_orders}]

    assert {:error, :asset_not_found} = Favn.get_asset({SampleAssets, :missing})
    assert {:error, :not_asset_module} = Favn.get_asset({Enum, :map})
  end

  test "inspects dependency queries through the public facade" do
    :ok =
      Favn.TestSetup.setup_asset_modules([SampleAssets, CrossModuleAssets], reload_graph?: true)

    assert {:ok, upstream_assets} = Favn.upstream_assets({CrossModuleAssets, :publish_orders})

    assert Enum.map(upstream_assets, & &1.ref) == [
             {SampleAssets, :extract_orders},
             {SampleAssets, :normalize_orders}
           ]

    assert {:ok, downstream_assets} =
             Favn.downstream_assets({SampleAssets, :extract_orders}, transitive: false)

    assert Enum.map(downstream_assets, & &1.ref) == [{SampleAssets, :normalize_orders}]

    assert {:ok, dependency_graph} =
             Favn.dependency_graph({CrossModuleAssets, :publish_orders}, tags: [:sales])

    assert Map.keys(dependency_graph.assets_by_ref) |> Enum.sort() == [
             {CrossModuleAssets, :publish_orders},
             {SampleAssets, :normalize_orders}
           ]
  end

  test "reports whether a module exposes Favn asset metadata" do
    assert Favn.asset_module?(SampleAssets)
    refute Favn.asset_module?(Enum)
    refute Favn.asset_module?(SpoofedAssets)
  end

  test "rejects modules that only spoof __favn_assets__/0" do
    assert {:error, :not_asset_module} = Favn.list_assets(SpoofedAssets)
    Application.put_env(:favn, :asset_modules, [SpoofedAssets])

    assert {:error, {:invalid_asset_module, SpoofedAssets}} = Favn.Assets.Registry.reload()
    assert {:error, :not_asset_module} = Favn.get_asset({SpoofedAssets, :normalize_orders})
  end

  test "reports invalid globally configured modules" do
    Application.put_env(:favn, :asset_modules, [SampleAssets, Enum])

    assert {:error, {:invalid_asset_module, Enum}} = Favn.Assets.Registry.reload()
  end

  test "reads from the startup-loaded cache until the registry is reloaded" do
    :ok = Favn.TestSetup.setup_asset_modules([SampleAssets])
    assert {:ok, assets} = Favn.list_assets()

    assert Enum.map(assets, & &1.ref) == [
             {SampleAssets, :extract_orders},
             {SampleAssets, :normalize_orders}
           ]

    Application.put_env(:favn, :asset_modules, [SampleAssets, CrossModuleAssets])

    assert {:ok, cached_assets} = Favn.list_assets()

    assert Enum.map(cached_assets, & &1.ref) == [
             {SampleAssets, :extract_orders},
             {SampleAssets, :normalize_orders}
           ]

    assert :ok = Favn.Assets.Registry.reload()
    assert {:ok, reloaded_assets} = Favn.list_assets()

    assert Enum.map(reloaded_assets, & &1.ref) == [
             {CrossModuleAssets, :publish_orders},
             {SampleAssets, :extract_orders},
             {SampleAssets, :normalize_orders}
           ]
  end

  test "public run facade returns canonical storage error contract" do
    Application.put_env(:favn, :storage_adapter, FacadeRawErrorStore)

    assert {:error, {:store_error, :read_failed}} = Favn.get_run("run-1")
    assert {:error, {:store_error, :list_failed}} = Favn.list_runs()
    assert {:error, :invalid_opts} = Favn.list_runs(status: :pending)
  end
end
