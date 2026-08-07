defmodule FavnLocal.PublicationTest do
  use ExUnit.Case, async: false

  alias FavnLocal.Publication

  defmodule DefaultAsset do
    use Favn.Asset

    def asset(_ctx), do: :ok
  end

  defmodule GPUAsset do
    use Favn.Asset

    runner_pool(:gpu)

    def asset(_ctx), do: :ok
  end

  defmodule CountingAssets do
    def __favn_assets__ do
      send(self(), {__MODULE__, :compiled})
      DefaultAsset.__favn_assets__() ++ GPUAsset.__favn_assets__()
    end
  end

  defmodule DuckDBPipeline do
    use Favn.Pipeline

    pipeline :daily do
      asset(DefaultAsset)
      runner_pool(:duckdb)
    end
  end

  setup do
    keys = [:asset_modules, :pipeline_modules, :schedule_modules, :connection_modules]
    previous = Map.new(keys, &{&1, Application.get_env(:favn, &1)})

    Application.put_env(:favn, :asset_modules, [CountingAssets])
    Application.put_env(:favn, :pipeline_modules, [DuckDBPipeline])
    Application.put_env(:favn, :schedule_modules, [])
    Application.put_env(:favn, :connection_modules, [])

    on_exit(fn ->
      Enum.each(previous, fn
        {key, nil} -> Application.delete_env(:favn, key)
        {key, value} -> Application.put_env(:favn, key, value)
      end)
    end)
  end

  test "build compiles once and binds the local release to every effective pool" do
    runner_release_id = FavnTestSupport.runner_release_id()

    assert {:ok, publication} = Publication.build(runner_release_id)

    assert_received {CountingAssets, :compiled}
    refute_received {CountingAssets, :compiled}

    assert publication.version.runner_releases == %{
             "default" => runner_release_id,
             "duckdb" => runner_release_id,
             "gpu" => runner_release_id
           }
  end

  test "build preserves an empty authored catalog" do
    Application.put_env(:favn, :asset_modules, [])
    Application.put_env(:favn, :pipeline_modules, [])

    assert {:ok, publication} = Publication.build(FavnTestSupport.runner_release_id())
    assert publication.version.runner_releases == %{}
  end
end
