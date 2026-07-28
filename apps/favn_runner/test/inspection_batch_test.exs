defmodule FavnRunner.InspectionBatchTest do
  use ExUnit.Case, async: false

  alias Favn.Contracts.RelationInspectionRequest
  alias Favn.Manifest
  alias Favn.Manifest.Version

  defmodule CountingManifestStore do
    use GenServer

    def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

    @impl true
    def init(opts), do: {:ok, Map.new(opts)}

    @impl true
    def handle_call({:fetch, manifest_version_id, content_hash}, _from, state) do
      send(state.test_pid, {:manifest_fetch, manifest_version_id, content_hash})

      result =
        case Map.fetch(state.versions, {manifest_version_id, content_hash}) do
          {:ok, version} -> {:ok, version}
          :error -> {:error, :manifest_not_found}
        end

      {:reply, result, state}
    end
  end

  test "fetches each manifest identity once and preserves ordered identity errors" do
    release_id = FavnTestSupport.runner_release_id()
    version = version("mv_cached", String.duplicate("a", 64), release_id)

    store =
      start_supervised!(
        {CountingManifestStore,
         test_pid: self(),
         versions: %{{version.manifest_version_id, version.content_hash} => version}}
      )

    cached_request = request(version.manifest_version_id, version.content_hash, release_id)
    missing_hash = String.duplicate("b", 64)
    missing_request = request("mv_missing", missing_hash, release_id)

    assert {:ok,
            [
              {:error, :asset_not_found},
              {:error, :asset_not_found},
              {:error, :manifest_not_found},
              {:error, :manifest_not_found}
            ]} =
             FavnRunner.inspect_relations(
               [cached_request, cached_request, missing_request, missing_request],
               manifest_store: store
             )

    assert_received {:manifest_fetch, "mv_cached", version_hash}
    assert version_hash == version.content_hash
    assert_received {:manifest_fetch, "mv_missing", ^missing_hash}
    refute_received {:manifest_fetch, _manifest_version_id, _content_hash}
  end

  defp version(manifest_version_id, content_hash, release_id) do
    %Version{
      manifest_version_id: manifest_version_id,
      content_hash: content_hash,
      required_runner_release_id: release_id,
      manifest: %Manifest{assets: []}
    }
  end

  defp request(manifest_version_id, content_hash, release_id) do
    %RelationInspectionRequest{
      manifest_version_id: manifest_version_id,
      manifest_content_hash: content_hash,
      required_runner_release_id: release_id,
      asset_ref: {__MODULE__, :missing},
      include: [:columns]
    }
  end
end
