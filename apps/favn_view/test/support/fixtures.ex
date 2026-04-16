defmodule FavnView.TestFixtures do
  @moduledoc false

  alias Favn.Manifest
  alias Favn.Manifest.Version
  alias FavnOrchestrator
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage
  alias FavnOrchestrator.Storage.Adapter.Memory

  @spec setup_orchestrator!() :: Favn.Manifest.Version.t()
  def setup_orchestrator! do
    Memory.reset()

    version = manifest_version("mv_view_#{System.unique_integer([:positive])}")

    :ok = FavnOrchestrator.register_manifest(version)
    :ok = FavnOrchestrator.activate_manifest(version.manifest_version_id)
    version
  end

  @spec manifest_version(String.t()) :: Version.t()
  def manifest_version(manifest_version_id) do
    manifest =
      %Manifest{
        assets: [
          %Favn.Manifest.Asset{
            ref: {MyApp.Assets.Raw, :asset},
            module: MyApp.Assets.Raw,
            name: :asset
          },
          %Favn.Manifest.Asset{
            ref: {MyApp.Assets.Gold, :asset},
            module: MyApp.Assets.Gold,
            name: :asset,
            depends_on: [{MyApp.Assets.Raw, :asset}]
          }
        ],
        pipelines: [
          %Favn.Manifest.Pipeline{
            module: MyApp.Pipelines.Daily,
            name: :daily,
            selectors: [{:asset, {MyApp.Assets.Gold, :asset}}],
            deps: :all,
            schedule: nil,
            metadata: %{}
          }
        ]
      }

    {:ok, version} = Version.new(manifest, manifest_version_id: manifest_version_id)
    version
  end

  @spec insert_running_run!(Version.t()) :: String.t()
  def insert_running_run!(%Version{} = version) do
    run_id = "run_view_running_#{System.unique_integer([:positive])}"

    pending =
      RunState.new(
        id: run_id,
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        asset_ref: {MyApp.Assets.Gold, :asset}
      )

    running = RunState.transition(pending, status: :running)

    :ok =
      Storage.persist_run_transition(pending, %{
        sequence: pending.event_seq,
        event_type: :run_created,
        occurred_at: DateTime.utc_now(),
        status: pending.status,
        data: %{}
      })

    :ok =
      Storage.persist_run_transition(running, %{
        sequence: running.event_seq,
        event_type: :run_started,
        occurred_at: DateTime.utc_now(),
        status: running.status,
        data: %{}
      })

    run_id
  end
end
