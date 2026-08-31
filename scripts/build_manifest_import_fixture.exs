Code.require_file(
  "../apps/favn_core/test/support/manifest_scalability_fixture.exs",
  __DIR__
)

defmodule Favn.ManifestImportFixture do
  @moduledoc false

  alias Favn.Manifest.{Publication, Version}
  alias FavnAuthoring.Deployment.{ManifestArchive, ManifestBuilder}
  alias FavnTestSupport.ManifestScalabilityFixture

  def run([asset_count, output_path, runner_release_id]) do
    count = parse_count!(asset_count)
    output_path = Path.expand(output_path)
    bundle_path = output_path <> ".bundle"

    File.rm_rf!(bundle_path)
    File.rm_rf!(output_path)

    {manifest, packages} =
      ManifestScalabilityFixture.build_with_packages(count,
        runner_release_id: runner_release_id
      )
    {:ok, version} = Version.new(manifest)
    {:ok, publication} = Publication.from_parts(version, packages)
    :ok = ManifestBuilder.write_bundle(bundle_path, publication)
    {:ok, archive} = ManifestArchive.write(bundle_path, output_path)

    File.rm_rf!(bundle_path)

    IO.puts(
      Jason.encode!(%{
        archive_path: output_path,
        archive_sha256: archive.sha256,
        asset_count: count,
        manifest_version_id: version.manifest_version_id,
        compressed_bytes: File.stat!(output_path).size
      })
    )
  end

  def run(_args) do
    raise ArgumentError,
          "usage: MIX_ENV=test mix run scripts/build_manifest_import_fixture.exs ASSET_COUNT OUTPUT_PATH RUNNER_RELEASE_ID"
  end

  defp parse_count!(value) do
    case Integer.parse(value) do
      {count, ""} when count in 1..10_000 -> count
      _invalid -> raise ArgumentError, "ASSET_COUNT must be an integer in 1..10000"
    end
  end
end

Favn.ManifestImportFixture.run(System.argv())
