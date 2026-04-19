defmodule Favn.Dev.RunnerControl do
  @moduledoc false

  alias Favn.Dev.Paths
  alias Favn.Manifest.Version

  @spec register_manifest(Version.t(), keyword()) :: :ok | {:error, term()}
  def register_manifest(%Version{} = version, opts \\ []) when is_list(opts) do
    root_dir = Paths.root_dir(opts)
    mix = System.find_executable("mix") || "mix"

    eval =
      """
      manifest_json = System.get_env("FAVN_DEV_MANIFEST_JSON") || "{}"
      manifest_version_id = System.get_env("FAVN_DEV_MANIFEST_VERSION_ID") || ""
      {:ok, manifest} = JSON.decode(manifest_json)
      {:ok, version} = Favn.Manifest.Version.new(manifest, manifest_version_id: manifest_version_id)
      Application.ensure_all_started(:favn_runner)
      :ok = FavnRunner.register_manifest(version)
      """
      |> String.trim()

    env = [
      {"MIX_ENV", "dev"},
      {"FAVN_DEV_MANIFEST_JSON", JSON.encode!(version.manifest)},
      {"FAVN_DEV_MANIFEST_VERSION_ID", version.manifest_version_id}
    ]

    case System.cmd(mix, ["run", "--no-start", "--eval", eval],
           cd: Path.join(root_dir, "apps/favn_runner"),
           env: env,
           stderr_to_stdout: true
         ) do
      {_output, 0} -> :ok
      {output, status} -> {:error, {:runner_register_failed, status, output}}
    end
  end
end
