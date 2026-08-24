defmodule Mix.Tasks.Favn.Build.Manifest do
  use Mix.Task

  @requirements ["app.config"]
  @shortdoc "Builds a manifest release aligned with a user-owned runner"

  @moduledoc """
  Builds `.favn/dist/manifest/<manifest_version_id>.tar.gz` for the immutable
  repeated `--runner-release pool=rr_...` mappings selected by the user or CI.
  """

  alias FavnAuthoring.Deployment.ManifestBuilder
  alias Mix.Tasks.Favn.CLIArgs

  @impl Mix.Task
  def run(args) do
    opts = parse_args(args)

    if Mix.env() == :prod do
      run_build(opts)
    else
      Mix.raise("manifest builds require MIX_ENV=prod")
    end
  end

  @doc false
  def run_build(opts) when is_list(opts) do
    case ManifestBuilder.run(opts) do
      {:ok, result} ->
        IO.puts("Favn manifest build complete")
        IO.puts("manifest version: #{result.manifest_version_id}")
        IO.puts("runner releases: #{inspect(result.runner_releases)}")
        IO.puts("archive: #{result.archive_path}")
        IO.puts("archive sha256: #{result.archive_sha256}")
        IO.puts("verified directory: #{result.dist_dir}")

      {:error, reason} ->
        Mix.raise("manifest build failed: #{format_reason(reason)}")
    end
  end

  @spec parse_args([String.t()]) :: keyword()
  def parse_args(args) do
    opts =
      CLIArgs.parse_no_args!("favn.build.manifest", args,
        runner_release: :keep,
        root_dir: :string
      )

    releases = Keyword.get_values(opts, :runner_release)

    if releases == [] do
      Mix.raise("missing required option(s): --runner-release pool=rr_...")
    end

    opts
    |> Keyword.delete(:runner_release)
    |> Keyword.put(:runner_releases, parse_runner_releases!(releases))
  end

  defp parse_runner_releases!(entries) do
    Map.new(entries, fn entry ->
      case String.split(entry, "=", parts: 2) do
        [pool, release_id] when pool != "" and release_id != "" -> {pool, release_id}
        _other -> Mix.raise("invalid --runner-release #{inspect(entry)}; expected pool=rr_...")
      end
    end)
  end

  defp format_reason(reason), do: inspect(reason)
end
