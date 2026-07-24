defmodule Mix.Tasks.Favn.Build.Manifest do
  use Mix.Task

  @requirements ["app.config"]
  @shortdoc "Builds a manifest release aligned with a user-owned runner"

  @moduledoc """
  Builds `.favn/dist/manifest/<manifest_version_id>` for the immutable
  `--runner-release-id` selected by the user or CI system.
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
        IO.puts("runner release: #{result.required_runner_release_id}")
        IO.puts("dist: #{result.dist_dir}")

      {:error, reason} ->
        Mix.raise("manifest build failed: #{format_reason(reason)}")
    end
  end

  @spec parse_args([String.t()]) :: keyword()
  def parse_args(args) do
    opts =
      CLIArgs.parse_no_args!("favn.build.manifest", args,
        runner_release_id: :string,
        root_dir: :string
      )

    if present?(opts[:runner_release_id]) do
      opts
    else
      Mix.raise("missing required option(s): --runner-release-id")
    end
  end

  defp present?(value), do: is_binary(value) and value != ""
  defp format_reason(reason), do: inspect(reason)
end
