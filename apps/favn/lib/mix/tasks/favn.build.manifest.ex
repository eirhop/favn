defmodule Mix.Tasks.Favn.Build.Manifest do
  use Mix.Task

  @requirements ["app.config"]
  @shortdoc "Builds a manifest release aligned with a runner descriptor"

  @moduledoc """
  Builds `.favn/dist/manifest/<manifest_version_id>` after proving that the
  current executable fingerprint exactly matches `--runner-release`.
  """

  alias Favn.Dev
  alias Mix.Tasks.Favn.CLIArgs
  alias Mix.Tasks.Favn.ProductionBuild

  @impl Mix.Task
  def run(args) do
    opts = parse_args(args)

    ProductionBuild.run("favn.build.manifest", args, fn -> run_build(opts) end)
  end

  @doc false
  def run_build(opts) when is_list(opts) do
    case Dev.build_manifest(opts) do
      {:ok, result} ->
        IO.puts("Favn manifest build complete")
        IO.puts("manifest version: #{result.manifest_version_id}")
        IO.puts("runner release: #{result.required_runner_release_id}")
        IO.puts("dist: #{result.dist_dir}")

      {:error, {:runner_rebuild_required, categories}} ->
        Mix.raise("runner_rebuild_required: #{Enum.join(categories, ",")}")

      {:error, reason} ->
        Mix.raise("manifest build failed: #{format_reason(reason)}")
    end
  end

  @spec parse_args([String.t()]) :: keyword()
  def parse_args(args) do
    opts =
      CLIArgs.parse_no_args!("favn.build.manifest", args,
        runner_release: :string,
        root_dir: :string
      )

    if present?(opts[:runner_release]) do
      opts
    else
      Mix.raise("missing required option(s): --runner-release")
    end
  end

  defp present?(value), do: is_binary(value) and value != ""
  defp format_reason({:runner_release_descriptor_invalid, reason}), do: Atom.to_string(reason)
  defp format_reason(reason), do: inspect(reason)
end
