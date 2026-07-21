defmodule Mix.Tasks.Favn.Build.ControlPlane do
  use Mix.Task

  @requirements ["app.config"]
  @shortdoc "Builds the maintainer control-plane OCI context"

  @moduledoc """
  Assembles the production `favn_control_plane` release context.

  This repository-maintainer command must run from the Favn repository root.
  Pass `--load` to build and load the unpublished Linux amd64 candidate image.
  Official GHCR images are published only by protected CI.
  """

  alias Favn.Dev.Build.ControlPlane
  alias Mix.Tasks.Favn.CLIArgs
  alias Mix.Tasks.Favn.ProductionBuild

  @impl Mix.Task
  def run(args) do
    opts = parse_args(args)
    ProductionBuild.run("favn.build.control_plane", args, fn -> run_build(opts) end)
  end

  @doc false
  def parse_args(args) when is_list(args) do
    CLIArgs.parse_no_args!("favn.build.control_plane", args, load: :boolean)
  end

  @doc false
  def run_build(opts) when is_list(opts) do
    case ControlPlane.run(opts) do
      {:ok, result} ->
        IO.puts("Favn control-plane build complete")
        IO.puts("control-plane build id: #{result.control_plane_build_id}")
        IO.puts("status: #{result.status}")
        IO.puts("context: #{result.context_dir}")
        IO.puts("descriptor: #{result.descriptor_path}")
        maybe_print_image(result)

      {:error, reason} ->
        Mix.raise("control-plane build failed: #{inspect(reason)}")
    end
  end

  defp maybe_print_image(%{image_status: :loaded} = result) do
    IO.puts("image status: loaded")
    IO.puts("candidate image: #{result.image_tag}")
    IO.puts("image id: #{result.image_id}")
    IO.puts("static asset digest: #{result.static_asset_digest}")
    IO.puts("candidate metadata: #{result.candidate_path}")
  end

  defp maybe_print_image(_result), do: :ok
end
