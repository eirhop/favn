defmodule Mix.Tasks.Favn.Build.Orchestrator do
  use Mix.Task

  @shortdoc "Builds the project-local orchestrator artifact"

  @moduledoc """
  Builds `.favn/build/orchestrator/<build_id>` and `.favn/dist/orchestrator/<build_id>`.
  """

  alias Favn.Dev

  @impl Mix.Task
  def run(args) do
    {opts, _rest, _invalid} = OptionParser.parse(args, strict: [root_dir: :string])

    case Dev.build_orchestrator(opts) do
      {:ok, %{build_id: build_id, dist_dir: dist_dir}} ->
        IO.puts("Favn orchestrator build complete")
        IO.puts("build id: #{build_id}")
        IO.puts("dist: #{dist_dir}")

      {:error, :install_required} ->
        Mix.raise("build blocked: install required; run mix favn.install")

      {:error, :install_stale} ->
        Mix.raise("build blocked: install stale; run mix favn.install --force")

      {:error, reason} ->
        Mix.raise("orchestrator build failed: #{inspect(reason)}")
    end
  end
end
