defmodule Mix.Tasks.Favn.Reset do
  use Mix.Task

  @shortdoc "Deletes project-local Favn state"

  @moduledoc """
  Stops and removes only the current project's Compose containers, network,
  PostgreSQL volume, generated runner images, and `.favn/` state.

      mix favn.reset --yes

  Without `--yes`, the command prints the exact deletion scope and refuses to
  make changes.
  """

  alias Favn.Dev
  alias Mix.Tasks.Favn.CLIArgs

  @impl Mix.Task
  def run(args) do
    opts = CLIArgs.parse_no_args!("favn.reset", args, root_dir: :string, yes: :boolean)

    case Dev.reset(opts) do
      :ok ->
        IO.puts("Favn local state reset complete")

      {:error, {:confirmation_required, resources}} ->
        Mix.raise(confirmation_message(resources))

      {:error, reason} ->
        Mix.raise("reset failed: #{inspect(reason)}")
    end
  end

  defp confirmation_message(resources) do
    runner_images =
      case resources.runner_images do
        [] -> "none"
        images -> Enum.join(images, ", ")
      end

    "reset requires --yes and would remove only: " <>
      "Compose project #{resources.compose_project}, " <>
      "PostgreSQL volume #{resources.postgres_volume}, " <>
      "runner images #{runner_images}, and #{resources.local_state}"
  end
end
