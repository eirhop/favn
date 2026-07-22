defmodule Mix.Tasks.Favn.Reset do
  use Mix.Task

  @shortdoc "Deletes project-local Favn state"

  @moduledoc """
  Removes generated local state and verified local runner image tags after proving
  known Favn roles are stopped. It preserves the consumer Compose file,
  services, containers, networks, volumes, and `.favn/data`.

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

    compose_file = resources.preserved_compose_file || "the selected consumer Compose file"

    "reset requires --yes and would remove generated state below " <>
      "#{resources.generated_state} (except #{resources.preserved_data}) and verified runner image tags #{runner_images}. " <>
      "It will not delete #{compose_file}, containers, services, networks, volumes, or data."
  end
end
