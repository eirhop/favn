defmodule Mix.Tasks.Favn.Postgres.ProvisionWorkspace do
  @moduledoc "Provisions an idempotent Storage V2 workspace with platform authority."

  use Mix.Task

  alias FavnStoragePostgres.Release
  alias Mix.Tasks.Favn.Postgres.ReleaseHelpers

  @shortdoc "Provisions a PostgreSQL Storage V2 workspace"

  @impl true
  def run(args) do
    Mix.Task.run("app.config")

    {options, positional, invalid} =
      OptionParser.parse(args,
        strict: [id: :string, slug: :string, name: :string]
      )

    if positional != [] or invalid != [] do
      usage!()
    end

    workspace_id = required_option!(options, :id)
    slug = Keyword.get(options, :slug, workspace_id)
    display_name = Keyword.get(options, :name, workspace_id)

    Release.provision_workspace(%{
      workspace_id: workspace_id,
      slug: slug,
      display_name: display_name
    })
    |> ReleaseHelpers.report("Workspace is provisioned")
  end

  defp required_option!(options, key) do
    case Keyword.get(options, key) do
      value when is_binary(value) and value != "" and byte_size(value) <= 255 -> value
      _invalid -> usage!()
    end
  end

  defp usage! do
    Mix.raise("usage: mix favn.postgres.provision_workspace --id ID [--slug SLUG] [--name NAME]")
  end
end
