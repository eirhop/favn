defmodule Mix.Tasks.Favn.Postgres.ProvisionWorkspace do
  @moduledoc "Provisions an idempotent Storage V2 workspace with platform authority."

  use Mix.Task

  alias FavnOrchestrator.Persistence.Commands.ProvisionWorkspace
  alias FavnOrchestrator.Persistence.PlatformContext
  alias FavnStoragePostgres.Config
  alias FavnStoragePostgres.Registry.Store
  alias FavnStoragePostgres.Repo

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

    {:ok, _applications} = Application.ensure_all_started(:ecto_sql)
    {:ok, _applications} = Application.ensure_all_started(:postgrex)

    {:ok, context} =
      PlatformContext.new("mix:workspace-provisioner", "local-cli", [:platform_admin])

    {:ok, repo} = Repo.start_link(repo_options!())

    try do
      command = %ProvisionWorkspace{
        platform_context: context,
        workspace_id: workspace_id,
        slug: slug,
        display_name: display_name,
        occurred_at: DateTime.utc_now()
      }

      case Store.provision_workspace(command) do
        :ok ->
          Mix.shell().info("Workspace #{workspace_id} is provisioned")
          Mix.shell().info("Add it to runtime configuration: FAVN_WORKSPACE_IDS=#{workspace_id}")

        {:error, error} ->
          Mix.raise("workspace provisioning failed: #{inspect(error)}")
      end
    after
      GenServer.stop(repo)
    end
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

  defp repo_options! do
    case Config.repo_options() do
      {:ok, options} -> options
      {:error, reason} -> Mix.raise("invalid PostgreSQL configuration: #{inspect(reason)}")
    end
  end
end
