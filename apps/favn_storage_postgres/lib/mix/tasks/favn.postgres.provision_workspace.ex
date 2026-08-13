defmodule Mix.Tasks.Favn.Postgres.ProvisionWorkspace do
  @moduledoc "Atomically provisions a workspace and its initial administrator."

  use Mix.Task

  alias FavnStoragePostgres.Release
  alias FavnStoragePostgres.WorkspaceProvisioning.Config
  alias Mix.Tasks.Favn.Postgres.ReleaseHelpers

  @shortdoc "Provisions a workspace and initial administrator"

  @impl true
  def run(args) do
    Mix.Task.run("app.config")

    {options, positional, invalid} =
      OptionParser.parse(args,
        strict: [config: :string, password_file: :string]
      )

    if positional != [] or invalid != [] do
      usage!()
    end

    config_path = required_option!(options, :config)

    env =
      System.get_env()
      |> Map.merge(ReleaseHelpers.migrator_env!())
      |> Map.put("FAVN_WORKSPACE_PROVISIONING_CONFIG_FILE", config_path)
      |> maybe_put_password_file(options)

    with {:ok, input} <- Config.load(env) do
      Release.provision_workspace_administrator(input, env)
    end
    |> ReleaseHelpers.report("Workspace and initial administrator are ready")
  end

  defp required_option!(options, key) do
    case Keyword.get(options, key) do
      value when is_binary(value) and value != "" and byte_size(value) <= 255 -> value
      _invalid -> usage!()
    end
  end

  defp maybe_put_password_file(env, options) do
    case Keyword.get(options, :password_file) do
      nil -> env
      path -> Map.put(env, "FAVN_WORKSPACE_ADMIN_PASSWORD_FILE", path)
    end
  end

  @spec usage!() :: no_return()
  defp usage! do
    Mix.raise("usage: mix favn.postgres.provision_workspace --config PATH [--password-file PATH]")
  end
end
