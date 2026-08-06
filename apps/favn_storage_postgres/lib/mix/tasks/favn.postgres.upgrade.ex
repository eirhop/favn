defmodule Mix.Tasks.Favn.Postgres.Upgrade do
  @moduledoc """
  Makes a development PostgreSQL database ready for the selected Favn version.

  The task uses `FAVN_DATABASE_MIGRATOR_URL` to apply migrations and converge
  runtime grants, then uses `FAVN_DATABASE_URL` to verify the exact schema.
  It never provisions workspaces or runs during `mix favn.dev` startup.
  If a stage fails, treat that stage as possibly partially applied and inspect
  the database before retrying.

      mix favn.postgres.upgrade
  """

  use Mix.Task

  alias FavnStoragePostgres.DevelopmentUpgrade

  @shortdoc "Upgrades and verifies the development PostgreSQL schema"

  @impl true
  def run([]) do
    Mix.Task.run("app.config")

    case DevelopmentUpgrade.run(progress_fun: &print_completed/1) do
      {:ok, %{completed: [:migrate, :grant_runtime, :verify_schema]}} ->
        Mix.shell().info("PostgreSQL is ready for Favn development")

      {:error, failure} ->
        Mix.raise(error_message(failure))
    end
  end

  def run(_args), do: Mix.raise("usage: mix favn.postgres.upgrade")

  defp print_completed(:migrate), do: Mix.shell().info("ok: migrations applied")
  defp print_completed(:grant_runtime), do: Mix.shell().info("ok: runtime permissions updated")
  defp print_completed(:verify_schema), do: Mix.shell().info("ok: schema verified")

  defp error_message(%{
         stage: :configuration,
         code: :missing_environment,
         variable: variable
       }),
       do: "missing required environment variable #{variable}"

  defp error_message(%{stage: :configuration, code: :database_roles_not_separated}),
    do: "FAVN_DATABASE_URL and FAVN_DATABASE_MIGRATOR_URL must use separate database roles"

  defp error_message(%{stage: stage, completed: completed, code: code}) do
    completed_text =
      case completed do
        [] -> "no stages are confirmed complete"
        stages -> "confirmed complete stages: #{Enum.join(stages, ", ")}"
      end

    "PostgreSQL development upgrade failed at #{stage}: #{code}; #{completed_text}. " <>
      "The failed stage may have partially applied changes; inspect the database before retrying."
  end
end
