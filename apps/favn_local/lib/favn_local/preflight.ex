defmodule FavnLocal.Preflight do
  @moduledoc false

  alias FavnLocal.Config
  alias FavnStoragePostgres.Release

  @spec run(Config.t()) :: :ok | {:error, term()}
  @spec run(Config.t(), keyword()) :: :ok | {:error, term()}
  def run(%Config{} = config, opts \\ []) when is_list(opts) do
    release = Keyword.get(opts, :release, Release)

    with {:ok, %{status: :ok}} <- release.verify_runtime_schema(),
         {:ok, %{state: :ready}} <- release.workspace_status(config.workspace_id) do
      :ok
    else
      {:error, %{code: :runtime_role_not_ready}} ->
        {:error, :postgres_runtime_role_not_ready}

      {:error, %{code: :schema_not_ready} = failure} ->
        {summary, command} = schema_remediation(Map.get(failure, :diagnostics, %{}))
        {:error, {:postgres_schema_not_ready, summary, command}}

      {:ok, %{state: :workspace_administrator_missing}} ->
        {:error,
         {:workspace_not_found, config.workspace_id,
          "mix favn.postgres.provision_workspace --config .favn/workspace-bootstrap.json"}}

      {:error, failure} ->
        {:error, {:postgres_preflight_failed, Map.get(failure, :code, :unavailable)}}
    end
  end

  defp schema_remediation(%{
         status: :incompatible,
         future_migration_versions: versions
       })
       when is_list(versions) and versions != [] do
    {"database has migrations from a newer Favn version (#{format_versions(versions)})", nil}
  end

  defp schema_remediation(%{
         status: :incompatible,
         engine: %{name: :postgresql, version: %{major: major}}
       })
       when is_integer(major) and major != 18 do
    {"PostgreSQL major #{major} is unsupported; Favn requires PostgreSQL 18", nil}
  end

  defp schema_remediation(%{status: :empty_database}),
    do: {"database schema is empty", "mix favn.postgres.upgrade"}

  defp schema_remediation(%{missing_migration_versions: versions})
       when is_list(versions) and versions != [] do
    {"missing migration #{format_versions(versions)}", "mix favn.postgres.upgrade"}
  end

  defp schema_remediation(_diagnostics),
    do: {"schema does not match the selected Favn version", "mix favn.postgres.upgrade"}

  defp format_versions(versions), do: Enum.join(versions, ", ")
end
