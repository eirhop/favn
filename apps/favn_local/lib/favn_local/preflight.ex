defmodule FavnLocal.Preflight do
  @moduledoc false

  alias FavnLocal.Config
  alias FavnStoragePostgres.Release

  @spec run(Config.t()) :: :ok | {:error, term()}
  def run(%Config{} = config) do
    with {:ok, %{status: :ok}} <- Release.verify_schema(),
         {:ok, %{status: :ok}} <- Release.verify_workspace(config.workspace_id) do
      :ok
    else
      {:error, %{code: :schema_not_ready}} ->
        {:error, {:postgres_schema_not_ready, "mix favn.postgres.migrate"}}

      {:error, %{code: :workspace_not_found}} ->
        {:error,
         {:workspace_not_found, config.workspace_id,
          "mix favn.postgres.provision_workspace --id #{config.workspace_id} --slug #{config.workspace_id} --name \"Local Development\""}}

      {:error, failure} ->
        {:error, {:postgres_preflight_failed, Map.get(failure, :code, :unavailable)}}
    end
  end
end
