defmodule FavnStoragePostgres.Backend do
  @moduledoc """
  Production PostgreSQL lifecycle and capability entrypoint for orchestrator persistence.

  Runtime nodes validate an already-migrated schema and never run migrations at boot.
  """

  @behaviour FavnOrchestrator.Persistence.Backend

  alias Ecto.Adapters.SQL
  alias FavnOrchestrator.Persistence.Diagnostics
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Readiness
  alias FavnOrchestrator.Persistence.Stores
  alias FavnStoragePostgres.BackendSupervisor
  alias FavnStoragePostgres.Config
  alias FavnStoragePostgres.ErrorMapper
  alias FavnStoragePostgres.Registry.ManifestCache
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.RuntimeInputKeys
  alias FavnStoragePostgres.StorageV2.Migrations

  @impl true
  def child_specs(options) when is_list(options) do
    with {:ok, repo_options} <- Config.repo_options(options),
         :ok <- Stores.validate(stores()),
         {:ok, {_version, _key}} <- RuntimeInputKeys.current() do
      {:ok, [Supervisor.child_spec({BackendSupervisor, repo_options}, id: BackendSupervisor)]}
    else
      {:error, reason} -> {:error, configuration_error(reason)}
    end
  end

  @impl true
  def stores do
    %Stores{
      registry: FavnStoragePostgres.Instrumented.Registry,
      runs: FavnStoragePostgres.Instrumented.Runs,
      run_ownership: FavnStoragePostgres.Instrumented.RunOwnership,
      scheduler: FavnStoragePostgres.Instrumented.Scheduler,
      admission: FavnStoragePostgres.Instrumented.Admission,
      materialization: FavnStoragePostgres.Instrumented.Materialization,
      backfills: FavnStoragePostgres.Instrumented.Backfills,
      operator_reads: FavnStoragePostgres.Instrumented.OperatorReads,
      logs: FavnStoragePostgres.Instrumented.Logs,
      identity: FavnStoragePostgres.Instrumented.Identity,
      maintenance: FavnStoragePostgres.Instrumented.Maintenance
    }
  end

  @impl true
  def readiness(_options) do
    case Migrations.diagnostics(Repo) do
      {:ok, diagnostics} ->
        runtime_input_keys = runtime_input_key_diagnostics(diagnostics)
        ready? = diagnostics.ready? and runtime_input_keys.ready?

        {:ok,
         %Readiness{
           status: if(ready?, do: :ready, else: readiness_status(diagnostics)),
           ready?: ready?,
           backend: __MODULE__,
           checks: %{
             engine: diagnostics.engine,
             schema: schema_summary(diagnostics),
             runtime_input_keys: runtime_input_keys
           }
         }}

      {:error, reason} ->
        {:error, ErrorMapper.map(reason)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def diagnostics(options) when is_list(options) do
    with {:ok, repo_options} <- Config.repo_options(options),
         {:ok, schema} <- Migrations.diagnostics(Repo) do
      runtime_input_keys = runtime_input_key_diagnostics(schema)

      {:ok,
       %Diagnostics{
         backend: __MODULE__,
         engine: schema.engine,
         schema: schema_summary(schema),
         pool: Config.redacted(repo_options),
         features: %{
           workspaces: true,
           multi_node_fencing: true,
           ordered_outbox: true,
           projections: true,
           encrypted_runtime_inputs: true
         },
         metadata: %{
           runtime_input_keys: runtime_input_keys,
           manifest_cache: ManifestCache.diagnostics()
         }
       }}
    else
      {:error, reason} -> {:error, ErrorMapper.map(reason)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  defp schema_summary(diagnostics) do
    Map.take(diagnostics, [
      :status,
      :ready?,
      :schema,
      :missing_tables,
      :missing_critical_indexes,
      :missing_columns,
      :unexpected_columns,
      :missing_critical_constraints,
      :missing_migration_versions,
      :future_migration_versions,
      :expected_migration_versions,
      :definition_fingerprint_matches?,
      :expected_definition_fingerprint,
      :actual_definition_fingerprint,
      :projection,
      :runtime_role
    ])
  end

  defp runtime_input_key_diagnostics(schema) do
    configured = RuntimeInputKeys.diagnostics()

    if "runtime_input_key_versions" in schema.missing_tables do
      Map.merge(configured, %{
        ready?: false,
        inventory_available?: false,
        referenced_versions: [],
        missing_referenced_versions: []
      })
    else
      %{rows: rows} =
        SQL.query!(
          Repo,
          "SELECT key_version FROM favn_control.runtime_input_key_versions ORDER BY key_version",
          []
        )

      referenced_versions = Enum.map(rows, fn [version] -> version end)
      missing_versions = referenced_versions -- configured.retained_versions

      Map.merge(configured, %{
        ready?:
          configured.configured? and configured.invalid_versions == [] and
            missing_versions == [],
        inventory_available?: true,
        referenced_versions: referenced_versions,
        missing_referenced_versions: missing_versions
      })
    end
  end

  defp readiness_status(%{ready?: false, status: status}), do: status
  defp readiness_status(_diagnostics), do: :not_ready

  defp configuration_error(reason) do
    Error.new(:invalid, "invalid PostgreSQL persistence configuration",
      details: %{reason: reason}
    )
  end
end
