defmodule Mix.Tasks.Favn.Postgres.Maintenance do
  @moduledoc """
  Runs one explicit, bounded PostgreSQL Storage V2 maintenance batch.

  Retention commands use an expected version from `retention-status`; after a lost
  acknowledgement read status before resuming. Repair commands retain their stable
  job ID and configuration while the returned status is `:running`.
  """

  use Mix.Task

  alias FavnOrchestrator.Persistence.Commands.BackfillMissingProjection
  alias FavnOrchestrator.Persistence.Commands.ReconcilePersistence
  alias FavnOrchestrator.Persistence.PlatformContext
  alias FavnOrchestrator.Persistence.Commands.ConfigureRetention
  alias FavnOrchestrator.Persistence.Commands.RetentionBatch
  alias FavnOrchestrator.Retention.Policy
  alias FavnStoragePostgres.Config
  alias FavnStoragePostgres.Maintenance.Store
  alias FavnStoragePostgres.Repo

  @shortdoc "Runs one bounded Storage V2 maintenance batch"

  @switches [
    job_id: :string,
    workspace: :string,
    projection: :string,
    target: :string,
    invariant: :string,
    limit: :integer,
    repair: :boolean,
    policy: :string,
    expected_version: :integer
  ]

  @impl true
  def run(args) do
    Mix.Task.run("app.config")

    {options, positional, invalid} = OptionParser.parse(args, strict: @switches)

    if invalid != [] do
      usage!()
    end

    operation =
      case positional do
        [value]
        when value in [
               "backfill-missing",
               "reconcile",
               "retention-status",
               "retention-preview",
               "retention-configure",
               "retention-run"
             ] ->
          value

        _invalid ->
          usage!()
      end

    {:ok, _applications} = Application.ensure_all_started(:ecto_sql)
    {:ok, _applications} = Application.ensure_all_started(:postgrex)

    {:ok, context} =
      PlatformContext.new("mix:postgres-maintenance", "local-cli", [:platform_admin])

    {:ok, repo} = Repo.start_link(repo_options!())

    try do
      operation
      |> command(options, context)
      |> execute(operation)
      |> report()
    after
      GenServer.stop(repo)
    end
  end

  defp command("retention-status", _options, context), do: context

  defp command("retention-preview", options, context) do
    family = Enum.find(Policy.families(), &(Atom.to_string(&1) == options[:target])) || usage!()
    {context, family}
  end

  defp command(operation, options, context)
       when operation in ["retention-configure", "retention-run"] do
    version = options[:expected_version]
    unless is_integer(version) and version >= 0, do: usage!()
    {:ok, state} = Store.retention_status(context)

    policy =
      case options[:policy] do
        nil ->
          state.policy

        path ->
          with {:ok, bytes} <- File.read(path),
               {:ok, value} <- Jason.decode(bytes),
               {:ok, policy} <- Policy.decode(value) do
            policy
          else
            _ -> Mix.raise("invalid retention policy file")
          end
      end

    if operation == "retention-configure",
      do: %ConfigureRetention{
        platform_context: context,
        policy: policy,
        expected_version: version
      },
      else: %RetentionBatch{platform_context: context, policy: policy, expected_version: version}
  end

  defp command("backfill-missing", options, context) do
    %BackfillMissingProjection{
      platform_context: context,
      job_id: required_id!(options, :job_id),
      workspace_id: required_id!(options, :workspace),
      projection: projection!(options),
      limit: limit(options, 100, 250)
    }
  end

  defp command("reconcile", options, context) do
    %ReconcilePersistence{
      platform_context: context,
      job_id: required_id!(options, :job_id),
      workspace_id: optional_id!(options, :workspace),
      invariant: invariant!(options),
      repair?: Keyword.get(options, :repair, false),
      limit: limit(options, 100, 1_000)
    }
  end

  defp execute(context, "retention-status"), do: Store.retention_status(context)

  defp execute({context, family}, "retention-preview"),
    do: Store.retention_preview(context, family)

  defp execute(command, "retention-configure"), do: Store.configure_retention(command)
  defp execute(command, "retention-run"), do: Store.retention_batch(command)

  defp execute(command, "backfill-missing"), do: Store.backfill_missing_projection(command)
  defp execute(command, "reconcile"), do: Store.reconcile(command)

  defp report({:ok, outcome}) do
    Mix.shell().info(inspect(outcome, pretty: true, limit: :infinity))
  end

  defp report({:error, error}), do: Mix.raise("maintenance batch failed: #{inspect(error)}")

  defp projection!(options) do
    case Keyword.get(options, :projection) do
      "execution-groups" -> :execution_groups
      "backfills" -> :backfills
      "target-statuses" -> :target_statuses
      "asset-attempts" -> :asset_attempts
      "freshness" -> :freshness
      _invalid -> usage!()
    end
  end

  defp invariant!(options) do
    case Keyword.get(options, :invariant) do
      "capacity-counters" -> :capacity_counters
      _invalid -> usage!()
    end
  end

  defp limit(options, default, maximum) do
    case Keyword.get(options, :limit, default) do
      value when is_integer(value) and value >= 1 and value <= maximum -> value
      _invalid -> usage!()
    end
  end

  defp required_id!(options, key) do
    case optional_id!(options, key) do
      nil -> usage!()
      value -> value
    end
  end

  defp optional_id!(options, key) do
    case Keyword.get(options, key) do
      nil -> nil
      value when is_binary(value) and value != "" and byte_size(value) <= 255 -> value
      _invalid -> usage!()
    end
  end

  @spec usage!() :: no_return()
  defp usage! do
    Mix.raise("""
    usage:
      mix favn.postgres.maintenance retention-status
      mix favn.postgres.maintenance retention-preview --target FAMILY
      mix favn.postgres.maintenance retention-configure --policy FILE --expected-version N
      mix favn.postgres.maintenance retention-run --expected-version N
      mix favn.postgres.maintenance backfill-missing --job-id ID --workspace ID \\
        --projection execution-groups|backfills|target-statuses|freshness [--limit N]
      mix favn.postgres.maintenance reconcile --job-id ID \\
        --invariant capacity-counters [--workspace ID] [--repair] [--limit N]
    """)
  end

  defp repo_options! do
    case Config.repo_options_from_env() do
      {:ok, options} -> options
      {:error, reason} -> Mix.raise("invalid PostgreSQL configuration: #{inspect(reason)}")
    end
  end
end
