defmodule Mix.Tasks.Favn.Postgres.Maintenance do
  @moduledoc """
  Runs one explicit, bounded PostgreSQL Storage V2 maintenance batch.

  Re-run the exact command and job id while the returned status is `:running`.
  """

  use Mix.Task

  alias FavnOrchestrator.Persistence.Commands.BackfillMissingProjection
  alias FavnOrchestrator.Persistence.Commands.PurgePersistence
  alias FavnOrchestrator.Persistence.Commands.ReconcilePersistence
  alias FavnOrchestrator.Persistence.PlatformContext
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
    cutoff: :string,
    limit: :integer,
    repair: :boolean
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
        [value] when value in ["backfill-missing", "reconcile", "purge"] -> value
        _invalid -> usage!()
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

  defp command("purge", options, context) do
    %PurgePersistence{
      platform_context: context,
      job_id: required_id!(options, :job_id),
      workspace_id: optional_id!(options, :workspace),
      target: purge_target!(options),
      cutoff: cutoff!(options),
      limit: limit(options, 1_000, 5_000)
    }
  end

  defp execute(command, "backfill-missing"), do: Store.backfill_missing_projection(command)
  defp execute(command, "reconcile"), do: Store.reconcile(command)
  defp execute(command, "purge"), do: Store.purge(command)

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

  defp purge_target!(options) do
    case Keyword.get(options, :target) do
      "logs" -> :logs
      "sessions" -> :sessions
      "idempotency" -> :idempotency
      "materialization-claims" -> :materialization_claims
      "projection-failures" -> :projection_failures
      "execution-packages" -> :execution_packages
      _invalid -> usage!()
    end
  end

  defp cutoff!(options) do
    with value when is_binary(value) <- Keyword.get(options, :cutoff),
         {:ok, cutoff, 0} <- DateTime.from_iso8601(value) do
      cutoff
    else
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

  defp usage! do
    Mix.raise("""
    usage:
      mix favn.postgres.maintenance backfill-missing --job-id ID --workspace ID \\
        --projection execution-groups|backfills|target-statuses|freshness [--limit N]
      mix favn.postgres.maintenance reconcile --job-id ID \\
        --invariant capacity-counters [--workspace ID] [--repair] [--limit N]
      mix favn.postgres.maintenance purge --job-id ID \\
        --target logs|sessions|idempotency|materialization-claims|projection-failures|execution-packages \\
        --cutoff ISO8601 [--workspace ID] [--limit N]
    """)
  end

  defp repo_options! do
    case Config.repo_options_from_env() do
      {:ok, options} -> options
      {:error, reason} -> Mix.raise("invalid PostgreSQL configuration: #{inspect(reason)}")
    end
  end
end
