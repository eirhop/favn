defmodule FavnStoragePostgres.Maintenance.Store do
  @moduledoc false

  @behaviour FavnOrchestrator.Persistence.MaintenanceStore

  import Ecto.Query

  alias Ecto.Adapters.SQL
  alias FavnOrchestrator.Persistence.Commands.BackfillMissingProjection
  alias FavnOrchestrator.Persistence.Commands.ReconcilePersistence
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.PlatformContext
  alias FavnOrchestrator.Persistence.Results.MaintenanceOutcome
  alias FavnStoragePostgres.ErrorMapper
  alias FavnStoragePostgres.Projections.MissingRowBackfiller
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.Schemas.MaintenanceJob

  alias FavnStoragePostgres.Maintenance.Retention

  @impl true
  def retention_status(context), do: Retention.status(context)
  @impl true
  def retention_preview(context, family), do: Retention.preview(context, family)
  @impl true
  def retention_batch(command), do: Retention.batch(command)
  @impl true
  def configure_retention(command), do: Retention.configure(command)

  @projections [:execution_groups, :backfills, :target_statuses, :asset_attempts, :freshness]

  @impl true
  def backfill_missing_projection(%BackfillMissingProjection{} = command) do
    with :ok <- validate_rebuild(command) do
      transaction(fn ->
        Retention.lock!()

        configuration = %{
          "projection" => Atom.to_string(command.projection),
          "workspace_id" => command.workspace_id,
          "limit" => command.limit
        }

        job = prepare_job!(command, "projection_missing_row_backfill", configuration)

        if job.status == "completed" do
          outcome(job, 0, %{})
        else
          batch =
            MissingRowBackfiller.backfill(
              command.projection,
              command.workspace_id,
              job.cursor,
              command.limit
            )

          status = if batch.count < command.limit, do: "completed", else: "running"
          updated = update_job!(job, status, batch.count, batch.cursor)

          outcome(updated, batch.count, %{"projection" => Atom.to_string(command.projection)})
        end
      end)
    end
  end

  @impl true
  def reconcile(%ReconcilePersistence{} = command) do
    with :ok <- validate_reconcile(command) do
      transaction(fn ->
        configuration = %{
          "invariant" => Atom.to_string(command.invariant),
          "workspace_id" => command.workspace_id,
          "repair" => command.repair?,
          "limit" => command.limit
        }

        job = prepare_job!(command, "reconcile", configuration)

        if job.status == "completed" do
          outcome(job, 0, %{})
        else
          batch = reconcile_capacity!(command, job.cursor)
          status = if batch.scanned_count < command.limit, do: "completed", else: "running"
          updated = update_job!(job, status, batch.mismatch_count, batch.cursor)

          outcome(updated, batch.scanned_count, %{
            "mismatch_count" => batch.mismatch_count,
            "repaired" => command.repair?
          })
        end
      end)
    end
  end

  defp prepare_job!(command, job_kind, configuration) do
    now = database_now!()
    scope_kind = if Map.get(command, :workspace_id), do: "workspace", else: "platform"

    case lock_job(command.job_id) do
      nil ->
        %MaintenanceJob{
          job_id: command.job_id,
          job_kind: job_kind,
          scope_kind: scope_kind,
          workspace_id: Map.get(command, :workspace_id),
          status: "running",
          configuration: configuration,
          owner_id: command.platform_context.principal_id,
          fencing_token: 1,
          processed_count: 0,
          version: 1,
          inserted_at: now,
          updated_at: now
        }
        |> Repo.insert!()

      %MaintenanceJob{} = job ->
        if job.job_kind == job_kind and job.configuration == configuration do
          if job.status == "completed" do
            job
          else
            job
            |> Ecto.Changeset.change(%{
              owner_id: command.platform_context.principal_id,
              fencing_token: job.fencing_token + 1,
              status: "running",
              version: job.version + 1,
              updated_at: now
            })
            |> Repo.update!()
          end
        else
          Repo.rollback(Error.new(:conflict, "maintenance job identity has different content"))
        end
    end
  end

  defp lock_job(job_id) do
    from(job in MaintenanceJob, where: job.job_id == ^job_id, lock: "FOR UPDATE")
    |> Repo.one()
  end

  defp update_job!(job, status, count, cursor) do
    job
    |> Ecto.Changeset.change(%{
      status: status,
      cursor: cursor || job.cursor,
      processed_count: job.processed_count + count,
      version: job.version + 1,
      updated_at: database_now!()
    })
    |> Repo.update!()
  end

  defp reconcile_capacity!(command, cursor) do
    after_scope_id = if cursor, do: Map.get(cursor, "scope_id", ""), else: ""

    %{rows: rows} =
      SQL.query!(
        Repo,
        """
        WITH candidates AS (
          SELECT scope_id, active_count
          FROM favn_control.capacity_scopes
          WHERE ($1::text IS NULL OR workspace_id = $1)
            AND scope_id > $2
          ORDER BY scope_id
          LIMIT $3
          FOR UPDATE
        ), expected AS (
          SELECT candidate.scope_id,
                 COALESCE(sum(membership.units) FILTER (
                   WHERE lease.status = 'active'
                 ), 0)::integer AS expected_count
          FROM candidates candidate
          LEFT JOIN favn_control.execution_lease_scopes membership
            ON membership.scope_id = candidate.scope_id
          LEFT JOIN favn_control.execution_leases lease
            ON lease.workspace_id = membership.workspace_id
           AND lease.lease_id = membership.lease_id
          GROUP BY candidate.scope_id
        )
        SELECT candidate.scope_id, candidate.active_count, expected.expected_count
        FROM candidates candidate
        JOIN expected ON expected.scope_id = candidate.scope_id
        ORDER BY candidate.scope_id
        """,
        [command.workspace_id, after_scope_id, command.limit]
      )

    mismatches = Enum.filter(rows, fn [_scope_id, actual, expected] -> actual != expected end)

    if command.repair? and mismatches != [] do
      scope_ids = Enum.map(mismatches, fn [scope_id, _actual, _expected] -> scope_id end)
      expected_counts = Enum.map(mismatches, fn [_scope_id, _actual, expected] -> expected end)

      SQL.query!(
        Repo,
        """
        WITH repaired AS (
          SELECT * FROM unnest($1::text[], $2::integer[])
            AS item(scope_id, expected_count)
        )
        UPDATE favn_control.capacity_scopes scope
        SET active_count = repaired.expected_count,
            version = scope.version + 1,
            updated_at = clock_timestamp()
        FROM repaired
        WHERE scope.scope_id = repaired.scope_id
        """,
        [scope_ids, expected_counts]
      )
    end

    cursor =
      case List.last(rows) do
        [scope_id, _actual, _expected] -> %{"scope_id" => scope_id}
        nil -> nil
      end

    %{scanned_count: length(rows), mismatch_count: length(mismatches), cursor: cursor}
  end

  defp outcome(job, batch_count, details) do
    %MaintenanceOutcome{
      job_id: job.job_id,
      status: String.to_existing_atom(job.status),
      processed_count: job.processed_count,
      batch_count: batch_count,
      cursor: job.cursor,
      details: details
    }
  end

  defp database_now! do
    %{rows: [[now]]} = SQL.query!(Repo, "SELECT clock_timestamp()", [])
    now
  end

  defp transaction(fun) do
    case Repo.transaction(fn ->
           SQL.query!(Repo, "SET LOCAL statement_timeout = '120s'", [])
           fun.()
         end) do
      {:ok, result} -> {:ok, result}
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, ErrorMapper.map(reason)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  defp validate_rebuild(command) do
    if maintenance_operator?(command.platform_context) and valid_id?(command.job_id) and
         command.projection in @projections and valid_id?(command.workspace_id) and
         valid_bound?(command.limit, 1, 250),
       do: :ok,
       else: {:error, ErrorMapper.map(:invalid)}
  end

  defp validate_reconcile(command) do
    authority? =
      if command.repair?,
        do: maintenance_operator?(command.platform_context),
        else: platform_context?(command.platform_context)

    if authority? and valid_id?(command.job_id) and
         command.invariant == :capacity_counters and
         (is_nil(command.workspace_id) or valid_id?(command.workspace_id)) and
         is_boolean(command.repair?) and valid_bound?(command.limit, 1, 1_000),
       do: :ok,
       else: {:error, ErrorMapper.map(:invalid)}
  end

  defp platform_context?(context), do: PlatformContext.valid?(context)

  defp maintenance_operator?(%PlatformContext{roles: roles} = context) do
    PlatformContext.valid?(context) and
      Enum.any?(roles, &(&1 in [:platform_operator, :platform_admin]))
  end

  defp maintenance_operator?(_context), do: false

  defp valid_bound?(value, min, max), do: is_integer(value) and value >= min and value <= max
  defp valid_id?(value), do: is_binary(value) and value != "" and byte_size(value) <= 255
end
