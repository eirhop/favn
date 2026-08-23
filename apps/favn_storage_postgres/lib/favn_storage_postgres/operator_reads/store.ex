defmodule FavnStoragePostgres.OperatorReads.Store do
  @moduledoc false

  @behaviour FavnOrchestrator.Persistence.OperatorReadStore

  import Ecto.Query

  alias Ecto.Adapters.SQL
  alias Favn.Window.Key, as: WindowKey
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.PlatformContext
  alias FavnOrchestrator.Persistence.Queries.FreshnessIdentity
  alias FavnOrchestrator.Persistence.Queries.CountExecutionGroups
  alias FavnOrchestrator.Persistence.Queries.CountSuccessfulAssetWindows
  alias FavnOrchestrator.Persistence.Queries.GetExecutionGroup
  alias FavnOrchestrator.Persistence.Queries.GetOperatorRunOverview
  alias FavnOrchestrator.Persistence.Queries.GetRunAssetAttempt
  alias FavnOrchestrator.Persistence.Queries.GetRunFlowDelta
  alias FavnOrchestrator.Persistence.Queries.GetRunFlowPage
  alias FavnOrchestrator.Persistence.Queries.ResolveRunSubscription
  alias FavnOrchestrator.Persistence.Queries.GetAssetWindowStates
  alias FavnOrchestrator.Persistence.Queries.GetFreshnessMany
  alias FavnOrchestrator.Persistence.Queries.GetTargetStatuses
  alias FavnOrchestrator.Persistence.Queries.GetSuccessfulAssetWindowKeys
  alias FavnOrchestrator.Persistence.Queries.PageExecutionGroups
  alias FavnOrchestrator.Persistence.Queries.PageGroupRuns
  alias FavnOrchestrator.Persistence.Queries.PageGroupWindows
  alias FavnOrchestrator.Persistence.Queries.PageManifests
  alias FavnOrchestrator.Persistence.Queries.PageRunEventSummaries
  alias FavnOrchestrator.Persistence.Queries.PageRunWindows
  alias FavnOrchestrator.Persistence.Queries.PageTargetRuns
  alias FavnOrchestrator.Persistence.RunEnum
  alias FavnOrchestrator.Persistence.Results.BackfillWindow, as: BackfillWindowResult
  alias FavnOrchestrator.Persistence.Results.AssetWindowState, as: AssetWindowResult
  alias FavnOrchestrator.Persistence.Results.AssetAttemptOverview, as: AssetAttemptResult
  alias FavnOrchestrator.Persistence.Results.CursorPage
  alias FavnOrchestrator.Persistence.Results.ExecutionGroup
  alias FavnOrchestrator.Persistence.Results.ExecutionGroupCounts
  alias FavnOrchestrator.Persistence.Results.ExecutionGroupOverview, as: GroupOverviewResult
  alias FavnOrchestrator.Persistence.Results.FreshnessState, as: FreshnessResult
  alias FavnOrchestrator.Persistence.Results.ManifestSummary
  alias FavnOrchestrator.Persistence.Results.OperatorRunOverview, as: OperatorRunOverviewResult
  alias FavnOrchestrator.Persistence.Results.RunAssetAttempt, as: RunAssetAttemptResult
  alias FavnOrchestrator.Persistence.Results.RunFlowHeader
  alias FavnOrchestrator.Persistence.Results.RunFlowDelta
  alias FavnOrchestrator.Persistence.Results.RunFlowPage
  alias FavnOrchestrator.Persistence.Results.RunFlowStep
  alias FavnOrchestrator.Persistence.Results.RunSubscriptionIdentity
  alias FavnOrchestrator.Persistence.Results.PlannedAssetStep
  alias FavnOrchestrator.Persistence.Results.RunSummary
  alias FavnOrchestrator.Persistence.Results.RunEventSummary
  alias FavnOrchestrator.Persistence.Results.RunSummaryPage
  alias FavnOrchestrator.Persistence.Results.RunWindowSummary
  alias FavnOrchestrator.Persistence.Results.TargetStatus, as: TargetStatusResult
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.Storage.ExactDateTimeCodec
  alias FavnStoragePostgres.ErrorMapper
  alias FavnStoragePostgres.Projections.Readiness
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.Schemas.Backfill
  alias FavnStoragePostgres.Schemas.BackfillOverview
  alias FavnStoragePostgres.Schemas.BackfillWindow
  alias FavnStoragePostgres.Schemas.AssetAttemptOverview
  alias FavnStoragePostgres.Schemas.AssetWindowState
  alias FavnStoragePostgres.Schemas.ExecutionGroupOverview
  alias FavnStoragePostgres.Schemas.ManifestVersion
  alias FavnStoragePostgres.Schemas.Run
  alias FavnStoragePostgres.Schemas.RunTarget
  alias FavnStoragePostgres.Schemas.TargetStatus

  @max_batch 500
  @group_statuses [:pending, :running, :succeeded, :failed]
  @group_orders [:started_desc, :started_asc]
  @active_group_statuses ~w(pending running)
  # One vocabulary for what happened to an asset step, shared by every read that
  # counts them. `skipped_fresh` is completed without being failed: the asset was
  # already up to date, which is a result rather than a non-event.
  @completed_asset_statuses ~w(ok error timed_out cancelled skipped_fresh blocked)
  @failed_asset_statuses ~w(error timed_out cancelled blocked)
  @running_asset_statuses ~w(running retrying)
  @no_asset_counts %{
    total: 0,
    completed: 0,
    succeeded: 0,
    skipped: 0,
    failed: 0,
    running: 0,
    queued: 0,
    planned: 0
  }
  @run_summary_fields [
    :workspace_id,
    :run_id,
    :root_execution_group_id,
    :parent_run_id,
    :rerun_of_run_id,
    :deployment_id,
    :manifest_version_id,
    :status,
    :submit_kind,
    :trigger_type,
    :event_sequence,
    :submitted_event_id,
    :latest_event_id,
    :inserted_at,
    :updated_at,
    :terminal_at
  ]

  @impl true
  def page_manifests(%PageManifests{} = page) do
    with :ok <- validate_manifest_page(page) do
      query =
        ManifestVersion
        |> after_manifest(page.after)
        |> order_by([manifest], desc: manifest.inserted_at, desc: manifest.manifest_version_id)
        |> limit(^(page.limit + 1))
        |> select([manifest], %ManifestVersion{
          manifest_version_id: manifest.manifest_version_id,
          content_hash: manifest.content_hash,
          schema_version: manifest.schema_version,
          runner_contract_version: manifest.runner_contract_version,
          runner_releases: manifest.runner_releases,
          inserted_at: manifest.inserted_at
        })

      rows = Repo.all(query)
      {:ok, cursor_page(rows, page.limit, &manifest_result/1, &manifest_cursor/1)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def page_execution_groups(%PageExecutionGroups{} = page) do
    with :ok <- validate_group_page(page),
         {:ok, trigger_type} <- encoded_trigger(page.trigger_type) do
      query =
        group_query()
        |> group_scope(page.scope)
        |> group_status(page.status)
        |> group_trigger(trigger_type)
        |> group_started_after(page.started_after)
        |> group_started_before(page.started_before)
        |> group_search(page.search, page.scope)
        |> after_group(page.after, page.order)
        |> order_groups(page.order)
        |> limit(^(page.limit + 1))

      rows = Repo.all(query)

      # Enriched from the page's own rows, so the extra queries are bounded by the
      # page limit rather than by the workspace's history.
      groups = Enum.take(rows, page.limit)
      targets = target_refs_by_group(groups)
      assets = asset_counts_by_group(groups)
      mapper = &group_result(&1, targets, assets)

      {:ok, cursor_page(rows, page.limit, mapper, &group_cursor/1)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def count_execution_groups(%CountExecutionGroups{} = query) do
    with :ok <- validate_count_groups(query),
         {:ok, trigger_type} <- encoded_trigger(query.trigger_type) do
      counts =
        group_query()
        |> group_scope(query.scope)
        |> group_trigger(trigger_type)
        |> group_started_after(query.started_after)
        |> group_started_before(query.started_before)
        |> group_search(query.search, query.scope)
        |> select([group: group], %{
          active: filter(count(), group.status in ^@active_group_statuses),
          failed: filter(count(), group.status == "failed"),
          succeeded: filter(count(), group.status == "succeeded"),
          total: count()
        })
        |> Repo.one()

      {:ok,
       %ExecutionGroupCounts{
         active: counts.active,
         failed: counts.failed,
         succeeded: counts.succeeded,
         total: counts.total
       }}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def get_execution_group(%GetExecutionGroup{} = query) do
    with :ok <- validate_get_group(query),
         %ExecutionGroupOverview{} = overview <-
           Repo.get_by(ExecutionGroupOverview,
             workspace_id: query.workspace_context.workspace_id,
             root_run_id: query.root_run_id
           ),
         {:ok, runs} <-
           page_group_runs(%PageGroupRuns{
             workspace_context: query.workspace_context,
             root_run_id: query.root_run_id,
             limit: query.detail_limit
           }),
         {:ok, windows} <-
           page_group_windows(%PageGroupWindows{
             workspace_context: query.workspace_context,
             root_run_id: query.root_run_id,
             limit: query.detail_limit
           }),
         {:ok, failures} <-
           failure_page(query.workspace_context, query.root_run_id, query.detail_limit) do
      {:ok,
       %ExecutionGroup{
         overview:
           group_result(
             overview,
             target_refs_by_group([overview]),
             asset_counts_by_group([overview])
           ),
         runs: runs,
         windows: windows,
         failures: failures
       }}
    else
      nil -> {:error, Error.new(:not_found, "execution group not found")}
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, ErrorMapper.map(reason)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def get_operator_run_overview(%GetOperatorRunOverview{} = query) do
    with :ok <- validate_operator_run_overview(query),
         %Run{} = selected <-
           Repo.get_by(Run,
             workspace_id: query.workspace_context.workspace_id,
             run_id: query.run_id
           ),
         root_run_id <- selected.root_execution_group_id,
         %ExecutionGroupOverview{} = overview <-
           Repo.get_by(ExecutionGroupOverview,
             workspace_id: query.workspace_context.workspace_id,
             root_run_id: root_run_id
           ),
         %{} = root <-
           get_run_summary(query.workspace_context.workspace_id, root_run_id),
         {:ok, runs} <-
           page_group_runs(%PageGroupRuns{
             workspace_context: query.workspace_context,
             root_run_id: root_run_id,
             limit: query.limit
           }),
         {:ok, requested_windows} <-
           page_group_windows(%PageGroupWindows{
             workspace_context: query.workspace_context,
             root_run_id: root_run_id,
             limit: query.limit
           }) do
      loaded_run_ids =
        [root_run_id | Enum.map(runs.items, & &1.run_id)]
        |> Enum.uniq()

      asset_counts_by_run =
        compact_asset_counts_by_run(
          query.workspace_context.workspace_id,
          root_run_id,
          loaded_run_ids
        )

      {attempts, attempts_truncated?} =
        compact_attempts(query.workspace_context.workspace_id, root_run_id, query.limit)

      {planned_steps, planned_steps_truncated?} =
        compact_planned_steps(query.workspace_context.workspace_id, root_run_id, query.limit)

      {:ok,
       %OperatorRunOverviewResult{
         overview:
           group_result(
             overview,
             target_refs_by_group([overview]),
             asset_counts_by_group([overview])
           ),
         root_run: run_result(root),
         runs: runs.items,
         requested_windows: requested_windows.items,
         requested_windows_truncated?: requested_windows.has_more?,
         requested_window_counts:
           requested_window_counts(query.workspace_context.workspace_id, root_run_id),
         attempts: attempts,
         asset_counts_by_run: asset_counts_by_run,
         planned_steps: planned_steps,
         planned_steps_truncated?: planned_steps_truncated?,
         attempt_counts:
           compact_attempt_counts(query.workspace_context.workspace_id, root_run_id),
         attempts_truncated?: attempts_truncated?,
         runs_truncated?: runs.has_more?,
         target_refs: target_refs(query.workspace_context.workspace_id, root_run_id)
       }}
    else
      nil -> {:error, Error.new(:not_found, "execution group not found")}
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, ErrorMapper.map(reason)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def resolve_run_subscription(%ResolveRunSubscription{} = query) do
    {:ok, result} =
      run_flow_transaction(fn ->
        from(run in Run,
          where:
            run.workspace_id == ^query.workspace_context.workspace_id and
              run.run_id == ^query.run_id,
          select: {run.run_id, run.root_execution_group_id},
          limit: 1
        )
        |> Repo.one()
      end)

    case result do
      {run_id, root_run_id} ->
        {:ok, %RunSubscriptionIdentity{run_id: run_id, root_run_id: root_run_id || run_id}}

      nil ->
        {:error, Error.new(:not_found, "run not found")}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def get_run_flow_page(%GetRunFlowPage{} = query) do
    with :ok <- validate_run_flow_page(query),
         {:ok, result} <- run_flow_transaction(fn -> run_flow_page!(query) end) do
      {:ok, result}
    else
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, ErrorMapper.map(reason)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def get_run_asset_attempt(%GetRunAssetAttempt{} = query) do
    {:ok, row} =
      run_flow_transaction(fn ->
        from(attempt in AssetAttemptOverview,
          where:
            attempt.workspace_id == ^query.workspace_context.workspace_id and
              attempt.run_id == ^query.run_id and attempt.asset_step_id == ^query.asset_step_id,
          select: attempt,
          limit: 1
        )
        |> Repo.one()
      end)

    case row do
      %AssetAttemptOverview{} = attempt ->
        {:ok,
         %RunAssetAttemptResult{
           summary: run_flow_step(attempt),
           error: attempt.error,
           output_metadata: attempt.output_metadata,
           window: restore_window(attempt.window)
         }}

      nil ->
        {:error, Error.new(:not_found, "asset attempt not found")}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def get_run_flow_delta(%GetRunFlowDelta{} = query) do
    with :ok <- validate_run_flow_delta(query),
         {:ok, result} <- run_flow_transaction(fn -> run_flow_delta!(query) end) do
      {:ok, result}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def page_run_windows(%PageRunWindows{} = page) do
    with :ok <- validate_run_summary_page(page),
         {:ok, {rows, total, projection_cursor}} <-
           run_flow_transaction(fn ->
             %{rows: rows} = run_window_rows!(page)
             {total, projection_cursor} = run_window_metadata!(page)
             {rows, total, projection_cursor}
           end) do
      has_more? = length(rows) > page.limit
      rows = Enum.take(rows, page.limit)

      items = Enum.map(rows, &run_window_summary/1)
      next_cursor = if has_more?, do: window_summary_cursor(List.last(items)), else: nil

      {:ok,
       %RunSummaryPage{
         items: items,
         total: total,
         has_more?: has_more?,
         next_cursor: next_cursor,
         projection_cursor: projection_cursor
       }}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  defp run_window_metadata!(page) do
    %{rows: [[total, projection_cursor]]} =
      SQL.query!(
        Repo,
        """
        SELECT count(*)::bigint,
               COALESCE((
                 SELECT last_publication_id
                 FROM favn_control.projection_cursors
                 WHERE projector_name = 'control_plane_v1' AND shard_id = 0
               ), 0)::bigint
        FROM favn_control.runs AS selected
        JOIN favn_control.backfills AS backfill
          ON backfill.workspace_id = selected.workspace_id
         AND backfill.root_run_id = selected.root_execution_group_id
        JOIN favn_control.backfill_windows AS run_window
          ON run_window.workspace_id = backfill.workspace_id
         AND run_window.backfill_id = backfill.backfill_id
        WHERE selected.workspace_id = $1 AND selected.run_id = $2
        """,
        [page.workspace_context.workspace_id, page.run_id]
      )

    {total, projection_cursor}
  end

  defp run_window_rows!(page) do
    cursor = page.after || %{window_start_at: nil, window_id: nil}

    SQL.query!(
      Repo,
      """
          WITH selected AS MATERIALIZED (
            SELECT root_execution_group_id
            FROM favn_control.runs
            WHERE workspace_id = $1 AND run_id = $2
          )
          SELECT run_window.window_id, run_window.run_id, run_window.status,
                 run_window.window_start, run_window.window_end, backfill.target_id,
                 CASE WHEN run.terminal_at IS NULL THEN NULL
                      ELSE GREATEST((extract(epoch FROM (run.terminal_at - run.inserted_at)) * 1000)::bigint, 0)
                 END AS duration_ms,
                 COALESCE(counts.total, 0), COALESCE(counts.succeeded, 0),
                 COALESCE(counts.skipped, 0), COALESCE(counts.failed, 0),
                 COALESCE(counts.running, 0), COALESCE(counts.queued, 0),
                 COALESCE(counts.planned, 0)
          FROM selected
          JOIN favn_control.backfills AS backfill
            ON backfill.workspace_id = $1
           AND backfill.root_run_id = selected.root_execution_group_id
          JOIN favn_control.backfill_windows AS run_window
            ON run_window.workspace_id = backfill.workspace_id
           AND run_window.backfill_id = backfill.backfill_id
          LEFT JOIN favn_control.runs AS run
            ON run.workspace_id = run_window.workspace_id AND run.run_id = run_window.run_id
          LEFT JOIN LATERAL (
            SELECT count(*)::bigint AS total,
                   count(*) FILTER (WHERE status = 'ok')::bigint AS succeeded,
                   count(*) FILTER (WHERE status = 'skipped_fresh')::bigint AS skipped,
                   count(*) FILTER (WHERE status IN ('error', 'timed_out', 'cancelled', 'blocked'))::bigint AS failed,
                   count(*) FILTER (WHERE status IN ('running', 'retrying'))::bigint AS running,
                   count(*) FILTER (WHERE status = 'queued')::bigint AS queued,
                   count(*) FILTER (WHERE status = 'planned')::bigint AS planned
            FROM favn_control.asset_attempt_overviews
            WHERE workspace_id = $1 AND run_id = run_window.run_id
          ) AS counts ON TRUE
          WHERE ($3::timestamptz IS NULL OR run_window.window_start < $3 OR
                 (run_window.window_start = $3 AND run_window.window_id < $4))
          ORDER BY run_window.window_start DESC, run_window.window_id DESC
          LIMIT $5
      """,
      [
        page.workspace_context.workspace_id,
        page.run_id,
        cursor.window_start_at,
        cursor.window_id,
        page.limit + 1
      ]
    )
  end

  @impl true
  def page_run_event_summaries(%PageRunEventSummaries{} = page) do
    with :ok <- validate_run_event_summary_page(page),
         {:ok, {rows, total, projection_cursor}} <-
           run_flow_transaction(fn ->
             %{rows: rows} = run_event_summary_rows!(page)
             {total, projection_cursor} = run_event_summary_metadata!(page)
             {rows, total, projection_cursor}
           end) do
      has_more? = length(rows) > page.limit
      rows = Enum.take(rows, page.limit)

      items = Enum.map(rows, &run_event_summary/1)
      next_cursor = if has_more?, do: List.last(items).event_id, else: nil

      {:ok,
       %RunSummaryPage{
         items: items,
         total: total,
         has_more?: has_more?,
         next_cursor: next_cursor,
         projection_cursor: projection_cursor
       }}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  defp run_event_summary_metadata!(page) do
    %{rows: [[total, projection_cursor]]} =
      SQL.query!(
        Repo,
        """
        SELECT count(*)::bigint,
               COALESCE((
                 SELECT last_publication_id
                 FROM favn_control.projection_cursors
                 WHERE projector_name = 'control_plane_v1' AND shard_id = 0
               ), 0)::bigint
        FROM favn_control.runs AS selected
        JOIN favn_control.runs AS member
          ON member.workspace_id = selected.workspace_id
         AND member.root_execution_group_id = selected.root_execution_group_id
        JOIN favn_control.run_events AS event
          ON event.workspace_id = member.workspace_id AND event.run_id = member.run_id
        WHERE selected.workspace_id = $1 AND selected.run_id = $2
        """,
        [page.workspace_context.workspace_id, page.run_id]
      )

    {total, projection_cursor}
  end

  defp run_event_summary_rows!(page) do
    SQL.query!(
      Repo,
      """
          WITH selected AS MATERIALIZED (
            SELECT root_execution_group_id
            FROM favn_control.runs
            WHERE workspace_id = $1 AND run_id = $2
          )
          SELECT event.event_id, event.run_id, event.sequence, event.occurred_at,
                 event.event_type, event.asset_step_id, event.status, event.stage,
                 concat_ws(' · ', replace(event.event_type, '_', ' '), event.status)
          FROM selected
          JOIN favn_control.runs AS member
            ON member.workspace_id = $1
           AND member.root_execution_group_id = selected.root_execution_group_id
          JOIN favn_control.run_events AS event
            ON event.workspace_id = member.workspace_id AND event.run_id = member.run_id
          WHERE ($3::bigint IS NULL OR event.event_id < $3)
          ORDER BY event.event_id DESC
          LIMIT $4
      """,
      [page.workspace_context.workspace_id, page.run_id, page.after_event_id, page.limit + 1]
    )
  end

  @impl true
  def page_group_runs(%PageGroupRuns{} = page) do
    with :ok <- validate_group_runs(page) do
      query =
        Run
        |> where(
          [run],
          run.workspace_id == ^page.workspace_context.workspace_id and
            run.root_execution_group_id == ^page.root_run_id
        )
        |> after_group_run(page.after)
        |> order_by([run], desc: run.submitted_event_id, desc: run.run_id)
        |> select_run_summary()
        |> limit(^(page.limit + 1))

      rows = Repo.all(query)
      run_page(rows, page.limit)
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def page_group_windows(%PageGroupWindows{} = page) do
    with :ok <- validate_group_windows(page) do
      workspace_id = page.workspace_context.workspace_id

      query =
        from(window in BackfillWindow,
          join: backfill in Backfill,
          on:
            backfill.workspace_id == window.workspace_id and
              backfill.backfill_id == window.backfill_id,
          where:
            backfill.workspace_id == ^workspace_id and
              backfill.root_run_id == ^page.root_run_id,
          order_by: [desc: window.window_start, desc: window.window_id],
          limit: ^(page.limit + 1),
          select: window
        )
        |> after_group_window(page.after)

      rows = Repo.all(query)
      {:ok, cursor_page(rows, page.limit, &window_result/1, &window_cursor/1)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def get_target_statuses(%GetTargetStatuses{} = query) do
    with :ok <- validate_target_statuses(query) do
      workspace_id = query.workspace_context.workspace_id
      target_kind = Atom.to_string(query.target_kind)

      rows =
        from(status in TargetStatus,
          where:
            status.workspace_id == ^workspace_id and
              status.target_kind == ^target_kind and status.target_id in ^query.target_ids,
          distinct: status.target_id,
          order_by: [asc: status.target_id, desc: status.source_publication_id],
          select: status
        )
        |> Repo.all()
        |> Map.new(&{&1.target_id, &1})

      {:ok,
       query.target_ids
       |> Enum.map(&Map.get(rows, &1))
       |> Enum.reject(&is_nil/1)
       |> Enum.map(&target_status_result/1)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def page_target_runs(%PageTargetRuns{} = page) do
    with :ok <- validate_target_runs(page) do
      after_event_id = page.after && page.after.submitted_event_id
      after_root_run_id = page.after && page.after.run_id

      %{rows: rows} =
        SQL.query!(
          Repo,
          """
          WITH candidate_roots AS MATERIALIZED (
            SELECT root.*
            FROM favn_control.runs AS root
            WHERE root.workspace_id = $1
              AND root.run_id = root.root_execution_group_id
              AND ($4::bigint IS NULL
                   OR root.submitted_event_id < $4
                   OR (root.submitted_event_id = $4 AND root.run_id < $5))
              AND EXISTS (
                SELECT 1
                FROM favn_control.runs AS member
                JOIN favn_control.run_targets AS target
                  ON target.workspace_id = member.workspace_id
                 AND target.run_id = member.run_id
                WHERE member.workspace_id = root.workspace_id
                  AND member.root_execution_group_id = root.run_id
                  AND target.target_kind = $2
                  AND target.target_id = $3
              )
            ORDER BY root.submitted_event_id DESC, root.run_id DESC
            LIMIT $6
          )
          SELECT selected.workspace_id,
                 selected.run_id,
                 selected.root_execution_group_id,
                 selected.parent_run_id,
                 selected.rerun_of_run_id,
                 selected.deployment_id,
                 selected.manifest_version_id,
                 manifest.runner_releases,
                 CASE coalesce(overview.status, root.status)
                   WHEN 'succeeded' THEN 'ok'
                   WHEN 'failed' THEN 'error'
                   ELSE coalesce(overview.status, root.status)
                 END AS status,
                 root.submit_kind,
                 root.trigger_type,
                 selected.event_sequence,
                 root.submitted_event_id,
                 coalesce(overview.latest_event_id, root.latest_event_id),
                 coalesce(overview.started_at, root.inserted_at),
                 coalesce(overview.updated_at, root.updated_at),
                 coalesce(overview.finished_at, root.terminal_at)
          FROM candidate_roots AS root
          JOIN LATERAL (
            SELECT member.*
            FROM favn_control.runs AS member
            JOIN favn_control.run_targets AS target
              ON target.workspace_id = member.workspace_id
             AND target.run_id = member.run_id
            WHERE member.workspace_id = root.workspace_id
              AND member.root_execution_group_id = root.run_id
              AND target.target_kind = $2
              AND target.target_id = $3
            ORDER BY (member.run_id <> root.run_id) DESC,
                     target.submitted_event_id DESC,
                     member.run_id DESC
            LIMIT 1
          ) AS selected ON true
          JOIN favn_control.manifest_versions AS manifest
            ON manifest.manifest_version_id = selected.manifest_version_id
          LEFT JOIN favn_control.execution_group_overviews AS overview
            ON overview.workspace_id = root.workspace_id
           AND overview.root_run_id = root.run_id
          ORDER BY root.submitted_event_id DESC, root.run_id DESC
          """,
          [
            page.workspace_context.workspace_id,
            Atom.to_string(page.target_kind),
            page.target_id,
            after_event_id,
            after_root_run_id,
            page.limit + 1
          ]
        )

      target_run_page(rows, page.limit)
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def get_freshness_many(%GetFreshnessMany{} = query) do
    with :ok <- validate_freshness(query) do
      {generation_ids, target_ids, freshness_keys} = freshness_arrays(query.identities)

      %{rows: rows} =
        SQL.query!(
          Repo,
          """
          WITH requested AS (
            SELECT *
            FROM unnest($2::text[], $3::text[], $4::text[])
              AS item(evidence_generation_id, target_id, freshness_key)
          )
          SELECT state.workspace_id, state.evidence_generation_id, state.deployment_id,
                 state.manifest_version_id, state.target_id, state.freshness_key,
                 state.latest_attempt_materialization_id,
                 state.latest_success_materialization_id, state.status, state.payload,
                 state.source_publication_id, state.updated_at
          FROM requested
          JOIN favn_control.asset_freshness_states state
            ON state.workspace_id = $1
           AND state.evidence_generation_id = requested.evidence_generation_id
           AND state.target_id = requested.target_id
           AND state.freshness_key = requested.freshness_key
          """,
          [
            query.workspace_context.workspace_id,
            generation_ids,
            target_ids,
            freshness_keys
          ]
        )

      indexed = Map.new(rows, fn row -> {freshness_identity(row), freshness_result(row)} end)

      {:ok,
       query.identities
       |> Enum.map(
         &Map.get(indexed, {
           &1.evidence_generation_id,
           &1.target_id,
           &1.freshness_key
         })
       )
       |> Enum.reject(&is_nil/1)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def get_asset_window_states(%GetAssetWindowStates{} = query) do
    with :ok <- validate_asset_detail_state(query) do
      workspace_id = query.workspace_context.workspace_id

      window_states =
        from(state in AssetWindowState,
          where:
            state.workspace_id == ^workspace_id and
              state.evidence_generation_id == ^query.evidence_generation_id and
              state.target_id == ^query.target_id,
          order_by: [desc: state.window_start, desc: state.window_key],
          limit: ^query.limit
        )
        |> Repo.all()
        |> Enum.map(&asset_window_result/1)

      {:ok, window_states}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def count_successful_asset_windows(%CountSuccessfulAssetWindows{} = query) do
    with :ok <- validate_coverage_count(query) do
      count =
        from(state in AssetWindowState,
          where:
            state.workspace_id == ^query.workspace_context.workspace_id and
              state.evidence_generation_id == ^query.evidence_generation_id and
              state.target_id == ^query.target_id and state.status == "succeeded" and
              state.window_start >= ^query.first_window_start and
              state.window_start <= ^query.last_window_start,
          select: count(state.window_key)
        )
        |> Repo.one()

      {:ok, count}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def get_successful_asset_window_keys(%GetSuccessfulAssetWindowKeys{} = query) do
    with :ok <- validate_coverage_keys(query) do
      keys =
        from(state in AssetWindowState,
          where:
            state.workspace_id == ^query.workspace_context.workspace_id and
              state.evidence_generation_id == ^query.evidence_generation_id and
              state.target_id == ^query.target_id and state.status == "succeeded" and
              state.window_key in ^query.window_keys,
          select: state.window_key
        )
        |> Repo.all()

      {:ok, keys}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  defp failure_page(context, root_run_id, limit) do
    rows =
      from(run in Run,
        where:
          run.workspace_id == ^context.workspace_id and
            run.root_execution_group_id == ^root_run_id and
            run.status in ["error", "cancelled", "timed_out"],
        order_by: [desc: run.latest_event_id, desc: run.run_id],
        limit: ^(limit + 1)
      )
      |> select_run_summary()
      |> Repo.all()

    run_page(rows, limit)
  end

  defp run_page(rows, limit) do
    page_rows = Enum.take(rows, limit)
    has_more? = length(rows) > limit

    {:ok,
     %CursorPage{
       items: Enum.map(page_rows, &run_result/1),
       limit: limit,
       has_more?: has_more?,
       next_cursor: if(has_more? and page_rows != [], do: run_cursor(List.last(page_rows)))
     }}
  end

  defp target_run_page(rows, limit) do
    page_rows = Enum.take(rows, limit)
    has_more? = length(rows) > limit

    items = Enum.map(page_rows, &target_group_run_result/1)

    next_cursor =
      if has_more? and page_rows != [] do
        row = List.last(page_rows)
        %{submitted_event_id: Enum.at(row, 12), run_id: Enum.at(row, 2)}
      end

    {:ok,
     %CursorPage{
       items: items,
       limit: limit,
       has_more?: has_more?,
       next_cursor: next_cursor
     }}
  end

  defp target_group_run_result(row) do
    %RunSummary{
      workspace_id: Enum.at(row, 0),
      run_id: Enum.at(row, 1),
      root_run_id: Enum.at(row, 2),
      parent_run_id: Enum.at(row, 3),
      rerun_of_run_id: Enum.at(row, 4),
      deployment_id: Enum.at(row, 5),
      manifest_version_id: Enum.at(row, 6),
      runner_releases: Enum.at(row, 7),
      status: RunEnum.decode!(:status, Enum.at(row, 8)),
      submit_kind: RunEnum.decode!(:submit_kind, Enum.at(row, 9)),
      trigger_type: RunEnum.decode!(:trigger_type, Enum.at(row, 10)),
      event_sequence: Enum.at(row, 11),
      submitted_event_id: Enum.at(row, 12),
      latest_event_id: Enum.at(row, 13),
      inserted_at: Enum.at(row, 14),
      updated_at: Enum.at(row, 15),
      terminal_at: Enum.at(row, 16)
    }
  end

  defp cursor_page(rows, limit, mapper, cursor) do
    page_rows = Enum.take(rows, limit)
    items = Enum.map(page_rows, mapper)
    has_more? = length(rows) > limit

    %CursorPage{
      items: items,
      limit: limit,
      has_more?: has_more?,
      next_cursor: if(has_more? and page_rows != [], do: cursor.(List.last(page_rows)))
    }
  end

  defp manifest_result(manifest) do
    %ManifestSummary{
      manifest_version_id: manifest.manifest_version_id,
      content_hash: Base.encode16(manifest.content_hash, case: :lower),
      schema_version: manifest.schema_version,
      runner_contract_version: manifest.runner_contract_version,
      runner_releases: manifest.runner_releases,
      inserted_at: manifest.inserted_at
    }
  end

  defp group_result(group, targets, assets) do
    key = {group.workspace_id, group.root_run_id}
    by_kind = Map.get(targets, key, %{})

    %GroupOverviewResult{
      workspace_id: group.workspace_id,
      root_run_id: group.root_run_id,
      status: String.to_existing_atom(group.status),
      run_count: group.run_count,
      pending_count: group.pending_count,
      running_count: group.running_count,
      succeeded_count: group.succeeded_count,
      failed_count: group.failed_count,
      latest_event_id: group.latest_event_id,
      source_publication_id: group.source_publication_id,
      updated_at: group.updated_at,
      # A trigger this release does not know costs the row its trigger, not the
      # caller its page: one unrecognised value must not fail a list of fifty.
      trigger_type: RunEnum.decode(:trigger_type, group.trigger_type),
      started_at: group.started_at,
      finished_at: group.finished_at,
      target_refs: Map.get(by_kind, "asset", []),
      pipeline_refs: Map.get(by_kind, "pipeline", []),
      asset_counts: Map.get(assets, key, @no_asset_counts)
    }
  end

  defp run_result(row) do
    %RunSummary{
      workspace_id: row.workspace_id,
      run_id: row.run_id,
      root_run_id: row.root_execution_group_id,
      parent_run_id: row.parent_run_id,
      deployment_id: row.deployment_id,
      manifest_version_id: row.manifest_version_id,
      runner_releases: row.runner_releases,
      status: RunEnum.decode!(:status, row.status),
      submit_kind: RunEnum.decode!(:submit_kind, row.submit_kind),
      trigger_type: RunEnum.decode(:trigger_type, row.trigger_type),
      submitted_event_id: row.submitted_event_id,
      latest_event_id: row.latest_event_id,
      event_sequence: row.event_sequence,
      inserted_at: row.inserted_at,
      updated_at: row.updated_at,
      terminal_at: row.terminal_at,
      rerun_of_run_id: row.rerun_of_run_id
    }
  end

  defp get_run_summary(workspace_id, run_id) do
    Run
    |> where([run], run.workspace_id == ^workspace_id and run.run_id == ^run_id)
    |> select_run_summary()
    |> Repo.one()
  end

  defp select_run_summary(query) do
    query
    |> join(:inner, [run], manifest in ManifestVersion,
      on: manifest.manifest_version_id == run.manifest_version_id
    )
    |> select(
      [run, manifest],
      merge(map(run, ^@run_summary_fields), %{
        runner_releases: manifest.runner_releases
      })
    )
  end

  defp window_result(window) do
    %BackfillWindowResult{
      workspace_id: window.workspace_id,
      backfill_id: window.backfill_id,
      window_id: window.window_id,
      window_key: window.window_key,
      window_start: window.window_start,
      window_end: window.window_end,
      status: String.to_existing_atom(window.status),
      claim_owner: window.claim_owner,
      fencing_token: window.fencing_token,
      claim_expires_at: window.claim_expires_at,
      run_id: window.run_id,
      attempt_count: window.attempt_count,
      last_error: window.last_error,
      payload: window.payload,
      version: window.version
    }
  end

  defp target_status_result(status) do
    %TargetStatusResult{
      workspace_id: status.workspace_id,
      deployment_id: status.deployment_id,
      target_kind: String.to_existing_atom(status.target_kind),
      target_id: status.target_id,
      status: String.to_existing_atom(status.status),
      run_id: status.run_id,
      event_id: status.event_id,
      source_publication_id: status.source_publication_id,
      updated_at: status.updated_at
    }
  end

  defp compact_attempts(workspace_id, root_run_id, limit) do
    rows =
      from(attempt in AssetAttemptOverview,
        where:
          attempt.workspace_id == ^workspace_id and attempt.root_run_id == ^root_run_id and
            attempt.status != "planned",
        order_by: [asc: attempt.window_identity, asc: attempt.asset_ref, asc: attempt.run_id],
        limit: ^(limit + 1)
      )
      |> Repo.all()

    {rows |> Enum.take(limit) |> Enum.map(&attempt_result/1), length(rows) > limit}
  end

  defp compact_planned_steps(workspace_id, root_run_id, limit) do
    %{rows: rows} =
      SQL.query!(
        Repo,
        """
        SELECT plan.run_id, run.root_execution_group_id, node.value
        FROM favn_control.run_plans AS plan
        JOIN favn_control.runs AS run
          ON run.workspace_id = plan.workspace_id
         AND run.run_id = plan.run_id
        CROSS JOIN LATERAL jsonb_array_elements(plan.plan->'nodes') AS node(value)
        WHERE run.workspace_id = $1
          AND run.root_execution_group_id = $2
        ORDER BY plan.run_id, node.value->'node_key', node.value->'ref'
        LIMIT $3
        """,
        [workspace_id, root_run_id, limit + 1]
      )

    page_rows = Enum.take(rows, limit)
    {Enum.map(page_rows, &planned_step_result/1), length(rows) > limit}
  end

  defp planned_step_result([run_id, root_run_id, node]) do
    window = restore_window(Map.get(node, "window"))

    %PlannedAssetStep{
      root_run_id: root_run_id,
      run_id: run_id,
      node_identity: planned_node_identity(Map.get(node, "node_key")),
      asset_ref: ref_text(Map.get(node, "ref")),
      window_identity: planned_window_identity(window),
      window: window,
      stage: Map.get(node, "stage"),
      execution_pool: Map.get(node, "execution_pool")
    }
  end

  defp planned_node_identity(node_key) do
    node_key
    |> Jason.encode!()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.url_encode64(padding: false)
  end

  defp planned_window_identity(nil), do: "none"
  defp planned_window_identity(%{key: key}) when is_binary(key), do: key

  defp planned_window_identity(window) do
    window
    |> :erlang.term_to_binary([:deterministic])
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.url_encode64(padding: false)
  end

  defp ref_text(%{"module" => module, "name" => name})
       when is_binary(module) and is_binary(name),
       do: module <> ":" <> name

  defp ref_text(value) when is_binary(value), do: value
  defp ref_text(_value), do: "unknown"

  defp compact_asset_counts_by_run(workspace_id, root_run_id, run_ids) do
    attempts =
      from(attempt in AssetAttemptOverview,
        where:
          attempt.workspace_id == ^workspace_id and attempt.root_run_id == ^root_run_id and
            attempt.run_id in ^run_ids and attempt.status != "planned",
        group_by: attempt.run_id,
        select: %{
          run_id: attempt.run_id,
          total: count(attempt.asset_step_id),
          completed: filter(count(), attempt.status in ^@completed_asset_statuses),
          succeeded: filter(count(), attempt.status == "ok"),
          skipped: filter(count(), attempt.status == "skipped_fresh"),
          failed: filter(count(), attempt.status in ^@failed_asset_statuses),
          running: filter(count(), attempt.status in ^@running_asset_statuses),
          queued: filter(count(), attempt.status == "queued")
        }
      )
      |> Repo.all()
      |> Map.new(fn row -> {row.run_id, Map.delete(row, :run_id)} end)

    %{rows: plan_rows} =
      SQL.query!(
        Repo,
        """
        SELECT plan.run_id,
               jsonb_array_length(COALESCE(plan.plan->'nodes', '[]'::jsonb))
        FROM favn_control.run_plans AS plan
        JOIN favn_control.runs AS run
          ON run.workspace_id = plan.workspace_id
         AND run.run_id = plan.run_id
        WHERE run.workspace_id = $1
          AND run.root_execution_group_id = $2
          AND plan.run_id = ANY($3::text[])
        """,
        [workspace_id, root_run_id, run_ids]
      )

    planned = Map.new(plan_rows, fn [run_id, total] -> {run_id, total} end)

    attempts
    |> Map.keys()
    |> Kernel.++(Map.keys(planned))
    |> Enum.uniq()
    |> Map.new(fn run_id ->
      attempted = Map.get(attempts, run_id, Map.delete(@no_asset_counts, :planned))
      total = max(Map.get(planned, run_id, 0), attempted.total)

      {run_id,
       attempted
       |> Map.put(:total, total)
       |> Map.put(:planned, max(total - attempted.total, 0))}
    end)
  end

  defp compact_attempt_counts(workspace_id, root_run_id) do
    observed =
      from(attempt in AssetAttemptOverview,
        where:
          attempt.workspace_id == ^workspace_id and attempt.root_run_id == ^root_run_id and
            attempt.status != "planned",
        select: %{
          total: count(attempt.asset_step_id),
          completed: filter(count(), attempt.status in ^@completed_asset_statuses),
          succeeded: filter(count(), attempt.status == "ok"),
          skipped: filter(count(), attempt.status == "skipped_fresh"),
          failed: filter(count(), attempt.status in ^@failed_asset_statuses),
          running: filter(count(), attempt.status in ^@running_asset_statuses),
          queued: filter(count(), attempt.status == "queued"),
          effective_windows:
            fragment(
              "count(DISTINCT ?) FILTER (WHERE ? <> 'none')",
              attempt.window_identity,
              attempt.window_identity
            )
        }
      )
      |> Repo.one!()

    planned = remaining_planned_asset_count(workspace_id, root_run_id)

    observed
    |> Map.put(:planned, planned)
    |> Map.update!(:total, &(&1 + planned))
  end

  defp remaining_planned_asset_count(workspace_id, root_run_id) do
    %{rows: [[count]]} =
      SQL.query!(
        Repo,
        """
        WITH attempt_counts AS (
          SELECT attempt.run_id, count(*) AS total
          FROM favn_control.asset_attempt_overviews AS attempt
          WHERE attempt.workspace_id = $1
            AND attempt.root_run_id = $2
            AND attempt.status <> 'planned'
          GROUP BY attempt.run_id
        )
        SELECT COALESCE(
                 sum(
                   GREATEST(
                     jsonb_array_length(COALESCE(plan.plan->'nodes', '[]'::jsonb)) -
                       COALESCE(attempt.total, 0),
                     0
                   )
                 ),
                 0
               )::bigint
        FROM favn_control.run_plans AS plan
        JOIN favn_control.runs AS run
          ON run.workspace_id = plan.workspace_id
         AND run.run_id = plan.run_id
        LEFT JOIN attempt_counts AS attempt
          ON attempt.run_id = plan.run_id
        WHERE run.workspace_id = $1
          AND run.root_execution_group_id = $2
        """,
        [workspace_id, root_run_id]
      )

    count
  end

  defp requested_window_counts(workspace_id, root_run_id) do
    row =
      from(backfill in Backfill,
        left_join: overview in BackfillOverview,
        on:
          overview.workspace_id == backfill.workspace_id and
            overview.backfill_id == backfill.backfill_id,
        where: backfill.workspace_id == ^workspace_id and backfill.root_run_id == ^root_run_id,
        select: {
          backfill.expected_window_count,
          overview.succeeded_count,
          overview.failed_count,
          overview.cancelled_count
        },
        limit: 1
      )
      |> Repo.one()

    case row do
      {total, succeeded, failed, cancelled} ->
        %{
          total: total || 0,
          completed: (succeeded || 0) + (failed || 0) + (cancelled || 0),
          failed: failed || 0
        }

      nil ->
        %{total: 0, completed: 0, failed: 0}
    end
  end

  # One query for a whole page of groups rather than one per row. The `in` pair is
  # a cross product of the page's workspaces and root runs, so it can return a
  # pair the page does not contain; that is harmless because every lookup is by
  # exact `{workspace_id, root_run_id}`, and the over-fetch is bounded by the page.
  defp target_refs_by_group([]), do: %{}

  # Pipelines come back alongside assets because a pipeline run's fourteen asset
  # targets do not say what the operator asked for; the pipeline does.
  defp target_refs_by_group(groups) do
    {workspace_ids, root_run_ids} = group_keys(groups)

    from(target in RunTarget,
      join: run in Run,
      on: run.workspace_id == target.workspace_id and run.run_id == target.run_id,
      where:
        run.workspace_id in ^workspace_ids and
          run.root_execution_group_id in ^root_run_ids and
          target.target_kind in ["asset", "pipeline"],
      select:
        {run.workspace_id, run.root_execution_group_id, target.target_kind, target.target_module,
         target.target_name},
      distinct: true
    )
    |> Repo.all()
    |> Enum.reduce(%{}, fn
      {workspace_id, root_run_id, kind, module, name}, acc
      when is_binary(module) and is_binary(name) ->
        ref = module <> ":" <> name

        Map.update(
          acc,
          {workspace_id, root_run_id},
          %{kind => [ref]},
          &Map.update(&1, kind, [ref], fn refs -> [ref | refs] end)
        )

      _incomplete_identity, acc ->
        acc
    end)
    |> Map.new(fn {key, by_kind} ->
      {key, Map.new(by_kind, fn {kind, refs} -> {kind, Enum.sort(refs)} end)}
    end)
  end

  defp asset_counts_by_group([]), do: %{}

  # What actually happened, per asset step, across every run in the group. A
  # backfill's windows each contribute their own steps, so this counts the work
  # rather than the submissions — which is the number a list of runs is read for.
  #
  # One grouped aggregate over the page's own groups, on the index whose leading
  # columns are exactly these two.
  defp asset_counts_by_group(groups) do
    {workspace_ids, root_run_ids} = group_keys(groups)

    from(attempt in AssetAttemptOverview,
      where:
        attempt.workspace_id in ^workspace_ids and
          attempt.root_run_id in ^root_run_ids,
      group_by: [attempt.workspace_id, attempt.root_run_id],
      select: %{
        workspace_id: attempt.workspace_id,
        root_run_id: attempt.root_run_id,
        total: count(),
        completed: filter(count(), attempt.status in ^@completed_asset_statuses),
        failed: filter(count(), attempt.status in ^@failed_asset_statuses),
        running: filter(count(), attempt.status in ^@running_asset_statuses),
        queued: filter(count(), attempt.status == "queued")
      }
    )
    |> Repo.all()
    |> Map.new(fn row ->
      {{row.workspace_id, row.root_run_id}, Map.drop(row, [:workspace_id, :root_run_id])}
    end)
  end

  defp group_keys(groups) do
    {
      groups |> Enum.map(& &1.workspace_id) |> Enum.uniq(),
      groups |> Enum.map(& &1.root_run_id) |> Enum.uniq()
    }
  end

  defp target_refs(workspace_id, root_run_id) do
    from(target in RunTarget,
      join: run in Run,
      on: run.workspace_id == target.workspace_id and run.run_id == target.run_id,
      where:
        run.workspace_id == ^workspace_id and run.root_execution_group_id == ^root_run_id and
          target.target_kind == "asset",
      select: {target.target_module, target.target_name},
      distinct: true
    )
    |> Repo.all()
    |> Enum.flat_map(fn
      {module, name} when is_binary(module) and is_binary(name) -> [module <> ":" <> name]
      _incomplete_identity -> []
    end)
    |> Enum.sort()
  end

  defp attempt_result(attempt) do
    %AssetAttemptResult{
      workspace_id: attempt.workspace_id,
      root_run_id: attempt.root_run_id,
      run_id: attempt.run_id,
      asset_step_id: attempt.asset_step_id,
      asset_ref: attempt.asset_ref,
      window_identity: attempt.window_identity,
      window: restore_window(attempt.window),
      status: String.to_existing_atom(attempt.status),
      stage: attempt.stage,
      attempt_number: attempt.attempt_number,
      execution_pool: attempt.execution_pool,
      queue_reason: attempt.queue_reason,
      started_at: attempt.started_at,
      finished_at: attempt.finished_at,
      duration_ms: attempt.duration_ms,
      error: attempt.error,
      output_metadata: attempt.output_metadata,
      source_publication_id: attempt.source_publication_id,
      updated_at: attempt.updated_at
    }
  end

  defp restore_window(nil), do: nil

  defp restore_window(%{"__type__" => "window_runtime"} = window) do
    with kind when not is_nil(kind) <- known_window_kind(map_field(window, "kind")),
         timezone when is_binary(timezone) <- map_field(window, "timezone"),
         {:ok, start_at} <- ExactDateTimeCodec.decode(map_field(window, "start_at")),
         {:ok, end_at} <- ExactDateTimeCodec.decode(map_field(window, "end_at")),
         key when is_binary(key) <- encoded_window_key(map_field(window, "key")),
         {:ok, decoded_key} <- WindowKey.decode(key),
         {:ok, expected_key} <- WindowKey.new(kind, start_at, timezone),
         true <- decoded_key == expected_key,
         :lt <- DateTime.compare(start_at, end_at) do
      %{
        key: key,
        label: nil,
        kind: kind,
        start_at: start_at,
        end_at: end_at,
        timezone: timezone
      }
    else
      reason -> raise ArgumentError, "invalid persisted runtime window: #{inspect(reason)}"
    end
  end

  defp restore_window(window) when is_map(window) do
    %{
      key: map_field(window, "key"),
      label: map_field(window, "label"),
      kind: known_window_kind(map_field(window, "kind")),
      start_at: parsed_datetime(map_field(window, "start_at")),
      end_at: parsed_datetime(map_field(window, "end_at")),
      timezone: map_field(window, "timezone")
    }
  end

  defp encoded_window_key(%{"__type__" => "window_key", "value" => value})
       when is_binary(value),
       do: value

  defp encoded_window_key(_value), do: nil

  defp known_window_kind("hour"), do: :hour
  defp known_window_kind("day"), do: :day
  defp known_window_kind("month"), do: :month
  defp known_window_kind("year"), do: :year
  defp known_window_kind(value) when value in [:hour, :day, :month, :year], do: value
  defp known_window_kind(_value), do: nil

  defp parsed_datetime(%DateTime{} = value), do: value

  defp parsed_datetime(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, parsed, _offset} -> parsed
      _error -> nil
    end
  end

  defp parsed_datetime(_value), do: nil

  defp map_field(map, key), do: Map.get(map, key) || Map.get(map, String.to_existing_atom(key))

  defp freshness_result([
         workspace_id,
         evidence_generation_id,
         deployment_id,
         manifest_version_id,
         target_id,
         freshness_key,
         latest_attempt,
         latest_success,
         status,
         payload,
         publication_id,
         updated_at
       ]) do
    %FreshnessResult{
      workspace_id: workspace_id,
      evidence_generation_id: evidence_generation_id,
      deployment_id: deployment_id,
      manifest_version_id: manifest_version_id,
      target_id: target_id,
      freshness_key: freshness_key,
      latest_attempt_materialization_id: latest_attempt,
      latest_success_materialization_id: latest_success,
      status: String.to_existing_atom(status),
      payload: payload,
      source_publication_id: publication_id,
      updated_at: updated_at
    }
  end

  defp asset_window_result(state) do
    %AssetWindowResult{
      workspace_id: state.workspace_id,
      evidence_generation_id: state.evidence_generation_id,
      manifest_version_id: state.manifest_version_id,
      target_id: state.target_id,
      window_key: state.window_key,
      window_start: state.window_start,
      window_end: state.window_end,
      status: String.to_existing_atom(state.status),
      run_id: state.run_id,
      materialization_id: state.materialization_id,
      payload: state.payload,
      source_publication_id: state.source_publication_id,
      updated_at: state.updated_at
    }
  end

  defp freshness_identity([_workspace, generation, _deployment, _manifest, target, key | _rest]),
    do: {generation, target, key}

  defp freshness_arrays(identities) do
    Enum.reduce(identities, {[], [], []}, fn identity, {generations, targets, keys} ->
      {
        [identity.evidence_generation_id | generations],
        [identity.target_id | targets],
        [identity.freshness_key | keys]
      }
    end)
    |> then(fn {generations, targets, keys} ->
      {Enum.reverse(generations), Enum.reverse(targets), Enum.reverse(keys)}
    end)
  end

  defp manifest_cursor(row),
    do: %{inserted_at: row.inserted_at, manifest_version_id: row.manifest_version_id}

  defp group_cursor(group),
    do: %{
      started_at: group.started_at,
      workspace_id: group.workspace_id,
      root_run_id: group.root_run_id
    }

  defp run_cursor(row),
    do: %{submitted_event_id: row.submitted_event_id, run_id: row.run_id}

  defp window_cursor(row),
    do: %{window_start: row.window_start, window_id: row.window_id}

  defp after_manifest(query, nil), do: query

  defp after_manifest(query, %{inserted_at: inserted_at, manifest_version_id: id}) do
    where(
      query,
      [manifest],
      manifest.inserted_at < ^inserted_at or
        (manifest.inserted_at == ^inserted_at and manifest.manifest_version_id < ^id)
    )
  end

  # One table. When the group started, what triggered it, and when it finished are
  # projected onto the group row, so ordering, the window filters, and the keyset
  # all read the same index instead of joining every group in the workspace to its
  # root run and sorting the result.
  defp group_query, do: from(group in ExecutionGroupOverview, as: :group)

  defp group_scope(query, %WorkspaceContext{workspace_id: workspace_id}),
    do: where(query, [group: group], group.workspace_id == ^workspace_id)

  defp group_scope(query, %PlatformContext{}), do: query

  defp group_status(query, nil), do: query

  defp group_status(query, statuses) when is_list(statuses),
    do: where(query, [group: group], group.status in ^Enum.map(statuses, &Atom.to_string/1))

  defp group_status(query, status),
    do: where(query, [group: group], group.status == ^Atom.to_string(status))

  defp group_trigger(query, nil), do: query

  defp group_trigger(query, trigger_type),
    do: where(query, [group: group], group.trigger_type == ^trigger_type)

  defp group_started_after(query, nil), do: query

  defp group_started_after(query, %DateTime{} = after_at),
    do: where(query, [group: group], group.started_at >= ^after_at)

  defp group_started_before(query, nil), do: query

  defp group_started_before(query, %DateTime{} = before_at),
    do: where(query, [group: group], group.started_at < ^before_at)

  defp group_search(query, nil, _scope), do: query

  defp group_search(query, search, scope) do
    case String.trim(search) do
      "" ->
        query

      term ->
        pattern = "%" <> escape_like(term) <> "%"

        where(
          query,
          [group: group],
          ilike(group.root_run_id, ^pattern) or exists(matching_target(pattern, scope))
        )
    end
  end

  # Correlated on the group rather than on one run, so searching for an asset
  # finds the backfill that touched it in one of fifty windows.
  #
  # The workspace is also pinned as a constant, not only through the correlation:
  # PostgreSQL is free to de-correlate this into a hashed subplan, and when it does,
  # a correlated-only predicate is evaluated after the join — which turns one
  # keystroke into a scan of every run target in the deployment.
  defp matching_target(pattern, scope) do
    from(target in RunTarget,
      join: run in Run,
      on: run.workspace_id == target.workspace_id and run.run_id == target.run_id,
      where:
        run.workspace_id == parent_as(:group).workspace_id and
          run.root_execution_group_id == parent_as(:group).root_run_id,
      where: ilike(target.target_module, ^pattern) or ilike(target.target_name, ^pattern)
    )
    |> search_scope(scope)
    |> select([_target, _run], 1)
  end

  defp search_scope(query, %WorkspaceContext{workspace_id: workspace_id}),
    do: where(query, [target, run], target.workspace_id == ^workspace_id)

  defp search_scope(query, _scope), do: query

  defp escape_like(term) do
    term
    |> String.replace("\\", "\\\\")
    |> String.replace("%", "\\%")
    |> String.replace("_", "\\_")
  end

  # A group with no start instant yet — a projection that saw a child run's event
  # before its root's — sorts to the oldest end in both directions, which is what
  # "not started as far as we know" means. Ascending nulls first is the exact
  # reverse of descending nulls last, so both directions are the one index read
  # forwards or backwards.
  defp order_groups(query, :started_asc) do
    order_by(query, [group: group],
      asc_nulls_first: group.started_at,
      asc: group.workspace_id,
      asc: group.root_run_id
    )
  end

  defp order_groups(query, :started_desc) do
    order_by(query, [group: group],
      desc_nulls_last: group.started_at,
      desc: group.workspace_id,
      desc: group.root_run_id
    )
  end

  defp after_group(query, nil, _order), do: query

  # Ascending, the nulls are already behind the cursor, so the next page is
  # everything with a start instant beyond it.
  defp after_group(query, %{started_at: nil} = cursor, :started_asc) do
    where(
      query,
      [group: group],
      not is_nil(group.started_at) or
        (group.workspace_id > ^cursor.workspace_id or
           (group.workspace_id == ^cursor.workspace_id and
              group.root_run_id > ^cursor.root_run_id))
    )
  end

  defp after_group(query, cursor, :started_asc) do
    where(
      query,
      [group: group],
      group.started_at > ^cursor.started_at or
        (group.started_at == ^cursor.started_at and
           (group.workspace_id > ^cursor.workspace_id or
              (group.workspace_id == ^cursor.workspace_id and
                 group.root_run_id > ^cursor.root_run_id)))
    )
  end

  # Descending, the nulls are the tail, so a cursor inside them can only be
  # followed by more of them.
  defp after_group(query, %{started_at: nil} = cursor, :started_desc) do
    where(
      query,
      [group: group],
      is_nil(group.started_at) and
        (group.workspace_id < ^cursor.workspace_id or
           (group.workspace_id == ^cursor.workspace_id and
              group.root_run_id < ^cursor.root_run_id))
    )
  end

  defp after_group(query, cursor, :started_desc) do
    where(
      query,
      [group: group],
      is_nil(group.started_at) or group.started_at < ^cursor.started_at or
        (group.started_at == ^cursor.started_at and
           (group.workspace_id < ^cursor.workspace_id or
              (group.workspace_id == ^cursor.workspace_id and
                 group.root_run_id < ^cursor.root_run_id)))
    )
  end

  defp encoded_trigger(nil), do: {:ok, nil}

  defp encoded_trigger(trigger_type) do
    case RunEnum.encode(:trigger_type, trigger_type) do
      {:ok, encoded} -> {:ok, encoded}
      :error -> {:error, ErrorMapper.map(:invalid)}
    end
  end

  defp after_group_run(query, nil), do: query

  defp after_group_run(query, cursor) do
    where(
      query,
      [run],
      run.submitted_event_id < ^cursor.submitted_event_id or
        (run.submitted_event_id == ^cursor.submitted_event_id and run.run_id < ^cursor.run_id)
    )
  end

  defp after_group_window(query, nil), do: query

  defp after_group_window(query, cursor) do
    where(
      query,
      [window, _backfill],
      window.window_start < ^cursor.window_start or
        (window.window_start == ^cursor.window_start and window.window_id < ^cursor.window_id)
    )
  end

  defp run_flow_transaction(fun) do
    if Repo.in_transaction?() do
      SQL.query!(Repo, "SET LOCAL statement_timeout = '750ms'")
      SQL.query!(Repo, "SET LOCAL idle_in_transaction_session_timeout = '1500ms'")
      {:ok, fun.()}
    else
      Repo.transaction(
        fn ->
          SQL.query!(Repo, "SET TRANSACTION READ ONLY")
          SQL.query!(Repo, "SET LOCAL statement_timeout = '750ms'")
          SQL.query!(Repo, "SET LOCAL transaction_timeout = '1500ms'")
          SQL.query!(Repo, "SET LOCAL idle_in_transaction_session_timeout = '1500ms'")
          fun.()
        end,
        isolation: :repeatable_read,
        timeout: 1_500
      )
    end
  end

  defp run_flow_page!(query) do
    pattern = literal_prefix_pattern(query.asset_prefix)
    header = run_flow_header!(query, pattern)
    {items, extra?} = run_flow_rows!(query, pattern)

    {has_previous?, has_next?} =
      cond do
        query.before -> {extra?, true}
        query.after -> {true, extra?}
        true -> {false, extra?}
      end

    %RunFlowPage{
      header: header,
      items: items,
      has_previous?: has_previous?,
      has_next?: has_next?
    }
  end

  defp run_flow_delta!(query) do
    pattern = literal_prefix_pattern(query.asset_prefix)

    header =
      run_flow_header!(
        %GetRunFlowPage{
          workspace_context: query.workspace_context,
          run_id: query.run_id,
          asset_prefix: query.asset_prefix
        },
        pattern
      )

    through = min(query.through_publication_id, header.projection_cursor)
    after_cursor = query.after || %{source_publication_id: 0, asset_step_id: ""}

    %{rows: rows} =
      SQL.query!(
        Repo,
        """
        SELECT attempt.run_id, attempt.asset_step_id, attempt.target_id, attempt.asset_ref,
               attempt.status, attempt.stage, attempt.window_kind, attempt.window_start_at,
               attempt.window_end_at, attempt.window_timezone, attempt.started_at,
               attempt.finished_at, attempt.duration_ms, attempt.attempt_number,
               attempt.execution_pool, attempt.queue_reason, attempt.failure_summary,
               attempt.source_publication_id
        FROM unnest($3::text[]) AS loaded(asset_step_id)
        JOIN favn_control.asset_attempt_overviews AS attempt
          ON attempt.workspace_id = $1
         AND attempt.run_id = $2
         AND attempt.asset_step_id = loaded.asset_step_id
        WHERE attempt.source_publication_id > $4
          AND attempt.source_publication_id <= $5
          AND ($6::text IS NULL OR attempt.asset_ref COLLATE "C" LIKE $6 COLLATE "C" ESCAPE '\\')
          AND (attempt.source_publication_id > $7 OR
               (attempt.source_publication_id = $7 AND attempt.asset_step_id > $8))
        ORDER BY attempt.source_publication_id, attempt.asset_step_id
        LIMIT $9
        """,
        [
          query.workspace_context.workspace_id,
          query.run_id,
          query.asset_step_ids,
          query.after_publication_id,
          through,
          pattern,
          after_cursor.source_publication_id,
          after_cursor.asset_step_id,
          query.limit + 1
        ]
      )

    has_more? = length(rows) > query.limit
    rows = Enum.take(rows, query.limit)
    items = Enum.map(rows, &run_flow_step_row/1)

    next_cursor =
      case List.last(rows) do
        nil ->
          nil

        row ->
          %{
            source_publication_id: Enum.at(row, 17),
            asset_step_id: Enum.at(row, 1)
          }
      end

    %RunFlowDelta{
      header: header,
      items: items,
      through_publication_id: through,
      has_more?: has_more?,
      next_cursor: next_cursor
    }
  end

  defp run_flow_header!(query, pattern) do
    %{rows: rows} =
      SQL.query!(
        Repo,
        """
        WITH selected AS MATERIALIZED (
          SELECT run.run_id, run.root_execution_group_id, run.parent_run_id,
                 run.rerun_of_run_id, run.manifest_version_id, run.status,
                 run.trigger_type, run.inserted_at, run.updated_at, run.terminal_at
          FROM favn_control.runs AS run
          WHERE run.workspace_id = $1 AND run.run_id = $2
        ), counts AS (
          SELECT count(*)::bigint AS unfiltered_total,
                 count(*) FILTER (
                   WHERE $3::text IS NULL OR
                         attempt.asset_ref COLLATE "C" LIKE $3 COLLATE "C" ESCAPE '\\'
                 )::bigint AS filtered_total,
                 count(*) FILTER (WHERE attempt.status = 'planned' AND
                   ($3::text IS NULL OR attempt.asset_ref COLLATE "C" LIKE $3 COLLATE "C" ESCAPE '\\'))::bigint AS planned,
                 count(*) FILTER (WHERE attempt.status = 'queued' AND
                   ($3::text IS NULL OR attempt.asset_ref COLLATE "C" LIKE $3 COLLATE "C" ESCAPE '\\'))::bigint AS queued,
                 count(*) FILTER (WHERE attempt.status IN ('running', 'retrying') AND
                   ($3::text IS NULL OR attempt.asset_ref COLLATE "C" LIKE $3 COLLATE "C" ESCAPE '\\'))::bigint AS running,
                 count(*) FILTER (WHERE attempt.status = 'ok' AND
                   ($3::text IS NULL OR attempt.asset_ref COLLATE "C" LIKE $3 COLLATE "C" ESCAPE '\\'))::bigint AS succeeded,
                 count(*) FILTER (WHERE attempt.status = 'skipped_fresh' AND
                   ($3::text IS NULL OR attempt.asset_ref COLLATE "C" LIKE $3 COLLATE "C" ESCAPE '\\'))::bigint AS skipped,
                 count(*) FILTER (WHERE attempt.status IN ('error', 'timed_out', 'cancelled', 'blocked') AND
                   ($3::text IS NULL OR attempt.asset_ref COLLATE "C" LIKE $3 COLLATE "C" ESCAPE '\\'))::bigint AS failed,
                 count(*) FILTER (WHERE attempt.status IN ('ok', 'skipped_fresh', 'error', 'timed_out', 'cancelled', 'blocked') AND
                   ($3::text IS NULL OR attempt.asset_ref COLLATE "C" LIKE $3 COLLATE "C" ESCAPE '\\'))::bigint AS completed
          FROM favn_control.asset_attempt_overviews AS attempt
          WHERE attempt.workspace_id = $1 AND attempt.run_id = $2
        ), target AS (
          SELECT target.target_id,
                 concat_ws(':', target.target_module, target.target_name) AS target_label
          FROM favn_control.run_targets AS target
          WHERE target.workspace_id = $1 AND target.run_id = $2
          ORDER BY target.is_primary DESC, target.submitted_event_id, target.target_id
          LIMIT 1
        ), window_stats AS (
          SELECT count(flow_window.*)::bigint AS total,
                 count(*) FILTER (WHERE flow_window.status IN ('succeeded', 'failed', 'cancelled'))::bigint AS completed,
                 count(*) FILTER (WHERE flow_window.status = 'failed')::bigint AS failed,
                 count(*) FILTER (
                   WHERE flow_window.status = 'failed' AND NOT EXISTS (
                     SELECT 1 FROM favn_control.asset_attempt_overviews AS attempted
                     WHERE attempted.workspace_id = flow_window.workspace_id
                       AND attempted.run_id = flow_window.run_id
                       AND attempted.status <> 'planned'
                   )
                 )::bigint AS failure_total
          FROM selected
          JOIN favn_control.backfills AS backfill
            ON backfill.workspace_id = $1
           AND backfill.root_run_id = selected.root_execution_group_id
          JOIN favn_control.backfill_windows AS flow_window
            ON flow_window.workspace_id = backfill.workspace_id
           AND flow_window.backfill_id = backfill.backfill_id
        ), window_failures AS (
          SELECT COALESCE(jsonb_agg(failure.summary ORDER BY failure.window_start, failure.window_id), '[]'::jsonb) AS items
          FROM (
            SELECT flow_window.window_id, flow_window.run_id AS child_run_id,
                   flow_window.window_start, flow_window.window_end,
                   jsonb_build_object(
                     'window_id', flow_window.window_id,
                     'child_run_id', flow_window.run_id,
                     'window_start_at', flow_window.window_start,
                     'window_end_at', flow_window.window_end,
                     'error_summary', left(COALESCE(flow_window.last_error->>'message', flow_window.last_error->>'reason', 'Window failed'), 1024)
                   ) AS summary
            FROM selected
            JOIN favn_control.backfills AS backfill
              ON backfill.workspace_id = $1
             AND backfill.root_run_id = selected.root_execution_group_id
            JOIN favn_control.backfill_windows AS flow_window
              ON flow_window.workspace_id = backfill.workspace_id
             AND flow_window.backfill_id = backfill.backfill_id
            WHERE flow_window.status = 'failed'
              AND NOT EXISTS (
                SELECT 1 FROM favn_control.asset_attempt_overviews AS attempted
                WHERE attempted.workspace_id = flow_window.workspace_id
                  AND attempted.run_id = flow_window.run_id
                  AND attempted.status <> 'planned'
              )
            ORDER BY flow_window.window_start, flow_window.window_id
            LIMIT 10
          ) AS failure
        )
        SELECT selected.run_id, selected.root_execution_group_id,
               selected.parent_run_id, selected.rerun_of_run_id,
               selected.manifest_version_id, selected.status, selected.trigger_type,
               selected.inserted_at, selected.updated_at, selected.terminal_at,
               target.target_id, target.target_label,
               counts.unfiltered_total, counts.filtered_total, counts.planned,
               counts.queued, counts.running, counts.succeeded, counts.skipped,
               counts.failed, counts.completed,
               COALESCE((SELECT last_publication_id
                         FROM favn_control.projection_cursors
                         WHERE projector_name = 'control_plane_v1' AND shard_id = 0), 0)::bigint,
               EXISTS (
                 SELECT 1 FROM favn_control.maintenance_jobs AS readiness
                 WHERE readiness.workspace_id = $1
                   AND readiness.job_id = $4
                   AND readiness.job_kind = 'projection_missing_row_backfill'
                   AND readiness.status = 'completed'
                   AND readiness.configuration->>'projection' = 'asset_attempts'
                   AND readiness.configuration->>'version' = '2'
               ) AND NOT EXISTS (
                 SELECT 1 FROM favn_control.outbox_events AS pending_projection
                 WHERE pending_projection.workspace_id = $1
                   AND pending_projection.aggregate_id = $2
                   AND pending_projection.event_kind LIKE 'run.%'
                   AND (pending_projection.publication_id IS NULL OR
                        pending_projection.publication_id > COALESCE((
                          SELECT last_publication_id
                          FROM favn_control.projection_cursors
                          WHERE projector_name = 'control_plane_v1' AND shard_id = 0
                        ), 0))
               ),
               COALESCE(window_stats.total, 0), COALESCE(window_stats.completed, 0),
               COALESCE(window_stats.failed, 0), COALESCE(window_stats.failure_total, 0),
               window_failures.items
        FROM selected CROSS JOIN counts LEFT JOIN target ON TRUE
        LEFT JOIN window_stats ON TRUE CROSS JOIN window_failures
        """,
        [
          query.workspace_context.workspace_id,
          query.run_id,
          pattern,
          Readiness.ready_job_id(query.workspace_context.workspace_id)
        ]
      )

    case rows do
      [
        [
          run_id,
          root_run_id,
          parent_run_id,
          rerun_of_run_id,
          manifest_version_id,
          status,
          trigger_type,
          started_at,
          updated_at,
          finished_at,
          target_id,
          target_label,
          unfiltered_total,
          filtered_total,
          planned,
          queued,
          running,
          succeeded,
          skipped,
          failed,
          completed,
          projection_cursor,
          projection_ready?,
          total_windows,
          completed_windows,
          failed_windows,
          window_failure_total,
          window_failures
        ]
      ] ->
        if not projection_ready? do
          Repo.rollback(
            Error.new(:unavailable, "asset attempt projection is not ready", retryable?: true)
          )
        end

        %RunFlowHeader{
          run_id: run_id,
          root_run_id: root_run_id,
          parent_run_id: parent_run_id,
          rerun_of_run_id: rerun_of_run_id,
          manifest_version_id: manifest_version_id,
          status: String.to_existing_atom(status),
          trigger_type: optional_existing_atom(trigger_type),
          started_at: started_at,
          updated_at: updated_at,
          finished_at: finished_at,
          target_id: target_id,
          target_label: target_label,
          unfiltered_total: unfiltered_total,
          filtered_total: filtered_total,
          counts: %{
            total: filtered_total,
            planned: planned,
            queued: queued,
            running: running,
            succeeded: succeeded,
            skipped: skipped,
            failed: failed,
            completed: completed
          },
          projection_cursor: projection_cursor,
          window_counts: %{
            total: total_windows,
            completed: completed_windows,
            failed: failed_windows
          },
          window_failure_total: window_failure_total,
          window_failures: window_failures
        }

      [] ->
        Repo.rollback(Error.new(:not_found, "run not found"))
    end
  end

  defp run_flow_rows!(query, pattern) do
    cursor = query.after || query.before || %{asset_ref: nil, asset_step_id: nil}
    backward? = not is_nil(query.before)
    comparator = if backward?, do: "<", else: ">"
    direction = if backward?, do: "DESC", else: "ASC"

    %{rows: rows} =
      SQL.query!(
        Repo,
        """
        SELECT run_id, asset_step_id, target_id, asset_ref, status, stage,
               window_kind, window_start_at, window_end_at, window_timezone,
               started_at, finished_at, duration_ms, attempt_number,
               execution_pool, queue_reason, failure_summary, source_publication_id
        FROM favn_control.asset_attempt_overviews
        WHERE workspace_id = $1 AND run_id = $2
          AND ($3::text IS NULL OR asset_ref COLLATE "C" LIKE $3 COLLATE "C" ESCAPE '\\')
          AND ($4::text IS NULL OR
               (asset_ref COLLATE "C", asset_step_id COLLATE "C") #{comparator}
               ($4 COLLATE "C", $5 COLLATE "C"))
        ORDER BY asset_ref COLLATE "C" #{direction}, asset_step_id COLLATE "C" #{direction}
        LIMIT $6
        """,
        [
          query.workspace_context.workspace_id,
          query.run_id,
          pattern,
          cursor.asset_ref,
          cursor.asset_step_id,
          query.limit + 1
        ]
      )

    extra? = length(rows) > query.limit
    rows = Enum.take(rows, query.limit)
    rows = if backward?, do: Enum.reverse(rows), else: rows
    {Enum.map(rows, &run_flow_step_row/1), extra?}
  end

  defp run_flow_step_row([
         run_id,
         asset_step_id,
         target_id,
         asset_ref,
         status,
         stage,
         window_kind,
         window_start_at,
         window_end_at,
         window_timezone,
         started_at,
         finished_at,
         duration_ms,
         attempt_number,
         execution_pool,
         queue_reason,
         failure_summary,
         source_publication_id
       ]) do
    %RunFlowStep{
      run_id: run_id,
      asset_step_id: asset_step_id,
      target_id: target_id,
      asset_ref: asset_ref,
      status: String.to_existing_atom(status),
      stage: stage,
      window_kind: optional_existing_atom(window_kind),
      window_start_at: window_start_at,
      window_end_at: window_end_at,
      window_timezone: window_timezone,
      started_at: started_at,
      finished_at: finished_at,
      duration_ms: duration_ms,
      attempt_number: attempt_number,
      execution_pool: execution_pool,
      queue_reason: queue_reason,
      failure_summary: failure_summary,
      source_publication_id: source_publication_id
    }
  end

  defp run_flow_step(attempt) do
    run_flow_step_row([
      attempt.run_id,
      attempt.asset_step_id,
      attempt.target_id,
      attempt.asset_ref,
      attempt.status,
      attempt.stage,
      attempt.window_kind,
      attempt.window_start_at,
      attempt.window_end_at,
      attempt.window_timezone,
      attempt.started_at,
      attempt.finished_at,
      attempt.duration_ms,
      attempt.attempt_number,
      attempt.execution_pool,
      attempt.queue_reason,
      attempt.failure_summary,
      attempt.source_publication_id
    ])
  end

  defp literal_prefix_pattern(nil), do: nil
  defp literal_prefix_pattern(""), do: nil

  defp literal_prefix_pattern(prefix) do
    prefix
    |> String.replace("\\", "\\\\")
    |> String.replace("%", "\\%")
    |> String.replace("_", "\\_")
    |> Kernel.<>("%")
  end

  defp optional_existing_atom(nil), do: nil
  defp optional_existing_atom(value), do: String.to_existing_atom(value)

  defp validate_run_flow_page(query) do
    cursor? =
      Enum.all?([query.after, query.before], fn
        nil ->
          true

        %{asset_ref: asset_ref, asset_step_id: asset_step_id} ->
          valid_cursor_field?(asset_ref, 1_024) and valid_id?(asset_step_id)

        _other ->
          false
      end)

    valid? =
      workspace_context?(query.workspace_context) and valid_id?(query.run_id) and cursor? and
        (is_nil(query.after) or is_nil(query.before)) and valid_bound?(query.limit, 1, 200) and
        (is_nil(query.asset_prefix) or
           (is_binary(query.asset_prefix) and byte_size(query.asset_prefix) <= 128))

    if valid?, do: :ok, else: {:error, ErrorMapper.map(:invalid)}
  end

  defp valid_cursor_field?(value, max_bytes),
    do: is_binary(value) and value != "" and byte_size(value) <= max_bytes

  defp validate_run_flow_delta(query) do
    valid? =
      workspace_context?(query.workspace_context) and valid_id?(query.run_id) and
        is_list(query.asset_step_ids) and length(query.asset_step_ids) <= 500 and
        Enum.all?(query.asset_step_ids, &valid_id?/1) and
        (is_nil(query.asset_prefix) or
           (is_binary(query.asset_prefix) and byte_size(query.asset_prefix) <= 128)) and
        is_integer(query.after_publication_id) and query.after_publication_id >= 0 and
        is_integer(query.through_publication_id) and
        query.through_publication_id >= query.after_publication_id and
        valid_bound?(query.limit, 1, 200)

    if valid?, do: :ok, else: {:error, ErrorMapper.map(:invalid)}
  end

  defp run_window_summary([
         window_id,
         run_id,
         status,
         window_start_at,
         window_end_at,
         asset_ref,
         duration_ms,
         total,
         succeeded,
         skipped,
         failed,
         running,
         queued,
         planned
       ]) do
    %RunWindowSummary{
      window_id: window_id,
      run_id: run_id,
      status: String.to_existing_atom(status),
      window_start_at: window_start_at,
      window_end_at: window_end_at,
      asset_ref: asset_ref,
      duration_ms: duration_ms,
      counts: %{
        total: total,
        succeeded: succeeded,
        skipped: skipped,
        failed: failed,
        running: running,
        queued: queued,
        planned: planned
      }
    }
  end

  defp window_summary_cursor(summary),
    do: %{window_start_at: summary.window_start_at, window_id: summary.window_id}

  defp run_event_summary([
         event_id,
         run_id,
         sequence,
         occurred_at,
         event_type,
         asset_step_id,
         status,
         stage,
         summary
       ]) do
    %RunEventSummary{
      event_id: event_id,
      run_id: run_id,
      sequence: sequence,
      occurred_at: occurred_at,
      event_type: event_type,
      asset_step_id: asset_step_id,
      status: status,
      stage: stage,
      summary: String.slice(summary, 0, 1_024)
    }
  end

  defp validate_run_summary_page(page) do
    cursor? =
      is_nil(page.after) or
        match?(%{window_start_at: %DateTime{}, window_id: id} when is_binary(id), page.after)

    if workspace_context?(page.workspace_context) and valid_id?(page.run_id) and cursor? and
         valid_bound?(page.limit, 1, 50),
       do: :ok,
       else: {:error, ErrorMapper.map(:invalid)}
  end

  defp validate_run_event_summary_page(page) do
    if workspace_context?(page.workspace_context) and valid_id?(page.run_id) and
         (is_nil(page.after_event_id) or
            (is_integer(page.after_event_id) and page.after_event_id > 0)) and
         valid_bound?(page.limit, 1, 50),
       do: :ok,
       else: {:error, ErrorMapper.map(:invalid)}
  end

  defp validate_manifest_page(page) do
    cursor? =
      is_nil(page.after) or
        match?(
          %{inserted_at: %DateTime{}, manifest_version_id: id} when is_binary(id),
          page.after
        )

    if PlatformContext.valid?(page.platform_context) and cursor? and valid_limit?(page.limit),
      do: :ok,
      else: {:error, ErrorMapper.map(:invalid)}
  end

  defp validate_group_page(page) do
    # A cursor may carry no start instant, because the row it was taken from had
    # none. Rejecting that would make the tail of the list unpageable.
    cursor? =
      is_nil(page.after) or
        match?(
          %{started_at: started_at, workspace_id: workspace_id, root_run_id: run_id}
          when is_binary(workspace_id) and is_binary(run_id) and
                 (is_nil(started_at) or is_struct(started_at, DateTime)),
          page.after
        )

    narrowing? =
      valid_group_status?(page.status) and
        (is_nil(page.search) or is_binary(page.search)) and
        (is_nil(page.trigger_type) or is_atom(page.trigger_type)) and
        valid_instant?(page.started_after) and valid_instant?(page.started_before)

    if group_scope?(page.scope) do
      if narrowing? and page.order in @group_orders and cursor? and valid_limit?(page.limit),
        do: :ok,
        else: {:error, ErrorMapper.map(:invalid)}
    else
      {:error, ErrorMapper.map(:invalid)}
    end
  end

  defp validate_count_groups(query) do
    valid? =
      group_scope?(query.scope) and
        (is_nil(query.search) or is_binary(query.search)) and
        (is_nil(query.trigger_type) or is_atom(query.trigger_type)) and
        valid_instant?(query.started_after) and valid_instant?(query.started_before)

    if valid?, do: :ok, else: {:error, ErrorMapper.map(:invalid)}
  end

  defp group_scope?(scope),
    do: WorkspaceContext.valid?(scope) or PlatformContext.valid?(scope)

  defp valid_instant?(nil), do: true
  defp valid_instant?(%DateTime{}), do: true
  defp valid_instant?(_value), do: false

  defp valid_group_status?(nil), do: true

  defp valid_group_status?([_ | _] = statuses),
    do: Enum.all?(statuses, &(&1 in @group_statuses))

  defp valid_group_status?(status), do: status in @group_statuses

  defp validate_get_group(query) do
    if workspace_context?(query.workspace_context) and valid_id?(query.root_run_id) and
         valid_bound?(query.detail_limit, 1, 200),
       do: :ok,
       else: {:error, ErrorMapper.map(:invalid)}
  end

  defp validate_operator_run_overview(query) do
    if workspace_context?(query.workspace_context) and valid_id?(query.run_id) and
         valid_bound?(query.limit, 1, 500),
       do: :ok,
       else: {:error, ErrorMapper.map(:invalid)}
  end

  defp validate_group_runs(page) do
    cursor? =
      is_nil(page.after) or
        match?(
          %{submitted_event_id: event_id, run_id: run_id}
          when is_integer(event_id) and is_binary(run_id),
          page.after
        )

    if workspace_context?(page.workspace_context) and valid_id?(page.root_run_id) and cursor? and
         valid_limit?(page.limit),
       do: :ok,
       else: {:error, ErrorMapper.map(:invalid)}
  end

  defp validate_group_windows(page) do
    cursor? =
      is_nil(page.after) or
        match?(%{window_start: %DateTime{}, window_id: id} when is_binary(id), page.after)

    if workspace_context?(page.workspace_context) and valid_id?(page.root_run_id) and cursor? and
         valid_limit?(page.limit),
       do: :ok,
       else: {:error, ErrorMapper.map(:invalid)}
  end

  defp validate_target_statuses(query) do
    ids = query.target_ids

    if workspace_context?(query.workspace_context) and
         query.target_kind in [:asset, :pipeline] and valid_id_list?(ids),
       do: :ok,
       else: {:error, ErrorMapper.map(:invalid)}
  end

  defp validate_target_runs(page) do
    cursor? =
      is_nil(page.after) or
        match?(
          %{submitted_event_id: event_id, run_id: run_id}
          when is_integer(event_id) and is_binary(run_id),
          page.after
        )

    if workspace_context?(page.workspace_context) and
         page.target_kind in [:asset, :pipeline] and valid_id?(page.target_id) and cursor? and
         valid_limit?(page.limit),
       do: :ok,
       else: {:error, ErrorMapper.map(:invalid)}
  end

  defp validate_freshness(query) do
    identities = query.identities

    if workspace_context?(query.workspace_context) and is_list(identities) and identities != [] and
         length(identities) <= @max_batch and Enum.all?(identities, &freshness_identity?/1) and
         length(identities) == length(Enum.uniq_by(identities, &Map.from_struct/1)),
       do: :ok,
       else: {:error, ErrorMapper.map(:invalid)}
  end

  defp validate_asset_detail_state(query) do
    if workspace_context?(query.workspace_context) and
         valid_id?(query.evidence_generation_id) and valid_id?(query.target_id) and
         valid_limit?(query.limit),
       do: :ok,
       else: {:error, ErrorMapper.map(:invalid)}
  end

  defp validate_coverage_count(query) do
    if workspace_context?(query.workspace_context) and
         valid_id?(query.evidence_generation_id) and valid_id?(query.target_id) and
         match?(%DateTime{}, query.first_window_start) and
         match?(%DateTime{}, query.last_window_start) and
         DateTime.compare(query.first_window_start, query.last_window_start) != :gt,
       do: :ok,
       else: {:error, ErrorMapper.map(:invalid)}
  end

  defp validate_coverage_keys(query) do
    if workspace_context?(query.workspace_context) and
         valid_id?(query.evidence_generation_id) and valid_id?(query.target_id) and
         valid_id_list?(query.window_keys),
       do: :ok,
       else: {:error, ErrorMapper.map(:invalid)}
  end

  defp freshness_identity?(%FreshnessIdentity{} = identity),
    do:
      valid_id?(identity.evidence_generation_id) and valid_id?(identity.target_id) and
        valid_id?(identity.freshness_key)

  defp freshness_identity?(_other), do: false

  defp valid_id_list?(ids),
    do:
      is_list(ids) and ids != [] and length(ids) <= @max_batch and Enum.all?(ids, &valid_id?/1) and
        length(ids) == length(Enum.uniq(ids))

  defp workspace_context?(context), do: WorkspaceContext.valid?(context)

  defp valid_limit?(limit), do: valid_bound?(limit, 1, @max_batch)
  defp valid_bound?(value, min, max), do: is_integer(value) and value >= min and value <= max
  defp valid_id?(value), do: is_binary(value) and value != "" and byte_size(value) <= 255
end
