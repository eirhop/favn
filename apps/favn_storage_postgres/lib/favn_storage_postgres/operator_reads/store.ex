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
  alias FavnOrchestrator.Persistence.Queries.GetRunAssetAttempt
  alias FavnOrchestrator.Persistence.Queries.GetRunFlow
  alias FavnOrchestrator.Persistence.Queries.GetRunHeader
  alias FavnOrchestrator.Persistence.Queries.GetAssetWindowStates
  alias FavnOrchestrator.Persistence.Queries.GetFreshnessMany
  alias FavnOrchestrator.Persistence.Queries.GetTargetStatuses
  alias FavnOrchestrator.Persistence.Queries.ListRunEventSummaries
  alias FavnOrchestrator.Persistence.Queries.ListRunWindows
  alias FavnOrchestrator.Persistence.Queries.GetSuccessfulAssetWindowKeys
  alias FavnOrchestrator.Persistence.Queries.PageExecutionGroups
  alias FavnOrchestrator.Persistence.Queries.PageGroupRuns
  alias FavnOrchestrator.Persistence.Queries.PageGroupWindows
  alias FavnOrchestrator.Persistence.Queries.PageManifests
  alias FavnOrchestrator.Persistence.Queries.PageTargetRuns
  alias FavnOrchestrator.Persistence.RunEnum
  alias FavnOrchestrator.Persistence.Results.BackfillWindow, as: BackfillWindowResult
  alias FavnOrchestrator.Persistence.Results.AssetWindowState, as: AssetWindowResult
  alias FavnOrchestrator.Persistence.Results.CursorPage
  alias FavnOrchestrator.Persistence.Results.ExecutionGroup
  alias FavnOrchestrator.Persistence.Results.ExecutionGroupCounts
  alias FavnOrchestrator.Persistence.Results.ExecutionGroupOverview, as: GroupOverviewResult
  alias FavnOrchestrator.Persistence.Results.FreshnessState, as: FreshnessResult
  alias FavnOrchestrator.Persistence.Results.ManifestSummary
  alias FavnOrchestrator.Persistence.Results.RunAssetAttempt, as: RunAssetAttemptResult
  alias FavnOrchestrator.Persistence.Results.RunEventSummary
  alias FavnOrchestrator.Persistence.Results.RunFlowCandidate
  alias FavnOrchestrator.Persistence.Results.RunFlowSnapshot
  alias FavnOrchestrator.Persistence.Results.RunSummary
  alias FavnOrchestrator.Persistence.Results.RunViewHeader
  alias FavnOrchestrator.Persistence.Results.RunWindowChoice
  alias FavnOrchestrator.Persistence.Results.RunWindowChoices
  alias FavnOrchestrator.Persistence.Results.TargetStatus, as: TargetStatusResult
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.Storage.ExactDateTimeCodec
  alias FavnStoragePostgres.ErrorMapper
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.Schemas.Backfill
  alias FavnStoragePostgres.Schemas.BackfillOverview
  alias FavnStoragePostgres.Schemas.BackfillWindow
  alias FavnStoragePostgres.Schemas.AssetAttemptOverview
  alias FavnStoragePostgres.Schemas.AssetWindowState
  alias FavnStoragePostgres.Schemas.ExecutionGroupOverview
  alias FavnStoragePostgres.Schemas.ManifestVersion
  alias FavnStoragePostgres.Schemas.Run
  alias FavnStoragePostgres.Schemas.RunEvent
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
  @attempt_statuses %{
    "queued" => :queued,
    "running" => :running,
    "retrying" => :retrying,
    "ok" => :ok,
    "error" => :error,
    "timed_out" => :timed_out,
    "cancelled" => :cancelled,
    "skipped_fresh" => :skipped_fresh,
    "blocked" => :blocked
  }
  # Window and backfill status vocabularies are mapped explicitly rather than
  # converted, so a value written by a newer release degrades to no status
  # instead of failing the read or creating an atom.
  @window_run_statuses %{
    "planned" => :planned,
    "ready" => :ready,
    "claimed" => :claimed,
    "running" => :running,
    "succeeded" => :succeeded,
    "failed" => :failed,
    "cancelled" => :cancelled
  }
  @backfill_statuses %{
    "planning" => :planning,
    "ready" => :ready,
    "running" => :running,
    "completed" => :completed,
    "failed" => :failed,
    "cancelled" => :cancelled
  }
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
      backfills = backfill_counts_by_group(groups)
      mapper = &group_result(&1, targets, assets, backfills)

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
      backfills = backfill_counts_by_group([overview])

      {:ok,
       %ExecutionGroup{
         overview:
           group_result(
             overview,
             target_refs_by_group([overview]),
             asset_counts_by_group([overview]),
             backfills
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
  def get_run_flow(%GetRunFlow{} = query) do
    with :ok <- validate_run_flow(query) do
      exact_read_transaction(fn ->
        with {:ok, header} <-
               exact_run_header(query.workspace_context.workspace_id, query.run_id) do
          counts = exact_run_counts!(query.workspace_context.workspace_id, query.run_id)

          {observed, candidates_overflow?} =
            exact_flow_candidates!(
              query.workspace_context.workspace_id,
              query.run_id,
              query.limit
            )

          {:ok,
           %RunFlowSnapshot{
             header: %{header | counts: counts},
             observed: observed,
             overflow?: candidates_overflow? or counts.total > query.limit
           }}
        end
      end)
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def get_run_header(%GetRunHeader{} = query) do
    with :ok <- validate_run_header(query) do
      exact_read_transaction(fn ->
        with {:ok, header} <-
               exact_run_header(query.workspace_context.workspace_id, query.run_id) do
          counts = exact_run_counts!(query.workspace_context.workspace_id, query.run_id)
          {:ok, %{header | counts: counts}}
        end
      end)
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def list_run_windows(%ListRunWindows{} = query) do
    with :ok <- validate_run_windows(query) do
      # Outer joins fold the "does this run exist?" probe into this statement. A
      # run with no backfill, or a backfill that has not started a window yet,
      # returns exactly one placeholder row whose window columns are null, while
      # a run that does not exist returns none. Placeholders sort last, so they
      # can never displace a real window run under the limit - neither the
      # pinned selected run nor the overflow sentinel.
      %{rows: rows} =
        SQL.query!(
          Repo,
          """
          WITH selected AS (
            SELECT COALESCE(root_execution_group_id, run_id) AS root_run_id
            FROM favn_control.runs
            WHERE workspace_id = $1 AND run_id = $2
          )
          SELECT window_run.run_id,
                 window_run.window_start,
                 window_run.window_end,
                 window_run.status,
                 window_run.window_key,
                 backfill.status,
                 backfill.backfill_id
          FROM selected
          LEFT JOIN favn_control.backfills AS backfill
            ON backfill.workspace_id = $1
           AND backfill.root_run_id = selected.root_run_id
          LEFT JOIN favn_control.backfill_windows AS window_run
            ON window_run.workspace_id = backfill.workspace_id
           AND window_run.backfill_id = backfill.backfill_id
           AND window_run.run_id IS NOT NULL
          ORDER BY (window_run.run_id IS NOT NULL) DESC,
                   (window_run.run_id = $2) DESC,
                   window_run.window_start DESC,
                   window_run.window_id DESC
          LIMIT $3
          """,
          [query.workspace_context.workspace_id, query.run_id, query.limit + 1],
          timeout: 1_000
        )

      case rows do
        [] ->
          {:error, Error.new(:not_found, "run not found")}

        rows ->
          window_rows = Enum.reject(rows, fn [run_id | _rest] -> is_nil(run_id) end)

          {:ok,
           %RunWindowChoices{
             items: window_rows |> Enum.take(query.limit) |> Enum.map(&run_window_choice/1),
             overflow?: length(window_rows) > query.limit,
             backfill_status: first_backfill_status(rows),
             backfill_id: first_backfill_id(rows)
           }}
      end
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def get_run_asset_attempt(%GetRunAssetAttempt{} = query) do
    with :ok <- validate_run_asset_attempt(query) do
      case Repo.get_by(AssetAttemptOverview,
             workspace_id: query.workspace_context.workspace_id,
             run_id: query.run_id,
             asset_step_id: query.asset_step_id
           ) do
        %AssetAttemptOverview{} = attempt -> {:ok, run_asset_attempt(attempt)}
        nil -> {:error, Error.new(:not_found, "asset attempt not found")}
      end
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def list_run_event_summaries(%ListRunEventSummaries{} = query) do
    with :ok <- validate_run_event_summaries(query),
         true <- exact_run_exists?(query.workspace_context.workspace_id, query.run_id) do
      rows =
        from(event in RunEvent,
          where:
            event.workspace_id == ^query.workspace_context.workspace_id and
              event.run_id == ^query.run_id,
          order_by: [desc: event.sequence],
          limit: ^query.limit,
          select: {
            event.run_id,
            event.sequence,
            event.occurred_at,
            event.event_type,
            event.status,
            fragment(
              "COALESCE(? #>> '{asset_ref,module}', ? #>> '{data,asset_ref,module}')",
              event.event,
              event.event
            ),
            fragment(
              "COALESCE(? #>> '{asset_ref,name}', ? #>> '{data,asset_ref,name}')",
              event.event,
              event.event
            ),
            fragment(
              """
              left(
                COALESCE(
                  ? #>> '{data,message}',
                  ? #>> '{data,error,message}',
                  ? #>> '{data,error,type}',
                  CASE WHEN jsonb_typeof(? #> '{data,error}') = 'string'
                       THEN ? #>> '{data,error}' END,
                  ? #>> '{data,reason}',
                  ''
                ),
                1024
              )
              """,
              event.event,
              event.event,
              event.event,
              event.event,
              event.event,
              event.event
            )
          }
        )
        |> Repo.all(timeout: 1_000)
        |> Enum.reverse()

      {:ok, Enum.map(rows, &run_event_summary/1)}
    else
      false -> {:error, Error.new(:not_found, "run not found")}
      {:error, %Error{} = error} -> {:error, error}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
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

  defp group_result(group, targets, assets, backfills) do
    key = {group.workspace_id, group.root_run_id}
    by_kind = Map.get(targets, key, %{})
    backfill = Map.get(backfills, key, empty_backfill_counts())

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
      backfill_status: backfill.status,
      target_refs: Map.get(by_kind, "asset", []),
      pipeline_refs: Map.get(by_kind, "pipeline", []),
      asset_counts: Map.get(assets, key, @no_asset_counts),
      window_counts: Map.delete(backfill, :status)
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
        succeeded: filter(count(), attempt.status == "ok"),
        skipped: filter(count(), attempt.status == "skipped_fresh"),
        failed: filter(count(), attempt.status in ^@failed_asset_statuses),
        running: filter(count(), attempt.status in ^@running_asset_statuses),
        queued: filter(count(), attempt.status == "queued"),
        planned: filter(count(), attempt.status == "planned")
      }
    )
    |> Repo.all()
    |> Map.new(fn row ->
      {{row.workspace_id, row.root_run_id}, Map.drop(row, [:workspace_id, :root_run_id])}
    end)
  end

  defp backfill_counts_by_group([]), do: %{}

  defp backfill_counts_by_group(groups) do
    {workspace_ids, root_run_ids} = group_keys(groups)

    from(backfill in Backfill,
      left_join: overview in BackfillOverview,
      on:
        overview.workspace_id == backfill.workspace_id and
          overview.backfill_id == backfill.backfill_id,
      where:
        backfill.workspace_id in ^workspace_ids and
          backfill.root_run_id in ^root_run_ids,
      select: %{
        workspace_id: backfill.workspace_id,
        root_run_id: backfill.root_run_id,
        status: coalesce(overview.status, backfill.status),
        total: coalesce(overview.total_count, backfill.expected_window_count),
        planned: coalesce(overview.planned_count, 0),
        ready: coalesce(overview.ready_count, 0),
        active: coalesce(overview.active_count, 0),
        succeeded: coalesce(overview.succeeded_count, 0),
        failed: coalesce(overview.failed_count, 0),
        cancelled: coalesce(overview.cancelled_count, 0)
      }
    )
    |> Repo.all()
    |> Enum.reduce(%{}, fn row, acc ->
      key = {row.workspace_id, row.root_run_id}
      counts = Map.drop(row, [:workspace_id, :root_run_id]) |> decode_backfill_counts()
      Map.update(acc, key, counts, &merge_backfill_counts(&1, counts))
    end)
  end

  defp decode_backfill_counts(counts) do
    %{counts | status: String.to_existing_atom(counts.status)}
  end

  defp merge_backfill_counts(left, right) do
    counts =
      empty_backfill_counts()
      |> Map.keys()
      |> List.delete(:status)
      |> Map.new(fn key -> {key, Map.fetch!(left, key) + Map.fetch!(right, key)} end)

    Map.put(counts, :status, dominant_backfill_status(left.status, right.status))
  end

  defp dominant_backfill_status(left, right) do
    Enum.max_by([left, right], &backfill_status_rank/1)
  end

  defp backfill_status_rank(:failed), do: 6
  defp backfill_status_rank(:running), do: 5
  defp backfill_status_rank(:ready), do: 4
  defp backfill_status_rank(:planning), do: 3
  defp backfill_status_rank(:cancelled), do: 2
  defp backfill_status_rank(:completed), do: 1

  defp empty_backfill_counts do
    %{
      status: nil,
      total: 0,
      planned: 0,
      ready: 0,
      active: 0,
      succeeded: 0,
      failed: 0,
      cancelled: 0
    }
  end

  defp group_keys(groups) do
    {
      groups |> Enum.map(& &1.workspace_id) |> Enum.uniq(),
      groups |> Enum.map(& &1.root_run_id) |> Enum.uniq()
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

  defp exact_run_header(workspace_id, run_id) do
    %{rows: rows} =
      SQL.query!(
        Repo,
        """
        SELECT selected_run.run_id,
               COALESCE(selected_run.root_execution_group_id, selected_run.run_id),
               selected_run.parent_run_id,
               selected_run.rerun_of_run_id,
               selected_run.manifest_version_id,
               selected_run.status,
               selected_run.submit_kind,
               selected_run.trigger_type,
               selected_run.event_sequence,
               selected_run.inserted_at,
               selected_run.updated_at,
               selected_run.terminal_at,
               selected_target.target_id,
               selected_target.target_label,
               COALESCE(
                 selected_window.window_start,
                 NULLIF(selected_run.snapshot #>> '{metadata,pipeline_context,anchor_window,start_at}', '')::timestamptz,
                 NULLIF(selected_run.snapshot #>> '{metadata,selected_window,start_at}', '')::timestamptz,
                 NULLIF(selected_run.snapshot #>> '{metadata,window,start_at}', '')::timestamptz,
                 NULLIF(selected_run.snapshot #>> '{params,window,start_at}', '')::timestamptz
               ),
               COALESCE(
                 selected_window.window_end,
                 NULLIF(selected_run.snapshot #>> '{metadata,pipeline_context,anchor_window,end_at}', '')::timestamptz,
                 NULLIF(selected_run.snapshot #>> '{metadata,selected_window,end_at}', '')::timestamptz,
                 NULLIF(selected_run.snapshot #>> '{metadata,window,end_at}', '')::timestamptz,
                 NULLIF(selected_run.snapshot #>> '{params,window,end_at}', '')::timestamptz
               ),
               selected_run.snapshot #>> '{error,type}',
               selected_run.snapshot #>> '{error,kind}',
               left(
                 COALESCE(
                   selected_run.snapshot #>> '{error,message}',
                   CASE WHEN jsonb_typeof(selected_run.snapshot -> 'error') = 'string'
                        THEN selected_run.snapshot #>> '{error}' END
                 ),
                 1024
               )
        FROM favn_control.runs AS selected_run
        LEFT JOIN LATERAL (
          SELECT candidate_target.target_id,
                 concat_ws(':', candidate_target.target_module, candidate_target.target_name) AS target_label
          FROM favn_control.run_targets AS candidate_target
          WHERE candidate_target.workspace_id = selected_run.workspace_id
            AND candidate_target.run_id = selected_run.run_id
          ORDER BY candidate_target.is_primary DESC,
                   candidate_target.submitted_event_id,
                   candidate_target.target_id
          LIMIT 1
        ) AS selected_target ON TRUE
        LEFT JOIN LATERAL (
          SELECT candidate_window.window_start, candidate_window.window_end
          FROM favn_control.backfill_windows AS candidate_window
          WHERE candidate_window.workspace_id = selected_run.workspace_id
            AND candidate_window.run_id = selected_run.run_id
          ORDER BY candidate_window.window_start DESC, candidate_window.window_id DESC
          LIMIT 1
        ) AS selected_window ON TRUE
        WHERE selected_run.workspace_id = $1 AND selected_run.run_id = $2
        """,
        [workspace_id, run_id],
        timeout: 1_000
      )

    case rows do
      [
        [
          loaded_run_id,
          root_run_id,
          parent_run_id,
          rerun_of_run_id,
          manifest_version_id,
          status,
          submit_kind,
          trigger_type,
          event_sequence,
          started_at,
          updated_at,
          finished_at,
          target_id,
          target_label,
          window_start_at,
          window_end_at,
          error_type,
          error_kind,
          error_message
        ]
      ] ->
        {:ok,
         %RunViewHeader{
           run_id: loaded_run_id,
           root_run_id: root_run_id,
           parent_run_id: parent_run_id,
           rerun_of_run_id: rerun_of_run_id,
           manifest_version_id: manifest_version_id,
           status: decode_operator_status(status),
           submit_kind: decode_operator_enum(:submit_kind, submit_kind),
           trigger_type: RunEnum.decode(:trigger_type, trigger_type),
           event_sequence: event_sequence,
           started_at: started_at,
           updated_at: updated_at,
           finished_at: finished_at,
           target_id: target_id,
           target_label: target_label,
           window_start_at: window_start_at,
           window_end_at: window_end_at,
           error_code: error_type || error_kind,
           error_message: error_message,
           counts: @no_asset_counts
         }}

      [] ->
        {:error, Error.new(:not_found, "run not found")}
    end
  end

  defp exact_run_counts!(workspace_id, run_id) do
    %{rows: [[total, completed, succeeded, skipped, failed, running, queued, planned]]} =
      SQL.query!(
        Repo,
        """
        -- Counts come from the run's attempts alone. The persisted plan is not
        -- consulted: expanding its node array to anticipate steps that have not
        -- been admitted yet cost a deTOAST and parse of the whole plan on every
        -- refresh, and the execution-group list has always counted this way.
        SELECT count(*)::bigint AS total,
               count(*) FILTER (
                 WHERE attempt.status IN ('ok', 'error', 'timed_out', 'cancelled', 'skipped_fresh', 'blocked')
               )::bigint AS completed,
               count(*) FILTER (WHERE attempt.status = 'ok')::bigint AS succeeded,
               count(*) FILTER (WHERE attempt.status = 'skipped_fresh')::bigint AS skipped,
               count(*) FILTER (
                 WHERE attempt.status IN ('error', 'timed_out', 'cancelled', 'blocked')
               )::bigint AS failed,
               count(*) FILTER (WHERE attempt.status IN ('running', 'retrying'))::bigint AS running,
               count(*) FILTER (WHERE attempt.status = 'queued')::bigint AS queued,
               count(*) FILTER (WHERE attempt.status = 'planned')::bigint AS planned
        FROM favn_control.asset_attempt_overviews AS attempt
        WHERE attempt.workspace_id = $1 AND attempt.run_id = $2
        """,
        [workspace_id, run_id],
        timeout: 1_000
      )

    %{
      total: total,
      completed: completed,
      succeeded: succeeded,
      skipped: skipped,
      failed: failed,
      running: running,
      queued: queued,
      planned: planned
    }
  end

  # Flow rows are the run's attempts. A step enters this table when it is
  # queued, so an asset appears here shortly before it runs rather than only
  # once it has, and the persisted plan is never read.
  defp exact_flow_candidates!(workspace_id, run_id, limit) do
    %{rows: rows} =
      SQL.query!(
        Repo,
        """
        SELECT attempt.asset_step_id,
               attempt.asset_ref,
               attempt.window_identity,
               attempt.status,
               attempt.started_at,
               attempt.finished_at,
               attempt.stage
        FROM favn_control.asset_attempt_overviews AS attempt
        WHERE attempt.workspace_id = $1 AND attempt.run_id = $2
        ORDER BY attempt.asset_ref, attempt.asset_step_id
        LIMIT $3
        """,
        [workspace_id, run_id, limit + 1],
        timeout: 1_000
      )

    observed =
      rows
      |> Enum.take(limit)
      |> Enum.map(fn [
                       asset_step_id,
                       asset_ref,
                       window_identity,
                       status,
                       started_at,
                       finished_at,
                       stage
                     ] ->
        %RunFlowCandidate{
          run_id: run_id,
          asset_step_id: asset_step_id,
          asset_ref: asset_ref,
          window_identity: window_identity,
          status: decode_attempt_status(status),
          started_at: started_at,
          finished_at: finished_at,
          stage: stage
        }
      end)

    {observed, length(rows) > limit}
  end

  defp run_window_choice([
         run_id,
         window_start_at,
         window_end_at,
         status,
         window_key,
         _backfill_status,
         _backfill_id
       ]) do
    {kind, timezone} = window_key_identity(window_key)

    %RunWindowChoice{
      run_id: run_id,
      window_start_at: window_start_at,
      window_end_at: window_end_at,
      status: decode_window_run_status(status),
      kind: kind,
      timezone: timezone
    }
  end

  # A key written before the current encoding still names a real window run. The
  # choice keeps its timestamps and loses only its calendar labelling.
  defp window_key_identity(window_key) when is_binary(window_key) do
    case WindowKey.decode(window_key) do
      {:ok, %{kind: kind, timezone: timezone}} -> {kind, timezone}
      {:error, _reason} -> {nil, nil}
    end
  end

  defp window_key_identity(_window_key), do: {nil, nil}

  # Placeholder rows carry the backfill status without a window run, so the
  # first row that names one wins whether or not any window has started.
  # A run with no backfill yields one placeholder row whose backfill columns are
  # null, and both readers return nil for it.
  #
  # `backfills_root_run_idx` is not unique, so two backfills can share one root
  # run and the join can return rows from both. Real window rows sort first, so
  # both readers answer for whichever backfill has started windows — the one the
  # rail is drawing. They can still disagree if both have windows, which is why
  # neither is treated as identifying the root's only backfill.
  defp first_backfill_status(rows) do
    Enum.find_value(rows, fn [_, _, _, _, _, status, _] -> decode_backfill_status(status) end)
  end

  defp first_backfill_id(rows) do
    Enum.find_value(rows, fn [_, _, _, _, _, _, backfill_id] -> backfill_id end)
  end

  defp decode_window_run_status(value), do: Map.get(@window_run_statuses, value)
  defp decode_backfill_status(value), do: Map.get(@backfill_statuses, value)

  defp run_asset_attempt(attempt) do
    %RunAssetAttemptResult{
      run_id: attempt.run_id,
      asset_step_id: attempt.asset_step_id,
      asset_ref: attempt.asset_ref,
      status: decode_attempt_status(attempt.status),
      started_at: attempt.started_at,
      finished_at: attempt.finished_at,
      duration_ms: attempt.duration_ms,
      attempt_number: attempt.attempt_number,
      stage: attempt.stage,
      execution_pool: attempt.execution_pool,
      queue_reason: attempt.queue_reason,
      window: restore_window(attempt.window),
      error: attempt.error,
      output_metadata: attempt.output_metadata
    }
  end

  defp run_event_summary(
         {run_id, sequence, occurred_at, event_type, status, module, name, summary}
       ) do
    asset_ref = if is_binary(module) and is_binary(name), do: module <> ":" <> name

    %RunEventSummary{
      run_id: run_id,
      sequence: sequence,
      occurred_at: occurred_at,
      event_type: event_type,
      status: if(is_binary(status), do: decode_attempt_status(status)),
      asset_ref: asset_ref,
      summary: if(summary == "", do: nil, else: summary)
    }
  end

  defp exact_run_exists?(workspace_id, run_id) do
    Run
    |> where([run], run.workspace_id == ^workspace_id and run.run_id == ^run_id)
    |> Repo.exists?(timeout: 1_000)
  end

  defp decode_operator_enum(kind, value), do: RunEnum.decode(kind, value) || :unknown
  defp decode_operator_status(value), do: decode_operator_enum(:status, value)

  # Asset attempts have their own status vocabulary. It overlaps the run
  # statuses only where the two mean the same thing; `queued`, `retrying`,
  # `skipped_fresh` and `blocked` belong to attempts alone, and decoding them
  # through the run enum turned every one of them into `:unknown`. Falling back
  # keeps this a superset, so the shared event read still decodes run statuses.
  defp decode_attempt_status(value) do
    case Map.fetch(@attempt_statuses, value) do
      {:ok, status} -> status
      :error -> decode_operator_status(value)
    end
  end

  defp exact_read_transaction(fun) do
    # SQL Sandbox hides its outer transaction to mimic production. It cannot
    # change isolation after fixture setup; normal pools set it before reading.
    set_transaction_mode? = not Repo.in_transaction?() and not ownership_pool?()

    Repo.transaction(
      fn ->
        if set_transaction_mode? do
          SQL.query!(
            Repo,
            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY",
            [],
            timeout: 1_000
          )
        end

        fun.()
      end,
      timeout: 2_000
    )
    |> transaction_result()
  end

  defp ownership_pool? do
    Ecto.Adapter.lookup_meta(Repo).opts[:pool] == DBConnection.Ownership
  end

  defp transaction_result({:ok, {:ok, result}}), do: {:ok, result}
  defp transaction_result({:ok, {:error, %Error{} = error}}), do: {:error, error}
  defp transaction_result({:ok, result}), do: {:ok, result}
  defp transaction_result({:error, %Error{} = error}), do: {:error, error}
  defp transaction_result({:error, reason}), do: {:error, ErrorMapper.map(reason)}

  defp validate_run_flow(query) do
    if workspace_context?(query.workspace_context) and valid_id?(query.run_id) and
         valid_bound?(query.limit, 1, 1_000),
       do: :ok,
       else: {:error, ErrorMapper.map(:invalid)}
  end

  defp validate_run_header(query) do
    if workspace_context?(query.workspace_context) and valid_id?(query.run_id),
      do: :ok,
      else: {:error, ErrorMapper.map(:invalid)}
  end

  defp validate_run_windows(query), do: validate_run_flow(query)

  defp validate_run_asset_attempt(query) do
    if workspace_context?(query.workspace_context) and valid_id?(query.run_id) and
         valid_id?(query.asset_step_id),
       do: :ok,
       else: {:error, ErrorMapper.map(:invalid)}
  end

  defp validate_run_event_summaries(query) do
    if workspace_context?(query.workspace_context) and valid_id?(query.run_id) and
         valid_bound?(query.limit, 1, 200),
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
