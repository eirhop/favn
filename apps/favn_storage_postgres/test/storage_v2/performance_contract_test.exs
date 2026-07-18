defmodule FavnStoragePostgres.StorageV2.PerformanceContractTest do
  use ExUnit.Case, async: false

  @moduletag :slow

  alias Ecto.Adapters.SQL
  alias Favn.Manifest
  alias Favn.Manifest.Version
  alias Favn.Plan
  alias Favn.Retry.Policy
  alias FavnOrchestrator.Persistence.Commands.ClaimBackfillWindows
  alias FavnOrchestrator.Persistence.Commands.CommitRunTransition
  alias FavnOrchestrator.Persistence.Commands.CreateRun
  alias FavnOrchestrator.Persistence.Commands.DeployManifest
  alias FavnOrchestrator.Persistence.Commands.DeploymentTarget
  alias FavnOrchestrator.Persistence.Commands.ProvisionWorkspace
  alias FavnOrchestrator.Persistence.Commands.RegisterManifest
  alias FavnOrchestrator.Persistence.Commands.RunTarget
  alias FavnOrchestrator.Persistence.PlatformContext
  alias FavnOrchestrator.Persistence.Queries.ManifestSelector.ByContentHash
  alias FavnOrchestrator.Persistence.Queries.PageBackfillWindows
  alias FavnOrchestrator.Persistence.Queries.PageRunnerExecutions
  alias FavnOrchestrator.Persistence.Queries.PageRuns
  alias FavnOrchestrator.Persistence.Queries.GetRun
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.TargetStatus
  alias FavnStoragePostgres.Backfills.Store, as: BackfillStore
  alias FavnStoragePostgres.Config
  alias FavnStoragePostgres.Registry.Store, as: RegistryStore
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.RunOwnership.Store, as: RunOwnershipStore
  alias FavnStoragePostgres.Runs.Store, as: RunStore
  alias FavnStoragePostgres.StorageV2.Migrations

  setup_all do
    url =
      System.get_env("FAVN_DATABASE_URL") ||
        raise "FAVN_DATABASE_URL is required for PostgreSQL storage tests"

    {:ok, options} = Config.repo_options(url: url, ssl_mode: :disable, pool_size: 8)
    start_supervised!({Repo, options})
    :ok = Migrations.migrate!(Repo)

    version = manifest_version("performance-mv-#{random_id()}")

    {:ok, platform_context} =
      PlatformContext.new("performance-test", "manifest-publisher", [:platform_admin])

    persisted_version =
      case RegistryStore.register_manifest(%RegisterManifest{
             platform_context: platform_context,
             version: version
           }) do
        {:ok, persisted} ->
          persisted

        {:error, %{kind: :conflict}} ->
          {:ok, persisted} =
            RegistryStore.get_manifest(%ByContentHash{content_hash: version.content_hash})

          persisted
      end

    {:ok, version: persisted_version}
  end

  setup %{version: version} do
    {:ok, provision_fixture(version)}
  end

  test "execution-package retention scans only never-linked candidates", _fixture do
    seed = random_id()
    cutoff = DateTime.utc_now()

    on_exit(fn ->
      SQL.query!(
        Repo,
        "DELETE FROM favn_control.execution_packages WHERE asset_module = $1",
        ["Elixir.Performance.Package.#{seed}"]
      )
    end)

    SQL.query!(
      Repo,
      """
      INSERT INTO favn_control.execution_packages
        (content_hash, asset_module, asset_name, payload, first_linked_at, inserted_at)
      SELECT decode(md5($1 || ':' || series::text) || md5('tail:' || $1 || ':' || series::text), 'hex'),
             'Elixir.Performance.Package.' || $1,
             'asset_' || series::text,
             '{}'::jsonb,
             CASE WHEN series <= 10000 THEN $2::timestamptz ELSE NULL END,
             $2::timestamptz - interval '1 day'
      FROM generate_series(1, 10100) AS series
      ON CONFLICT DO NOTHING
      """,
      [seed, cutoff]
    )

    SQL.query!(Repo, "ANALYZE favn_control.execution_packages", [])

    plan =
      explain(
        """
        SELECT package.content_hash
        FROM favn_control.execution_packages package
        WHERE NULL::text IS NULL
          AND package.first_linked_at IS NULL
          AND package.inserted_at < $1
          AND NOT EXISTS (
            SELECT 1
            FROM favn_control.manifest_execution_packages manifest_package
            WHERE manifest_package.package_hash = package.content_hash
          )
        ORDER BY package.inserted_at, package.content_hash
        LIMIT 50
        FOR UPDATE OF package SKIP LOCKED
        """,
        [cutoff]
      )

    assert "execution_packages_unlinked_retention_idx" in index_names(plan)
  end

  test "run-transition query work is independent of ten thousand group siblings", fixture do
    small = create_run!(fixture)
    {small_result, small_queries} = measure_queries(fn -> transition(fixture, small) end)
    assert {:ok, _committed} = small_result

    large = create_run!(fixture)
    insert_sibling_runs!(fixture, large, 10_000)
    SQL.query!(Repo, "ANALYZE favn_control.runs", [])

    {large_result, large_queries} = measure_queries(fn -> transition(fixture, large) end)
    assert {:ok, _committed} = large_result

    assert large_queries == small_queries
    assert large_queries <= 8

    plan =
      explain(
        """
        SELECT run_id
        FROM favn_control.runs
        WHERE workspace_id = $1 AND root_execution_group_id = $2
        ORDER BY submitted_event_id DESC, run_id DESC
        LIMIT 51
        """,
        [fixture.workspace_id, large.id]
      )

    assert "runs_group_children_idx" in index_names(plan)
  end

  test "runner execution recovery pages stay index-backed at large run cardinality", fixture do
    run = create_run!(fixture)
    owner_id = "performance-runner-owner"
    insert_sibling_runs!(fixture, run, 9)

    SQL.query!(
      Repo,
      """
      WITH execution_runs AS (
        SELECT run_id, row_number() OVER (ORDER BY run_id) - 1 AS run_ordinal
        FROM favn_control.runs
        WHERE workspace_id = $1 AND root_execution_group_id = $2
      )
      INSERT INTO favn_control.runner_executions
        (workspace_id, runner_execution_id, run_id, dispatch_id, last_command_id,
         owner_id, run_fencing_token, status, version, dispatch_payload,
         dispatched_at, terminal_at, inserted_at, updated_at)
      SELECT $1,
             'execution-' || lpad(series::text, 8, '0') || '-' ||
               lpad(execution_runs.run_ordinal::text, 2, '0'),
             execution_runs.run_id,
             'dispatch-' || lpad(series::text, 8, '0') || '-' ||
               lpad(execution_runs.run_ordinal::text, 2, '0'),
             'command-' || lpad(series::text, 8, '0') || '-' ||
               lpad(execution_runs.run_ordinal::text, 2, '0'),
             CASE WHEN execution_runs.run_ordinal = 0
                  THEN $3
                  ELSE $3 || '-' || execution_runs.run_ordinal::text END,
             1, CASE WHEN series % 100 = 0 THEN 'running' ELSE 'ok' END,
             1, '{}'::jsonb,
             clock_timestamp(),
             CASE WHEN series % 100 = 0 THEN NULL ELSE clock_timestamp() END,
             clock_timestamp(), clock_timestamp()
      FROM execution_runs
      CROSS JOIN generate_series(1, 2000) AS series
      """,
      [fixture.workspace_id, run.id, owner_id]
    )

    SQL.query!(Repo, "ANALYZE favn_control.runner_executions", [])

    {active_result, active_queries} =
      capture_queries(fn ->
        RunOwnershipStore.page_executions(%PageRunnerExecutions{
          workspace_context: fixture.workspace_context,
          run_id: run.id,
          after: %{runner_execution_id: "execution-00001000"},
          limit: 100
        })
      end)

    assert {:ok, _page} = active_result
    run_active_page = active_queries |> runner_execution_query!() |> explain_captured()

    {history_result, history_queries} =
      capture_queries(fn ->
        RunOwnershipStore.page_executions(%PageRunnerExecutions{
          workspace_context: fixture.workspace_context,
          run_id: run.id,
          after: %{runner_execution_id: "execution-00001000"},
          active_only?: false,
          limit: 100
        })
      end)

    assert {:ok, _page} = history_result
    run_history_page = history_queries |> runner_execution_query!() |> explain_captured()

    owner_page =
      explain(
        """
        SELECT runner_execution_id
        FROM favn_control.runner_executions
        WHERE workspace_id = $1 AND owner_id = $2 AND terminal_at IS NULL
          AND runner_execution_id > $3
        ORDER BY runner_execution_id
        LIMIT 101
        """,
        [fixture.workspace_id, owner_id, "execution-00001000"]
      )

    assert "runner_executions_run_active_page_idx" in index_names(run_active_page)
    assert "runner_executions_run_page_idx" in index_names(run_history_page)
    assert "runner_executions_owner_active_idx" in index_names(owner_page)
  end

  test "status-filtered run history uses its keyset indexes for workspace and platform pages",
       fixture do
    root = create_run!(fixture)
    insert_status_history_runs!(fixture, root, 50_000)
    SQL.query!(Repo, "ANALYZE favn_control.runs", [])

    {workspace_result, workspace_queries} =
      capture_queries(fn ->
        RunStore.page_run_summaries(%PageRuns{
          scope: fixture.workspace_context,
          status: :timed_out,
          limit: 100
        })
      end)

    assert {:ok, _page} = workspace_result
    workspace_plan = workspace_queries |> runs_page_query!() |> explain_captured()
    assert "runs_workspace_status_recent_idx" in index_names(workspace_plan)

    {:ok, platform_context} =
      PlatformContext.new("performance-test", "run-history", [:platform_admin])

    {platform_result, platform_queries} =
      capture_queries(fn ->
        RunStore.page_run_summaries(%PageRuns{
          scope: platform_context,
          status: :timed_out,
          limit: 100
        })
      end)

    assert {:ok, _page} = platform_result
    platform_plan = platform_queries |> runs_page_query!() |> explain_captured()
    assert "runs_platform_status_recent_idx" in index_names(platform_plan)
  end

  test "large immutable plans are stored once outside bounded transition snapshots", fixture do
    run_id = "performance-large-plan-#{random_id()}"
    ref = {MyApp.PerformanceAsset, :asset}
    node_key = {ref, nil}
    repeated_node_keys = List.duplicate(node_key, 35_000)

    plan = %Plan{
      target_refs: [ref],
      target_node_keys: [node_key],
      dependencies: :all,
      nodes: %{
        node_key => %{
          ref: ref,
          node_key: node_key,
          window: nil,
          upstream: repeated_node_keys,
          downstream: repeated_node_keys,
          stage: 0,
          execution_pool: nil,
          action: :run,
          retry_policy: Policy.default(),
          retry_policy_source: :default
        }
      },
      topo_order: List.duplicate(ref, 35_000),
      stages: List.duplicate([ref], 35_000),
      node_stages: List.duplicate([node_key], 35_000)
    }

    run =
      RunState.new(
        id: run_id,
        workspace_id: fixture.workspace_id,
        deployment_id: fixture.deployment_id,
        manifest_version_id: fixture.version.manifest_version_id,
        manifest_content_hash: fixture.version.content_hash,
        asset_ref: ref,
        target_refs: [ref],
        plan: plan
      )

    assert {:ok, _created} =
             RunStore.create_run(%CreateRun{
               workspace_context: fixture.workspace_context,
               command_id: "create:" <> run_id,
               deployment_id: fixture.deployment_id,
               run: run,
               targets: [
                 %RunTarget{
                   target_kind: :asset,
                   target_id: fixture.target_id,
                   target_module: "MyApp.PerformanceAsset",
                   target_name: "asset",
                   is_primary: true
                 }
               ],
               event: %{
                 run_id: run_id,
                 sequence: 1,
                 event_type: :run_submitted,
                 status: :pending,
                 occurred_at: run.inserted_at
               }
             })

    running = RunState.transition(run, status: :running)

    assert {:ok, _transition} =
             RunStore.commit_transition(%CommitRunTransition{
               workspace_context: fixture.workspace_context,
               command_id: "transition:" <> run_id,
               expected_sequence: 1,
               run: running,
               event: %{
                 run_id: run_id,
                 sequence: 2,
                 event_type: :run_started,
                 status: :running,
                 occurred_at: running.updated_at
               }
             })

    %{rows: [[snapshot_bytes, plan_bytes]]} =
      SQL.query!(
        Repo,
        """
        SELECT octet_length(run.snapshot::text), octet_length(plan.plan::text)
        FROM favn_control.runs AS run
        JOIN favn_control.run_plans AS plan
          USING (workspace_id, run_id)
        WHERE run.workspace_id = $1 AND run.run_id = $2
        """,
        [fixture.workspace_id, run_id]
      )

    assert snapshot_bytes < 4 * 1_024 * 1_024
    assert plan_bytes > 4 * 1_024 * 1_024

    assert {:ok, loaded} =
             RunStore.get_run(%GetRun{
               workspace_context: fixture.workspace_context,
               run_id: run_id
             })

    assert loaded.plan_hash == run.plan_hash
    assert length(loaded.plan.nodes[node_key].upstream) == 35_000
  end

  test "hundred-thousand-window backfills retain bounded page and claim work", fixture do
    root = create_run!(fixture)
    backfill_id = "performance-backfill-#{random_id()}"
    insert_backfill_windows!(fixture, root, backfill_id, 100_000)
    SQL.query!(Repo, "ANALYZE favn_control.backfill_windows", [])

    {page_result, page_queries} =
      measure_queries(fn ->
        BackfillStore.page_windows(%PageBackfillWindows{
          workspace_context: fixture.workspace_context,
          backfill_id: backfill_id,
          limit: 100
        })
      end)

    assert {:ok, %{items: items, has_more?: true}} = page_result
    assert length(items) == 100
    assert page_queries == 1

    {claim_result, claim_queries} =
      measure_queries(fn ->
        BackfillStore.claim_windows(%ClaimBackfillWindows{
          workspace_context: fixture.workspace_context,
          batch_id: "performance-claim-#{random_id()}",
          owner_id: "performance-node",
          lease_duration_ms: 60_000,
          backfill_id: backfill_id,
          limit: 100
        })
      end)

    assert {:ok, claimed} = claim_result
    assert length(claimed) == 100
    assert claim_queries <= 5

    page_plan =
      explain(
        """
        SELECT window_id
        FROM favn_control.backfill_windows
        WHERE workspace_id = $1 AND backfill_id = $2
        ORDER BY window_key, window_id
        LIMIT 101
        """,
        [fixture.workspace_id, backfill_id]
      )

    assert "backfill_windows_key_uidx" in index_names(page_plan)

    claim_plan =
      explain(
        """
        SELECT workspace_id, backfill_id, window_id
        FROM favn_control.backfill_windows
        WHERE workspace_id = $1 AND backfill_id = $2
          AND (status = 'ready' OR
               (status = 'claimed' AND claim_expires_at <= clock_timestamp()))
        ORDER BY window_start, window_id
        LIMIT 100
        FOR UPDATE SKIP LOCKED
        """,
        [fixture.workspace_id, backfill_id]
      )

    assert Enum.any?(index_names(claim_plan), &String.ends_with?(&1, "claim_idx"))
  end

  test "ten-thousand run targets are inserted atomically in protocol-safe chunks", fixture do
    SQL.query!(
      Repo,
      """
      INSERT INTO favn_control.workspace_deployment_targets
        (workspace_id, deployment_id, target_kind, target_id, selection_source,
         customer_visible, descriptor, inserted_at)
      SELECT $1, $2, 'asset', 'bulk-target-' || lpad(series::text, 5, '0'),
             'dependency', false, '{}'::jsonb, clock_timestamp()
      FROM generate_series(1, 10000) AS series
      """,
      [fixture.workspace_id, fixture.deployment_id]
    )

    run_id = "performance-target-run-#{random_id()}"

    run =
      RunState.new(
        id: run_id,
        workspace_id: fixture.workspace_id,
        deployment_id: fixture.deployment_id,
        manifest_version_id: fixture.version.manifest_version_id,
        manifest_content_hash: fixture.version.content_hash,
        asset_ref: {MyApp.PerformanceAsset, :asset},
        target_refs: [{MyApp.PerformanceAsset, :asset}]
      )

    targets =
      Enum.map(1..10_000, fn index ->
        %RunTarget{
          target_kind: :asset,
          target_id: "bulk-target-" <> String.pad_leading(Integer.to_string(index), 5, "0"),
          target_module: "MyApp.PerformanceAsset",
          target_name: nil,
          is_primary: index == 1
        }
      end)

    assert {:ok, _created} =
             RunStore.create_run(%CreateRun{
               workspace_context: fixture.workspace_context,
               command_id: "create:" <> run_id,
               deployment_id: fixture.deployment_id,
               run: run,
               targets: targets,
               event: %{
                 run_id: run_id,
                 sequence: 1,
                 event_type: :run_submitted,
                 status: :pending,
                 occurred_at: run.inserted_at
               }
             })

    %{rows: [[10_000]]} =
      SQL.query!(
        Repo,
        "SELECT count(*) FROM favn_control.run_targets WHERE workspace_id = $1 AND run_id = $2",
        [fixture.workspace_id, run_id]
      )
  end

  defp provision_fixture(version) do
    unique = random_id()
    workspace_id = "performance-ws-#{unique}"
    deployment_id = "performance-deploy-#{unique}"
    now = DateTime.utc_now()

    {:ok, platform_context} =
      PlatformContext.new("performance-test", "grant-#{unique}", [:platform_admin])

    :ok =
      RegistryStore.provision_workspace(%ProvisionWorkspace{
        platform_context: platform_context,
        workspace_id: workspace_id,
        slug: "performance-#{unique}",
        display_name: "Performance #{unique}",
        occurred_at: now
      })

    {:ok, workspace_context} =
      WorkspaceContext.new(workspace_id, "performance-test", [:workspace_admin])

    target_id = TargetStatus.target_id_for_asset({MyApp.PerformanceAsset, :asset})

    assert {:ok, _runtime} =
             RegistryStore.deploy_manifest(%DeployManifest{
               platform_context: platform_context,
               workspace_context: workspace_context,
               deployment_id: deployment_id,
               manifest_version_id: version.manifest_version_id,
               configuration: %{"resources" => %{}},
               targets: [
                 %DeploymentTarget{
                   target_kind: :asset,
                   target_id: target_id,
                   selection_source: :common,
                   customer_visible: true,
                   descriptor: %{"target_id" => target_id, "label" => target_id}
                 }
               ],
               occurred_at: now
             })

    %{
      workspace_id: workspace_id,
      workspace_context: workspace_context,
      deployment_id: deployment_id,
      version: version,
      target_id: target_id
    }
  end

  defp create_run!(fixture) do
    run_id = "performance-run-#{random_id()}"

    run =
      RunState.new(
        id: run_id,
        workspace_id: fixture.workspace_id,
        deployment_id: fixture.deployment_id,
        manifest_version_id: fixture.version.manifest_version_id,
        manifest_content_hash: fixture.version.content_hash,
        asset_ref: {MyApp.PerformanceAsset, :asset},
        target_refs: [{MyApp.PerformanceAsset, :asset}]
      )

    assert {:ok, _created} =
             RunStore.create_run(%CreateRun{
               workspace_context: fixture.workspace_context,
               command_id: "create:" <> run_id,
               deployment_id: fixture.deployment_id,
               run: run,
               targets: [
                 %RunTarget{
                   target_kind: :asset,
                   target_id: fixture.target_id,
                   target_module: "MyApp.PerformanceAsset",
                   target_name: "asset",
                   is_primary: true
                 }
               ],
               event: %{
                 run_id: run_id,
                 sequence: 1,
                 event_type: :run_submitted,
                 status: :pending,
                 occurred_at: run.inserted_at
               }
             })

    run
  end

  defp transition(fixture, run) do
    running = RunState.transition(run, status: :running)

    RunStore.commit_transition(%CommitRunTransition{
      workspace_context: fixture.workspace_context,
      command_id: "transition:" <> run.id,
      expected_sequence: 1,
      run: running,
      event: %{
        run_id: run.id,
        sequence: 2,
        event_type: :run_started,
        status: :running,
        occurred_at: DateTime.utc_now()
      }
    })
  end

  defp insert_sibling_runs!(fixture, root, count) do
    prefix = "sibling-#{random_id()}-"

    SQL.query!(
      Repo,
      """
      INSERT INTO favn_control.runs
        (workspace_id, run_id, deployment_id, manifest_version_id,
         root_execution_group_id, parent_run_id, rerun_of_run_id, submit_kind,
         trigger_type, status, event_sequence, submitted_event_id, latest_event_id,
         snapshot_version, creation_hash, snapshot_hash, snapshot,
         inserted_at, updated_at, terminal_at)
      SELECT template.workspace_id, $3 || series::text, template.deployment_id,
             template.manifest_version_id, $2, NULL, NULL, template.submit_kind,
             template.trigger_type, template.status, template.event_sequence,
             template.submitted_event_id, template.latest_event_id,
             template.snapshot_version, template.creation_hash, template.snapshot_hash,
             template.snapshot, template.inserted_at, template.updated_at, NULL
      FROM favn_control.runs template
      CROSS JOIN generate_series(1, $4) AS series
      WHERE template.workspace_id = $1 AND template.run_id = $2
      """,
      [fixture.workspace_id, root.id, prefix, count]
    )
  end

  defp insert_status_history_runs!(fixture, root, count) do
    prefix = "status-history-#{random_id()}-"

    SQL.query!(
      Repo,
      """
      INSERT INTO favn_control.runs
        (workspace_id, run_id, deployment_id, manifest_version_id,
         root_execution_group_id, parent_run_id, rerun_of_run_id, submit_kind,
         trigger_type, status, event_sequence, submitted_event_id, latest_event_id,
         snapshot_version, creation_hash, snapshot_hash, snapshot,
         inserted_at, updated_at, terminal_at)
      SELECT template.workspace_id, $3 || lpad(series::text, 8, '0'),
             template.deployment_id, template.manifest_version_id, template.run_id,
             NULL, NULL, template.submit_kind, template.trigger_type,
             CASE WHEN series % 10 = 0 THEN 'timed_out' ELSE 'ok' END,
             template.event_sequence, template.submitted_event_id,
             template.latest_event_id, template.snapshot_version,
             template.creation_hash, template.snapshot_hash, template.snapshot,
             template.inserted_at, template.updated_at, clock_timestamp()
      FROM favn_control.runs AS template
      CROSS JOIN generate_series(1, $4) AS series
      WHERE template.workspace_id = $1 AND template.run_id = $2
      """,
      [fixture.workspace_id, root.id, prefix, count]
    )
  end

  defp insert_backfill_windows!(fixture, root, backfill_id, count) do
    now = DateTime.utc_now()
    range_end = DateTime.add(now, count + 1, :second)
    fingerprint = :crypto.hash(:sha256, backfill_id)
    batch_count = div(count + 499, 500)

    Repo.transaction(fn ->
      SQL.query!(
        Repo,
        """
        INSERT INTO favn_control.backfills
          (workspace_id, backfill_id, root_run_id, start_command_id, request_hash,
           deployment_id, manifest_version_id, target_kind, target_id,
           range_start, range_end, status, expected_window_count,
           expected_batch_count, appended_window_count, appended_batch_count,
           plan_hash, metadata, version, inserted_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, 'asset', $8, $9, $10,
                'ready', $11, $12, $11, $12, $5, '{}'::jsonb, 1, $9, $9)
        """,
        [
          fixture.workspace_id,
          backfill_id,
          root.id,
          "start:" <> backfill_id,
          fingerprint,
          fixture.deployment_id,
          fixture.version.manifest_version_id,
          fixture.target_id,
          now,
          range_end,
          count,
          batch_count
        ]
      )

      SQL.query!(
        Repo,
        """
        INSERT INTO favn_control.backfill_plan_batches
          (workspace_id, backfill_id, batch_index, command_id, batch_hash,
           window_count, inserted_at)
        SELECT $1, $2, batch_index, $3 || ':' || batch_index::text, $4,
               LEAST(500, $5 - batch_index * 500), $6
        FROM generate_series(0, $7 - 1) AS batch_index
        """,
        [
          fixture.workspace_id,
          backfill_id,
          "batch:" <> backfill_id,
          fingerprint,
          count,
          now,
          batch_count
        ]
      )

      SQL.query!(
        Repo,
        """
        INSERT INTO favn_control.backfill_windows
          (workspace_id, backfill_id, window_id, batch_index, window_key,
           window_start, window_end, status, fencing_token, attempt_count,
           payload, version, inserted_at, updated_at)
        SELECT $1, $2, 'window-' || series::text, ((series - 1) / 500)::integer,
               lpad(series::text, 8, '0'),
               $3::timestamptz + series * interval '1 second',
               $3::timestamptz + (series + 1) * interval '1 second',
               'ready', 0, 0, '{}'::jsonb, 1, $3::timestamptz, $3::timestamptz
        FROM generate_series(1, $4) AS series
        """,
        [fixture.workspace_id, backfill_id, now, count]
      )
    end)
  end

  defp measure_queries(function) do
    handler_id = {__MODULE__, self(), make_ref()}

    :ok =
      :telemetry.attach(
        handler_id,
        [:favn_storage_postgres, :repo, :query],
        fn _event, _measurements, _metadata, pid -> send(pid, :storage_query) end,
        self()
      )

    try do
      result = function.()
      {result, collect_query_count(0)}
    after
      :telemetry.detach(handler_id)
    end
  end

  defp capture_queries(function) do
    handler_id = {__MODULE__, self(), make_ref()}

    :ok =
      :telemetry.attach(
        handler_id,
        [:favn_storage_postgres, :repo, :query],
        fn _event, _measurements, metadata, pid ->
          send(pid, {:captured_storage_query, metadata.query, metadata.params})
        end,
        self()
      )

    try do
      result = function.()
      {result, collect_captured_queries([])}
    after
      :telemetry.detach(handler_id)
    end
  end

  defp collect_captured_queries(queries) do
    receive do
      {:captured_storage_query, query, params} ->
        collect_captured_queries([{query, params} | queries])
    after
      10 -> Enum.reverse(queries)
    end
  end

  defp runner_execution_query!(queries),
    do: query_containing!(queries, ~s(FROM "favn_control"."runner_executions"))

  defp runs_page_query!(queries),
    do: query_containing!(queries, ~s(FROM "favn_control"."runs"))

  defp query_containing!(queries, fragment) do
    Enum.find(queries, fn {query, _params} -> String.contains?(query, fragment) end) ||
      flunk("expected captured query containing #{inspect(fragment)}, got: #{inspect(queries)}")
  end

  defp explain_captured({sql, params}), do: explain(sql, params)

  defp collect_query_count(count) do
    receive do
      :storage_query -> collect_query_count(count + 1)
    after
      10 -> count
    end
  end

  defp explain(sql, params) do
    %{rows: [[plan]]} = SQL.query!(Repo, "EXPLAIN (FORMAT JSON) " <> sql, params)
    plan
  end

  defp index_names(value) when is_list(value), do: Enum.flat_map(value, &index_names/1)

  defp index_names(value) when is_map(value) do
    own =
      case Map.get(value, "Index Name") do
        name when is_binary(name) -> [name]
        _missing -> []
      end

    own ++ (value |> Map.values() |> Enum.flat_map(&index_names/1))
  end

  defp index_names(_value), do: []

  defp manifest_version(manifest_version_id) do
    manifest = %Manifest{
      assets: [
        %Favn.Manifest.Asset{
          ref: {MyApp.PerformanceAsset, :asset},
          module: MyApp.PerformanceAsset,
          name: :asset
        }
      ]
    }

    {:ok, version} =
      Version.new(FavnTestSupport.with_manifest_graph(manifest),
        manifest_version_id: manifest_version_id
      )

    version
  end

  defp random_id, do: :crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower)
end
