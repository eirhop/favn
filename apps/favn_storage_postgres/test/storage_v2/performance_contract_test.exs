defmodule FavnStoragePostgres.StorageV2.PerformanceContractTest do
  use ExUnit.Case, async: false

  @moduletag :slow

  alias Ecto.Adapters.SQL
  alias Favn.Manifest
  alias Favn.Manifest.Version
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
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.TargetStatus
  alias FavnStoragePostgres.Backfills.Store, as: BackfillStore
  alias FavnStoragePostgres.Config
  alias FavnStoragePostgres.Registry.Store, as: RegistryStore
  alias FavnStoragePostgres.Repo
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
         customer_visible, inserted_at)
      SELECT $1, $2, 'asset', 'bulk-target-' || lpad(series::text, 5, '0'),
             'dependency', false, clock_timestamp()
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
               workspace_context: workspace_context,
               deployment_id: deployment_id,
               manifest_version_id: version.manifest_version_id,
               configuration: %{"resources" => %{}},
               targets: [
                 %DeploymentTarget{
                   target_kind: :asset,
                   target_id: target_id,
                   selection_source: :common,
                   customer_visible: true
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
