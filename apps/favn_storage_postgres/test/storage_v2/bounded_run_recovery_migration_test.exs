defmodule FavnStoragePostgres.StorageV2.BoundedRunRecoveryMigrationTest do
  use ExUnit.Case, async: false
  alias Ecto.Adapters.SQL
  alias FavnStoragePostgres.{Config, Repo}
  alias FavnStoragePostgres.StorageV2.Migrations
  alias FavnStoragePostgres.TestSupport.{IsolatedDatabase, RunFixture}
  alias FavnOrchestrator.Persistence.Commands.ClaimRun

  test "populated RC17 schema upgrades without changing existing authority or run evidence" do
    url = IsolatedDatabase.create!(System.fetch_env!("FAVN_DATABASE_URL"))
    {:ok, options} = Config.repo_options(url: url, ssl_mode: :disable, pool_size: 2)
    start_supervised!({Repo, options})
    :ok = Migrations.migrate!(Repo)
    context = RunFixture.create("upgrade", ["upgrade-run"])

    assert {:ok, owner} =
             FavnStoragePostgres.RunOwnership.Store.claim_run(%ClaimRun{
               workspace_context: context,
               command_id: "before-upgrade",
               run_id: "upgrade-run",
               owner_id: "retained-owner",
               lease_duration_ms: 120_000
             })

    before =
      SQL.query!(
        Repo,
        "SELECT snapshot, snapshot_hash FROM favn_control.runs WHERE run_id='upgrade-run'",
        []
      ).rows

    # Reconstruct the exact predecessor schema on this disposable database.
    SQL.query!(
      Repo,
      "ALTER TABLE favn_control.rebuild_operations DROP COLUMN validation_request",
      []
    )

    SQL.query!(
      Repo,
      "ALTER TABLE favn_control.runner_tasks DROP CONSTRAINT runner_tasks_kind_valid",
      []
    )

    SQL.query!(
      Repo,
      "ALTER TABLE favn_control.runner_tasks ADD CONSTRAINT runner_tasks_kind_valid CHECK (task_kind IN ('asset_attempt','relation_inspection','generation_capabilities','generation_marker_read','generation_marker_initialize','generation_activate','generation_reconcile','generation_discard'))",
      []
    )

    SQL.query!(
      Repo,
      "DELETE FROM favn_control.schema_migrations WHERE version=20260923000000",
      []
    )

    SQL.query!(
      Repo,
      "ALTER TABLE favn_control.runner_tasks DROP COLUMN cleanup_fencing_token",
      []
    )

    SQL.query!(
      Repo,
      "ALTER TABLE favn_control.target_operation_locks DROP COLUMN last_renewal_id",
      []
    )

    SQL.query!(
      Repo,
      "ALTER TABLE favn_control.run_ownerships DROP COLUMN recovery_disposition, DROP COLUMN recovery_attempts, DROP COLUMN claim_purpose, DROP COLUMN next_recovery_at, DROP COLUMN attention_revision, DROP COLUMN last_renewed_at, DROP COLUMN diagnosis_reason",
      []
    )

    SQL.query!(
      Repo,
      "DELETE FROM favn_control.schema_migrations WHERE version=20260922000000",
      []
    )

    assert {:ok, old} = Migrations.diagnostics(Repo)

    assert old.actual_definition_fingerprint ==
             "7fa0d28caae6dbf2905134c3e49dc784a46ff123ebe824d8a8df6d13fa56763d"

    refute old.ready?

    SQL.query!(
      Repo,
      """
      INSERT INTO favn_control.rebuild_operations
        (workspace_id, operation_id, root_target_id, manifest_version_id, plan_hash,
         plan_version, plan_payload, trigger, actor_id, reason, idempotency_key,
         evaluated_at, action_count, window_count, state, phase, dispatcher_owner,
         dispatcher_expires_at, inserted_at, updated_at)
      SELECT $1, 'legacy-planning', 'legacy-target', manifest_version_id, repeat('a',64),
         1, '{"status":"planning"}'::jsonb, 'manual', 'operator', 'legacy test', 'legacy-planning',
         clock_timestamp(), 0, 0, 'planning', 'planning', 'old-owner',
         clock_timestamp() + interval '30 seconds', clock_timestamp(), clock_timestamp()
      FROM favn_control.manifest_versions LIMIT 1
      """,
      [context.workspace_id]
    )

    :ok = Migrations.migrate!(Repo)

    assert [["failed", "terminal", nil, nil, "rebuild_planning_failed", 0]] =
             SQL.query!(
               Repo,
               "SELECT state,phase,dispatcher_owner,dispatcher_expires_at,terminal_error->>'reason_code',action_count FROM favn_control.rebuild_operations WHERE workspace_id=$1 AND operation_id='legacy-planning'",
               [context.workspace_id]
             ).rows

    assert {:ok, %{ready?: true}} = Migrations.diagnostics(Repo)

    assert SQL.query!(
             Repo,
             "SELECT snapshot, snapshot_hash FROM favn_control.runs WHERE run_id='upgrade-run'",
             []
           ).rows == before

    assert %{rows: [["retained-owner", fence, expires, "automatic", 0, "execution"]]} =
             SQL.query!(
               Repo,
               "SELECT owner_id,fencing_token,expires_at,recovery_disposition,recovery_attempts,claim_purpose FROM favn_control.run_ownerships WHERE run_id='upgrade-run'",
               []
             )

    assert fence == owner.fencing_token
    assert expires == owner.expires_at

    assert_raise RuntimeError, ~r/cannot be discarded by binary rollback/, fn ->
      Ecto.Migrator.run(
        Repo,
        [{20_260_922_000_000, FavnStoragePostgres.Migrations.AddBoundedRunRecoveryV2}],
        :down,
        all: true,
        prefix: "favn_control"
      )
    end

    assert {:ok, %{ready?: true}} = Migrations.diagnostics(Repo)
  end
end
