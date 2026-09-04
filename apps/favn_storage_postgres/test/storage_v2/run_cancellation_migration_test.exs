defmodule FavnStoragePostgres.StorageV2.RunCancellationMigrationTest do
  use ExUnit.Case, async: false
  @moduletag :slow

  alias Ecto.Adapters.SQL
  alias FavnOrchestrator.Persistence.BackfillPlan
  alias FavnOrchestrator.Persistence.Commands, as: C
  alias FavnOrchestrator.RunSubmission.Intent
  alias FavnStoragePostgres.Backfills.Store, as: Backfills
  alias FavnStoragePostgres.Config
  alias FavnStoragePostgres.Migrations.AddRunCancellationV2
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.ResourceCircuits.Store, as: Circuits
  alias FavnStoragePostgres.RunSubmissions.Store, as: Submissions
  alias FavnStoragePostgres.StorageV2.Migrations
  alias FavnStoragePostgres.TestSupport.RunFixture

  @version 20_260_904_010_000

  test "upgrades combined windows, pending work and recovery chains without adopting explicit reruns" do
    source = System.fetch_env!("FAVN_DATABASE_URL")
    database = "favn_cancel_upgrade_" <> Base.encode16(:crypto.strong_rand_bytes(6), case: :lower)
    source_tool = String.replace_prefix(source, "ecto://", "postgresql://")

    assert {_, 0} =
             System.cmd("createdb", ["--maintenance-db", source_tool, database],
               stderr_to_stdout: true
             )

    target = URI.to_string(%{URI.parse(source) | path: "/" <> database})
    {:ok, options} = Config.repo_options(url: target, ssl_mode: :disable, pool_size: 4)
    start_supervised!({Repo, options})

    on_exit(fn ->
      System.cmd("dropdb", ["--force", "--if-exists", "--maintenance-db", source_tool, database],
        stderr_to_stdout: true
      )
    end)

    assert :ok = Migrations.migrate!(Repo)
    assert {:ok, %{ready?: true}} = Migrations.diagnostics(Repo)

    context = RunFixture.create("cancel-upgrade", ["root", "explicit-run"])

    root =
      Repo.get_by!(FavnStoragePostgres.Schemas.Run,
        workspace_id: context.workspace_id,
        run_id: "root"
      )

    target_id = FavnOrchestrator.TargetStatus.target_id_for_asset({RunFixture, :asset})

    windows =
      for index <- 1..2 do
        start = DateTime.add(~U[2026-01-01 00:00:00Z], index * 86_400)

        %C.BackfillPlanWindow{
          window_id: "w#{index}",
          window_key: "w#{index}",
          window_start: start,
          window_end: DateTime.add(start, 86_400),
          payload: %{"execution_group_id" => "combined"}
        }
      end

    batch_hash = BackfillPlan.batch_hash(windows)

    start = %C.StartBackfillPlan{
      workspace_context: context,
      command_id: "start",
      backfill_id: "backfill",
      root_run_id: "root",
      deployment_id: root.deployment_id,
      manifest_version_id: root.manifest_version_id,
      target_kind: :asset,
      target_id: target_id,
      range_start: hd(windows).window_start,
      range_end: List.last(windows).window_end,
      expected_window_count: 2,
      expected_batch_count: 1,
      plan_hash: BackfillPlan.plan_hash([batch_hash]),
      occurred_at: DateTime.utc_now()
    }

    assert {:ok, _} = Backfills.start_plan(start)

    assert {:ok, _} =
             Backfills.append_plan_batch(%C.AppendBackfillPlanBatch{
               workspace_context: context,
               command_id: "append",
               backfill_id: "backfill",
               batch_index: 0,
               batch_hash: batch_hash,
               windows: windows,
               occurred_at: DateTime.utc_now()
             })

    child = BackfillPlan.child_run_id("backfill", "w1", hd(windows).payload)

    {:ok, backfill_intent} =
      Intent.new(:asset, target_id,
        metadata: %{backfill_id: "backfill", backfill_window_id: "w1"}
      )

    enqueue!(context, root, target_id, child, :backfill, backfill_intent)
    # The ledger may link a reserved submission before admission commits a Run.
    SQL.query!(Repo, "UPDATE favn_control.backfill_windows SET run_id=$1 WHERE workspace_id=$2", [
      child,
      context.workspace_id
    ])

    admit!(context)
    RunFixture.create(context.workspace_id, [child])
    first_recovery = recovery!(context, root, target_id, child, "first")
    admit!(context)
    RunFixture.create(context.workspace_id, [first_recovery])
    second_recovery = recovery!(context, root, target_id, first_recovery, "second")
    {:ok, explicit_intent} = Intent.new(:rerun, child, [])
    enqueue!(context, root, target_id, "explicit", :child_run, explicit_intent)

    SQL.query!(
      Repo,
      "UPDATE favn_control.runs SET root_execution_group_id='root' WHERE workspace_id=$1",
      [context.workspace_id]
    )

    assert {:ok, _} =
             Backfills.start_plan(%{start | command_id: "ambiguous", backfill_id: "ambiguous"})

    # Reproduce the previous schema, retaining all durable membership and payloads.
    restore_previous_schema!()

    assert_raise RuntimeError, ~r/ambiguous cancellation owner/, &upgrade!/0
    SQL.query!(Repo, "DELETE FROM favn_control.backfills WHERE backfill_id='ambiguous'", [])

    SQL.query!(Repo, "UPDATE favn_control.run_submissions SET intent='{}' WHERE run_id=$1", [
      second_recovery
    ])

    assert_raise RuntimeError, ~r/cancellation ownership cannot be verified/, &upgrade!/0
    {:ok, second_intent} = recovery_intent(first_recovery, "second")

    SQL.query!(Repo, "UPDATE favn_control.run_submissions SET intent=$2 WHERE run_id=$1", [
      second_recovery,
      second_intent
    ])

    assert [@version] = upgrade!()

    assert SQL.query!(
             Repo,
             "SELECT DISTINCT cancellation_owner_run_id FROM favn_control.run_submissions WHERE run_id=ANY($1)",
             [[child, first_recovery, second_recovery]]
           ).rows == [["root"]]

    assert SQL.query!(
             Repo,
             "SELECT DISTINCT cancellation_owner_run_id FROM favn_control.runs WHERE run_id=ANY($1)",
             [["root", child, first_recovery]]
           ).rows == [["root"]]

    assert SQL.query!(
             Repo,
             "SELECT cancellation_owner_run_id FROM favn_control.run_submissions WHERE run_id='explicit'",
             []
           ).rows == [["explicit"]]

    assert SQL.query!(
             Repo,
             "SELECT cancellation_owner_run_id FROM favn_control.runs WHERE run_id='explicit-run'",
             []
           ).rows == [["explicit-run"]]

    assert {:ok, %{ready?: true}} = Migrations.diagnostics(Repo)
  end

  defp admit!(context) do
    unique = System.unique_integer([:positive]) |> Integer.to_string()

    assert {:ok, [owned]} =
             Submissions.claim(%C.ClaimRunSubmissions{
               workspace_context: context,
               command_id: "claim:" <> unique,
               owner_id: "worker",
               limit: 1,
               lease_duration_ms: 60_000,
               occurred_at: DateTime.utc_now()
             })

    assert {:ok, _} =
             Submissions.mark_admitting(%C.MarkRunSubmissionAdmitting{
               workspace_context: context,
               command_id: "admit:" <> unique,
               submission_id: owned.submission_id,
               owner_id: owned.claim_owner,
               claim_generation: owned.claim_generation,
               preparation: %{},
               occurred_at: DateTime.utc_now()
             })
  end

  defp recovery!(context, root, target_id, source, candidate_id) do
    candidate = %C.RecordResourceRecoveryCandidate{
      workspace_context: context,
      candidate_id: candidate_id,
      source_run_id: source,
      node_key: {{RunFixture, :asset}, nil},
      resource: Favn.Resource.Ref.new!(:connection, "warehouse"),
      reason: :safe_failure,
      max_age_ms: 60_000,
      occurred_at: DateTime.utc_now()
    }

    assert :ok = Circuits.record_recovery_candidate(candidate)

    run_id =
      FavnOrchestrator.ResourceRecovery.recovery_run_id(
        context.workspace_id,
        source,
        [candidate],
        candidate.resource
      )

    {:ok, intent} = recovery_intent(source, candidate_id)
    enqueue!(context, root, target_id, run_id, :recovery, intent)
    run_id
  end

  defp recovery_intent(source, candidate_id),
    do:
      Intent.new(:rerun, source,
        metadata: %{
          resource_recovery_source_run_id: source,
          resource_recovery_candidate_ids: [candidate_id]
        }
      )

  defp enqueue!(context, root, target_id, run_id, source, intent) do
    assert {:ok, _} =
             Submissions.enqueue(%C.EnqueueRunSubmission{
               workspace_context: context,
               command_id: "enqueue:" <> run_id,
               submission_id: "sub:" <> run_id,
               source: source,
               idempotency_key: run_id,
               request_hash: :crypto.hash(:sha256, run_id),
               deployment_id: root.deployment_id,
               manifest_version_id: root.manifest_version_id,
               target_kind: "asset",
               target_id: target_id,
               run_id: run_id,
               intent: intent,
               occurred_at: DateTime.utc_now()
             })
  end

  defp restore_previous_schema! do
    for table <- ~w(runs run_submissions),
        do:
          SQL.query!(
            Repo,
            "ALTER TABLE favn_control.#{table} DROP COLUMN cancellation_owner_run_id",
            []
          )

    SQL.query!(
      Repo,
      "ALTER TABLE favn_control.runs DROP COLUMN cancellation_requested_at, DROP COLUMN cancellation_status",
      []
    )

    SQL.query!(
      Repo,
      "ALTER TABLE favn_control.backfills DROP COLUMN cancellation_requested_at, DROP COLUMN cancellation_reason",
      []
    )

    for index <-
          ~w(backfills_cancelling_idx run_submissions_requested_cancellation_idx resource_recovery_candidates_source_idx resource_recovery_candidates_run_idx materializations_run_generation_idx),
        do: SQL.query!(Repo, "DROP INDEX IF EXISTS favn_control.#{index}", [])

    for {table, constraint, remove} <- [
          {"backfills", "backfills_values_valid",
           ", 'cancelling'::text, 'needs_attention'::text"},
          {"resource_recovery_candidates", "resource_recovery_candidates_values_valid",
           ", 'cancelled'::text"}
        ] do
      %{rows: [[definition]]} =
        SQL.query!(
          Repo,
          "SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname=$1 AND conrelid=$2::text::regclass",
          [constraint, "favn_control." <> table]
        )

      SQL.query!(
        Repo,
        "ALTER TABLE favn_control.#{table} DROP CONSTRAINT #{constraint}, ADD CONSTRAINT #{constraint} #{String.replace(definition, remove, "")}",
        []
      )
    end

    SQL.query!(Repo, "DELETE FROM favn_control.schema_migrations WHERE version=$1", [@version])
  end

  defp upgrade!,
    do:
      Ecto.Migrator.run(Repo, [{@version, AddRunCancellationV2}], :up,
        all: true,
        prefix: "favn_control"
      )
end
