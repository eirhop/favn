defmodule FavnStoragePostgres.StorageV2.RunSubmissionsTest do
  use ExUnit.Case, async: false

  alias Ecto.Adapters.SQL
  alias Favn.Manifest
  alias Favn.Manifest.Version
  alias FavnOrchestrator.Persistence.Commands.AcknowledgeRunSubmissionCancellation
  alias FavnOrchestrator.Persistence.Commands.ClaimRunSubmissions
  alias FavnOrchestrator.Persistence.Commands.ClaimStaleRunSubmissions
  alias FavnOrchestrator.Persistence.Commands.CreateRun
  alias FavnOrchestrator.Persistence.Commands.DeployManifest
  alias FavnOrchestrator.Persistence.Commands.DeploymentTarget
  alias FavnOrchestrator.Persistence.Commands.EnqueueRunSubmission
  alias FavnOrchestrator.Persistence.Commands.EnsureRunnerCapacityDemand
  alias FavnOrchestrator.Persistence.Commands.MarkRunSubmissionAdmitting
  alias FavnOrchestrator.Persistence.Commands.MarkRunSubmissionFailed
  alias FavnOrchestrator.Persistence.Commands.MarkRunSubmissionSubmitted
  alias FavnOrchestrator.Persistence.Commands.ProvisionWorkspace
  alias FavnOrchestrator.Persistence.Commands.RegisterManifest
  alias FavnOrchestrator.Persistence.Commands.RenewRunSubmissionClaim
  alias FavnOrchestrator.Persistence.Commands.RequestRunSubmissionCancellation
  alias FavnOrchestrator.Persistence.Commands.RequeueRunSubmission
  alias FavnOrchestrator.Persistence.Commands.RetryFailedRunSubmission
  alias FavnOrchestrator.Persistence.Commands.RunTarget
  alias FavnOrchestrator.Persistence.Commands.SupersedeRunSubmission
  alias FavnOrchestrator.Persistence.PlatformContext
  alias FavnOrchestrator.Persistence.Queries.GetRunSubmission
  alias FavnOrchestrator.Persistence.Queries.GetRunSubmissionByRunId
  alias FavnOrchestrator.Persistence.Queries.GetRunSubmissionStats
  alias FavnOrchestrator.Persistence.Queries.GetRun
  alias FavnOrchestrator.Persistence.Queries.GetRunnerReleaseDrain
  alias FavnOrchestrator.Persistence.Queries.PageClaimableRunSubmissionWorkspaces
  alias FavnOrchestrator.Persistence.Queries.PageRunSubmissions
  alias FavnOrchestrator.Persistence.RunSubmissionAuthority
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.RunSubmission.Worker
  alias FavnOrchestrator.TargetStatus
  alias FavnStoragePostgres.CanonicalJSON
  alias FavnStoragePostgres.Config
  alias FavnStoragePostgres.Registry.Store, as: RegistryStore
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.Runs.Store, as: RunStore
  alias FavnStoragePostgres.RunSubmissions.Store
  alias FavnStoragePostgres.RunnerTasks.Store, as: RunnerTaskStore
  alias FavnStoragePostgres.StorageV2.Migrations

  defmodule IntegrationPreparation do
    def prepare(_context, _submission),
      do: {:ok, :prepared_submission, %{"preparation" => "bounded"}}
  end

  defmodule CrashDuringPreparation do
    def prepare(_context, submission) do
      send(:persistent_term.get({__MODULE__, :test}), {:preparation_started, submission})

      receive do
        :continue -> {:ok, :prepared_submission, %{"preparation" => "bounded"}}
      end
    end
  end

  defmodule CrashDuringAdmission do
    def admit_claimed_submission(_prepared) do
      send(:persistent_term.get({__MODULE__, :test}), :admission_started)

      receive do
        :continue -> {:error, :unexpected_continue}
      end
    end
  end

  defmodule LostAcknowledgementRunManager do
    alias FavnStoragePostgres.Runs.Store, as: RunStore

    def admit_claimed_submission(:prepared_submission) do
      case RunStore.create_run(:persistent_term.get({__MODULE__, :command})) do
        {:ok, committed} -> {:error, {:admission_acknowledgement_lost, committed.run.id}}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  defmodule IntegrationRuns do
    alias FavnOrchestrator.Persistence.Queries.GetRun
    alias FavnStoragePostgres.Runs.Store, as: RunStore

    def get(context, run_id),
      do: RunStore.get_run(%GetRun{workspace_context: context, run_id: run_id})
  end

  setup_all do
    url =
      System.get_env("FAVN_DATABASE_URL") ||
        raise "FAVN_DATABASE_URL is required for PostgreSQL storage tests"

    {:ok, options} = Config.repo_options(url: url, ssl_mode: :disable, pool_size: 24)
    start_supervised!({Repo, options})
    :ok = Migrations.migrate!(Repo)

    version = manifest_version("run-submission-mv-#{random_id()}")

    {:ok, platform_context} =
      PlatformContext.new("run-submission-test", "manifest-publisher", [:platform_admin])

    {:ok, version} =
      RegistryStore.register_manifest(%RegisterManifest{
        platform_context: platform_context,
        version: version
      })

    {:ok, version: version}
  end

  setup %{version: version} do
    unique = random_id()
    workspace_id = "run-submission-ws-#{unique}"
    deployment_id = "run-submission-deployment-#{unique}"
    target_id = TargetStatus.target_id_for_asset({MyApp.RunSubmissionAsset, :asset})
    now = DateTime.utc_now()

    {:ok, platform_context} =
      PlatformContext.new("run-submission-test", "grant-#{unique}", [:platform_admin])

    :ok =
      RegistryStore.provision_workspace(%ProvisionWorkspace{
        platform_context: platform_context,
        workspace_id: workspace_id,
        slug: "run-submission-#{unique}",
        display_name: "Run submission #{unique}",
        occurred_at: now
      })

    {:ok, operator_context} =
      WorkspaceContext.new(workspace_id, "run-submission-worker", [:workspace_admin],
        request_id: "request-#{unique}"
      )

    {:ok, reader_context} =
      WorkspaceContext.new(workspace_id, "run-submission-reader", [:customer_reader])

    assert {:ok, _runtime} =
             RegistryStore.deploy_manifest(%DeployManifest{
               platform_context: platform_context,
               workspace_context: operator_context,
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
               schedules: [],
               capacity_scopes: [],
               occurred_at: now
             })

    on_exit(fn ->
      SQL.query!(
        Repo,
        "DELETE FROM favn_control.run_submission_commands WHERE workspace_id = $1",
        [workspace_id]
      )

      SQL.query!(
        Repo,
        "DELETE FROM favn_control.run_submissions WHERE workspace_id = $1",
        [workspace_id]
      )
    end)

    {:ok,
     workspace_id: workspace_id,
     workspace_context: operator_context,
     platform_context: platform_context,
     reader_context: reader_context,
     deployment_id: deployment_id,
     target_id: target_id,
     version: version}
  end

  test "concurrent enqueue is idempotent and conflicting intent is rejected", fixture do
    command = enqueue_command(fixture, "idempotent")

    results =
      1..2
      |> Task.async_stream(fn _index -> Store.enqueue(command) end,
        max_concurrency: 2,
        ordered: false,
        timeout: 30_000
      )
      |> Enum.map(fn {:ok, {:ok, result}} -> result end)

    assert [first, second] = results
    assert first == second
    assert first.status == :queued

    assert first.authority == %RunSubmissionAuthority{
             workspace_id: fixture.workspace_id,
             principal_id: fixture.workspace_context.principal_id,
             roles: [:workspace_admin],
             request_id: fixture.workspace_context.request_id
           }

    refute Map.has_key?(Map.from_struct(command), :authority)

    assert_raise KeyError, fn ->
      command
      |> Map.from_struct()
      |> Map.put(:authority, %{"token" => "must-not-be-persisted"})
      |> then(&struct!(EnqueueRunSubmission, &1))
    end

    %{rows: [[persisted_authority]]} =
      SQL.query!(
        Repo,
        """
        SELECT authority
        FROM favn_control.run_submissions
        WHERE workspace_id = $1 AND submission_id = $2
        """,
        [fixture.workspace_id, first.submission_id]
      )

    assert Map.keys(persisted_authority) |> Enum.sort() ==
             ~w(principal_id request_id roles workspace_id)

    assert persisted_authority["workspace_id"] == fixture.workspace_id
    refute Map.has_key?(persisted_authority, "token")

    assert_raise Postgrex.Error, fn ->
      SQL.query!(
        Repo,
        """
        UPDATE favn_control.run_submissions
        SET authority = '{
          "workspace_id": null,
          "principal_id": null,
          "roles": ["workspace_admin"],
          "request_id": null
        }'::jsonb
        WHERE workspace_id = $1 AND submission_id = $2
        """,
        [fixture.workspace_id, first.submission_id]
      )
    end

    assert_raise Postgrex.Error, fn ->
      SQL.query!(
        Repo,
        """
        UPDATE favn_control.run_submissions
        SET authority = jsonb_set(
          authority,
          '{roles}',
          '["workspace_admin", "workspace_admin"]'::jsonb
        )
        WHERE workspace_id = $1 AND submission_id = $2
        """,
        [fixture.workspace_id, first.submission_id]
      )
    end

    %{rows: [[1]]} =
      SQL.query!(
        Repo,
        "SELECT count(*) FROM favn_control.run_submissions WHERE workspace_id = $1",
        [fixture.workspace_id]
      )

    conflicting = %{
      command
      | command_id: "enqueue-conflict-#{random_id()}",
        submission_id: "submission-conflict-#{random_id()}",
        request_hash: :crypto.hash(:sha256, "different intent"),
        intent: %{"selection" => "different"}
    }

    assert {:error, %{kind: :conflict}} = Store.enqueue(conflicting)

    run_collision = %{
      enqueue_command(fixture, "run-collision")
      | run_id: first.run_id
    }

    assert {:error, %{kind: :conflict}} = Store.enqueue(run_collision)
  end

  test "an active run keeps its release alive between runnable asset tasks", fixture do
    now = DateTime.utc_now()
    release_id = fixture.version.runner_releases["default"]

    assert {:ok, _demand} =
             RunnerTaskStore.ensure_demand(%EnsureRunnerCapacityDemand{
               platform_context: fixture.platform_context,
               runner_pool: "default",
               required_runner_release_id: release_id,
               occurred_at: now
             })

    create_run!(fixture, "drain-gap-#{random_id()}")

    assert {:ok, drain} =
             RunnerTaskStore.release_drain(%GetRunnerReleaseDrain{
               platform_context: fixture.platform_context,
               runner_pool: "default",
               required_runner_release_id: release_id
             })

    assert drain.outstanding_task_count == 0
    assert drain.active_run_count >= 1
    assert drain.blocker_count >= 1
    refute drain.durable_drained?
  end

  test "malformed commands return typed errors before request encoding", fixture do
    enqueue = enqueue_command(fixture, "malformed")

    assert {:error, %{kind: :forbidden}} =
             Store.enqueue(%{enqueue | workspace_context: %{}})

    assert {:error, %{kind: :invalid}} =
             Store.enqueue(%{enqueue | source: "api"})

    assert {:error, %{kind: :invalid}} =
             Store.enqueue(%{enqueue | target_kind: "unknown"})

    assert {:error, %{kind: :invalid}} =
             Store.enqueue(%{enqueue | occurred_at: "not-a-datetime"})

    {:ok, _submission} = Store.enqueue(enqueue_command(fixture, "malformed-failure"))
    {:ok, [owned]} = Store.claim(claim_command(fixture, "claim-malformed-failure", "worker"))

    malformed_failure =
      fixture
      |> mark_failed_command(owned, owned.claim_owner, :safe)
      |> Map.put(:failure_kind, "safe")

    assert {:error, %{kind: :invalid}} = Store.mark_failed(malformed_failure)
  end

  test "claims use deterministic FIFO order and exact replay preserves list shape", fixture do
    available_at = DateTime.add(DateTime.utc_now(), -1, :second)

    submissions =
      Enum.map(1..3, fn index ->
        ordered_at = DateTime.add(available_at, index, :microsecond)

        {:ok, submission} =
          Store.enqueue(enqueue_command(fixture, "fifo-#{index}", available_at: ordered_at))

        submission
      end)

    command = claim_command(fixture, "claim-fifo", "worker", limit: 3)
    assert {:ok, claimed} = Store.claim(command)
    assert Enum.map(claimed, & &1.submission_id) == Enum.map(submissions, & &1.submission_id)
    assert Enum.all?(claimed, &(&1.status == :preparing))

    assert {:ok, replayed} = Store.claim(command)
    assert is_list(replayed)
    assert Enum.map(replayed, & &1.submission_id) == Enum.map(submissions, & &1.submission_id)
  end

  test "claimable workspace discovery is bounded, keyset-paged, and includes stale leases",
       fixture do
    future = DateTime.add(DateTime.utc_now(), 1, :hour)

    assert {:ok, _future} =
             Store.enqueue(
               enqueue_command(fixture, "future-workspace-discovery", available_at: future)
             )

    query = %PageClaimableRunSubmissionWorkspaces{
      platform_context: fixture.platform_context,
      limit: 1
    }

    assert {:ok, %{workspace_ids: []}} = Store.page_claimable_workspaces(query)

    assert {:ok, _available} =
             Store.enqueue(enqueue_command(fixture, "available-workspace-discovery"))

    assert {:ok, %{workspace_ids: [workspace_id], has_more?: false, next: nil}} =
             Store.page_claimable_workspaces(query)

    assert workspace_id == fixture.workspace_id

    assert {:ok, %{workspace_ids: []}} =
             Store.page_claimable_workspaces(%{query | after: fixture.workspace_id})

    assert {:ok, [claimed]} =
             Store.claim(
               claim_command(fixture, "claim-workspace-discovery", "workspace-discovery-worker")
             )

    assert {:ok, %{workspace_ids: []}} = Store.page_claimable_workspaces(query)
    expire_claim!(fixture, claimed)

    assert {:ok, %{workspace_ids: [workspace_id]}} =
             Store.page_claimable_workspaces(query)

    assert workspace_id == fixture.workspace_id

    assert {:error, %{kind: :invalid}} =
             Store.page_claimable_workspaces(%{query | limit: 201})
  end

  test "concurrent claims never assign one submission twice", fixture do
    Enum.each(1..3, fn index ->
      assert {:ok, _submission} = Store.enqueue(enqueue_command(fixture, "atomic-#{index}"))
    end)

    commands = [
      claim_command(fixture, "claim-a", "worker-a", limit: 2),
      claim_command(fixture, "claim-b", "worker-b", limit: 2)
    ]

    claimed =
      commands
      |> Task.async_stream(&Store.claim/1,
        max_concurrency: 2,
        ordered: false,
        timeout: 30_000
      )
      |> Enum.flat_map(fn {:ok, {:ok, batch}} -> batch end)

    assert length(claimed) == 3
    assert MapSet.size(MapSet.new(claimed, & &1.submission_id)) == 3
  end

  test "maximum claim batch fits its durable receipt at maximum identifier bounds", fixture do
    submissions =
      Enum.map(1..50, fn index ->
        submission_id = maximum_identifier("submission-#{index}")

        command =
          fixture
          |> enqueue_command("maximum-receipt-#{index}")
          |> Map.put(:submission_id, submission_id)

        assert {:ok, submission} = Store.enqueue(command)
        submission
      end)

    owner_id = maximum_identifier("claim-owner")
    command = claim_command(fixture, "maximum-receipt-claim", owner_id, limit: 50)

    assert {:ok, claimed} = Store.claim(command)
    assert length(claimed) == 50
    assert Enum.all?(claimed, &(&1.claim_owner == owner_id))
    assert Enum.map(claimed, & &1.submission_id) == Enum.map(submissions, & &1.submission_id)
    assert {:ok, ^claimed} = Store.claim(command)

    %{rows: [[stored_size, encoded_size]]} =
      SQL.query!(
        Repo,
        """
        SELECT pg_column_size(result), octet_length(result::text)
        FROM favn_control.run_submission_commands
        WHERE workspace_id = $1 AND command_id = $2
        """,
        [fixture.workspace_id, command.command_id]
      )

    assert stored_size <= 65_536
    assert encoded_size <= 65_536
  end

  test "expired claims are re-fenced and stale owners cannot commit", fixture do
    {:ok, _submission} = Store.enqueue(enqueue_command(fixture, "stale"))
    claim = claim_command(fixture, "claim-stale-source", "original-worker")
    assert {:ok, [owned]} = Store.claim(claim)

    SQL.query!(
      Repo,
      """
      UPDATE favn_control.run_submissions
      SET claim_expires_at = clock_timestamp() - interval '1 second'
      WHERE workspace_id = $1 AND submission_id = $2
      """,
      [fixture.workspace_id, owned.submission_id]
    )

    recover = %ClaimStaleRunSubmissions{
      workspace_context: fixture.workspace_context,
      command_id: "recover-#{random_id()}",
      owner_id: "recovery-worker",
      lease_duration_ms: 60_000,
      occurred_at: DateTime.utc_now(),
      limit: 1
    }

    assert {:ok, [recovered]} = Store.claim_stale(recover)
    assert recovered.status == :preparing
    assert recovered.claim_generation == owned.claim_generation + 1

    assert {:error, %{kind: :fenced}} = Store.claim(claim)

    assert {:error, %{kind: :fenced}} =
             Store.mark_failed(mark_failed_command(fixture, owned, "original-worker", :safe))

    expire_claim!(fixture, recovered)

    second_recover = %{
      recover
      | command_id: "second-recover-#{random_id()}",
        owner_id: "second-recovery-worker",
        occurred_at: DateTime.utc_now()
    }

    assert {:ok, [second_recovered]} = Store.claim_stale(second_recover)
    assert second_recovered.claim_generation == recovered.claim_generation + 1
    assert {:error, %{kind: :fenced}} = Store.claim_stale(recover)

    assert {:error, %{kind: :fenced}} =
             Store.mark_failed(mark_failed_command(fixture, recovered, "recovery-worker", :safe))

    assert {:ok, failed} =
             Store.mark_failed(
               mark_failed_command(fixture, second_recovered, "second-recovery-worker", :safe)
             )

    assert failed.status == :failed
  end

  test "claim renewal is live-only and nonterminal command receipts never upgrade fences",
       fixture do
    {:ok, _submission} = Store.enqueue(enqueue_command(fixture, "renew-fence"))
    assert {:ok, [owned]} = Store.claim(claim_command(fixture, "claim-renew", "worker"))

    renew = %RenewRunSubmissionClaim{
      workspace_context: fixture.workspace_context,
      command_id: "renew-#{random_id()}",
      submission_id: owned.submission_id,
      owner_id: owned.claim_owner,
      claim_generation: owned.claim_generation,
      lease_duration_ms: 120_000,
      occurred_at: DateTime.utc_now()
    }

    assert {:ok, renewed} = Store.renew(renew)
    assert DateTime.compare(renewed.claim_expires_at, owned.claim_expires_at) == :gt

    admitting_command = %MarkRunSubmissionAdmitting{
      workspace_context: fixture.workspace_context,
      command_id: "admitting-#{random_id()}",
      submission_id: renewed.submission_id,
      owner_id: renewed.claim_owner,
      claim_generation: renewed.claim_generation,
      preparation: %{"target_id" => fixture.target_id},
      occurred_at: DateTime.utc_now()
    }

    assert {:ok, admitting} = Store.mark_admitting(admitting_command)
    expire_claim!(fixture, admitting)

    recover = %ClaimStaleRunSubmissions{
      workspace_context: fixture.workspace_context,
      command_id: "recover-renew-#{random_id()}",
      owner_id: "recovery-worker",
      lease_duration_ms: 60_000,
      occurred_at: DateTime.utc_now(),
      limit: 1
    }

    assert {:ok, [_recovered]} = Store.claim_stale(recover)
    assert {:error, %{kind: :fenced}} = Store.renew(renew)
    assert {:error, %{kind: :fenced}} = Store.mark_admitting(admitting_command)
  end

  test "expired claims cannot be renewed", fixture do
    {:ok, _submission} = Store.enqueue(enqueue_command(fixture, "expired-renew"))
    assert {:ok, [owned]} = Store.claim(claim_command(fixture, "claim-expired-renew", "worker"))
    expire_claim!(fixture, owned)

    assert {:error, %{kind: :fenced}} =
             Store.renew(%RenewRunSubmissionClaim{
               workspace_context: fixture.workspace_context,
               command_id: "renew-expired-#{random_id()}",
               submission_id: owned.submission_id,
               owner_id: owned.claim_owner,
               claim_generation: owned.claim_generation,
               lease_duration_ms: 60_000,
               occurred_at: DateTime.utc_now()
             })
  end

  test "requeue clears ownership and honours future availability", fixture do
    {:ok, _submission} = Store.enqueue(enqueue_command(fixture, "requeue"))
    assert {:ok, [owned]} = Store.claim(claim_command(fixture, "claim-requeue", "worker"))
    available_at = DateTime.add(DateTime.utc_now(), 60, :second)

    requeue = %RequeueRunSubmission{
      workspace_context: fixture.workspace_context,
      command_id: "requeue-#{random_id()}",
      submission_id: owned.submission_id,
      owner_id: owned.claim_owner,
      claim_generation: owned.claim_generation,
      reason: %{"reason" => "safe preparation retry"},
      available_at: available_at,
      occurred_at: DateTime.utc_now()
    }

    assert {:ok, queued} =
             Store.requeue(requeue)

    assert queued.status == :queued
    assert queued.claim_owner == nil
    assert queued.error == %{"reason" => "safe preparation retry"}
    assert {:ok, []} = Store.claim(claim_command(fixture, "claim-too-early", "other-worker"))

    SQL.query!(
      Repo,
      """
      UPDATE favn_control.run_submissions
      SET available_at = clock_timestamp() - interval '1 second'
      WHERE workspace_id = $1 AND submission_id = $2
      """,
      [fixture.workspace_id, queued.submission_id]
    )

    assert {:ok, [_reclaimed]} =
             Store.claim(claim_command(fixture, "claim-requeued", "other-worker"))

    assert {:error, %{kind: :fenced}} = Store.requeue(requeue)
  end

  test "admitting cancellation is rejected and proven-safe recovery remains available", fixture do
    {:ok, _submission} = Store.enqueue(enqueue_command(fixture, "admitting-cancel"))
    assert {:ok, [owned]} = Store.claim(claim_command(fixture, "claim-admitting", "worker"))

    assert {:ok, admitting} =
             Store.mark_admitting(%MarkRunSubmissionAdmitting{
               workspace_context: fixture.workspace_context,
               command_id: "prepare-admitting-#{random_id()}",
               submission_id: owned.submission_id,
               owner_id: owned.claim_owner,
               claim_generation: owned.claim_generation,
               preparation: %{"target_id" => fixture.target_id},
               occurred_at: DateTime.utc_now()
             })

    assert {:error, %{kind: :conflict}} =
             Store.request_cancellation(
               cancellation_command(fixture, admitting, "too late for submission cancellation")
             )

    assert {:ok, queued} =
             Store.requeue(%RequeueRunSubmission{
               workspace_context: fixture.workspace_context,
               command_id: "requeue-admitting-#{random_id()}",
               submission_id: admitting.submission_id,
               owner_id: admitting.claim_owner,
               claim_generation: admitting.claim_generation,
               reason: %{"reason" => "proven safe before durable admission"},
               available_at: DateTime.utc_now(),
               occurred_at: DateTime.utc_now()
             })

    assert queued.status == :queued
    assert queued.cancellation_requested_at == nil
  end

  test "queued and active cancellation have explicit terminal handshakes", fixture do
    {:ok, queued} = Store.enqueue(enqueue_command(fixture, "queued-cancel"))

    assert {:ok, cancelled} =
             Store.request_cancellation(cancellation_command(fixture, queued, "not needed"))

    assert cancelled.status == :cancelled
    assert cancelled.terminal_at

    {:ok, _submission} = Store.enqueue(enqueue_command(fixture, "active-cancel"))
    assert {:ok, [owned]} = Store.claim(claim_command(fixture, "claim-active-cancel", "worker"))

    assert {:ok, cancellation_requested} =
             Store.request_cancellation(
               cancellation_command(fixture, owned, "operator cancelled")
             )

    assert cancellation_requested.status == :preparing
    assert cancellation_requested.cancellation_requested_at

    assert {:error, %{kind: :conflict}} =
             Store.requeue(%RequeueRunSubmission{
               workspace_context: fixture.workspace_context,
               command_id: "requeue-cancelled-#{random_id()}",
               submission_id: owned.submission_id,
               owner_id: owned.claim_owner,
               claim_generation: owned.claim_generation,
               reason: %{"reason" => "must acknowledge cancellation"},
               available_at: DateTime.utc_now(),
               occurred_at: DateTime.utc_now()
             })

    assert {:ok, cancelled} =
             Store.acknowledge_cancellation(%AcknowledgeRunSubmissionCancellation{
               workspace_context: fixture.workspace_context,
               command_id: "ack-cancel-#{random_id()}",
               submission_id: owned.submission_id,
               owner_id: owned.claim_owner,
               claim_generation: owned.claim_generation,
               occurred_at: DateTime.utc_now()
             })

    assert cancelled.status == :cancelled
    assert cancelled.claim_owner == nil
  end

  test "distinct deliberate commands create immutable retries only from safe failures", fixture do
    safe_failure = fail_submission(fixture, "safe-retry", :safe)

    {:ok, retry_context} =
      WorkspaceContext.new(fixture.workspace_id, "retry-operator", [:customer_operator],
        request_id: "retry-request-#{random_id()}"
      )

    retry = %RetryFailedRunSubmission{
      workspace_context: retry_context,
      command_id: "retry-command-#{random_id()}",
      failed_submission_id: safe_failure.submission_id,
      submission_id: "retry-submission-#{random_id()}",
      idempotency_key: "retry-idempotency-#{random_id()}",
      run_id: "retry-run-#{random_id()}",
      occurred_at: DateTime.utc_now()
    }

    assert {:ok, child} = Store.retry_failed(retry)
    assert child.status == :queued
    assert child.retry_root_id == safe_failure.retry_root_id
    assert child.retry_of_submission_id == safe_failure.submission_id
    assert child.intent == safe_failure.intent
    assert child.source == :operator
    assert child.authority.principal_id == retry_context.principal_id
    assert child.authority.request_id == retry_context.request_id
    assert {:ok, ^child} = Store.retry_failed(retry)

    assert {:ok, unchanged_failure} =
             Store.get(%GetRunSubmission{
               workspace_context: fixture.reader_context,
               submission_id: safe_failure.submission_id
             })

    assert unchanged_failure == safe_failure

    second_retry = %{
      retry
      | command_id: "second-retry-command-#{random_id()}",
        submission_id: "second-retry-submission-#{random_id()}",
        idempotency_key: "second-retry-idempotency-#{random_id()}",
        run_id: "second-retry-run-#{random_id()}"
    }

    assert {:ok, second_child} = Store.retry_failed(second_retry)
    assert second_child.submission_id != child.submission_id
    assert second_child.retry_of_submission_id == safe_failure.submission_id
    assert second_child.retry_command_id == second_retry.command_id

    assert {:error, %{kind: :conflict}} =
             Store.retry_failed(%{
               retry
               | command_id: "same-run-retry-command-#{random_id()}",
                 submission_id: "same-run-retry-submission-#{random_id()}",
                 idempotency_key: "same-run-retry-idempotency-#{random_id()}",
                 run_id: safe_failure.run_id
             })

    existing_retry_run_id = "existing-retry-run-#{random_id()}"
    create_run!(fixture, existing_retry_run_id)

    assert {:error, %{kind: :conflict}} =
             Store.retry_failed(%{
               retry
               | command_id: "existing-run-retry-command-#{random_id()}",
                 submission_id: "existing-run-retry-submission-#{random_id()}",
                 idempotency_key: "existing-run-retry-idempotency-#{random_id()}",
                 run_id: existing_retry_run_id
             })

    unknown_failure = fail_submission(fixture, "unknown-retry", :unknown)

    assert {:error, %{kind: :conflict}} =
             Store.retry_failed(%{
               retry
               | command_id: "unknown-retry-command-#{random_id()}",
                 failed_submission_id: unknown_failure.submission_id,
                 submission_id: "unknown-retry-submission-#{random_id()}",
                 idempotency_key: "unknown-retry-idempotency-#{random_id()}",
                 run_id: "unknown-retry-run-#{random_id()}"
             })
  end

  test "submitted state requires the exact durable run", fixture do
    run_id = "submission-run-#{random_id()}"
    {:ok, _submission} = Store.enqueue(enqueue_command(fixture, "submitted", run_id: run_id))
    assert {:ok, [owned]} = Store.claim(claim_command(fixture, "claim-submitted", "worker"))

    assert {:ok, admitting} =
             Store.mark_admitting(%MarkRunSubmissionAdmitting{
               workspace_context: fixture.workspace_context,
               command_id: "prepare-submitted-#{random_id()}",
               submission_id: owned.submission_id,
               owner_id: owned.claim_owner,
               claim_generation: owned.claim_generation,
               preparation: %{"target_id" => fixture.target_id},
               occurred_at: DateTime.utc_now()
             })

    submit_command = %MarkRunSubmissionSubmitted{
      workspace_context: fixture.workspace_context,
      command_id: "mark-submitted-#{random_id()}",
      submission_id: admitting.submission_id,
      owner_id: admitting.claim_owner,
      claim_generation: admitting.claim_generation,
      run_id: run_id,
      outcome: %{"run_id" => run_id},
      occurred_at: DateTime.utc_now()
    }

    assert {:error, %{kind: :constraint}} = Store.mark_submitted(submit_command)
    create_run!(fixture, run_id)
    assert {:ok, submitted} = Store.mark_submitted(submit_command)
    assert submitted.status == :submitted
    assert submitted.run_id == run_id
    assert submitted.claim_owner == nil
  end

  test "run creation cannot attach to a submission with different deployment, manifest, or target",
       fixture do
    mismatches = [
      [deployment_id: "other-deployment-#{random_id()}"],
      [manifest_version_id: "other-manifest-#{random_id()}"],
      [target_id: "other-target-#{random_id()}"]
    ]

    Enum.each(Enum.with_index(mismatches, 1), fn {opts, index} ->
      run_id = "colliding-submission-run-#{index}-#{random_id()}"

      _submission =
        admit_submission!(fixture, "collision-#{index}", Keyword.put(opts, :run_id, run_id))

      assert {:error, %{kind: :conflict}} =
               RunStore.create_run(create_run_command(fixture, run_id))
    end)
  end

  test "asset reconciliation requires the requested target to be primary", fixture do
    run_id = "non-primary-asset-run-#{random_id()}"
    admitting = admit_submission!(fixture, "non-primary-asset", run_id: run_id)

    non_primary_targets = [
      %RunTarget{
        target_kind: :asset,
        target_id: fixture.target_id,
        target_module: "MyApp.RunSubmissionAsset",
        target_name: "asset",
        is_primary: false
      }
    ]

    assert {:error, %{kind: :conflict}} =
             RunStore.create_run(
               create_run_command(fixture, run_id, targets: non_primary_targets)
             )

    create_run!(fixture, run_id)

    SQL.query!(
      Repo,
      """
      UPDATE favn_control.run_targets
      SET is_primary = false
      WHERE workspace_id = $1 AND run_id = $2 AND target_id = $3
      """,
      [fixture.workspace_id, run_id, fixture.target_id]
    )

    assert {:error, %{kind: :constraint}} =
             Store.mark_submitted(mark_submitted_command(fixture, admitting))
  end

  test "pipeline reconciliation uses its exact non-primary pipeline target", fixture do
    run_id = "pipeline-submission-run-#{random_id()}"
    pipeline_target_id = "pipeline-target-#{random_id()}"
    insert_deployment_target!(fixture, "pipeline", pipeline_target_id)

    admitting =
      admit_submission!(fixture, "pipeline-submission",
        run_id: run_id,
        target_kind: "pipeline",
        target_id: pipeline_target_id
      )

    pipeline_targets = [
      %RunTarget{
        target_kind: :asset,
        target_id: fixture.target_id,
        target_module: "MyApp.RunSubmissionAsset",
        target_name: "asset",
        is_primary: true
      },
      %RunTarget{
        target_kind: :pipeline,
        target_id: pipeline_target_id,
        target_module: "MyApp.RunSubmissionPipeline",
        target_name: "pipeline",
        is_primary: false
      }
    ]

    create_run!(fixture, run_id, targets: pipeline_targets)

    assert {:ok, submitted} =
             Store.mark_submitted(mark_submitted_command(fixture, admitting))

    assert submitted.status == :submitted
  end

  test "pre-existing durable runs and concurrent creators cannot steal submission identity",
       fixture do
    existing_run_id = "existing-durable-run-#{random_id()}"
    create_run!(fixture, existing_run_id)

    assert {:error, %{kind: :conflict}} =
             Store.enqueue(
               enqueue_command(fixture, "existing-durable-run", run_id: existing_run_id)
             )

    racing_run_id = "racing-run-identity-#{random_id()}"
    enqueue = enqueue_command(fixture, "racing-run-identity", run_id: racing_run_id)
    create = create_run_command(fixture, racing_run_id)

    results =
      [fn -> Store.enqueue(enqueue) end, fn -> RunStore.create_run(create) end]
      |> Task.async_stream(fn operation -> operation.() end,
        max_concurrency: 2,
        ordered: false,
        timeout: 30_000
      )
      |> Enum.map(fn {:ok, result} -> result end)

    assert Enum.count(results, &match?({:ok, _result}, &1)) == 1
    assert Enum.count(results, &match?({:error, %{kind: :conflict}}, &1)) == 1
  end

  test "read models are tenant-scoped, keyset-paged, and separately authorized", fixture do
    Enum.each(1..3, fn index ->
      assert {:ok, _submission} = Store.enqueue(enqueue_command(fixture, "page-#{index}"))
    end)

    assert {:ok, first_page} =
             Store.page(%PageRunSubmissions{
               workspace_context: fixture.reader_context,
               status: :queued,
               limit: 2
             })

    assert length(first_page.items) == 2
    assert first_page.next

    assert {:ok, second_page} =
             Store.page(%PageRunSubmissions{
               workspace_context: fixture.reader_context,
               status: :queued,
               after: first_page.next,
               limit: 2
             })

    assert length(second_page.items) == 1

    assert {:ok, fetched} =
             Store.get(%GetRunSubmission{
               workspace_context: fixture.reader_context,
               submission_id: hd(first_page.items).submission_id
             })

    assert fetched.workspace_id == fixture.workspace_id

    assert {:error, %{kind: :forbidden}} =
             Store.enqueue(
               enqueue_command(
                 %{fixture | workspace_context: fixture.reader_context},
                 "forbidden"
               )
             )

    {:ok, other_context} =
      WorkspaceContext.new("other-workspace", "reader", [:customer_reader])

    assert {:error, %{kind: :not_found}} =
             Store.get(%GetRunSubmission{
               workspace_context: other_context,
               submission_id: fetched.submission_id
             })

    {:ok, nonexistent_writer} =
      WorkspaceContext.new("missing-workspace", "worker", [:customer_operator])

    assert {:error, %{kind: :constraint}} =
             Store.claim(%ClaimRunSubmissions{
               workspace_context: nonexistent_writer,
               command_id: "missing-workspace-claim",
               owner_id: "worker",
               lease_duration_ms: 60_000,
               occurred_at: DateTime.utc_now()
             })
  end

  test "operator diagnostics expose queue age, retry, cancellation, and failure state", fixture do
    {:ok, _retrying} = Store.enqueue(enqueue_command(fixture, "stats-retrying"))

    {:ok, [retrying]} =
      Store.claim(claim_command(fixture, "claim-stats-retrying", "retry-worker"))

    now = DateTime.utc_now()
    retry_at = DateTime.add(now, 1, :hour)

    assert {:ok, %{status: :queued}} =
             Store.requeue(%RequeueRunSubmission{
               workspace_context: fixture.workspace_context,
               command_id: "requeue-stats-#{random_id()}",
               submission_id: retrying.submission_id,
               owner_id: retrying.claim_owner,
               claim_generation: retrying.claim_generation,
               reason: %{"reason" => "retry"},
               available_at: retry_at,
               occurred_at: now
             })

    {:ok, _cancelling} = Store.enqueue(enqueue_command(fixture, "stats-cancelling"))

    {:ok, [cancelling]} =
      Store.claim(claim_command(fixture, "claim-stats-cancelling", "cancel-worker"))

    assert {:ok, %{status: :preparing}} =
             Store.request_cancellation(
               cancellation_command(fixture, cancelling, "operator requested cancellation")
             )

    failed = fail_submission(fixture, "stats-failed", :unknown)
    {:ok, _queued} = Store.enqueue(enqueue_command(fixture, "stats-queued"))

    assert {:ok, stats} =
             Store.stats(%GetRunSubmissionStats{workspace_context: fixture.reader_context})

    assert stats.total == 4
    assert stats.counts == %{failed: 1, preparing: 1, queued: 2}
    assert stats.failure_counts == %{unknown: 1}
    assert stats.queued_depth == 2
    assert stats.active_depth == 1
    assert stats.retrying_depth == 1
    assert stats.cancellation_requested_depth == 1
    assert DateTime.compare(stats.oldest_queued_at, retrying.enqueued_at) in [:eq, :gt]
    assert stats.oldest_queued_age_ms >= 0
    assert %DateTime{} = stats.observed_at

    assert {:ok, fetched} =
             Store.get_by_run_id(%GetRunSubmissionByRunId{
               workspace_context: fixture.reader_context,
               run_id: failed.run_id
             })

    assert fetched.submission_id == failed.submission_id
    assert fetched.failure_kind == :unknown
  end

  test "a claimed intent reaches one durable run after a lost admission acknowledgement",
       fixture do
    command = enqueue_command(fixture, "worker-admission")
    assert {:ok, queued} = Store.enqueue(command)

    :persistent_term.put(
      {LostAcknowledgementRunManager, :command},
      create_run_command(fixture, queued.run_id)
    )

    lifecycle =
      start_supervised!(
        {FavnOrchestrator.Lifecycle,
         name: :"run_submission_lifecycle_#{random_id()}", shutdown_drain_timeout_ms: 1_000}
      )

    :ok = FavnOrchestrator.Lifecycle.mark_accepting(lifecycle)

    on_exit(fn ->
      :persistent_term.erase({LostAcknowledgementRunManager, :command})
    end)

    assert {:ok, %{status: :submitted, run_id: run_id}} =
             Worker.run(fixture.workspace_id,
               store: Store,
               lifecycle: lifecycle,
               owner_id: "integration-worker",
               lease_duration_ms: 60_000,
               renewal_interval_ms: 10_000,
               preparation: IntegrationPreparation,
               run_manager: LostAcknowledgementRunManager,
               runs: IntegrationRuns
             )

    assert run_id == queued.run_id

    assert {:ok, %RunState{id: ^run_id}} =
             RunStore.get_run(%GetRun{
               workspace_context: fixture.reader_context,
               run_id: run_id
             })

    assert :empty =
             Worker.run(fixture.workspace_id,
               store: Store,
               lifecycle: lifecycle,
               owner_id: "integration-worker-replay",
               lease_duration_ms: 60_000,
               renewal_interval_ms: 10_000,
               preparation: IntegrationPreparation,
               run_manager: LostAcknowledgementRunManager,
               runs: IntegrationRuns
             )
  end

  test "new workers recover queued, preparing, and admitting submissions after owner loss",
       fixture do
    lifecycle =
      start_supervised!(
        {FavnOrchestrator.Lifecycle,
         name: :"run_submission_restart_lifecycle_#{random_id()}",
         shutdown_drain_timeout_ms: 1_000}
      )

    :ok = FavnOrchestrator.Lifecycle.mark_accepting(lifecycle)
    :persistent_term.put({CrashDuringPreparation, :test}, self())
    :persistent_term.put({CrashDuringAdmission, :test}, self())

    on_exit(fn ->
      :persistent_term.erase({CrashDuringPreparation, :test})
      :persistent_term.erase({CrashDuringAdmission, :test})
      :persistent_term.erase({LostAcknowledgementRunManager, :command})
    end)

    Enum.each([:queued, :preparing, :admitting], fn interrupted_state ->
      command = enqueue_command(fixture, "restart-#{interrupted_state}")
      assert {:ok, queued} = Store.enqueue(command)

      interrupted =
        interrupt_submission_at!(
          fixture,
          queued,
          interrupted_state,
          lifecycle
        )

      if interrupted.status in [:preparing, :admitting], do: expire_claim!(fixture, interrupted)

      :persistent_term.put(
        {LostAcknowledgementRunManager, :command},
        create_run_command(fixture, queued.run_id)
      )

      assert {:ok, %{status: :submitted, run_id: run_id}} =
               recover_with_new_worker(fixture, lifecycle, interrupted_state)

      assert run_id == queued.run_id

      assert {:ok, %RunState{id: ^run_id}} =
               RunStore.get_run(%GetRun{
                 workspace_context: fixture.reader_context,
                 run_id: run_id
               })

      %{rows: [[1]]} =
        SQL.query!(
          Repo,
          """
          SELECT count(*)
          FROM favn_control.runs
          WHERE workspace_id = $1 AND run_id = $2
          """,
          [fixture.workspace_id, run_id]
        )
    end)
  end

  test "supersession locks both rows and cannot form a concurrent cycle", fixture do
    {:ok, original} = Store.enqueue(enqueue_command(fixture, "superseded-original"))
    {:ok, replacement} = Store.enqueue(enqueue_command(fixture, "superseded-replacement"))

    commands = [
      %SupersedeRunSubmission{
        workspace_context: fixture.workspace_context,
        command_id: "supersede-forward-#{random_id()}",
        submission_id: original.submission_id,
        replacement_submission_id: replacement.submission_id,
        occurred_at: DateTime.utc_now()
      },
      %SupersedeRunSubmission{
        workspace_context: fixture.workspace_context,
        command_id: "supersede-reverse-#{random_id()}",
        submission_id: replacement.submission_id,
        replacement_submission_id: original.submission_id,
        occurred_at: DateTime.utc_now()
      }
    ]

    results =
      commands
      |> Task.async_stream(&Store.supersede/1,
        max_concurrency: 2,
        ordered: false,
        timeout: 30_000
      )
      |> Enum.map(fn {:ok, result} -> result end)

    assert Enum.count(results, &match?({:ok, %{status: :superseded}}, &1)) == 1
    assert Enum.count(results, &match?({:error, %{kind: :conflict}}, &1)) == 1

    assert {:ok, current_original} =
             Store.get(%GetRunSubmission{
               workspace_context: fixture.reader_context,
               submission_id: original.submission_id
             })

    assert {:ok, current_replacement} =
             Store.get(%GetRunSubmission{
               workspace_context: fixture.reader_context,
               submission_id: replacement.submission_id
             })

    refute current_original.status == :superseded and
             current_replacement.status == :superseded
  end

  test "schema diagnostics include the exact run-submission authority surface" do
    assert {:ok, diagnostics} = Migrations.diagnostics(Repo)
    assert diagnostics.ready?
    assert diagnostics.definition_fingerprint_matches?
    assert diagnostics.missing_tables == []
    assert diagnostics.missing_critical_indexes == []
    assert diagnostics.missing_critical_constraints == []
  end

  test "expired receipts are pruned and their commands cannot mutate later work", fixture do
    old_command = %ClaimRunSubmissions{
      workspace_context: fixture.workspace_context,
      command_id: "expired-command-#{random_id()}",
      owner_id: "expired-worker",
      lease_duration_ms: 60_000,
      occurred_at: DateTime.add(DateTime.utc_now(), -8, :day),
      limit: 1
    }

    {:ok, request_hash} =
      CanonicalJSON.hash(%{
        "owner_id" => old_command.owner_id,
        "lease_duration_ms" => old_command.lease_duration_ms,
        "occurred_at" => DateTime.to_iso8601(old_command.occurred_at),
        "limit" => old_command.limit
      })

    SQL.query!(
      Repo,
      """
      INSERT INTO favn_control.run_submission_commands
        (workspace_id, command_id, submission_id, command_kind, request_hash, result, inserted_at)
      VALUES ($1, $2, NULL, 'claim', $3,
        '{"kind":"many","submission_ids":[],"result_fences":[]}'::jsonb,
        clock_timestamp() - interval '8 days')
      """,
      [fixture.workspace_id, old_command.command_id, request_hash]
    )

    assert {:ok, []} =
             Store.claim(claim_command(fixture, "trigger-receipt-prune", "current-worker"))

    %{rows: [[0]]} =
      SQL.query!(
        Repo,
        """
        SELECT count(*)
        FROM favn_control.run_submission_commands
        WHERE workspace_id = $1 AND command_id = $2
        """,
        [fixture.workspace_id, old_command.command_id]
      )

    {:ok, queued} = Store.enqueue(enqueue_command(fixture, "after-receipt-expiry"))
    assert {:error, %{kind: :invalid}} = Store.claim(old_command)

    assert {:ok, still_queued} =
             Store.get(%GetRunSubmission{
               workspace_context: fixture.reader_context,
               submission_id: queued.submission_id
             })

    assert still_queued.status == :queued
  end

  test "claim, stale-recovery, discovery, retention, and keyset reads use dedicated indexes",
       fixture do
    seed_page_plan_rows!(fixture)

    {:ok, {_claim, _stale, _discovery, _page, _status_page, _retention}} =
      Repo.transaction(fn ->
        SQL.query!(Repo, "SET LOCAL enable_seqscan = off", [])
        SQL.query!(Repo, "SET LOCAL enable_sort = off", [])

        claim =
          explain(
            """
            SELECT submission_id
            FROM favn_control.run_submissions
            WHERE workspace_id = $1
              AND status = 'queued'
              AND available_at <= clock_timestamp()
            ORDER BY available_at, enqueued_at, submission_id
            LIMIT 100
            FOR UPDATE SKIP LOCKED
            """,
            [fixture.workspace_id]
          )

        stale =
          explain(
            """
            SELECT submission_id
            FROM favn_control.run_submissions
            WHERE workspace_id = $1
              AND status IN ('preparing', 'admitting')
              AND claim_expires_at <= clock_timestamp()
            ORDER BY claim_expires_at, submission_id
            LIMIT 100
            FOR UPDATE SKIP LOCKED
            """,
            [fixture.workspace_id]
          )

        discovery =
          explain(
            """
            SELECT workspace_id
            FROM (
              SELECT workspace_id
              FROM favn_control.run_submissions
              WHERE status = 'queued'
                AND available_at <= clock_timestamp()
                AND workspace_id > $1
              UNION ALL
              SELECT workspace_id
              FROM favn_control.run_submissions
              WHERE status IN ('preparing', 'admitting')
                AND claim_expires_at <= clock_timestamp()
                AND workspace_id > $1
            ) AS candidates
            GROUP BY workspace_id
            ORDER BY workspace_id
            LIMIT 201
            """,
            [""]
          )

        page =
          explain(
            """
            SELECT submission_id, inserted_at
            FROM favn_control.run_submissions
            WHERE workspace_id = $1
            ORDER BY inserted_at DESC, submission_id DESC
            LIMIT 201
            """,
            [fixture.workspace_id]
          )

        status_page =
          explain(
            """
            SELECT submission_id, inserted_at
            FROM favn_control.run_submissions
            WHERE workspace_id = $1
              AND status = 'queued'
            ORDER BY inserted_at DESC, submission_id DESC
            LIMIT 201
            """,
            [fixture.workspace_id]
          )

        retention =
          explain(
            """
            SELECT workspace_id, command_id
            FROM favn_control.run_submission_commands
            WHERE inserted_at < clock_timestamp() - interval '7 days'
            ORDER BY inserted_at
            LIMIT 100
            FOR UPDATE SKIP LOCKED
            """,
            []
          )

        assert "run_submissions_claim_idx" in index_names(claim)
        assert "run_submissions_stale_claim_idx" in index_names(stale)
        assert "run_submissions_queued_workspace_idx" in index_names(discovery)
        assert "run_submissions_stale_claim_idx" in index_names(discovery)
        assert "run_submissions_page_idx" in index_names(page)
        assert "run_submissions_status_page_idx" in index_names(status_page)
        assert "run_submission_commands_retention_idx" in index_names(retention)
        {claim, stale, discovery, page, status_page, retention}
      end)
  end

  defp enqueue_command(fixture, name, opts \\ []) do
    run_id = Keyword.get(opts, :run_id, "run-#{name}-#{random_id()}")
    intent = %{"selection" => name}

    %EnqueueRunSubmission{
      workspace_context: fixture.workspace_context,
      command_id: "enqueue-#{name}-#{random_id()}",
      submission_id: "submission-#{name}-#{random_id()}",
      source: :api,
      idempotency_key: "idempotency-#{name}-#{random_id()}",
      request_hash: :crypto.hash(:sha256, Jason.encode!(intent)),
      deployment_id: Keyword.get(opts, :deployment_id, fixture.deployment_id),
      manifest_version_id:
        Keyword.get(opts, :manifest_version_id, fixture.version.manifest_version_id),
      target_kind: Keyword.get(opts, :target_kind, "asset"),
      target_id: Keyword.get(opts, :target_id, fixture.target_id),
      run_id: run_id,
      intent: intent,
      occurred_at: DateTime.utc_now(),
      available_at: Keyword.get(opts, :available_at)
    }
  end

  defp claim_command(fixture, command_name, owner_id, opts \\ []) do
    %ClaimRunSubmissions{
      workspace_context: fixture.workspace_context,
      command_id: "#{command_name}-#{random_id()}",
      owner_id: owner_id,
      lease_duration_ms: 60_000,
      occurred_at: DateTime.utc_now(),
      limit: Keyword.get(opts, :limit, 1)
    }
  end

  defp mark_failed_command(fixture, submission, owner_id, failure_kind) do
    %MarkRunSubmissionFailed{
      workspace_context: fixture.workspace_context,
      command_id: "fail-#{submission.submission_id}-#{owner_id}-#{random_id()}",
      submission_id: submission.submission_id,
      owner_id: owner_id,
      claim_generation: submission.claim_generation,
      failure_kind: failure_kind,
      error: %{"kind" => Atom.to_string(failure_kind)},
      occurred_at: DateTime.utc_now()
    }
  end

  defp mark_submitted_command(fixture, submission) do
    %MarkRunSubmissionSubmitted{
      workspace_context: fixture.workspace_context,
      command_id: "mark-submitted-#{submission.submission_id}-#{random_id()}",
      submission_id: submission.submission_id,
      owner_id: submission.claim_owner,
      claim_generation: submission.claim_generation,
      run_id: submission.run_id,
      outcome: %{"run_id" => submission.run_id},
      occurred_at: DateTime.utc_now()
    }
  end

  defp interrupt_submission_at!(_fixture, queued, :queued, _lifecycle), do: queued

  defp interrupt_submission_at!(fixture, queued, state, lifecycle)
       when state in [:preparing, :admitting] do
    preparation =
      if state == :preparing, do: CrashDuringPreparation, else: IntegrationPreparation

    run_manager =
      if state == :admitting, do: CrashDuringAdmission, else: LostAcknowledgementRunManager

    {worker, monitor} =
      spawn_monitor(fn ->
        Worker.run(fixture.workspace_id,
          store: Store,
          lifecycle: lifecycle,
          owner_id: "interrupted-#{state}-#{random_id()}",
          lease_duration_ms: 60_000,
          renewal_interval_ms: 10_000,
          preparation: preparation,
          run_manager: run_manager,
          runs: IntegrationRuns
        )
      end)

    case state do
      :preparing ->
        assert_receive {:preparation_started, %{run_id: run_id}} when run_id == queued.run_id

      :admitting ->
        assert_receive :admission_started
    end

    Process.exit(worker, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^worker, :killed}

    assert {:ok, interrupted} =
             Store.get(%GetRunSubmission{
               workspace_context: fixture.reader_context,
               submission_id: queued.submission_id
             })

    assert interrupted.status == state
    interrupted
  end

  defp recover_with_new_worker(fixture, lifecycle, state) do
    Worker.run(fixture.workspace_id,
      store: Store,
      lifecycle: lifecycle,
      owner_id: "recovery-#{state}-#{random_id()}",
      lease_duration_ms: 60_000,
      renewal_interval_ms: 10_000,
      preparation: IntegrationPreparation,
      run_manager: LostAcknowledgementRunManager,
      runs: IntegrationRuns
    )
  end

  defp cancellation_command(fixture, submission, reason) do
    %RequestRunSubmissionCancellation{
      workspace_context: fixture.workspace_context,
      command_id: "cancel-#{submission.submission_id}-#{random_id()}",
      submission_id: submission.submission_id,
      reason: reason,
      occurred_at: DateTime.utc_now()
    }
  end

  defp fail_submission(fixture, name, failure_kind) do
    {:ok, _submission} = Store.enqueue(enqueue_command(fixture, name))
    {:ok, [owned]} = Store.claim(claim_command(fixture, "claim-#{name}", "worker-#{name}"))

    {:ok, failed} =
      Store.mark_failed(mark_failed_command(fixture, owned, owned.claim_owner, failure_kind))

    failed
  end

  defp admit_submission!(fixture, name, enqueue_opts) do
    {:ok, _submission} = Store.enqueue(enqueue_command(fixture, name, enqueue_opts))
    {:ok, [owned]} = Store.claim(claim_command(fixture, "claim-#{name}", "worker-#{name}"))

    {:ok, admitting} =
      Store.mark_admitting(%MarkRunSubmissionAdmitting{
        workspace_context: fixture.workspace_context,
        command_id: "prepare-#{name}-#{random_id()}",
        submission_id: owned.submission_id,
        owner_id: owned.claim_owner,
        claim_generation: owned.claim_generation,
        preparation: %{"target_id" => owned.target_id},
        occurred_at: DateTime.utc_now()
      })

    admitting
  end

  defp expire_claim!(fixture, submission) do
    SQL.query!(
      Repo,
      """
      UPDATE favn_control.run_submissions
      SET claim_expires_at = clock_timestamp() - interval '1 second'
      WHERE workspace_id = $1 AND submission_id = $2
      """,
      [fixture.workspace_id, submission.submission_id]
    )
  end

  defp insert_deployment_target!(fixture, target_kind, target_id) do
    SQL.query!(
      Repo,
      """
      INSERT INTO favn_control.workspace_deployment_targets (
        workspace_id, deployment_id, target_kind, target_id, selection_source,
        customer_visible, descriptor, inserted_at
      )
      VALUES (
        $1, $2, $3, $4, 'common', true,
        jsonb_build_object('target_id', $4::text, 'label', $4::text),
        clock_timestamp()
      )
      """,
      [fixture.workspace_id, fixture.deployment_id, target_kind, target_id]
    )
  end

  defp seed_page_plan_rows!(fixture) do
    prefix = "page-plan-#{random_id()}"

    SQL.query!(
      Repo,
      """
      INSERT INTO favn_control.run_submissions (
        workspace_id, submission_id, source, idempotency_key, request_hash,
        authority, deployment_id, manifest_version_id, target_kind, target_id,
        run_id, intent, status, attempt, claim_generation, error, failure_kind,
        retry_root_id, enqueued_at, available_at, terminal_at, inserted_at, updated_at
      )
      SELECT
        $1::text,
        $5::text || '-submission-' || item,
        'api',
        $5::text || '-idempotency-' || item,
        decode(repeat('00', 32), 'hex'),
        jsonb_build_object(
          'workspace_id', $1::text,
          'principal_id', 'query-plan-seed',
          'roles', jsonb_build_array('workspace_admin'),
          'request_id', NULL
        ),
        $2::text,
        $3::text,
        'asset',
        $4::text,
        $5::text || '-run-' || item,
        '{}'::jsonb,
        CASE WHEN item % 50 = 0 THEN 'queued' ELSE 'failed' END,
        0,
        0,
        CASE WHEN item % 50 = 0 THEN NULL ELSE '{}'::jsonb END,
        CASE WHEN item % 50 = 0 THEN NULL ELSE 'safe' END,
        $5::text || '-submission-' || item,
        clock_timestamp() - item * interval '1 microsecond',
        clock_timestamp() - item * interval '1 microsecond',
        CASE
          WHEN item % 50 = 0 THEN NULL
          ELSE clock_timestamp() - item * interval '1 microsecond'
        END,
        clock_timestamp() - item * interval '1 microsecond',
        clock_timestamp() - item * interval '1 microsecond'
      FROM generate_series(1, 2_000) AS item
      """,
      [
        fixture.workspace_id,
        fixture.deployment_id,
        fixture.version.manifest_version_id,
        fixture.target_id,
        prefix
      ]
    )

    SQL.query!(Repo, "ANALYZE favn_control.run_submissions", [])
  end

  defp create_run!(fixture, run_id, opts \\ []) do
    assert {:ok, _created} = RunStore.create_run(create_run_command(fixture, run_id, opts))
  end

  defp create_run_command(fixture, run_id, opts \\ []) do
    run =
      RunState.new(
        id: run_id,
        workspace_id: fixture.workspace_id,
        deployment_id: fixture.deployment_id,
        manifest_version_id: fixture.version.manifest_version_id,
        manifest_content_hash: fixture.version.content_hash,
        runner_releases: fixture.version.runner_releases,
        asset_ref: {MyApp.RunSubmissionAsset, :asset},
        target_refs: [{MyApp.RunSubmissionAsset, :asset}]
      )

    targets =
      Keyword.get(opts, :targets, [
        %RunTarget{
          target_kind: :asset,
          target_id: fixture.target_id,
          target_module: "MyApp.RunSubmissionAsset",
          target_name: "asset",
          is_primary: true
        }
      ])

    %CreateRun{
      workspace_context: fixture.workspace_context,
      command_id: "create-#{run_id}-#{random_id()}",
      deployment_id: fixture.deployment_id,
      run: run,
      targets: targets,
      event: %{
        run_id: run.id,
        sequence: 1,
        event_type: :run_submitted,
        status: :pending,
        occurred_at: run.inserted_at
      }
    }
  end

  defp manifest_version(manifest_version_id) do
    manifest = %Manifest{
      metadata: %{"fixture_id" => manifest_version_id},
      assets: [
        %Favn.Manifest.Asset{
          ref: {MyApp.RunSubmissionAsset, :asset},
          module: MyApp.RunSubmissionAsset,
          name: :asset
        }
      ],
      pipelines: []
    }

    {:ok, version} =
      Version.new(
        manifest
        |> FavnTestSupport.with_manifest_contract()
        |> FavnTestSupport.with_manifest_graph(),
        manifest_version_id: manifest_version_id
      )

    version
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

  defp random_id, do: :crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower)

  defp maximum_identifier(seed) do
    0..4
    |> Enum.map_join(fn part ->
      :sha256
      |> :crypto.hash("#{seed}-#{part}")
      |> Base.encode16(case: :lower)
    end)
    |> binary_part(0, 255)
  end
end
