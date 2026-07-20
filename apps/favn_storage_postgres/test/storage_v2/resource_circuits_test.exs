defmodule FavnStoragePostgres.StorageV2.ResourceCircuitsTest do
  use ExUnit.Case, async: false

  alias Favn.CircuitBreaker.Policy
  alias Favn.Contracts.ResourceOutcome
  alias Favn.Resource.Ref
  alias FavnOrchestrator.Persistence.Commands.AcquireResourceCircuits
  alias FavnOrchestrator.Persistence.Commands.ClaimResourceRecovery
  alias FavnOrchestrator.Persistence.Commands.CompleteResourceRecovery
  alias FavnOrchestrator.Persistence.Commands.ListPendingResourceRecoveries
  alias FavnOrchestrator.Persistence.Commands.RecordResourceOutcomes
  alias FavnOrchestrator.Persistence.Commands.RecordResourceRecoveryCandidate
  alias FavnOrchestrator.Persistence.Commands.ReleaseResourceCircuitPermits
  alias FavnOrchestrator.Persistence.Commands.ResourceCircuitRequest
  alias FavnOrchestrator.Persistence.Results.ResourceCircuitAdmission
  alias FavnOrchestrator.Persistence.Results.ResourceCircuitBlocker
  alias FavnOrchestrator.Persistence.Results.ResourceCircuitPermit
  alias FavnOrchestrator.Persistence.Results.ResourceCircuitUpdate
  alias FavnOrchestrator.Persistence.Results.ResourceRecoveryBatch
  alias FavnOrchestrator.Persistence.Results.ResourceRecoveryWakeup
  alias FavnOrchestrator.Persistence.SystemContext
  alias FavnStoragePostgres.Config
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.ResourceCircuits.Store
  alias FavnStoragePostgres.StorageV2.Migrations

  setup_all do
    url =
      System.get_env("FAVN_DATABASE_URL") ||
        raise "FAVN_DATABASE_URL is required for PostgreSQL storage tests"

    migrator_url =
      System.get_env("FAVN_DATABASE_MIGRATOR_URL") ||
        "ecto://favn_migrator:favn_migrator_local@127.0.0.1:5432/favn_dev"

    {:ok, migrator_options} =
      Config.repo_options(url: migrator_url, ssl_mode: :disable, pool_size: 2)

    {:ok, migrator} = Repo.start_link(migrator_options)
    :ok = Migrations.migrate!(Repo)
    GenServer.stop(migrator)

    {:ok, options} = Config.repo_options(url: url, ssl_mode: :disable, pool_size: 8)
    start_supervised!({Repo, options})
    :ok
  end

  setup do
    unique = :crypto.strong_rand_bytes(8) |> Base.url_encode64(padding: false)
    workspace_id = "resource-circuit-test-#{unique}"
    resource = Ref.new!(:connection, "warehouse-#{unique}")

    {:ok, workspace_id: workspace_id, resource: resource}
  end

  test "only one concurrent caller receives the due half-open probe", fixture do
    now = DateTime.utc_now()
    first = acquire(fixture, "owner-first", now)
    assert %ResourceCircuitAdmission{status: :allowed} = first

    [%ResourceCircuitPermit{} = permit] = first.permits

    assert {:ok, %ResourceCircuitUpdate{closed_resources: []}} =
             Store.record_outcomes(outcome_command(fixture, permit, "failure-1", :failure, now))

    assert %ResourceCircuitAdmission{
             status: :blocked,
             blockers: [
               %ResourceCircuitBlocker{
                 failure_threshold: 1,
                 consecutive_failures: 1,
                 retry_at: %DateTime{}
               }
             ]
           } = acquire(fixture, "owner-too-early", now)

    due = DateTime.add(now, 2, :millisecond)

    results =
      ["owner-probe-a", "owner-probe-b"]
      |> Task.async_stream(&acquire(fixture, &1, due),
        max_concurrency: 2,
        ordered: false,
        timeout: 10_000
      )
      |> Enum.map(fn {:ok, result} -> result end)

    assert 1 == Enum.count(results, &match?(%ResourceCircuitAdmission{status: :allowed}, &1))
    assert 1 == Enum.count(results, &match?(%ResourceCircuitAdmission{status: :blocked}, &1))

    %ResourceCircuitAdmission{permits: [probe]} =
      Enum.find(results, &match?(%ResourceCircuitAdmission{status: :allowed}, &1))

    assert probe.probe?

    assert {:ok, %ResourceCircuitUpdate{closed_resources: []}} =
             Store.record_outcomes(
               outcome_command(fixture, probe, "probe-failure", :failure, due)
             )

    assert %ResourceCircuitAdmission{status: :blocked} = acquire(fixture, "owner-reopened", due)

    second_due = DateTime.add(due, 2, :millisecond)

    %ResourceCircuitAdmission{permits: [second_probe]} =
      acquire(fixture, "owner-second-probe", second_due)

    assert second_probe.probe?

    assert {:ok, %ResourceCircuitUpdate{closed_resources: [closed]}} =
             Store.record_outcomes(
               outcome_command(fixture, second_probe, "probe-success", :success, second_due)
             )

    assert closed == fixture.resource

    assert %ResourceCircuitAdmission{status: :allowed} =
             acquire(fixture, "owner-after", second_due)
  end

  test "terminal outcomes are idempotent and recovery candidates are durably claimable",
       fixture do
    now = DateTime.utc_now()
    %ResourceCircuitAdmission{permits: [permit]} = acquire(fixture, "owner", now)
    command = outcome_command(fixture, permit, "same-outcome", :failure, now)

    assert {:ok, %ResourceCircuitUpdate{}} = Store.record_outcomes(command)
    assert {:ok, %ResourceCircuitUpdate{}} = Store.record_outcomes(command)

    assert {:ok, %ResourceCircuitUpdate{}} =
             Store.record_outcomes(%{command | command_id: "same-terminal-identity-new-command"})

    candidate = %RecordResourceRecoveryCandidate{
      workspace_context: context(fixture),
      candidate_id: "candidate-1",
      source_run_id: "source-run",
      node_key: {{__MODULE__, :asset}, nil},
      resource: fixture.resource,
      reason: :safe_failure,
      max_age_ms: 60_000,
      occurred_at: now
    }

    assert :ok = Store.record_recovery_candidate(candidate)
    assert :ok = Store.record_recovery_candidate(candidate)

    assert {:ok, wakeups_while_open} =
             Store.list_pending_recoveries(%ListPendingResourceRecoveries{
               platform_context: SystemContext.platform(:resource_recovery_test),
               limit: 500,
               occurred_at: now
             })

    refute Enum.any?(wakeups_while_open, fn
             %ResourceRecoveryWakeup{workspace_id: workspace_id, resource: resource} ->
               workspace_id == fixture.workspace_id and resource == fixture.resource
           end)

    due = DateTime.add(now, 2, :millisecond)
    %ResourceCircuitAdmission{permits: [probe]} = acquire(fixture, "candidate-probe", due)

    assert {:ok, %ResourceCircuitUpdate{closed_resources: [_resource]}} =
             Store.record_outcomes(
               outcome_command(fixture, probe, "candidate-probe-success", :success, due)
             )

    assert {:ok, wakeups_after_close} =
             Store.list_pending_recoveries(%ListPendingResourceRecoveries{
               platform_context: SystemContext.platform(:resource_recovery_test),
               limit: 500,
               occurred_at: due
             })

    assert Enum.any?(wakeups_after_close, fn
             %ResourceRecoveryWakeup{workspace_id: workspace_id, resource: resource} ->
               workspace_id == fixture.workspace_id and resource == fixture.resource
           end)

    assert {:ok, %ResourceRecoveryBatch{candidates: [claimed]}} =
             Store.claim_recovery(%ClaimResourceRecovery{
               workspace_context: context(fixture),
               command_id: "claim-1",
               owner_id: "recovery-owner",
               resource: fixture.resource,
               limit: 10,
               claim_lease_ms: 30_000,
               occurred_at: now
             })

    assert claimed.source_run_id == "source-run"
    assert claimed.node_key == candidate.node_key
    assert claimed.reason == :safe_failure

    complete = %CompleteResourceRecovery{
      workspace_context: context(fixture),
      owner_id: "wrong-owner",
      candidate_ids: [candidate.candidate_id],
      status: :submitted,
      recovery_run_id: "recovery-run",
      occurred_at: due
    }

    assert {:error, %{kind: :conflict}} = Store.complete_recovery(complete)
    assert :ok = Store.complete_recovery(%{complete | owner_id: "recovery-owner"})
  end

  test "terminal success resets the consecutive resource failure count", fixture do
    now = DateTime.utc_now()
    policy = Policy.new!(failure_threshold: 2, probe_after_ms: 1)

    %ResourceCircuitAdmission{permits: [first]} = acquire(fixture, "reset-first", now, policy)

    assert {:ok, %ResourceCircuitUpdate{}} =
             Store.record_outcomes(
               outcome_command(fixture, first, "reset-failure-1", :failure, now)
             )

    %ResourceCircuitAdmission{permits: [success]} =
      acquire(fixture, "reset-success", now, policy)

    assert {:ok, %ResourceCircuitUpdate{}} =
             Store.record_outcomes(
               outcome_command(fixture, success, "reset-success", :success, now)
             )

    %ResourceCircuitAdmission{permits: [after_reset]} =
      acquire(fixture, "reset-failure-2", now, policy)

    assert {:ok, %ResourceCircuitUpdate{}} =
             Store.record_outcomes(
               outcome_command(fixture, after_reset, "reset-failure-2", :failure, now)
             )

    assert %ResourceCircuitAdmission{status: :allowed} =
             acquire(fixture, "still-closed-after-reset", now, policy)
  end

  test "an in-flight terminal success closes a circuit opened by a sibling", fixture do
    now = DateTime.utc_now()

    %ResourceCircuitAdmission{permits: [failing]} = acquire(fixture, "inflight-failure", now)
    %ResourceCircuitAdmission{permits: [succeeding]} = acquire(fixture, "inflight-success", now)

    assert {:ok, %ResourceCircuitUpdate{closed_resources: []}} =
             Store.record_outcomes(
               outcome_command(fixture, failing, "inflight-failure", :failure, now)
             )

    assert {:ok, %ResourceCircuitUpdate{closed_resources: [closed]}} =
             Store.record_outcomes(
               outcome_command(fixture, succeeding, "inflight-success", :success, now)
             )

    assert closed == fixture.resource

    assert %ResourceCircuitAdmission{status: :allowed} =
             acquire(fixture, "after-inflight-success", now)
  end

  test "an unused half-open permit can be released for another probe", fixture do
    now = DateTime.utc_now()
    %ResourceCircuitAdmission{permits: [first]} = acquire(fixture, "release-failure", now)

    assert {:ok, %ResourceCircuitUpdate{}} =
             Store.record_outcomes(
               outcome_command(fixture, first, "release-failure", :failure, now)
             )

    due = DateTime.add(now, 2, :millisecond)

    %ResourceCircuitAdmission{permits: [unused_probe]} =
      acquire(fixture, "unused-probe", due)

    assert unused_probe.probe?

    assert :ok =
             Store.release_permits(%ReleaseResourceCircuitPermits{
               workspace_context: context(fixture),
               owner_id: unused_probe.owner_id,
               permits: [unused_probe],
               occurred_at: due
             })

    assert %ResourceCircuitAdmission{permits: [next_probe]} =
             acquire(fixture, "next-probe", due)

    assert next_probe.probe?
  end

  test "one logical probe owner remains admitted across node attempts", fixture do
    now = DateTime.utc_now()
    %ResourceCircuitAdmission{permits: [first]} = acquire(fixture, "retry-failure", now)

    assert {:ok, %ResourceCircuitUpdate{}} =
             Store.record_outcomes(
               outcome_command(fixture, first, "retry-failure", :failure, now)
             )

    due = DateTime.add(now, 2, :millisecond)

    %ResourceCircuitAdmission{permits: [probe]} =
      acquire(fixture, "stable-probe-owner", due)

    assert probe.probe?

    assert %ResourceCircuitAdmission{permits: [retry_permit]} =
             acquire(fixture, "stable-probe-owner", due)

    assert retry_permit.probe?
    assert retry_permit.owner_id == probe.owner_id
    assert %ResourceCircuitAdmission{status: :blocked} = acquire(fixture, "other-owner", due)

    assert {:ok, %ResourceCircuitUpdate{closed_resources: [_resource]}} =
             Store.record_outcomes(
               outcome_command(fixture, retry_permit, "retry-success", :success, due)
             )
  end

  test "safe recovery is recorded atomically only when the failure leaves the circuit open",
       fixture do
    now = DateTime.utc_now()
    policy = Policy.new!(failure_threshold: 2, probe_after_ms: 1)

    %ResourceCircuitAdmission{permits: [first]} = acquire(fixture, "below-threshold", now, policy)
    first_outcome = outcome_command(fixture, first, "below-threshold", :failure, now)

    assert {:ok, %ResourceCircuitUpdate{}} =
             Store.record_outcomes(%{
               first_outcome
               | recovery_candidates: [recovery_candidate(fixture, first_outcome, "safe-node")]
             })

    refute recovery_wakeup?(fixture, now)

    %ResourceCircuitAdmission{permits: [second]} = acquire(fixture, "opens", now, policy)
    opens = outcome_command(fixture, second, "opens", :failure, now)

    assert {:ok, %ResourceCircuitUpdate{}} =
             Store.record_outcomes(%{
               opens
               | recovery_candidates: [recovery_candidate(fixture, opens, "safe-node")]
             })

    refute recovery_wakeup?(fixture, now)

    due = DateTime.add(now, 2, :millisecond)
    %ResourceCircuitAdmission{permits: [probe]} = acquire(fixture, "closes", due, policy)

    assert {:ok, %ResourceCircuitUpdate{closed_resources: [_resource]}} =
             Store.record_outcomes(outcome_command(fixture, probe, "closes", :success, due))

    assert recovery_wakeup?(fixture, due)
  end

  defp acquire(fixture, owner_id, occurred_at, policy \\ nil) do
    policy = policy || Policy.new!(failure_threshold: 1, probe_after_ms: 1)

    {:ok, admission} =
      Store.acquire(%AcquireResourceCircuits{
        workspace_context: context(fixture),
        command_id: "acquire-#{owner_id}",
        owner_id: owner_id,
        run_id: "run-#{owner_id}",
        asset_step_id: "step-#{owner_id}",
        requests: [
          %ResourceCircuitRequest{
            resource: fixture.resource,
            policy: policy
          }
        ],
        probe_lease_ms: 30_000,
        occurred_at: occurred_at
      })

    admission
  end

  defp outcome_command(fixture, permit, command_id, status, occurred_at) do
    %RecordResourceOutcomes{
      workspace_context: context(fixture),
      command_id: command_id,
      owner_id: permit.owner_id,
      run_id: "run-#{command_id}",
      asset_step_id: "step-#{command_id}",
      attempt: 1,
      permits: [permit],
      outcomes: [ResourceOutcome.new!(resource: fixture.resource, status: status)],
      occurred_at: occurred_at
    }
  end

  defp recovery_candidate(fixture, outcome, candidate_id) do
    %RecordResourceRecoveryCandidate{
      workspace_context: context(fixture),
      candidate_id: candidate_id,
      source_run_id: outcome.run_id,
      node_key: {{__MODULE__, :asset}, nil},
      resource: fixture.resource,
      reason: :safe_failure,
      max_age_ms: 60_000,
      occurred_at: outcome.occurred_at
    }
  end

  defp recovery_wakeup?(fixture, occurred_at) do
    {:ok, wakeups} =
      Store.list_pending_recoveries(%ListPendingResourceRecoveries{
        platform_context: SystemContext.platform(:resource_recovery_test),
        limit: 500,
        occurred_at: occurred_at
      })

    Enum.any?(wakeups, fn wakeup ->
      wakeup.workspace_id == fixture.workspace_id and wakeup.resource == fixture.resource
    end)
  end

  defp context(fixture),
    do: SystemContext.workspace(fixture.workspace_id, :resource_circuit_test)
end
