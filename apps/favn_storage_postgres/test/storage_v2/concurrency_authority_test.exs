defmodule FavnStoragePostgres.StorageV2.ConcurrencyAuthorityTest do
  use ExUnit.Case, async: false

  alias Ecto.Adapters.SQL
  alias Favn.Manifest
  alias Favn.Manifest.Version
  alias FavnOrchestrator.Persistence.BackfillPlan
  alias FavnOrchestrator.Persistence.Commands.ActivateBackfillPlan
  alias FavnOrchestrator.Persistence.Commands.AdmitExecution
  alias FavnOrchestrator.Persistence.Commands.AppendLogBatch
  alias FavnOrchestrator.Persistence.Commands.AppendBackfillPlanBatch
  alias FavnOrchestrator.Persistence.Commands.BackfillPlanWindow
  alias FavnOrchestrator.Persistence.Commands.CapacityRequest
  alias FavnOrchestrator.Persistence.Commands.ClaimDueSchedules
  alias FavnOrchestrator.Persistence.Commands.ClaimBackfillWindows
  alias FavnOrchestrator.Persistence.Commands.ClaimMaterialization
  alias FavnOrchestrator.Persistence.Commands.ClaimRecoveryBatch
  alias FavnOrchestrator.Persistence.Commands.ClaimRun
  alias FavnOrchestrator.Persistence.Commands.CommitRunTransition
  alias FavnOrchestrator.Persistence.Commands.CreateRun
  alias FavnOrchestrator.Persistence.Commands.DeployManifest
  alias FavnOrchestrator.Persistence.Commands.DeploymentCapacityScope
  alias FavnOrchestrator.Persistence.Commands.DeploymentSchedule
  alias FavnOrchestrator.Persistence.Commands.DeploymentTarget
  alias FavnOrchestrator.Persistence.Commands.ExpireAdmission
  alias FavnOrchestrator.Persistence.Commands.FinishMaterialization
  alias FavnOrchestrator.Persistence.Commands.LogEntry
  alias FavnOrchestrator.Persistence.Commands.ProvisionWorkspace
  alias FavnOrchestrator.Persistence.Commands.RegisterManifest
  alias FavnOrchestrator.Persistence.Commands.ReleaseExecutionLease
  alias FavnOrchestrator.Persistence.Commands.RunTarget
  alias FavnOrchestrator.Persistence.Commands.StartBackfillPlan
  alias FavnOrchestrator.Persistence.Commands.TransitionBackfillWindow
  alias FavnOrchestrator.Persistence.CommandIdempotency
  alias FavnOrchestrator.Persistence.PlatformContext
  alias FavnOrchestrator.Persistence.Queries.ManifestSelector.ByContentHash
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Logs
  alias FavnOrchestrator.LogWriter
  alias FavnOrchestrator.TargetStatus
  alias FavnStoragePostgres.Admission.Store, as: AdmissionStore
  alias FavnStoragePostgres.Backfills.Store, as: BackfillStore
  alias FavnStoragePostgres.Config
  alias FavnStoragePostgres.Idempotency.Transaction, as: IdempotencyTransaction
  alias FavnStoragePostgres.Materialization.Store, as: MaterializationStore
  alias FavnStoragePostgres.Logs.Store, as: LogStore
  alias FavnStoragePostgres.Outbox.Sequencer
  alias FavnStoragePostgres.Outbox.Writer, as: OutboxWriter
  alias FavnStoragePostgres.Projections.Projector
  alias FavnStoragePostgres.Registry.Store, as: RegistryStore
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.RunOwnership.Store, as: RunOwnershipStore
  alias FavnStoragePostgres.Runs.Store, as: RunStore
  alias FavnStoragePostgres.Scheduler.Store, as: SchedulerStore
  alias FavnStoragePostgres.StorageV2.Migrations
  alias FavnStoragePostgres.Schemas.OutboxEvent

  @node_ids ~w(node-a node-b node-c)

  test "concurrent API command retries commit one mutation and conflicting reuse cannot mutate",
       fixture do
    %{rows: [[initial_version]]} =
      SQL.query!(Repo, "SELECT version FROM favn_control.workspaces WHERE workspace_id = $1", [
        fixture.workspace_id
      ])

    idempotency = command_idempotency("workspace.touch", "same request")

    results =
      1..2
      |> Task.async_stream(
        fn _index -> execute_idempotent_touch(fixture.workspace_id, idempotency) end,
        max_concurrency: 2,
        ordered: false,
        timeout: 30_000
      )
      |> Enum.map(fn {:ok, {:ok, version}} -> version end)

    assert results == [initial_version + 1, initial_version + 1]

    %{rows: [[committed_version]]} =
      SQL.query!(Repo, "SELECT version FROM favn_control.workspaces WHERE workspace_id = $1", [
        fixture.workspace_id
      ])

    assert committed_version == initial_version + 1

    conflicting = %{
      idempotency
      | request_fingerprint: :crypto.hash(:sha256, "different request")
    }

    assert {:error, %{kind: :conflict}} =
             execute_idempotent_touch(fixture.workspace_id, conflicting)

    %{rows: [[version_after_conflict]]} =
      SQL.query!(Repo, "SELECT version FROM favn_control.workspaces WHERE workspace_id = $1", [
        fixture.workspace_id
      ])

    assert version_after_conflict == committed_version
  end

  setup_all do
    url =
      System.get_env("FAVN_DATABASE_URL") ||
        raise "FAVN_DATABASE_URL is required for PostgreSQL storage tests"

    {:ok, options} = Config.repo_options(url: url, ssl_mode: :disable, pool_size: 24)
    start_supervised!({Repo, options})
    :ok = Migrations.migrate!(Repo)

    version =
      manifest_version("concurrency-mv-#{System.unique_integer([:positive, :monotonic])}")

    {:ok, platform_context} =
      PlatformContext.new("concurrency-test", "manifest-publisher", [:platform_admin])

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
    {:ok, provision_fixture(36, version)}
  end

  test "concurrent workspace provisioning is one exact-retry-safe mutation" do
    unique = random_id()
    workspace_id = "concurrent-provision-#{unique}"

    {:ok, platform_context} =
      PlatformContext.new("concurrency-test", "provision-grant-#{unique}", [:platform_admin])

    command = %ProvisionWorkspace{
      platform_context: platform_context,
      workspace_id: workspace_id,
      slug: "concurrent-provision-#{unique}",
      display_name: "Concurrent provision #{unique}",
      occurred_at: DateTime.utc_now()
    }

    results =
      1..2
      |> Task.async_stream(
        fn _index -> RegistryStore.provision_workspace(command) end,
        max_concurrency: 2,
        ordered: false,
        timeout: 30_000
      )
      |> Enum.map(fn {:ok, result} -> result end)

    assert results == [:ok, :ok]

    assert {:error, %{kind: :conflict}} =
             RegistryStore.provision_workspace(%{
               command
               | display_name: "Conflicting workspace name"
             })

    assert %{rows: [[1]]} =
             SQL.query!(
               Repo,
               "SELECT count(*) FROM favn_control.workspaces WHERE workspace_id = $1",
               [workspace_id]
             )

    assert %{rows: [[1]]} =
             SQL.query!(
               Repo,
               "SELECT count(*) FROM favn_control.outbox_events WHERE command_id = $1",
               ["workspace.provision:" <> workspace_id]
             )
  end

  test "three owners claim every recoverable run once and stale writers are fenced", fixture do
    runs = Enum.map(1..36, fn _index -> create_run!(fixture) end)

    results =
      @node_ids
      |> Task.async_stream(
        fn owner_id ->
          RunOwnershipStore.claim_recovery_batch(%ClaimRecoveryBatch{
            workspace_context: fixture.workspace_context,
            batch_id: "recovery:#{fixture.workspace_id}:#{owner_id}",
            owner_id: owner_id,
            lease_duration_ms: 60_000,
            limit: 20
          })
        end,
        max_concurrency: 3,
        ordered: false,
        timeout: 30_000
      )
      |> Enum.flat_map(fn {:ok, {:ok, claims}} -> claims end)

    assert length(results) == 36
    assert results |> Enum.map(& &1.run_id) |> Enum.uniq() |> length() == 36
    assert Enum.all?(results, &(&1.owner_id in @node_ids))

    claim = hd(results)
    run = Enum.find(runs, &(&1.id == claim.run_id))
    running = RunState.transition(run, status: :running)

    assert {:error, %{kind: :fenced}} =
             RunStore.commit_transition(%CommitRunTransition{
               workspace_context: fixture.workspace_context,
               command_id: "stale:" <> run.id,
               expected_sequence: 1,
               owner_id: claim.owner_id,
               fencing_token: claim.fencing_token + 1,
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

  @tag :slow
  test "three BEAM nodes partition PostgreSQL work and fence a crashed owner", fixture do
    peers = start_postgres_peers!(3)

    on_exit(fn ->
      Enum.each(peers, &stop_peer/1)
    end)

    admission_runs = Enum.map(1..3, fn _index -> create_run!(fixture) end)

    admissions =
      peers
      |> Enum.zip(admission_runs)
      |> Task.async_stream(
        fn {peer, run} ->
          command = %{admit_command(fixture, run.id) | owner_id: peer.owner_id}
          peer_call(peer, AdmissionStore, :admit, [command])
        end,
        max_concurrency: 3,
        ordered: false,
        timeout: 30_000
      )
      |> Enum.map(fn {:ok, {:ok, decision}} -> decision end)

    assert Enum.count(admissions, &(&1.status == :admitted)) == 1
    assert Enum.count(admissions, &(&1.status == :waiting)) == 2

    materializations =
      peers
      |> Enum.zip(admission_runs)
      |> Task.async_stream(
        fn {peer, run} ->
          peer_call(peer, MaterializationStore, :claim, [
            %ClaimMaterialization{
              workspace_context: fixture.workspace_context,
              command_id: "multi-node-materialization:#{run.id}",
              claim_key: "multi-node-shared-materialization:#{fixture.workspace_id}",
              deployment_id: fixture.deployment_id,
              target_kind: :asset,
              target_id: fixture.target_id,
              partition_key: "2026-07-18",
              run_id: run.id,
              owner_id: peer.owner_id,
              lease_duration_ms: 60_000,
              occurred_at: DateTime.utc_now()
            }
          ])
        end,
        max_concurrency: 3,
        ordered: false,
        timeout: 30_000
      )
      |> Enum.map(fn {:ok, {:ok, decision}} -> decision end)

    assert Enum.count(materializations, &(&1.status == :claimed)) == 1
    assert Enum.count(materializations, &(&1.status == :competing)) == 2

    release_projector_claim!()
    drain_outbox()
    drain_projector()
    release_projector_claim!()

    runs = Enum.map(1..36, fn _index -> create_run!(fixture) end)

    run_claims =
      peers
      |> Task.async_stream(
        fn peer ->
          peer_call(peer, RunOwnershipStore, :claim_recovery_batch, [
            %ClaimRecoveryBatch{
              workspace_context: fixture.workspace_context,
              batch_id: "multi-node-recovery:#{fixture.workspace_id}:#{peer.owner_id}",
              owner_id: peer.owner_id,
              lease_duration_ms: 60_000,
              limit: 20
            }
          ])
        end,
        max_concurrency: 3,
        ordered: false,
        timeout: 30_000
      )
      |> Enum.flat_map(fn {:ok, {:ok, claims}} -> claims end)

    recoverable_runs = admission_runs ++ runs

    assert length(run_claims) == length(recoverable_runs)

    assert run_claims |> Enum.map(& &1.run_id) |> Enum.uniq() |> length() ==
             length(recoverable_runs)

    assert Enum.all?(run_claims, &(&1.owner_id in Enum.map(peers, fn peer -> peer.owner_id end)))

    assert Enum.all?(recoverable_runs, fn run ->
             Enum.any?(run_claims, &(&1.run_id == run.id))
           end)

    schedule_claims =
      peers
      |> Task.async_stream(
        fn peer ->
          peer_call(peer, SchedulerStore, :claim_due_schedules, [
            %ClaimDueSchedules{
              workspace_context: fixture.workspace_context,
              batch_id: "multi-node-schedules:#{fixture.workspace_id}:#{peer.owner_id}",
              owner_id: peer.owner_id,
              lease_duration_ms: 60_000,
              limit: 20
            }
          ])
        end,
        max_concurrency: 3,
        ordered: false,
        timeout: 30_000
      )
      |> Enum.flat_map(fn {:ok, {:ok, claims}} -> claims end)

    assert length(schedule_claims) == 36
    assert schedule_claims |> Enum.map(& &1.schedule_id) |> Enum.uniq() |> length() == 36

    publications =
      peers
      |> Task.async_stream(
        fn peer -> peer_call(peer, Sequencer, :sequence_batch, [20]) end,
        max_concurrency: 3,
        ordered: false,
        timeout: 30_000
      )
      |> Enum.flat_map(fn {:ok, {:ok, batch}} -> batch end)

    assert length(publications) == 36
    assert publications |> Enum.map(& &1.outbox_event_id) |> Enum.uniq() |> length() == 36
    assert publications |> Enum.map(& &1.publication_id) |> Enum.uniq() |> length() == 36

    projector_results =
      peers
      |> Task.async_stream(
        fn peer ->
          result =
            peer_call(peer, Projector, :project_batch, [
              peer.owner_id,
              [limit: 12, lease_duration_ms: 1_000]
            ])

          {peer, result}
        end,
        max_concurrency: 3,
        ordered: false,
        timeout: 30_000
      )
      |> Enum.map(fn {:ok, result} -> result end)

    assert [{projector_peer, %{count: 12}}] =
             for(
               {peer, {:ok, projection}} <- projector_results,
               do: {peer, projection}
             )

    assert projector_results
           |> Enum.count(fn {_peer, result} -> match?({:error, %{kind: :conflict}}, result) end) ==
             2

    crash_run = create_run!(fixture)
    remaining_peers = Enum.reject(peers, &(&1.control == projector_peer.control))
    [takeover_peer, stale_writer_peer] = remaining_peers

    assert {:ok, stale_ownership} =
             peer_call(projector_peer, RunOwnershipStore, :claim_run, [
               %ClaimRun{
                 workspace_context: fixture.workspace_context,
                 command_id: "multi-node-initial-claim:#{crash_run.id}",
                 run_id: crash_run.id,
                 owner_id: projector_peer.owner_id,
                 lease_duration_ms: 1_000
               }
             ])

    :ok = :peer.stop(projector_peer.control)
    await_ownership_expiry!(fixture.workspace_id, crash_run.id, 5_000)
    await_projector_expiry!(5_000)

    assert {:ok, current_ownership} =
             peer_call(takeover_peer, RunOwnershipStore, :claim_run, [
               %ClaimRun{
                 workspace_context: fixture.workspace_context,
                 command_id: "multi-node-takeover:#{crash_run.id}",
                 run_id: crash_run.id,
                 owner_id: takeover_peer.owner_id,
                 lease_duration_ms: 60_000
               }
             ])

    assert current_ownership.fencing_token == stale_ownership.fencing_token + 1

    running = RunState.transition(crash_run, status: :running)

    transition = fn owner_id, fencing_token, suffix ->
      %CommitRunTransition{
        workspace_context: fixture.workspace_context,
        command_id: "multi-node-transition:#{crash_run.id}:#{suffix}",
        expected_sequence: 1,
        owner_id: owner_id,
        fencing_token: fencing_token,
        run: running,
        event: %{
          run_id: crash_run.id,
          sequence: 2,
          event_type: :run_started,
          status: :running,
          occurred_at: DateTime.utc_now()
        }
      }
    end

    assert {:error, %{kind: :fenced}} =
             peer_call(stale_writer_peer, RunStore, :commit_transition, [
               transition.(
                 stale_ownership.owner_id,
                 stale_ownership.fencing_token,
                 "stale"
               )
             ])

    assert {:ok, %{run: %{status: :running}}} =
             peer_call(takeover_peer, RunStore, :commit_transition, [
               transition.(
                 current_ownership.owner_id,
                 current_ownership.fencing_token,
                 "current"
               )
             ])

    assert {:ok, %{count: 24, last_publication_id: projected_through}} =
             peer_call(takeover_peer, Projector, :project_batch, [
               takeover_peer.owner_id,
               [limit: 250, lease_duration_ms: 60_000]
             ])

    %{rows: [[last_publication_id]]} =
      SQL.query!(
        Repo,
        """
        SELECT last_publication_id
        FROM favn_control.outbox_publication_state
        WHERE singleton_id = 1
        """,
        []
      )

    assert projected_through == last_publication_id
  end

  test "run identity and expected sequence serialize conflicting writers", fixture do
    run_id = "concurrent-identity-#{random_id()}"
    run = new_run(fixture, run_id)

    command = %{
      create_run_command(fixture, run)
      | idempotency: command_idempotency("run.create.#{run_id}", "same run request")
    }

    exact_results =
      1..2
      |> Task.async_stream(fn _index -> RunStore.create_run(command) end,
        max_concurrency: 2,
        timeout: 30_000
      )
      |> Enum.map(fn {:ok, result} -> result end)

    assert Enum.all?(exact_results, &match?({:ok, _result}, &1))

    assert exact_results
           |> Enum.map(fn {:ok, result} -> result.replayed? end)
           |> Enum.sort() == [false, true]

    conflict_id = "concurrent-conflict-#{random_id()}"
    first = new_run(fixture, conflict_id)
    second = %{first | params: %{"different" => true}}

    conflicting_results =
      [create_run_command(fixture, first), create_run_command(fixture, second)]
      |> Task.async_stream(&RunStore.create_run/1, max_concurrency: 2, timeout: 30_000)
      |> Enum.map(fn {:ok, result} -> result end)

    assert Enum.count(conflicting_results, &match?({:ok, _result}, &1)) == 1
    assert Enum.count(conflicting_results, &match?({:error, %{kind: :conflict}}, &1)) == 1

    running = RunState.transition(run, status: :running)
    cancelled = RunState.transition(run, status: :cancelled)

    transition_results =
      [
        transition_command(fixture, running, :run_started),
        transition_command(fixture, cancelled, :run_cancelled)
      ]
      |> Task.async_stream(&RunStore.commit_transition/1, max_concurrency: 2, timeout: 30_000)
      |> Enum.map(fn {:ok, result} -> result end)

    assert Enum.count(transition_results, &match?({:ok, _result}, &1)) == 1
    assert Enum.count(transition_results, &match?({:error, %{kind: :conflict}}, &1)) == 1
  end

  defp execute_idempotent_touch(workspace_id, context) do
    case Repo.transaction(fn ->
           IdempotencyTransaction.execute!(
             workspace_id,
             context,
             fn ->
               %{rows: [[version]]} =
                 SQL.query!(
                   Repo,
                   "UPDATE favn_control.workspaces SET version = version + 1 WHERE workspace_id = $1 RETURNING version",
                   [workspace_id]
                 )

               version
             end,
             fn version ->
               {:ok,
                %{
                  response: %{"version" => version},
                  response_status: 200,
                  resource_kind: "workspace",
                  resource_id: workspace_id
                }}
             end,
             fn %{response: %{"version" => version}} when is_integer(version) ->
               {:ok, version}
             end
           )
         end) do
      {:ok, result} -> {:ok, result}
      {:error, reason} -> {:error, reason}
    end
  end

  defp command_idempotency(operation, request) do
    {:ok, context} =
      CommandIdempotency.new(
        operation,
        :actor,
        "actor-idempotency",
        :crypto.hash(:sha256, "one caller key"),
        :crypto.hash(:sha256, request),
        DateTime.add(DateTime.utc_now(), 3_600, :second)
      )

    context
  end

  test "capacity and materialization exclusion hold across competing connections", fixture do
    runs = Enum.map(1..24, fn _index -> create_run!(fixture) end)

    admissions =
      runs
      |> Task.async_stream(
        fn run -> AdmissionStore.admit(admit_command(fixture, run.id)) end,
        max_concurrency: 24,
        ordered: false,
        timeout: 30_000
      )
      |> Enum.map(fn {:ok, {:ok, decision}} -> decision end)

    assert Enum.count(admissions, &(&1.status == :admitted)) == 1
    assert Enum.count(admissions, &(&1.status == :waiting)) == 23

    %{rows: [[active_count]]} =
      SQL.query!(
        Repo,
        "SELECT active_count FROM favn_control.capacity_scopes WHERE scope_id = $1",
        [fixture.capacity_scope_id]
      )

    assert active_count == 1

    decisions =
      runs
      |> Task.async_stream(
        fn run ->
          MaterializationStore.claim(%ClaimMaterialization{
            workspace_context: fixture.workspace_context,
            command_id: "materialization:#{run.id}",
            claim_key: "shared-materialization:#{fixture.workspace_id}",
            deployment_id: fixture.deployment_id,
            target_kind: :asset,
            target_id: fixture.target_id,
            partition_key: "2026-07-17",
            run_id: run.id,
            owner_id: Enum.at(@node_ids, rem(:erlang.phash2(run.id), 3)),
            lease_duration_ms: 60_000,
            occurred_at: DateTime.utc_now()
          })
        end,
        max_concurrency: 24,
        ordered: false,
        timeout: 30_000
      )
      |> Enum.map(fn {:ok, {:ok, decision}} -> decision end)

    assert Enum.count(decisions, &(&1.status == :claimed)) == 1
    assert Enum.count(decisions, &(&1.status == :competing)) == 23
  end

  test "capacity locks are canonical and release racing expiry decrements exactly once",
       fixture do
    [first_scope, second_scope] = fixture.capacity_scope_ids
    [first_run, second_run, expiring_run] = Enum.map(1..3, fn _index -> create_run!(fixture) end)

    commands = [
      %{
        admit_command(fixture, first_run.id)
        | requests: [
            %CapacityRequest{scope_id: first_scope},
            %CapacityRequest{scope_id: second_scope}
          ]
      },
      %{
        admit_command(fixture, second_run.id)
        | requests: [
            %CapacityRequest{scope_id: second_scope},
            %CapacityRequest{scope_id: first_scope}
          ]
      }
    ]

    decisions =
      commands
      |> Task.async_stream(&AdmissionStore.admit/1, max_concurrency: 2, timeout: 30_000)
      |> Enum.map(fn {:ok, {:ok, decision}} -> decision end)

    assert Enum.count(decisions, &(&1.status == :admitted)) == 1
    assert Enum.count(decisions, &(&1.status == :waiting)) == 1

    admitted = Enum.find(decisions, &(&1.status == :admitted))

    assert {:ok, _released} =
             AdmissionStore.release_lease(%ReleaseExecutionLease{
               workspace_context: fixture.workspace_context,
               lease_id: admitted.lease.lease_id,
               owner_id: admitted.lease.owner_id,
               owner_generation: admitted.lease.owner_generation
             })

    expiring_command = %{admit_command(fixture, expiring_run.id) | lease_duration_ms: 1}
    assert {:ok, %{status: :admitted, lease: lease}} = AdmissionStore.admit(expiring_command)
    Process.sleep(5)

    race_results =
      [
        fn ->
          AdmissionStore.release_lease(%ReleaseExecutionLease{
            workspace_context: fixture.workspace_context,
            lease_id: lease.lease_id,
            owner_id: lease.owner_id,
            owner_generation: lease.owner_generation
          })
        end,
        fn ->
          AdmissionStore.expire(%ExpireAdmission{
            workspace_context: fixture.workspace_context,
            batch_id: "expiry:#{lease.lease_id}",
            limit: 10
          })
        end
      ]
      |> Task.async_stream(fn operation -> operation.() end,
        max_concurrency: 2,
        timeout: 30_000
      )
      |> Enum.map(fn {:ok, result} -> result end)

    assert Enum.all?(race_results, &match?({:ok, _release}, &1))

    %{rows: counts} =
      SQL.query!(
        Repo,
        "SELECT scope_id, active_count FROM favn_control.capacity_scopes WHERE scope_id = ANY($1::text[]) ORDER BY scope_id",
        [fixture.capacity_scope_ids]
      )

    assert Enum.all?(counts, fn [_scope_id, count] -> count == 0 end)
  end

  test "an expired materialization owner cannot finish after a fenced takeover", fixture do
    run = create_run!(fixture)
    claim_key = "takeover:#{random_id()}"
    now = DateTime.utc_now()

    first_command = %ClaimMaterialization{
      workspace_context: fixture.workspace_context,
      command_id: "claim:first:#{claim_key}",
      claim_key: claim_key,
      deployment_id: fixture.deployment_id,
      target_kind: :asset,
      target_id: fixture.target_id,
      partition_key: "2026-07-17",
      run_id: run.id,
      owner_id: "owner-old",
      lease_duration_ms: 1,
      occurred_at: now
    }

    assert {:ok, %{status: :claimed, claim: first}} = MaterializationStore.claim(first_command)
    Process.sleep(5)

    assert {:ok, %{status: :claimed, claim: takeover}} =
             MaterializationStore.claim(%{
               first_command
               | command_id: "claim:new:#{claim_key}",
                 owner_id: "owner-new",
                 lease_duration_ms: 60_000,
                 occurred_at: DateTime.utc_now()
             })

    assert takeover.fencing_token == first.fencing_token + 1

    assert {:error, %{kind: :fenced}} =
             MaterializationStore.finish(%FinishMaterialization{
               workspace_context: fixture.workspace_context,
               command_id: "finish:stale:#{claim_key}",
               claim_key: claim_key,
               owner_id: first.owner_id,
               fencing_token: first.fencing_token,
               expected_version: first.version,
               status: :succeeded,
               materialization_id: "materialization:stale:#{claim_key}",
               payload: %{"rows" => 1},
               occurred_at: DateTime.utc_now()
             })

    %{rows: [[owner_id, fencing_token, status]]} =
      SQL.query!(
        Repo,
        "SELECT owner_id, fencing_token, status FROM favn_control.materialization_claims WHERE workspace_id = $1 AND claim_key = $2",
        [fixture.workspace_id, claim_key]
      )

    assert {owner_id, fencing_token, status} ==
             {takeover.owner_id, takeover.fencing_token, "claimed"}
  end

  test "an expired backfill transition racing reclaim is always fenced", fixture do
    root = create_run!(fixture)
    backfill_id = "backfill-race-#{random_id()}"
    now = DateTime.utc_now()

    window = %BackfillPlanWindow{
      window_id: "window-1",
      window_key: "2026-07-17",
      window_start: now,
      window_end: DateTime.add(now, 3_600, :second),
      payload: %{}
    }

    batch_hash = BackfillPlan.batch_hash([window])
    plan_hash = BackfillPlan.plan_hash([batch_hash])

    assert {:ok, _planning} =
             BackfillStore.start_plan(%StartBackfillPlan{
               workspace_context: fixture.workspace_context,
               command_id: "backfill-start:#{backfill_id}",
               backfill_id: backfill_id,
               root_run_id: root.id,
               deployment_id: fixture.deployment_id,
               manifest_version_id: fixture.version.manifest_version_id,
               target_kind: :asset,
               target_id: fixture.target_id,
               range_start: window.window_start,
               range_end: window.window_end,
               expected_window_count: 1,
               expected_batch_count: 1,
               plan_hash: plan_hash,
               occurred_at: now
             })

    assert {:ok, appended} =
             BackfillStore.append_plan_batch(%AppendBackfillPlanBatch{
               workspace_context: fixture.workspace_context,
               command_id: "backfill-batch:#{backfill_id}",
               backfill_id: backfill_id,
               batch_index: 0,
               batch_hash: batch_hash,
               windows: [window],
               occurred_at: now
             })

    assert {:ok, _ready} =
             BackfillStore.activate_plan(%ActivateBackfillPlan{
               workspace_context: fixture.workspace_context,
               command_id: "backfill-activate:#{backfill_id}",
               backfill_id: backfill_id,
               expected_version: appended.version,
               occurred_at: now
             })

    assert {:ok, [claimed]} =
             BackfillStore.claim_windows(%ClaimBackfillWindows{
               workspace_context: fixture.workspace_context,
               batch_id: "backfill-claim-old:#{backfill_id}",
               owner_id: "owner-old",
               lease_duration_ms: 1,
               backfill_id: backfill_id,
               limit: 1
             })

    Process.sleep(5)

    results =
      [
        fn ->
          BackfillStore.claim_windows(%ClaimBackfillWindows{
            workspace_context: fixture.workspace_context,
            batch_id: "backfill-claim-new:#{backfill_id}",
            owner_id: "owner-new",
            lease_duration_ms: 60_000,
            backfill_id: backfill_id,
            limit: 1
          })
        end,
        fn ->
          BackfillStore.transition_window(%TransitionBackfillWindow{
            workspace_context: fixture.workspace_context,
            command_id: "backfill-transition-stale:#{backfill_id}",
            backfill_id: backfill_id,
            window_id: claimed.window_id,
            owner_id: claimed.claim_owner,
            fencing_token: claimed.fencing_token,
            expected_version: claimed.version,
            status: :running,
            run_id: root.id,
            occurred_at: DateTime.utc_now()
          })
        end
      ]
      |> Task.async_stream(fn operation -> operation.() end,
        max_concurrency: 2,
        timeout: 30_000
      )
      |> Enum.map(fn {:ok, result} -> result end)

    assert Enum.count(results, &match?({:ok, [_takeover]}, &1)) == 1, inspect(results)
    assert Enum.count(results, &match?({:error, %{kind: :fenced}}, &1)) == 1, inspect(results)

    %{rows: [[owner_id, fencing_token, status]]} =
      SQL.query!(
        Repo,
        "SELECT claim_owner, fencing_token, status FROM favn_control.backfill_windows WHERE workspace_id = $1 AND backfill_id = $2 AND window_id = $3",
        [fixture.workspace_id, backfill_id, window.window_id]
      )

    assert {owner_id, fencing_token, status} ==
             {"owner-new", claimed.fencing_token + 1, "claimed"}
  end

  test "three scheduler owners partition due work without duplicate claims", fixture do
    claims =
      @node_ids
      |> Task.async_stream(
        fn owner_id ->
          SchedulerStore.claim_due_schedules(%ClaimDueSchedules{
            workspace_context: fixture.workspace_context,
            batch_id: "schedules:#{fixture.workspace_id}:#{owner_id}",
            owner_id: owner_id,
            lease_duration_ms: 60_000,
            limit: 20
          })
        end,
        max_concurrency: 3,
        ordered: false,
        timeout: 30_000
      )
      |> Enum.flat_map(fn {:ok, {:ok, schedules}} -> schedules end)

    assert length(claims) == 36
    assert claims |> Enum.map(& &1.schedule_id) |> Enum.uniq() |> length() == 36
    assert Enum.all?(claims, &(&1.owner_id in @node_ids))
  end

  test "publication order follows commit visibility rather than identity allocation", fixture do
    drain_outbox()
    parent = self()
    suffix = random_id()

    late =
      Task.async(fn ->
        Repo.transaction(fn ->
          event = insert_outbox!(fixture, "late:" <> suffix)
          send(parent, {:late_inserted, event.outbox_event_id})

          receive do
            :commit -> event.outbox_event_id
          after
            10_000 -> Repo.rollback(:commit_signal_timeout)
          end
        end)
      end)

    assert_receive {:late_inserted, late_id}, 5_000

    {:ok, early_id} =
      Repo.transaction(fn -> insert_outbox!(fixture, "early:" <> suffix).outbox_event_id end)

    assert late_id < early_id

    assert {:ok, [%{outbox_event_id: ^early_id, publication_id: early_publication}]} =
             Sequencer.sequence_batch(10)

    send(late.pid, :commit)
    assert {:ok, ^late_id} = Task.await(late, 10_000)

    assert {:ok, [%{outbox_event_id: ^late_id, publication_id: late_publication}]} =
             Sequencer.sequence_batch(10)

    assert late_publication == early_publication + 1
  end

  test "log replay cannot skip a batch that commits after a later log identity", fixture do
    drain_outbox()
    parent = self()
    suffix = random_id()
    late_command = log_batch_command(fixture, "late-" <> suffix)
    early_command = log_batch_command(fixture, "early-" <> suffix)

    late =
      Task.async(fn ->
        Repo.transaction(fn ->
          assert {:ok, [entry]} = LogStore.append_batch(late_command)
          send(parent, {:late_log_inserted, entry.log_id})

          receive do
            :commit -> entry.log_id
          after
            10_000 -> Repo.rollback(:commit_signal_timeout)
          end
        end)
      end)

    assert_receive {:late_log_inserted, late_log_id}, 5_000
    assert {:ok, [early_entry]} = LogStore.append_batch(early_command)
    assert late_log_id < early_entry.log_id

    assert {:ok, [_early_publication]} = Sequencer.sequence_batch(10)
    assert {:ok, [early_replay]} = Logs.replay(fixture.workspace_context, 0, %{}, limit: 10)
    assert early_replay.message == early_command.entries |> hd() |> Map.fetch!(:message)

    send(late.pid, :commit)
    assert {:ok, ^late_log_id} = Task.await(late, 10_000)
    assert {:ok, [_late_publication]} = Sequencer.sequence_batch(10)

    assert {:ok, [late_replay]} =
             Logs.replay(
               fixture.workspace_context,
               early_replay.global_sequence,
               %{},
               limit: 10
             )

    assert late_replay.message == late_command.entries |> hd() |> Map.fetch!(:message)
    assert late_replay.global_sequence > early_replay.global_sequence
  end

  test "canonical node and asset identities match historical, replay, and live logs", fixture do
    {:ok, _started} = Application.ensure_all_started(:phoenix_pubsub)

    if is_nil(Process.whereis(FavnOrchestrator.PubSub)) do
      start_supervised!({Phoenix.PubSub, name: FavnOrchestrator.PubSub})
    end

    drain_outbox()
    suffix = random_id()
    run_id = "identity-run:" <> suffix
    node_key = {{MyApp.ConcurrentAsset, :asset}, nil}
    asset_ref = {MyApp.ConcurrentAsset, :asset}
    filter = %Favn.Log.Filter{run_id: run_id, node_key: node_key, asset_ref: asset_ref}

    assert {:ok, subscription} = Logs.subscribe_logs(fixture.workspace_context, filter)

    on_exit(fn ->
      _ = Logs.unsubscribe_logs(subscription)
    end)

    assert {:ok, [_persisted]} =
             LogWriter.write(
               fixture.workspace_context,
               %Favn.Log.Entry{
                 source: :runner,
                 level: :info,
                 message: "canonical identity",
                 run_id: run_id,
                 node_key: node_key,
                 asset_ref: asset_ref,
                 occurred_at: DateTime.utc_now()
               },
               batch_id: "identity-log-batch:" <> suffix,
               command_id: "identity-log-command:" <> suffix
             )

    assert_receive {:favn_log_entry, live}, 5_000
    assert String.starts_with?(live.metadata["node_key"], "node:")
    assert live.metadata["asset_ref"] == "asset:Elixir.MyApp.ConcurrentAsset:asset"

    assert {:ok, historical} =
             Logs.page(fixture.workspace_context, filter, direction: :older, limit: 10)

    assert Enum.map(historical.items, & &1.message) == ["canonical identity"]

    assert {:ok, [_publication]} = Sequencer.sequence_batch(10)

    assert {:ok, [replayed]} =
             Logs.replay(fixture.workspace_context, 0, filter, limit: 10)

    assert replayed.node_key == live.metadata["node_key"]
    assert replayed.asset_ref == live.metadata["asset_ref"]
  end

  test "log replay applies asset filters before its bounded publication page", fixture do
    drain_outbox()
    suffix = random_id()
    now = DateTime.utc_now()
    run_id = "filtered-run:" <> suffix
    target_step = "target-step:" <> suffix

    entries =
      Enum.map(1..200, fn index ->
        %LogEntry{
          source: "runner",
          level: :info,
          message: "unrelated #{index}",
          run_id: run_id,
          occurred_at: now,
          metadata: %{"asset_step_id" => "other-step:" <> Integer.to_string(index)}
        }
      end) ++
        [
          %LogEntry{
            source: "runner",
            level: :info,
            message: "matching log",
            run_id: run_id,
            occurred_at: now,
            metadata: %{"asset_step_id" => target_step}
          }
        ]

    assert {:ok, persisted} =
             LogStore.append_batch(%AppendLogBatch{
               workspace_context: fixture.workspace_context,
               command_id: "filtered-log-command:" <> suffix,
               batch_id: "filtered-log-batch:" <> suffix,
               occurred_at: now,
               entries: entries
             })

    assert length(persisted) == 201
    assert {:ok, [_publication]} = Sequencer.sequence_batch(10)

    assert {:ok, [matching]} =
             Logs.replay(
               fixture.workspace_context,
               0,
               %Favn.Log.Filter{run_id: run_id, asset_step_id: target_step},
               limit: 200
             )

    assert matching.message == "matching log"
  end

  test "competing projectors advance one durable ordered cursor", fixture do
    drain_outbox()
    release_projector_claim!()
    drain_projector()
    Enum.each(1..30, fn _index -> create_run!(fixture) end)
    drain_outbox()

    batches =
      @node_ids
      |> Task.async_stream(
        &{&1, Projector.project_batch(&1, limit: 10, lease_duration_ms: 30_000)},
        max_concurrency: 3,
        ordered: false,
        timeout: 30_000
      )
      |> Enum.map(fn {:ok, result} -> result end)

    successes = for {owner, {:ok, result}} <- batches, do: {owner, result}
    conflicts = for {_owner, {:error, error}} <- batches, do: error

    assert [{owner, %{count: 10}}] = successes
    assert length(conflicts) == 2
    assert Enum.all?(conflicts, &(&1.kind == :conflict and &1.retryable?))
    drain_projector(owner)

    %{rows: [[last_publication_id]]} =
      SQL.query!(
        Repo,
        "SELECT last_publication_id FROM favn_control.outbox_publication_state WHERE singleton_id = 1",
        []
      )

    %{rows: [[projected_publication_id]]} =
      SQL.query!(
        Repo,
        "SELECT last_publication_id FROM favn_control.projection_cursors WHERE projector_name = 'control_plane_v1' AND shard_id = 0",
        []
      )

    assert projected_publication_id == last_publication_id
    release_projector_claim!()
  end

  test "late failure recording cannot block readiness after projector takeover" do
    drain_outbox()
    release_projector_claim!()

    %{rows: [[projected_through]]} =
      SQL.query!(
        Repo,
        "SELECT last_publication_id FROM favn_control.outbox_publication_state WHERE singleton_id = 1",
        []
      )

    SQL.query!(
      Repo,
      """
      INSERT INTO favn_control.projection_cursors
        (projector_name, shard_id, last_publication_id, fencing_token, version, updated_at)
      VALUES ('control_plane_v1', 0, 0, 0, 1, clock_timestamp())
      ON CONFLICT (projector_name, shard_id) DO UPDATE
      SET last_publication_id = 0, owner_id = NULL, claim_expires_at = NULL,
          updated_at = clock_timestamp()
      """,
      []
    )

    parent = self()

    takeover =
      Task.async(fn ->
        Repo.transaction(fn ->
          SQL.query!(
            Repo,
            """
            SELECT last_publication_id
            FROM favn_control.projection_cursors
            WHERE projector_name = 'control_plane_v1' AND shard_id = 0
            FOR UPDATE
            """,
            []
          )

          send(parent, :cursor_locked)

          receive do
            :commit_takeover -> :ok
          end

          SQL.query!(
            Repo,
            """
            UPDATE favn_control.projection_cursors
            SET last_publication_id = $1, updated_at = clock_timestamp()
            WHERE projector_name = 'control_plane_v1' AND shard_id = 0
            """,
            [projected_through]
          )
        end)
      end)

    assert_receive :cursor_locked, 5_000

    event = %OutboxEvent{
      publication_id: projected_through,
      workspace_id: "takeover-workspace",
      event_kind: "run.submitted"
    }

    recorder = Task.async(fn -> Projector.record_failure_if_unprojected(event, :unexpected) end)
    send(takeover.pid, :commit_takeover)

    assert {:ok, _transaction_result} = Task.await(takeover, 5_000)
    assert :already_projected = Task.await(recorder, 5_000)

    %{rows: [[failure_count]]} =
      SQL.query!(
        Repo,
        """
        SELECT count(*)
        FROM favn_control.projection_failures
        WHERE projector_name = 'control_plane_v1' AND shard_id = 0 AND publication_id = $1
        """,
        [projected_through]
      )

    assert failure_count == 0
  end

  defp provision_fixture(schedule_count, version) do
    unique = :crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower)
    workspace_id = "concurrency-ws-#{unique}"
    deployment_id = "concurrency-deploy-#{unique}"
    now = DateTime.utc_now()

    {:ok, platform_context} =
      PlatformContext.new("concurrency-test", "grant-#{unique}", [:platform_admin])

    :ok =
      RegistryStore.provision_workspace(%ProvisionWorkspace{
        platform_context: platform_context,
        workspace_id: workspace_id,
        slug: "concurrency-#{unique}",
        display_name: "Concurrency #{unique}",
        occurred_at: now
      })

    {:ok, workspace_context} =
      WorkspaceContext.new(workspace_id, "concurrency-test", [:workspace_admin])

    target_id = TargetStatus.target_id_for_asset({MyApp.ConcurrentAsset, :asset})

    pipeline_target_id =
      TargetStatus.target_id_for_pipeline({MyApp.ConcurrentPipeline, :pipeline})

    capacity_scope_id = "workspace:" <> workspace_id
    secondary_capacity_scope_id = "pool:" <> workspace_id

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
                 },
                 %DeploymentTarget{
                   target_kind: :pipeline,
                   target_id: pipeline_target_id,
                   selection_source: :common,
                   customer_visible: true,
                   descriptor: %{
                     "target_id" => pipeline_target_id,
                     "label" => pipeline_target_id
                   }
                 }
               ],
               schedules:
                 Enum.map(1..schedule_count, fn index ->
                   %DeploymentSchedule{
                     pipeline_target_id: pipeline_target_id,
                     schedule_id: "schedule-#{index}",
                     schedule_fingerprint: "schedule-fingerprint-#{index}",
                     definition: %{
                       "pipeline_module" => "Elixir.Scale.Pipeline",
                       "pipeline_name" => "daily",
                       "cron" => "0 0 * * *",
                       "timezone" => "Etc/UTC",
                       "overlap" => "forbid",
                       "missed" => "skip",
                       "window" => nil
                     },
                     next_due_at: DateTime.add(now, -1, :second),
                     cursor: %{}
                   }
                 end),
               capacity_scopes: [
                 %DeploymentCapacityScope{
                   scope_id: capacity_scope_id,
                   scope_kind: :workspace,
                   scope_key: workspace_id,
                   capacity_limit: 1
                 },
                 %DeploymentCapacityScope{
                   scope_id: secondary_capacity_scope_id,
                   scope_kind: :pool,
                   scope_key: "default",
                   capacity_limit: 1
                 }
               ],
               occurred_at: now
             })

    %{
      workspace_id: workspace_id,
      workspace_context: workspace_context,
      deployment_id: deployment_id,
      version: version,
      target_id: target_id,
      capacity_scope_id: capacity_scope_id,
      capacity_scope_ids: [capacity_scope_id, secondary_capacity_scope_id]
    }
  end

  defp create_run!(fixture) do
    run_id = "concurrent-run-#{random_id()}"

    run = new_run(fixture, run_id)

    assert {:ok, _created} =
             fixture |> create_run_command(run) |> RunStore.create_run()

    run
  end

  defp new_run(fixture, run_id) do
    RunState.new(
      id: run_id,
      workspace_id: fixture.workspace_id,
      deployment_id: fixture.deployment_id,
      manifest_version_id: fixture.version.manifest_version_id,
      manifest_content_hash: fixture.version.content_hash,
      asset_ref: {MyApp.ConcurrentAsset, :asset},
      target_refs: [{MyApp.ConcurrentAsset, :asset}]
    )
  end

  defp create_run_command(fixture, run) do
    %CreateRun{
      workspace_context: fixture.workspace_context,
      command_id: "create:" <> run.id,
      deployment_id: fixture.deployment_id,
      run: run,
      targets: [
        %RunTarget{
          target_kind: :asset,
          target_id: fixture.target_id,
          target_module: "MyApp.ConcurrentAsset",
          target_name: "asset",
          is_primary: true
        }
      ],
      event: %{
        run_id: run.id,
        sequence: 1,
        event_type: :run_submitted,
        status: :pending,
        occurred_at: run.inserted_at
      }
    }
  end

  defp transition_command(fixture, run, event_type) do
    %CommitRunTransition{
      workspace_context: fixture.workspace_context,
      command_id: "transition:#{run.id}:#{event_type}",
      expected_sequence: 1,
      run: run,
      event: %{
        run_id: run.id,
        sequence: 2,
        event_type: event_type,
        status: run.status,
        occurred_at: run.updated_at || DateTime.utc_now()
      }
    }
  end

  defp admit_command(fixture, run_id) do
    %AdmitExecution{
      workspace_context: fixture.workspace_context,
      command_id: "admit:" <> run_id,
      lease_id: "lease:" <> run_id,
      waiter_id: "waiter:" <> run_id,
      run_id: run_id,
      step_id: "step",
      owner_id: Enum.at(@node_ids, rem(:erlang.phash2(run_id), 3)),
      owner_generation: 1,
      lease_duration_ms: 60_000,
      waiter_ttl_ms: 60_000,
      requests: [%CapacityRequest{scope_id: fixture.capacity_scope_id}],
      occurred_at: DateTime.utc_now()
    }
  end

  defp insert_outbox!(fixture, command_id) do
    OutboxWriter.insert!(%{
      workspace_id: fixture.workspace_id,
      command_id: command_id,
      event_kind: "test.commit_order",
      aggregate_kind: "test",
      aggregate_id: command_id,
      aggregate_version: 1,
      payload: %{"command_id" => command_id},
      occurred_at: DateTime.utc_now()
    })
  end

  defp drain_outbox do
    case Sequencer.sequence_batch(5_000) do
      {:ok, publications} when length(publications) == 5_000 -> drain_outbox()
      {:ok, _publications} -> :ok
    end
  end

  defp log_batch_command(fixture, suffix) do
    now = DateTime.utc_now()

    %AppendLogBatch{
      workspace_context: fixture.workspace_context,
      command_id: "log-command:" <> suffix,
      batch_id: "log-batch:" <> suffix,
      occurred_at: now,
      entries: [
        %LogEntry{
          source: "runner",
          level: :info,
          message: "log " <> suffix,
          occurred_at: now
        }
      ]
    }
  end

  defp drain_projector(owner_id \\ "node-a") do
    case Projector.project_batch(owner_id, limit: 250, lease_duration_ms: 30_000) do
      {:ok, %{count: 250}} -> drain_projector(owner_id)
      {:ok, _result} -> :ok
    end
  end

  defp release_projector_claim! do
    SQL.query!(
      Repo,
      """
      UPDATE favn_control.projection_cursors
      SET owner_id = NULL, claim_expires_at = NULL
      WHERE projector_name = 'control_plane_v1' AND shard_id = 0
      """,
      []
    )

    :ok
  end

  defp start_postgres_peers!(count) do
    url = System.fetch_env!("FAVN_DATABASE_URL")
    code_paths = :code.get_path()
    unique = random_id()

    {:ok, repo_options} =
      Config.repo_options(url: url, ssl_mode: :disable, pool_size: 4)

    repo_child = Supervisor.child_spec({Repo, repo_options}, id: Repo)

    Enum.map(1..count, fn index ->
      {:ok, control, node} =
        :peer.start_link(%{
          name: "favn_storage_#{index}_#{unique}",
          connection: :standard_io,
          wait_boot: 30_000
        })

      :ok = :peer.call(control, :code, :add_paths, [code_paths], 30_000)
      {:ok, _started} = :peer.call(control, Application, :ensure_all_started, [:ecto_sql], 30_000)
      :ok = :peer.call(control, Logger, :configure, [[level: :warning]], 30_000)

      {:ok, _repo} =
        :peer.call(control, Supervisor, :start_child, [:kernel_sup, repo_child], 30_000)

      %{control: control, node: node, owner_id: Atom.to_string(node)}
    end)
  end

  defp peer_call(peer, module, function, arguments) do
    :peer.call(peer.control, module, function, arguments, 30_000)
  end

  defp stop_peer(peer) do
    if Process.alive?(peer.control) do
      try do
        :peer.stop(peer.control)
      catch
        :exit, _reason -> :ok
      end
    end

    :ok
  end

  defp await_ownership_expiry!(workspace_id, run_id, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_await_ownership_expiry!(workspace_id, run_id, deadline)
  end

  defp await_projector_expiry!(timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_await_projector_expiry!(deadline)
  end

  defp do_await_ownership_expiry!(workspace_id, run_id, deadline) do
    %{rows: [[expired?]]} =
      SQL.query!(
        Repo,
        """
        SELECT expires_at <= clock_timestamp()
        FROM favn_control.run_ownerships
        WHERE workspace_id = $1 AND run_id = $2
        """,
        [workspace_id, run_id]
      )

    cond do
      expired? ->
        :ok

      System.monotonic_time(:millisecond) < deadline ->
        Process.sleep(25)
        do_await_ownership_expiry!(workspace_id, run_id, deadline)

      true ->
        flunk("run ownership did not expire before the multi-node test deadline")
    end
  end

  defp do_await_projector_expiry!(deadline) do
    %{rows: [[expired?]]} =
      SQL.query!(
        Repo,
        """
        SELECT claim_expires_at <= clock_timestamp()
        FROM favn_control.projection_cursors
        WHERE projector_name = 'control_plane_v1' AND shard_id = 0
        """,
        []
      )

    cond do
      expired? ->
        :ok

      System.monotonic_time(:millisecond) < deadline ->
        Process.sleep(25)
        do_await_projector_expiry!(deadline)

      true ->
        flunk("projection ownership did not expire before the multi-node test deadline")
    end
  end

  defp manifest_version(manifest_version_id) do
    manifest = %Manifest{
      assets: [
        %Favn.Manifest.Asset{
          ref: {MyApp.ConcurrentAsset, :asset},
          module: MyApp.ConcurrentAsset,
          name: :asset
        }
      ],
      pipelines: [
        %Favn.Manifest.Pipeline{module: MyApp.ConcurrentPipeline, name: :pipeline}
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
