# Separate BEAM with no warm run state. PostgreSQL is the only handoff.
Logger.configure(level: :warning)
alias Ecto.Adapters.SQL
alias FavnOrchestrator.Persistence.Commands, as: C
alias FavnOrchestrator.Persistence.Queries, as: Q
alias FavnOrchestrator.Persistence.{Runtime, SystemContext}
alias FavnOrchestrator.{RunManager, RunOwnership, RunnerTasks, RunnerTaskResultRouter}
alias FavnStoragePostgres.{Backend, Config, Repo}

# The write counter belongs to the actual asset callback, so every invocation is visible.
defmodule CrashRecoveryFixture do
  def asset(context) do
    step_id =
      FavnOrchestrator.AssetStepIdentity.asset_step_id(
        context.run_id,
        {{__MODULE__, :asset}, context.window.key},
        {__MODULE__, :asset}
      )

    Ecto.Adapters.SQL.query!(
      FavnStoragePostgres.Repo,
      "INSERT INTO public.favn_run_crash_effects(workspace_id,step_id,effects) VALUES ($1,$2,1) ON CONFLICT (workspace_id,step_id) DO UPDATE SET effects=favn_run_crash_effects.effects+1",
      [context.params["probe_workspace"], step_id]
    )

    {:ok, %{manifest_uri: "test://landing/" <> step_id, pages_written: 1}}
  end
end

[phase, fixture_file] = System.argv()
f = fixture_file |> File.read!() |> Jason.decode!()
{:ok, _} = Application.ensure_all_started(:ecto_sql)
{:ok, _} = Application.ensure_all_started(:phoenix_pubsub)

{:ok, options} =
  Config.repo_options(
    url: System.fetch_env!("FAVN_DATABASE_URL"),
    ssl_mode: :disable,
    pool_size: 5
  )

{:ok, _} = Repo.start_link(options)
{:ok, runtime} = Runtime.new(Backend, [])
{:ok, _} = Runtime.start_link(runtime)
{:ok, _} = FavnOrchestrator.Lifecycle.start_link(shutdown_drain_timeout_ms: 120_000)
:ok = FavnOrchestrator.Lifecycle.mark_accepting()
{:ok, _} = FavnOrchestrator.ExecutionAdmission.Coordinator.start_link([])
{:ok, _} = Task.Supervisor.start_link(name: FavnOrchestrator.RunnerClaimSupervisor)
{:ok, _} = RunnerTaskResultRouter.start_link([])

{:ok, _} =
  DynamicSupervisor.start_link(name: FavnOrchestrator.RunSupervisor, strategy: :one_for_one)

{:ok, _} = RunManager.start_link([])
context = SystemContext.workspace(f["workspace"], :run_worker)

{:ok, run} =
  FavnStoragePostgres.Runs.Store.get_run(%Q.GetRun{
    workspace_context: context,
    run_id: f["run_id"]
  })

{:ok, version} =
  FavnStoragePostgres.Registry.Store.get_manifest(%Q.ManifestSelector.ById{
    manifest_version_id: run.manifest_version_id
  })

:ok =
  FavnRunner.ReleaseVerifier.verify_startup(%{
    "FAVN_RUNNER_RELEASE_ID" => version.runner_releases[f["pool"]]
  })

if phase != "finish" do
  :ok =
    :telemetry.attach(
      :crash_barrier,
      [:favn, :persistence, :operation, :stop],
      fn _, _, metadata, _ ->
        if metadata.store == :runs and metadata.operation == :commit_transition and
             metadata.result == :ok do
          rows =
            SQL.query!(
              Repo,
              "SELECT event_type FROM favn_control.run_events WHERE workspace_id=$1 AND run_id=$2 ORDER BY sequence DESC LIMIT 1",
              [f["workspace"], f["run_id"]]
            ).rows

          if rows == [[phase]] do
            IO.puts("BARRIER " <> phase)
            receive do: (:never -> :ok)
          end
        end
      end,
      nil
    )
end

# Use the same fenced batch and manager entrypoint as the production recovery sweep.
{:ok, [ownership]} =
  RunOwnership.claim_recovery_batch(context, "fresh-process-recovery", unowned_grace_period_ms: 0)

{:ok, _} = RunManager.recover_claimed_run(context, ownership)
server = Map.fetch!(:sys.get_state(RunManager).run_pids, {f["workspace"], run.id})
monitor = Process.monitor(server)

if phase != "step_settled" do
  spawn_link(fn ->
    loop = fn loop ->
      now = DateTime.utc_now()

      {:ok, task} =
        FavnStoragePostgres.RunnerTasks.Store.claim(%C.ClaimRunnerTask{
          platform_context:
            SystemContext.platform(:crash_runner_test, roles: [:platform_operator]),
          command_id: "claim:" <> Ecto.UUID.generate(),
          runner_instance_id: "runner:" <> f["workspace"],
          runner_session_generation: 1,
          runner_pool: f["pool"],
          required_runner_release_id: version.runner_releases[f["pool"]],
          supported_task_kinds: [:asset_attempt],
          capabilities: ["asset_execution"],
          lease_duration_ms: 30_000,
          issued_at: now,
          occurred_at: now
        })

      if task do
        work = task.payload

        {:ok, _} =
          RunnerTasks.started(%Favn.Contracts.RunnerTask.Started{
            workspace_id: task.workspace_id,
            task_id: task.task_id,
            runner_instance_id: task.assigned_runner_instance_id,
            runner_session_generation: task.assigned_runner_session_generation,
            assignment_generation: task.assignment_generation,
            issued_at: now,
            occurred_at: now
          })

        {:ok, _worker} =
          FavnRunner.Worker.start_link(%{
            server: self(),
            execution_id: task.task_id,
            work: work,
            version: version,
            asset: Enum.find(version.manifest.assets, &(&1.ref == work.asset_ref))
          })

        task_id = task.task_id

        result =
          receive do
            {:runner_result, ^task_id, result} -> result
          after
            10_000 -> raise "asset callback timed out"
          end

        unless result.status == :ok, do: raise("asset failed: #{inspect(result.error)}")

        {:ok, _} =
          RunnerTasks.complete(%Favn.Contracts.RunnerTask.Result{
            workspace_id: task.workspace_id,
            task_id: task.task_id,
            task_kind: task.task_kind,
            runner_instance_id: task.assigned_runner_instance_id,
            runner_session_generation: task.assigned_runner_session_generation,
            assignment_generation: task.assignment_generation,
            outcome: :succeeded,
            retry_class: :terminal,
            result: result,
            finished_at: DateTime.utc_now()
          })

        if phase == "step_finished", do: Process.sleep(:infinity)
      end

      Process.sleep(10)
      loop.(loop)
    end

    loop.(loop)
  end)
end

receive do
  {:DOWN, ^monitor, :process, ^server, reason} ->
    {:ok, final} =
      FavnStoragePostgres.Runs.Store.get_run(%Q.GetRun{
        workspace_context: context,
        run_id: f["run_id"]
      })

    unless reason == :normal and final.status == :ok,
      do: raise("run recovery failed: #{inspect({reason, final.status, final.error})}")

    IO.puts("RECOVERED whole run")
after
  30_000 -> raise "run recovery timed out"
end
