# OS-process crash probe: real PostgreSQL task lifecycle and a durable SQL effect.
# The effect counter substitutes for a data-system adapter; it lives outside BEAM.
Logger.configure(level: :warning)
alias Ecto.Adapters.SQL
alias FavnOrchestrator.Persistence.Commands, as: C
alias FavnOrchestrator.Persistence.Queries, as: Q
alias FavnOrchestrator.Persistence.{PlatformContext, WorkspaceContext}
alias FavnStoragePostgres.{Config, Repo}
alias FavnStoragePostgres.RunnerTasks.Store
[mode, phase, fixture_file] = System.argv()
f = fixture_file |> File.read!() |> Jason.decode!()
{:ok, _} = Application.ensure_all_started(:ecto_sql)

{:ok, options} =
  Config.repo_options(
    url: System.fetch_env!("FAVN_DATABASE_URL"),
    ssl_mode: :disable,
    pool_size: 2
  )

{:ok, _} = Repo.start_link(options)
{:ok, context} = WorkspaceContext.new(f["workspace"], "test-admin", [:workspace_admin])
{:ok, platform} = PlatformContext.new("test-admin", f["workspace"], [:platform_admin])

if mode == "generation" do
  {:ok, version} =
    FavnStoragePostgres.Registry.Store.get_manifest(%Q.ManifestSelector.ById{
      manifest_version_id: f["manifest_id"]
    })

  descriptor = hd(version.manifest.assets).target_descriptor

  {:ok, %{generation: generation}} =
    FavnStoragePostgres.TargetGenerations.Store.ensure_writable(%C.EnsureWritableTargetGeneration{
      workspace_context: context,
      command_id: "fresh-generation",
      target_id: f["target_id"],
      manifest_version_id: version.manifest_version_id,
      descriptor: descriptor,
      occurred_at: DateTime.utc_now()
    })

  true = generation.target_generation_id == f["generation_id"]
  true = Atom.to_string(generation.status) == "building"
  IO.puts("GENERATION restored")
  System.halt(0)
end

{:ok, task} = Store.get(%Q.GetRunnerTask{workspace_context: context, task_id: f["task_id"]})
true = task.data_state == :available

if mode == "inspect" do
  true = is_struct(task.payload, Favn.Contracts.RelationInspectionRequest)
  IO.puts("INSPECTION restored")
  System.halt(0)
end

now = DateTime.utc_now()

claim = %C.ClaimRunnerTask{
  platform_context: platform,
  command_id: "process-claim-" <> f["task_id"],
  runner_instance_id: "process-runner-" <> f["workspace"],
  runner_session_generation: 1,
  runner_pool: f["pool"],
  required_runner_release_id: task.required_runner_release_id,
  supported_task_kinds: [:generation_marker_initialize],
  capabilities: [],
  lease_duration_ms: 30_000,
  issued_at: now,
  occurred_at: now
}

if mode == "write" do
  if phase != "queued" do
    {:ok, assigned} = Store.claim(claim)

    if phase not in ["assigned"] do
      transition = %C.TransitionRunnerTask{
        workspace_context: context,
        command_id: "prepare-" <> task.task_id,
        task_id: assigned.task_id,
        runner_instance_id: assigned.assigned_runner_instance_id,
        runner_session_generation: assigned.assigned_runner_session_generation,
        assignment_generation: assigned.assignment_generation,
        transition: :preparing,
        issued_at: now,
        occurred_at: now
      }

      {:ok, _} = Store.transition(transition)

      if phase != "preparing" do
        {:ok, _} =
          Store.transition(%{
            transition
            | command_id: "start-" <> task.task_id,
              transition: :running
          })
      end

      if phase == "cancelling" do
        {:ok, _} =
          Store.request_cancellation(%C.RequestRunnerTaskCancellation{
            workspace_context: context,
            command_id: "cancel-" <> task.task_id,
            task_id: task.task_id,
            reason: :operator_request,
            issued_at: now,
            occurred_at: now
          })
      end

      if phase in ["effect_committed", "result_committed"] do
        SQL.query!(
          Repo,
          "UPDATE public.favn_crash_probe SET effects = effects + 1 WHERE probe_id=$1",
          [task.task_id]
        )
      end

      if phase == "result_committed" do
        request = task.payload

        marker = %Favn.Contracts.GenerationMarker{
          target_id: request.target_id,
          active_relation: request.active_relation,
          active_generation_id: request.target_generation_id,
          activation_operation_id: request.initialization_operation_id,
          activation_token: request.initialization_token,
          activated_at: now
        }

        result = %Favn.Contracts.GenerationMarkerInitializationResult{
          required_runner_release_id: request.required_runner_release_id,
          target_id: request.target_id,
          target_generation_id: request.target_generation_id,
          initialization_token: request.initialization_token,
          outcome: :succeeded,
          observed_marker: marker,
          physical_fingerprint: request.expected_physical_fingerprint,
          completed_at: now
        }

        {:ok, encoded} =
          Favn.Contracts.RunnerTask.PersistenceCodec.encode_result(
            :generation_marker_initialize,
            :succeeded,
            result
          )

        {:ok, _} =
          Store.complete(%C.CompleteRunnerTask{
            workspace_context: context,
            command_id: "complete-" <> task.task_id,
            task_id: task.task_id,
            runner_instance_id: assigned.assigned_runner_instance_id,
            runner_session_generation: assigned.assigned_runner_session_generation,
            assignment_generation: assigned.assignment_generation,
            outcome: :succeeded,
            retry_class: :terminal,
            result_version: 1,
            result: encoded,
            issued_at: now,
            occurred_at: now
          })
      end
    end
  end

  IO.puts("BARRIER " <> phase)
  Process.sleep(:infinity)
else
  if task.status in [:assigned, :preparing, :running, :cancelling] do
    {:ok, recovered} =
      Store.recover_expired(%C.RecoverRunnerTasks{
        platform_context: platform,
        command_id: "recover-" <> mode <> "-" <> task.task_id,
        owner_id: "recovery-" <> mode,
        limit: 50,
        issued_at: now,
        occurred_at: now
      })

    fenced = Enum.find(recovered, &(&1.task_id == task.task_id)) || raise "task was not recovered"
    disposition = if task.status in [:assigned, :preparing], do: :requeue, else: :unknown

    {:ok, _} =
      Store.release(%C.ReleaseRunnerTask{
        workspace_context: context,
        command_id: "release-" <> mode <> "-" <> task.task_id,
        task_id: task.task_id,
        runner_instance_id: fenced.assigned_runner_instance_id,
        runner_session_generation: fenced.assigned_runner_session_generation,
        assignment_generation: fenced.assignment_generation,
        disposition: disposition,
        reason: Favn.Contracts.RunnerError.new(outcome: :unknown, type: "process_crashed"),
        issued_at: now,
        occurred_at: now
      })
  end

  {:ok, current} = Store.get(%Q.GetRunnerTask{workspace_context: context, task_id: task.task_id})
  true = current.data_state == :available

  %{rows: [[effects]]} =
    SQL.query!(Repo, "SELECT effects FROM public.favn_crash_probe WHERE probe_id=$1", [
      task.task_id
    ])

  IO.puts(
    "RECOVERED " <>
      Jason.encode!(%{
        status: current.status,
        effects: effects,
        generation: current.assignment_generation
      })
  )
end
