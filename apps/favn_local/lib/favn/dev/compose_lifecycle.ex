defmodule Favn.Dev.ComposeLifecycle do
  @moduledoc """
  Owns the project-scoped Docker Compose development lifecycle.

  All container operations use the immutable control-plane image selected by
  install state and the customer runner image selected by its runner release
  ID. Commands never place credentials in process arguments.
  """

  alias Favn.Dev.{
    Activate,
    ComposeDeployment,
    ComposeProject,
    Config,
    Docker,
    Install,
    LocalHttpClient,
    Lock,
    OrchestratorClient,
    OutputRedactor,
    Paths,
    Publish,
    RunnerImage,
    Secrets,
    State
  }

  alias Favn.Dev.Maintainer.{Candidate, RunnerBuildCapability}

  @runtime_schema_version 6
  @maintenance_schema_version 2
  @maintenance_token ~r/\A[A-Za-z0-9_-]{43}\z/
  @runner_release_id ~r/\Arr_[0-9a-f]{64}\z/
  @default_ready_timeout_ms 120_000
  @default_log_tail 100
  @runtime_roles [:postgres, :runner, :control_plane]

  @type start_result :: %{
          runner_release_id: String.t(),
          manifest_version_id: String.t(),
          runner_image_id: String.t(),
          view_url: String.t(),
          orchestrator_url: String.t()
        }

  @doc "Starts and bootstraps the complete local Compose application."
  @spec start(keyword()) :: {:ok, start_result()} | {:error, term()}
  def start(opts \\ []) when is_list(opts) do
    Lock.with_lock(opts, fn -> start_locked(opts) end)
  end

  defp start_locked(opts) do
    with {:ok, compose_file} <- Config.resolve_compose_file(opts),
         :ok <- Install.ensure_ready(opts),
         {:ok, install} <- State.read_install(opts),
         :ok <- ensure_project_roles_startable(opts),
         config = Config.resolve(opts),
         {:ok, secrets} <- Secrets.resolve(config, opts),
         {:ok, project} <-
           ComposeProject.write(
             install,
             secrets,
             config,
             Keyword.put(opts, :compose_file, compose_file)
           ),
         :ok <- put_runtime_configuration(project, opts),
         {:ok, runner} <- RunnerImage.ensure(project, opts),
         {:ok, deployment} <-
           ComposeDeployment.resolve(
             project,
             install,
             runner,
             Keyword.put(opts, :required_profile, :local)
           ),
         {:ok, preexisting} <- ensure_startable_stack(deployment, opts),
         :ok <- State.clear_maintenance(opts) do
      case do_start(install, project, deployment, runner, opts) do
        {:ok, _result} = success ->
          success

        {:error, reason} = error ->
          _ = cleanup_failed_start(deployment, preexisting, opts)
          _ = record_failure("dev", reason, opts)
          error
      end
    else
      {:error, reason} = error ->
        _ = record_failure("dev", reason, opts)
        error
    end
  end

  defp do_start(install, project, deployment, runner, opts) do
    postgres = ComposeDeployment.service!(deployment, :postgres)
    runner_service = ComposeDeployment.service!(deployment, :runner)
    control_plane = ComposeDeployment.service!(deployment, :control_plane)

    with :ok <- compose(deployment, ["up", "--detach", "--wait", postgres], :postgres, opts),
         :ok <- release_operations(deployment, opts),
         :ok <-
           compose(deployment, ["up", "--detach", "--wait", runner_service], :runner, opts),
         :ok <-
           compose(
             deployment,
             ["up", "--detach", "--wait", control_plane],
             :control_plane,
             opts
           ),
         :ok <- await_liveness(deployment, opts),
         {:ok, manifest_deployment} <- deploy_manifest(deployment, runner, opts),
         :ok <- await_readiness(deployment, opts),
         {:ok, runner_environment_identity} <-
           ComposeProject.runner_environment_identity(project),
         result <- start_result(deployment, runner, manifest_deployment),
         :ok <-
           write_runtime(
             install,
             deployment,
             runner,
             runner_environment_identity,
             result,
             opts
           ),
         :ok <- remove_obsolete_generated_compose(deployment, opts) do
      {:ok, result}
    end
  end

  @doc "Starts the local stack, streams Compose logs, and stops it on exit."
  @spec start_foreground(keyword()) :: :ok | {:error, term()}
  def start_foreground(opts \\ []) when is_list(opts) do
    with {:ok, result} <- start(opts), do: finish_foreground_start(result, opts)
  end

  @doc "Selects an exact local candidate and starts or reloads maintainer development."
  @spec maintainer_dev(Candidate.t(), keyword()) :: :ok | {:error, term()}
  def maintainer_dev(%Candidate{} = candidate, opts \\ []) when is_list(opts) do
    result = Lock.with_lock(opts, fn -> maintainer_dev_locked(candidate, opts) end)

    case result do
      {:ok, {:started, start_result}} ->
        progress(opts, "Maintainer control plane selected by image ID #{candidate.image_id}")
        finish_foreground_start(start_result, opts)

      {:ok, :reloaded} ->
        progress(opts, "Maintainer checkout reload complete")
        :ok

      {:error, _reason} = error ->
        error
    end
  end

  defp maintainer_dev_locked(candidate, opts) do
    with {:ok, capability} <- RunnerBuildCapability.from_candidate(candidate, opts) do
      opts = Keyword.put(opts, :maintainer_runner_build, capability)

      case State.read_runtime(opts) do
        {:error, :not_found} ->
          with :ok <- Install.select_maintainer(candidate, opts),
               {:ok, result} <- start_locked(opts) do
            {:ok, {:started, result}}
          end

        {:ok,
         %{
           "schema_version" => @runtime_schema_version,
           "control_plane_image_reference" => image_reference
         }} ->
          if image_reference == candidate.image_id do
            with :ok <- Install.select_maintainer(candidate, opts),
                 :ok <- reload_locked(opts) do
              {:ok, :reloaded}
            end
          else
            {:error,
             {:maintainer_restart_required,
              %{current_image: image_reference, candidate_image: candidate.image_id}}}
          end

        {:ok, _stale_runtime} ->
          {:error, :stale_pre_migration_runtime_state}

        {:error, reason} ->
          {:error, {:local_runtime_state_unavailable, reason}}
      end
    end
  end

  defp finish_foreground_start(result, opts) do
    progress(opts, "Favn local stack ready")
    progress(opts, "View: #{result.view_url}")
    progress(opts, "Private API: #{result.orchestrator_url}")

    if Keyword.get(opts, :foreground, true) do
      try do
        logs(Keyword.put(opts, :follow, true))
      after
        _ = stop(opts)
      end
    else
      :ok
    end
  end

  @doc "Rebuilds an aligned release and applies the canonical local change class."
  @spec reload(keyword()) :: :ok | {:error, term()}
  def reload(opts \\ []) when is_list(opts) do
    Lock.with_lock(opts, fn -> reload_locked(opts) end)
  end

  defp reload_locked(opts) do
    result =
      with {:ok, active_deployment} <- running_project_from_state(opts),
           :ok <- validate_recorded_compose_file(active_deployment, opts),
           {:ok, previous_runner} <- reload_previous_runner(opts) do
        reload_candidate(active_deployment, previous_runner, opts)
      end

    case result do
      :ok ->
        :ok

      {:error, reason} = error ->
        _ = record_failure("reload", reason, opts)
        error
    end
  end

  defp reload_candidate(active_deployment, previous_runner, opts) do
    case prepare_reload_candidate(active_deployment, opts) do
      {:ok, prepared} ->
        apply_prepared_reload(prepared, previous_runner, opts)

      {:error, reason} = error ->
        case restore_uncommitted_runner(active_deployment, previous_runner, opts) do
          :ok ->
            error

          {:error, restore_reason} ->
            {:error, {:reload_preparation_restore_failed, reason, restore_reason}}
        end
    end
  end

  defp prepare_reload_candidate(active_deployment, opts) do
    with :ok <- Install.ensure_ready(opts),
         {:ok, install} <- State.read_install(opts),
         config = Config.resolve(opts),
         {:ok, secrets} <- Secrets.resolve(config, opts),
         {:ok, project} <-
           ComposeProject.write(
             install,
             secrets,
             config,
             Keyword.put(opts, :compose_file, active_deployment.compose_file)
           ),
         :ok <- put_runtime_configuration(project, opts),
         {:ok, runner_environment_identity} <-
           ComposeProject.runner_environment_identity(project),
         {:ok, runner} <- RunnerImage.ensure(project, opts),
         {:ok, current_deployment} <-
           ComposeDeployment.resolve(
             project,
             install,
             runner,
             Keyword.put(opts, :required_profile, :local)
           ),
         :ok <- unchanged_deployment(active_deployment, current_deployment),
         :ok <-
           reload_preflight(
             current_deployment,
             Keyword.put_new(opts, :ready_timeout_ms, 5_000)
           ) do
      {:ok,
       %{
         deployment: current_deployment,
         runner: runner,
         runner_environment_identity: runner_environment_identity
       }}
    end
  end

  defp apply_prepared_reload(prepared, previous_runner, opts) do
    case ensure_reload_recovery(prepared.deployment, previous_runner, opts) do
      {:ok, recovery} ->
        apply_recoverable_reload(prepared, recovery, opts)

      {:error, reason} = error ->
        case restore_uncommitted_runner(prepared.deployment, previous_runner, opts) do
          :ok ->
            error

          {:error, restore_reason} ->
            {:error, {:reload_preparation_restore_failed, reason, restore_reason}}
        end
    end
  end

  defp apply_recoverable_reload(prepared, recovery, opts) do
    with {:ok, runtime} <- State.read_runtime(opts),
         change <-
           classify_reload(
             runtime,
             recovery,
             prepared.runner,
             prepared.runner_environment_identity
           ),
         {:ok, manifest_deployment} <-
           apply_runner_change(prepared.deployment, recovery, prepared.runner, change, opts),
         :ok <-
           update_runtime_after_reload(
             prepared.deployment,
             prepared.runner,
             prepared.runner_environment_identity,
             manifest_deployment,
             change,
             opts
           ) do
      progress(opts, reload_message(change, prepared.runner, manifest_deployment))
      :ok
    end
  end

  defp restore_uncommitted_runner(active_deployment, previous_runner, opts) do
    case State.read_maintenance(opts) do
      {:error, :not_found} ->
        restore_runner_selection(active_deployment, previous_runner, opts)

      {:ok, _existing_recovery} ->
        :ok

      {:error, reason} ->
        {:error, {:reload_recovery_state_unavailable, reason}}
    end
  end

  defp put_runtime_configuration(project, opts) do
    config = Config.resolve(opts)

    with :ok <-
           ComposeProject.put_runner_environment(
             project,
             Keyword.get(opts, :env_file_loaded, %{})
           ),
         :ok <- ComposeProject.put_scheduler_enabled(project, config.scheduler_enabled) do
      :ok
    end
  end

  @doc "Stops the local Compose application without deleting its database volume."
  @spec stop(keyword()) :: :ok | {:error, term()}
  def stop(opts \\ []) when is_list(opts) do
    Lock.with_lock(opts, fn -> stop_locked(opts) end)
  end

  defp stop_locked(opts) do
    case running_project_from_state(opts) do
      {:ok, deployment} ->
        control_plane = ComposeDeployment.service!(deployment, :control_plane)
        runner = ComposeDeployment.service!(deployment, :runner)
        postgres = Map.get(deployment.services, :postgres)

        with :ok <-
               compose(
                 deployment,
                 ["stop", "--timeout", "180", control_plane],
                 :control_plane,
                 opts
               ),
             :ok <- compose(deployment, ["stop", "--timeout", "180", runner], :runner, opts),
             :ok <- maybe_stop_postgres(deployment, postgres, opts),
             :ok <- State.clear_runtime(opts),
             :ok <- State.clear_maintenance(opts) do
          :ok
        end

      {:error, :stack_not_running} ->
        stop_unrecorded_roles(opts)

      {:error, _reason} = error ->
        error
    end
  end

  defp stop_unrecorded_roles(opts) do
    project_name = opts |> Paths.root_dir() |> Path.expand() |> ComposeProject.project_name()

    with {:ok, containers} <- Docker.project_role_containers(project_name, opts),
         :ok <- Docker.stop_containers(containers, 180, opts),
         :ok <- State.clear_maintenance(opts) do
      :ok
    else
      {:error, reason} -> {:error, {:local_compose_state_unavailable, reason}}
    end
  end

  @doc "Returns bounded Compose and release status without reading secret values."
  @spec status(keyword()) :: map()
  def status(opts \\ []) when is_list(opts) do
    result =
      case project_from_state(opts) do
        {:ok, deployment} ->
          {output, command_status} =
            Docker.compose(deployment, ["ps", "--format", "json"], opts)

          rendered_services = parse_compose_ps(output, command_status)
          services = role_service_statuses(deployment, rendered_services)
          runtime = runtime_status(rendered_services, deployment, opts)

          %{
            stack_status: stack_status(deployment, rendered_services, command_status),
            storage: :postgres,
            services: services,
            runner: bounded_runner_state(State.read_runner_latest(opts)),
            active_manifest_version_id: active_manifest(opts),
            user_urls: %{
              web: deployment.view_url,
              orchestrator_api: deployment.orchestrator_url
            },
            compose_file: ComposeDeployment.relative_compose_file(deployment),
            compose_contract_version: deployment.contract_version,
            compose_profile: deployment.profile,
            compose_project: deployment.project_name,
            runtime: runtime,
            last_failure: last_failure(opts)
          }

        {:error, reason} ->
          selection = inactive_compose_selection(opts)

          Map.merge(
            %{
              stack_status: if(reason == :stack_not_running, do: :stopped, else: :unknown),
              storage: :postgres,
              services: %{},
              runner: bounded_runner_state(State.read_runner_latest(opts)),
              active_manifest_version_id: active_manifest(opts),
              user_urls: %{},
              runtime: %{"status" => "unavailable"},
              last_failure: last_failure(opts),
              error: reason
            },
            selection
          )
      end

    OutputRedactor.redact_term(result, opts)
  end

  @doc "Reads bounded, prefixed service logs through Docker Compose."
  @spec logs(keyword()) :: :ok | {:error, term()}
  def logs(opts \\ []) when is_list(opts) do
    with {:ok, deployment} <- project_from_state(opts),
         {:ok, services} <- selected_services(deployment, opts) do
      args = ["logs", "--tail", Integer.to_string(log_tail(opts)), "--no-color"]
      args = if Keyword.get(opts, :follow, false), do: args ++ ["--follow"], else: args
      args = args ++ services

      {output, status} =
        Docker.compose(
          deployment,
          args,
          Keyword.merge(opts,
            compose_command_timeout_ms:
              if(Keyword.get(opts, :follow, false), do: 86_400_000, else: 30_000),
            docker_output_writer: Keyword.get(opts, :writer, &IO.binwrite/1)
          )
        )

      if status == 0, do: :ok, else: {:error, {:compose_logs_failed, status, bounded(output)}}
    end
  end

  @doc "Returns local Docker, install, Compose, and runtime diagnostics."
  @spec diagnostics(keyword()) :: {:ok, map()} | {:error, term()}
  def diagnostics(opts \\ []) when is_list(opts) do
    with {:ok, probe} <- Docker.probe(opts),
         :ok <- Install.ensure_ready(opts),
         {:ok, install} <- State.read_install(opts) do
      compose_status = status(opts)
      deployment_contract = deployment_contract_diagnostics(install, opts)

      report = %{
        "status" => diagnostics_status(deployment_contract, compose_status),
        "docker" => probe,
        "control_plane" => control_plane_diagnostics(install),
        "deployment_contract" => deployment_contract,
        "runner_inputs" => runner_input_diagnostics(opts),
        "compose" => compose_status,
        "runtime" => compose_status.runtime
      }

      {:ok, OutputRedactor.redact_term(report, opts)}
    end
  end

  defp control_plane_diagnostics(install) do
    base = %{
      "source" => install["source"],
      "image_reference" => install["image_reference"],
      "image_id" => install["image_id"],
      "build_id" => install["control_plane_build_id"]
    }

    if install["source"] == "maintainer" do
      Map.merge(base, %{
        "checkout" => install["checkout"],
        "checkout_revision" => install["checkout_revision"],
        "checkout_dirty" => install["checkout_dirty"],
        "image_source_revision" => install["image_source_revision"],
        "image_source_dirty" => install["image_source_dirty"]
      })
    else
      base
    end
  end

  defp runner_input_diagnostics(opts) do
    case State.read_runner_latest(opts) do
      {:ok, %{"source_inputs" => source_inputs}} when is_map(source_inputs) ->
        Map.take(source_inputs, [
          "application_count",
          "file_count",
          "total_bytes",
          "current_application_roots"
        ])

      {:ok, _state} ->
        %{}

      {:error, _reason} ->
        %{}
    end
  end

  defp inactive_compose_selection(opts) do
    root_dir = opts |> Paths.root_dir() |> Path.expand()

    case selected_compose_file(opts) do
      {:ok, compose_file} ->
        %{
          compose_file: Path.relative_to(compose_file, root_dir),
          compose_contract_version: ComposeDeployment.contract_version(),
          compose_profile: :local,
          compose_project: ComposeProject.project_name(root_dir)
        }

      {:error, _reason} ->
        %{compose_project: ComposeProject.project_name(root_dir)}
    end
  end

  defp deployment_contract_diagnostics(install, opts) do
    with {:ok, compose_file} <- selected_compose_file(opts),
         {:ok, runner_state} <- State.read_runner_latest(opts),
         {:ok, runner} <- diagnostic_runner(runner_state),
         {:ok, deployment} <-
           ComposeDeployment.resolve(
             diagnostic_project(compose_file, opts),
             install,
             runner,
             Keyword.put(opts, :required_profile, :local)
           ) do
      %{
        "status" => "ok",
        "compose_file" => ComposeDeployment.relative_compose_file(deployment),
        "contract_version" => deployment.contract_version,
        "profile" => profile_name(deployment.profile),
        "services" => ComposeDeployment.encoded_services(deployment)
      }
    else
      {:error, reason} ->
        %{"status" => "error", "error" => bounded(reason)}
    end
  end

  defp diagnostic_runner(%{
         "image_reference" => image_reference,
         "image_id" => image_id,
         "runner_release_id" => runner_release_id
       })
       when is_binary(image_reference) and is_binary(image_id) and is_binary(runner_release_id) do
    {:ok,
     %{
       image_reference: image_reference,
       image_id: image_id,
       runner_release_id: runner_release_id
     }}
  end

  defp diagnostic_runner(_invalid), do: {:error, :runner_image_not_prepared}

  defp diagnostic_project(compose_file, opts) do
    root_dir = opts |> Paths.root_dir() |> Path.expand()
    config = Config.resolve(opts)

    %{
      "project_name" => ComposeProject.project_name(root_dir),
      "compose_path" => compose_file,
      "env_path" => Paths.compose_env_path(root_dir),
      "workspace_id" => config.workspace_id,
      "view_url" => "http://127.0.0.1:#{config.web_port}",
      "orchestrator_url" => "http://127.0.0.1:#{config.orchestrator_port}"
    }
  end

  defp diagnostics_status(%{"status" => "ok"}, %{stack_status: status})
       when status in [:running, :stopped],
       do: "ok"

  defp diagnostics_status(_deployment, _compose_status), do: "error"

  defp project_from_state(opts) do
    case running_project_from_state(opts) do
      {:error, :stack_not_running} ->
        case State.read_compose_selection(opts) do
          {:ok, %{"schema_version" => @runtime_schema_version} = selection} ->
            ComposeDeployment.from_runtime(selection, opts)

          {:ok, _invalid} ->
            {:error, :stale_pre_migration_runtime_state}

          {:error, :not_found} ->
            {:error, :stack_not_running}

          {:error, reason} ->
            {:error, {:local_compose_selection_unavailable, reason}}
        end

      result ->
        result
    end
  end

  defp running_project_from_state(opts) do
    case State.read_runtime(opts) do
      {:ok, %{"schema_version" => @runtime_schema_version} = runtime} ->
        ComposeDeployment.from_runtime(runtime, opts)

      {:ok, _old_runtime} ->
        {:error, :stale_pre_migration_runtime_state}

      {:error, :not_found} ->
        {:error, :stack_not_running}

      {:error, reason} ->
        {:error, {:local_runtime_state_unavailable, reason}}
    end
  end

  defp selected_compose_file(opts) do
    recorded =
      case State.read_runtime(opts) do
        {:ok, %{"compose_file" => path}} when is_binary(path) -> {:ok, path}
        _unavailable -> selected_compose_file_from_history(opts)
      end

    case recorded do
      {:ok, path} -> Config.resolve_compose_file(Keyword.put(opts, :compose_file, path))
      :not_found -> Config.resolve_compose_file(opts)
      {:error, _reason} = error -> error
    end
  end

  defp selected_compose_file_from_history(opts) do
    case State.read_compose_selection(opts) do
      {:ok, %{"compose_file" => path}} when is_binary(path) -> {:ok, path}
      {:ok, _invalid} -> {:error, :invalid_compose_selection}
      {:error, :not_found} -> :not_found
      {:error, reason} -> {:error, {:local_compose_selection_unavailable, reason}}
    end
  end

  defp release_operations(deployment, opts) do
    Enum.reduce_while(
      ["migrate", "grant-runtime", "verify-schema", "provision-workspace"],
      :ok,
      fn operation, :ok ->
        service =
          if operation == "verify-schema",
            do: ComposeDeployment.service!(deployment, :control_plane_verify),
            else: ComposeDeployment.service!(deployment, :control_plane_ops)

        case compose(
               deployment,
               ["--profile", "operations", "run", "--rm", service, operation],
               {:release_operation, operation},
               Keyword.put_new(opts, :compose_command_timeout_ms, 600_000)
             ) do
          :ok -> {:cont, :ok}
          {:error, _reason} = error -> {:halt, error}
        end
      end
    )
  end

  defp deploy_manifest(deployment, runner, opts) do
    case Keyword.get(opts, :deploy_fun) do
      fun when is_function(fun, 3) ->
        if Mix.env() == :test,
          do: fun.(deployment, runner, opts),
          else: {:error, :deployment_injection_not_allowed}

      _other ->
        do_deploy_manifest(deployment, runner, opts)
    end
  end

  defp do_deploy_manifest(deployment, runner, opts) do
    with {:ok, secrets} <- State.read_secrets(opts),
         token when is_binary(token) <- secrets["service_token"],
         manifest_path <- Path.join(runner.manifest_dir, "manifest-index.json"),
         {:ok, published} <-
           Publish.run(
             manifest_path: manifest_path,
             orchestrator_url: deployment.orchestrator_url,
             env: %{"FAVN_ORCHESTRATOR_SERVICE_TOKEN" => token},
             client: Keyword.get(opts, :orchestrator_client, OrchestratorClient),
             maintenance_token: Keyword.get(opts, :maintenance_token)
           ),
         {:ok, activated} <-
           Activate.run(
             orchestrator_url: deployment.orchestrator_url,
             manifest_version_id: published.manifest_version_id,
             workspace_id: deployment.workspace_id,
             env: %{"FAVN_ORCHESTRATOR_SERVICE_TOKEN" => token},
             client: Keyword.get(opts, :orchestrator_client, OrchestratorClient),
             maintenance_token: Keyword.get(opts, :maintenance_token)
           ) do
      {:ok, %{published: published, activated: activated}}
    else
      nil -> {:error, :invalid_local_secrets}
      {:error, _reason} = error -> error
    end
  end

  defp await_readiness(deployment, opts) do
    timeout_ms = Keyword.get(opts, :ready_timeout_ms, @default_ready_timeout_ms)
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    readiness_fun = Keyword.get(opts, :readiness_fun, &readiness/1)
    do_await_readiness(deployment.orchestrator_url, readiness_fun, deadline, opts)
  end

  defp reload_preflight(deployment, opts) do
    case State.read_maintenance(opts) do
      {:ok, maintenance} ->
        with {:ok, recovery} <- validate_maintenance(maintenance) do
          case recovery["phase"] do
            "preparing" -> await_readiness(deployment, opts)
            "active" -> await_liveness(deployment, opts)
          end
        end

      {:error, :not_found} ->
        await_readiness(deployment, opts)

      _invalid ->
        {:error, :invalid_local_maintenance_state}
    end
  end

  defp await_liveness(deployment, opts) do
    timeout_ms = Keyword.get(opts, :ready_timeout_ms, @default_ready_timeout_ms)
    deadline = System.monotonic_time(:millisecond) + timeout_ms

    liveness_fun =
      Keyword.get(
        opts,
        :liveness_fun,
        Keyword.get(opts, :readiness_fun, &liveness/1)
      )

    do_await_liveness(deployment.orchestrator_url, liveness_fun, deadline, opts)
  end

  defp do_await_liveness(url, liveness_fun, deadline, opts) do
    case liveness_fun.(url) do
      :ok ->
        :ok

      {:error, reason} ->
        if System.monotonic_time(:millisecond) >= deadline do
          {:error, {:control_plane_not_live, reason}}
        else
          Process.sleep(Keyword.get(opts, :ready_poll_interval_ms, 500))
          do_await_liveness(url, liveness_fun, deadline, opts)
        end
    end
  end

  defp do_await_readiness(url, readiness_fun, deadline, opts) do
    case readiness_fun.(url) do
      :ok ->
        :ok

      {:error, reason} ->
        if System.monotonic_time(:millisecond) >= deadline do
          {:error, {:control_plane_not_ready, reason}}
        else
          Process.sleep(Keyword.get(opts, :ready_poll_interval_ms, 500))
          do_await_readiness(url, readiness_fun, deadline, opts)
        end
    end
  end

  defp readiness(base_url) do
    case LocalHttpClient.request(:get, base_url <> "/api/orchestrator/v1/health/ready") do
      {:ok, %{"data" => %{"status" => "ready"}}} -> :ok
      {:ok, %{"status" => "ready"}} -> :ok
      {:ok, response} -> {:error, {:unexpected_readiness, bounded(response)}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp liveness(base_url) do
    case LocalHttpClient.request(:get, base_url <> "/api/orchestrator/v1/health/live") do
      {:ok, %{"data" => %{"status" => "ok"}}} -> :ok
      {:ok, %{"status" => "ok"}} -> :ok
      {:ok, response} -> {:error, {:unexpected_liveness, bounded(response)}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp classify_reload(runtime, recovery, runner, runner_environment_identity) do
    previous_release = get_in(recovery, ["previous_runner", "runner_release_id"])
    active_manifest = get_in(recovery, ["active_manifest", "manifest_version_id"])

    cond do
      previous_release != runner.runner_release_id -> :runner_rebuild
      runtime["runner_environment_identity"] != runner_environment_identity -> :runner_environment
      active_manifest != runner.manifest_version_id -> :manifest_only
      true -> :runner_image_reused
    end
  end

  defp apply_runner_change(deployment, _recovery, runner, change, opts)
       when change in [:manifest_only, :runner_image_reused] do
    with {:ok, manifest_deployment} <- deploy_manifest(deployment, runner, opts),
         :ok <- await_readiness(deployment, opts),
         :ok <- State.clear_maintenance(opts) do
      {:ok, manifest_deployment}
    end
  end

  defp apply_runner_change(deployment, recovery, runner, change, opts)
       when change in [:runner_rebuild, :runner_environment] do
    coordinated_runner_replacement(deployment, recovery, runner, opts)
  end

  defp coordinated_runner_replacement(deployment, recovery, runner, opts) do
    previous = previous_runner_from_recovery(recovery)

    with {:ok, active_recovery} <- begin_runner_replacement(deployment, recovery, opts) do
      maintenance_token = active_recovery["token"]

      case safely_replace_runner(
             deployment,
             previous,
             runner,
             active_recovery,
             maintenance_token,
             opts
           ) do
        {:ok, manifest_deployment} ->
          with :ok <- finish_runner_replacement(deployment, maintenance_token, opts),
               :ok <- await_readiness(deployment, opts) do
            {:ok, manifest_deployment}
          end

        {:error, reason, :rollback_verified} ->
          case finish_runner_replacement(deployment, maintenance_token, opts) do
            :ok ->
              {:error, reason}

            {:error, finish_reason} ->
              {:error, {:runner_replacement_finish_failed, reason, finish_reason}}
          end

        {:error, reason, {:rollback_failed, rollback_reason}} ->
          {:error, {:runner_replacement_rollback_failed, reason, rollback_reason}}
      end
    else
      {:error, _reason} = error ->
        _ = restore_runner_selection(deployment, previous, opts)
        error
    end
  end

  defp safely_replace_runner(
         deployment,
         previous,
         runner,
         recovery,
         maintenance_token,
         opts
       ) do
    replace_runner(deployment, previous, runner, recovery, maintenance_token, opts)
  rescue
    exception ->
      replacement_failure(
        {:runner_replacement_exception, Exception.message(exception)},
        restore_previous_runner(deployment, previous, recovery, maintenance_token, opts)
      )
  catch
    kind, reason ->
      replacement_failure(
        {:runner_replacement_caught, kind, reason},
        restore_previous_runner(deployment, previous, recovery, maintenance_token, opts)
      )
  end

  defp replace_runner(deployment, previous, runner, recovery, maintenance_token, opts) do
    case await_runner_drain(deployment, opts) do
      :ok ->
        do_replace_runner(deployment, previous, runner, recovery, maintenance_token, opts)

      {:error, reason} ->
        replacement_failure(
          reason,
          verify_unchanged_previous_runner(
            deployment,
            previous,
            recovery,
            maintenance_token,
            opts
          )
        )
    end
  end

  defp do_replace_runner(deployment, previous, runner, recovery, maintenance_token, opts) do
    runner_service = ComposeDeployment.service!(deployment, :runner)

    replacement =
      with :ok <-
             compose(
               deployment,
               ["stop", "--timeout", "180", runner_service],
               :runner_drain,
               opts
             ),
           :ok <-
             compose(
               deployment,
               ["up", "--detach", "--wait", "--no-deps", "--force-recreate", runner_service],
               :runner_replacement,
               opts
             ),
           :ok <- verify_replacement_runner(deployment, maintenance_token, runner, opts),
           {:ok, manifest_deployment} <-
             deploy_manifest(
               deployment,
               runner,
               Keyword.put(opts, :maintenance_token, maintenance_token)
             ) do
        {:ok, manifest_deployment}
      end

    case replacement do
      {:ok, _deployment} = success ->
        success

      {:error, reason} ->
        rollback =
          restore_previous_runner(deployment, previous, recovery, maintenance_token, opts)

        _ = record_failure("reload_runner", {runner.runner_release_id, reason}, opts)
        replacement_failure(reason, rollback)
    end
  end

  defp await_runner_drain(deployment, opts) do
    timeout_ms = Keyword.get(opts, :runner_drain_timeout_ms, 120_000)
    deadline = System.monotonic_time(:millisecond) + max(timeout_ms, 0)
    do_await_runner_drain(deployment, deadline, opts)
  end

  defp do_await_runner_drain(deployment, deadline, opts) do
    with {:ok, status} <- runner_replacement_status(deployment, opts),
         true <- maintenance_drained?(status),
         :ok <- ensure_no_in_flight_runs(deployment, opts) do
      :ok
    else
      false ->
        retry_runner_drain(deployment, deadline, {:active_admissions, :present}, opts)

      {:error, {:in_flight_runs, _run_ids} = reason} ->
        retry_runner_drain(deployment, deadline, reason, opts)

      {:error, _reason} = error ->
        error
    end
  end

  defp retry_runner_drain(deployment, deadline, reason, opts) do
    if System.monotonic_time(:millisecond) >= deadline do
      {:error, reason}
    else
      Process.sleep(Keyword.get(opts, :runner_drain_poll_interval_ms, 250))
      do_await_runner_drain(deployment, deadline, opts)
    end
  end

  defp ensure_no_in_flight_runs(deployment, opts) do
    case Keyword.get(opts, :in_flight_fun) do
      fun when is_function(fun, 1) ->
        if Mix.env() == :test,
          do: ensure_empty_run_ids(fun.(deployment)),
          else: {:error, :in_flight_injection_not_allowed}

      _other ->
        fetch_in_flight_runs(deployment, opts)
    end
  end

  defp fetch_in_flight_runs(deployment, opts) do
    client = Keyword.get(opts, :orchestrator_client, OrchestratorClient)

    with {:ok, secrets} <- State.read_secrets(opts),
         token when is_binary(token) <- secrets["service_token"],
         {:ok, run_ids} <-
           client.in_flight_runs(
             deployment.orchestrator_url,
             token,
             local_context(deployment)
           ) do
      ensure_empty_run_ids({:ok, run_ids})
    else
      nil -> {:error, :invalid_local_secrets}
      {:error, _reason} = error -> error
    end
  end

  defp ensure_empty_run_ids({:ok, []}), do: :ok

  defp ensure_empty_run_ids({:ok, run_ids}) when is_list(run_ids),
    do: {:error, {:in_flight_runs, Enum.take(run_ids, 100)}}

  defp ensure_empty_run_ids({:error, _reason} = error), do: error
  defp ensure_empty_run_ids(_invalid), do: {:error, :invalid_in_flight_response}

  defp begin_runner_replacement(deployment, recovery, opts) do
    with {:ok, active_recovery} <- mark_recovery_active(recovery, opts),
         maintenance_token <- active_recovery["token"],
         {:ok, service_token} <- service_token(opts),
         {:ok, ^maintenance_token} <-
           client(opts).begin_runner_replacement(
             deployment.orchestrator_url,
             service_token,
             maintenance_token
           ) do
      {:ok, active_recovery}
    else
      {:ok, _different_token} -> {:error, :maintenance_token_mismatch}
      {:error, _reason} = error -> error
    end
  end

  defp runner_replacement_status(deployment, opts) do
    with {:ok, token} <- service_token(opts) do
      client(opts).runner_replacement_status(deployment.orchestrator_url, token)
    end
  end

  defp finish_runner_replacement(deployment, maintenance_token, opts) do
    with {:ok, token} <- service_token(opts) do
      with :ok <-
             client(opts).finish_runner_replacement(
               deployment.orchestrator_url,
               token,
               maintenance_token
             ),
           :ok <- State.clear_maintenance(opts) do
        :ok
      end
    end
  end

  defp verify_replacement_runner(deployment, maintenance_token, runner, opts) do
    with {:ok, token} <- service_token(opts),
         {:ok, _verified} <-
           client(opts).verify_replacement_runner(
             deployment.orchestrator_url,
             token,
             maintenance_token,
             runner.runner_release_id
           ) do
      :ok
    end
  end

  defp maintenance_drained?(status) when is_map(status) do
    maintenance? = Map.get(status, "maintenance?", Map.get(status, :maintenance?))
    kind = Map.get(status, "maintenance_kind", Map.get(status, :maintenance_kind))
    admissions = Map.get(status, "active_admissions", Map.get(status, :active_admissions))

    maintenance? == true and kind in ["runner_replacement", :runner_replacement] and
      admissions == 0
  end

  defp maintenance_drained?(_status), do: false

  defp service_token(opts) do
    case State.read_secrets(opts) do
      {:ok, %{"service_token" => token}} when is_binary(token) and token != "" -> {:ok, token}
      _invalid -> {:error, :invalid_local_secrets}
    end
  end

  defp client(opts), do: Keyword.get(opts, :orchestrator_client, OrchestratorClient)

  defp ensure_reload_recovery(deployment, previous_runner, opts) do
    case State.read_maintenance(opts) do
      {:ok, maintenance} ->
        validate_maintenance(maintenance)

      {:error, :not_found} ->
        with previous when is_map(previous) <- previous_runner,
             {:ok, active_manifest} <- active_manifest_identity(deployment, previous, opts),
             recovery <- new_recovery(previous, active_manifest),
             :ok <- State.write_maintenance(recovery, opts) do
          {:ok, recovery}
        else
          nil -> {:error, :previous_runner_unavailable}
          {:error, _reason} = error -> error
        end

      _invalid ->
        {:error, :invalid_local_maintenance_state}
    end
  end

  defp reload_previous_runner(opts) do
    case State.read_maintenance(opts) do
      {:ok, maintenance} ->
        with {:ok, _validated} <- validate_maintenance(maintenance), do: {:ok, nil}

      {:error, :not_found} ->
        load_previous_runner(opts)

      _invalid ->
        {:error, :invalid_local_maintenance_state}
    end
  end

  defp new_recovery(previous, active_manifest) do
    %{
      "schema_version" => @maintenance_schema_version,
      "kind" => "runner_replacement",
      "phase" => "preparing",
      "token" => maintenance_token(),
      "previous_runner" => %{
        "runner_release_id" => previous.runner_release_id,
        "image_reference" => previous.image_reference,
        "state" => previous.state
      },
      "active_manifest" => active_manifest,
      "created_at" => DateTime.utc_now() |> DateTime.to_iso8601()
    }
  end

  defp mark_recovery_active(%{"phase" => "active"} = recovery, _opts), do: {:ok, recovery}

  defp mark_recovery_active(%{"phase" => "preparing"} = recovery, opts) do
    active = Map.put(recovery, "phase", "active")
    with :ok <- State.write_maintenance(active, opts), do: {:ok, active}
  end

  defp validate_maintenance(
         %{
           "schema_version" => @maintenance_schema_version,
           "kind" => "runner_replacement",
           "phase" => phase,
           "token" => token,
           "previous_runner" => previous,
           "active_manifest" => active_manifest
         } = maintenance
       )
       when phase in ["preparing", "active"] and is_binary(token) do
    with true <- Regex.match?(@maintenance_token, token),
         {:ok, previous_release_id} <- validate_previous_runner(previous),
         :ok <- validate_active_manifest(active_manifest, previous_release_id) do
      {:ok, maintenance}
    else
      _invalid -> {:error, :invalid_local_maintenance_state}
    end
  end

  defp validate_maintenance(_invalid), do: {:error, :invalid_local_maintenance_state}

  defp validate_previous_runner(%{
         "runner_release_id" => release_id,
         "image_reference" => image_reference,
         "state" => %{
           "runner_release_id" => release_id,
           "image_reference" => image_reference
         }
       })
       when is_binary(release_id) and is_binary(image_reference) and image_reference != "" do
    if Regex.match?(@runner_release_id, release_id),
      do: {:ok, release_id},
      else: {:error, :invalid_previous_runner}
  end

  defp validate_previous_runner(_invalid), do: {:error, :invalid_previous_runner}

  defp validate_active_manifest(
         %{
           "manifest_version_id" => manifest_version_id,
           "required_runner_release_id" => runner_release_id
         },
         runner_release_id
       )
       when is_binary(manifest_version_id) and manifest_version_id != "",
       do: :ok

  defp validate_active_manifest(_invalid, _runner_release_id),
    do: {:error, :invalid_active_manifest}

  defp active_manifest_identity(deployment, previous, opts) do
    with {:ok, identity} <- fetch_active_manifest_identity(deployment, opts),
         true <- identity["required_runner_release_id"] == previous.runner_release_id do
      {:ok, identity}
    else
      false -> {:error, :active_manifest_runner_mismatch}
      {:error, _reason} = error -> error
    end
  end

  defp fetch_active_manifest_identity(deployment, opts) do
    result =
      case Keyword.get(opts, :active_manifest_fun) do
        fun when is_function(fun, 1) ->
          if Mix.env() == :test,
            do: fun.(deployment),
            else: {:error, :active_manifest_injection_not_allowed}

        _other ->
          with {:ok, token} <- service_token(opts) do
            client(opts).bootstrap_active_manifest(
              deployment.orchestrator_url,
              token,
              local_context(deployment)
            )
          end
      end

    with {:ok, response} when is_map(response) <- result,
         manifest when is_map(manifest) <- Map.get(response, "manifest", response),
         manifest_version_id when is_binary(manifest_version_id) and manifest_version_id != "" <-
           manifest["manifest_version_id"],
         required_runner_release_id when is_binary(required_runner_release_id) <-
           manifest["required_runner_release_id"] do
      {:ok,
       %{
         "manifest_version_id" => manifest_version_id,
         "required_runner_release_id" => required_runner_release_id
       }}
    else
      {:error, _reason} = error ->
        error

      _invalid ->
        {:error, :invalid_active_manifest_response}
    end
  end

  defp maintenance_token do
    32
    |> :crypto.strong_rand_bytes()
    |> Base.url_encode64(padding: false)
  end

  defp verify_unchanged_previous_runner(
         _deployment,
         nil,
         _recovery,
         _maintenance_token,
         _opts
       ),
       do: {:error, :previous_runner_unavailable}

  defp verify_unchanged_previous_runner(
         deployment,
         previous,
         recovery,
         maintenance_token,
         opts
       ) do
    with :ok <- restore_runner_selection(deployment, previous, opts),
         :ok <-
           verify_replacement_runner(
             deployment,
             maintenance_token,
             %{runner_release_id: previous.runner_release_id},
             opts
           ),
         :ok <- verify_active_manifest_alignment(deployment, recovery, opts) do
      :ok
    end
  end

  defp restore_previous_runner(_deployment, nil, _recovery, _maintenance_token, _opts),
    do: {:error, :previous_runner_unavailable}

  defp restore_previous_runner(
         deployment,
         %{image_reference: image_reference} = previous,
         recovery,
         maintenance_token,
         opts
       )
       when is_binary(image_reference) do
    runner_service = ComposeDeployment.service!(deployment, :runner)

    with :ok <- restore_runner_selection(deployment, previous, opts),
         :ok <-
           compose(
             deployment,
             ["up", "--detach", "--wait", "--no-deps", "--force-recreate", runner_service],
             :runner_rollback,
             opts
           ),
         :ok <-
           verify_replacement_runner(
             deployment,
             maintenance_token,
             %{runner_release_id: previous.runner_release_id},
             opts
           ),
         :ok <- verify_active_manifest_alignment(deployment, recovery, opts) do
      :ok
    end
  end

  defp restore_previous_runner(_deployment, _previous, _recovery, _maintenance_token, _opts),
    do: {:error, :invalid_previous_runner}

  defp verify_active_manifest_alignment(deployment, recovery, opts) do
    expected = recovery["active_manifest"]

    with {:ok, actual} <- fetch_active_manifest_identity(deployment, opts) do
      if actual == expected,
        do: :ok,
        else: {:error, {:active_manifest_mismatch, expected, actual}}
    end
  end

  defp replacement_failure(reason, :ok), do: {:error, reason, :rollback_verified}

  defp replacement_failure(reason, {:error, rollback_reason}),
    do: {:error, reason, {:rollback_failed, rollback_reason}}

  defp restore_runner_selection(
         deployment,
         %{image_reference: image_reference, state: state},
         opts
       )
       when is_binary(image_reference) and is_map(state) do
    with :ok <- Favn.Dev.ComposeProject.put_runner_image(deployment, image_reference),
         :ok <- State.write_runner_latest(state, opts) do
      :ok
    end
  end

  defp restore_runner_selection(_deployment, _previous, _opts), do: :ok

  defp load_previous_runner(opts) do
    case State.read_runner_latest(opts) do
      {:ok, state} ->
        previous = %{
          runner_release_id: state["runner_release_id"],
          image_reference: state["image_reference"],
          state: state
        }

        case validate_previous_runner(%{
               "runner_release_id" => previous.runner_release_id,
               "image_reference" => previous.image_reference,
               "state" => state
             }) do
          {:ok, _release_id} -> {:ok, previous}
          {:error, _reason} -> {:error, :previous_runner_unavailable}
        end

      {:error, _reason} ->
        {:error, :previous_runner_unavailable}
    end
  end

  defp previous_runner_from_recovery(%{"previous_runner" => previous}) do
    %{
      runner_release_id: previous["runner_release_id"],
      image_reference: previous["image_reference"],
      state: previous["state"]
    }
  end

  defp compose(deployment, args, phase, opts) do
    {output, status} = Docker.compose(deployment, args, opts)

    if status == 0 do
      :ok
    else
      service_logs = failure_service_logs(deployment, phase, opts)
      service_health = failure_service_health(deployment, phase, opts)

      failure_output =
        [output, service_health, service_logs]
        |> Enum.map_join("\n", &bounded/1)
        |> OutputRedactor.redact(opts)

      _ = preserve_failure_logs(failure_output, opts)
      {:error, {:compose_command_failed, phase, status, failure_output}}
    end
  end

  defp preserve_failure_logs(output, opts) do
    root_dir = Favn.Dev.Paths.root_dir(opts)
    path = Favn.Dev.Paths.compose_failure_log_path(root_dir)
    output = OutputRedactor.redact(output, opts)

    with :ok <- File.mkdir_p(Path.dirname(path)),
         :ok <- File.write(path, output),
         :ok <- File.chmod(path, 0o600) do
      :ok
    end
  end

  defp failure_service_logs(deployment, phase, opts) do
    case failure_service(deployment, phase) do
      nil ->
        ""

      service ->
        {logs, _status} =
          Docker.compose(
            deployment,
            ["logs", "--tail", "200", "--no-color", service],
            Keyword.put(opts, :compose_command_timeout_ms, 30_000)
          )

        "\n#{service} logs:\n" <> logs
    end
  end

  defp failure_service_health(deployment, phase, opts) do
    with service when is_binary(service) <- failure_service(deployment, phase),
         {container, 0} <- Docker.compose(deployment, ["ps", "--quiet", service], opts),
         container when container != "" <- String.trim(container),
         {:ok, state} <- Docker.inspect_container_state(container, opts) do
      "#{service} state:\n" <> inspect(state, limit: 50, printable_limit: 4_096)
    else
      _unavailable -> ""
    end
  end

  defp failure_service(deployment, phase) do
    role =
      case phase do
        :postgres ->
          :postgres

        value when value in [:runner, :runner_drain, :runner_replacement, :runner_rollback] ->
          :runner

        :control_plane ->
          :control_plane

        {:release_operation, "verify-schema"} ->
          :control_plane_verify

        {:release_operation, _operation} ->
          :control_plane_ops

        _other ->
          nil
      end

    if role, do: Map.get(deployment.services, role), else: nil
  end

  defp ensure_startable_stack(deployment, opts) do
    {output, status} =
      Docker.compose(deployment, ["ps", "--all", "--format", "json"], opts)

    if status == 0 do
      case decode_compose_services(output) do
        {:ok, services} ->
          runtime_services = runtime_service_names(deployment)

          running =
            runtime_services
            |> Enum.filter(&match?(%{status: :running}, Map.get(services, &1)))
            |> MapSet.new()

          cond do
            MapSet.size(running) == 0 ->
              {:ok, running}

            MapSet.size(running) == length(runtime_services) ->
              {:error, :stack_already_running}

            true ->
              states =
                Map.new(@runtime_roles, fn role ->
                  service = ComposeDeployment.service!(deployment, role)

                  {role,
                   %{service: service, status: get_in(services, [service, :status]) || :stopped}}
                end)

              {:error, {:stack_partially_running, states}}
          end

        {:error, reason} ->
          {:error, {:invalid_compose_status, reason}}
      end
    else
      {:error, {:compose_status_unavailable, status, bounded(output)}}
    end
  end

  defp ensure_project_roles_startable(opts) do
    project_name = opts |> Paths.root_dir() |> Path.expand() |> ComposeProject.project_name()

    with {:ok, containers} <- Docker.project_role_containers(project_name, opts) do
      running = Enum.filter(containers, & &1.running?)
      running_roles = MapSet.new(running, & &1.role)
      runtime_roles = MapSet.new(["postgres", "runner", "control-plane"])

      cond do
        running == [] ->
          :ok

        MapSet.equal?(running_roles, runtime_roles) ->
          {:error, :stack_already_running}

        true ->
          states =
            Map.new(@runtime_roles, fn role ->
              label = role |> Atom.to_string() |> String.replace("_", "-")
              container = Enum.find(containers, &(&1.role == label))

              {role,
               %{
                 service: if(container, do: container.name, else: label),
                 status: if(container && container.running?, do: :running, else: :stopped)
               }}
            end)

          {:error, {:stack_partially_running, states}}
      end
    end
  end

  defp cleanup_failed_start(deployment, preexisting, opts) do
    newly_started =
      [:control_plane, :runner, :postgres]
      |> Enum.map(&ComposeDeployment.service!(deployment, &1))
      |> Enum.reject(&MapSet.member?(preexisting, &1))

    case newly_started do
      [] ->
        :ok

      services ->
        {_output, _status} =
          Docker.compose(deployment, ["stop", "--timeout", "30" | services], opts)

        :ok
    end
  end

  defp write_runtime(
         install,
         deployment,
         runner,
         runner_environment_identity,
         result,
         opts
       ) do
    runtime =
      %{
        "schema_version" => @runtime_schema_version,
        "kind" => "docker_compose",
        "compose_contract_version" => deployment.contract_version,
        "compose_profile" => profile_name(deployment.profile),
        "compose_file" => deployment.compose_file,
        "compose_project" => deployment.project_name,
        "compose_services" => ComposeDeployment.encoded_services(deployment),
        "workspace_id" => deployment.workspace_id,
        "view_url" => deployment.view_url,
        "orchestrator_url" => deployment.orchestrator_url,
        "control_plane_image_reference" => install["image_reference"],
        "runner_image_reference" => runner.image_reference,
        "runner_release_id" => result.runner_release_id,
        "runner_image_id" => result.runner_image_id,
        "runner_environment_identity" => runner_environment_identity,
        "active_manifest_version_id" => result.manifest_version_id,
        "started_at" => DateTime.utc_now() |> DateTime.to_iso8601()
      }

    with :ok <- State.write_compose_selection(runtime, opts) do
      State.write_runtime(runtime, opts)
    end
  end

  defp update_runtime_after_reload(
         compose_deployment,
         runner,
         runner_environment_identity,
         manifest_deployment,
         change,
         opts
       ) do
    runtime =
      case State.read_runtime(opts) do
        {:ok, state} ->
          state

        {:error, _reason} ->
          %{"schema_version" => @runtime_schema_version, "kind" => "docker_compose"}
      end

    updated =
      Map.merge(runtime, %{
        "compose_contract_version" => compose_deployment.contract_version,
        "compose_profile" => profile_name(compose_deployment.profile),
        "compose_file" => compose_deployment.compose_file,
        "compose_project" => compose_deployment.project_name,
        "compose_services" => ComposeDeployment.encoded_services(compose_deployment),
        "workspace_id" => compose_deployment.workspace_id,
        "view_url" => compose_deployment.view_url,
        "orchestrator_url" => compose_deployment.orchestrator_url,
        "control_plane_image_reference" => compose_deployment.control_plane_image,
        "runner_image_reference" => runner.image_reference,
        "runner_release_id" => runner.runner_release_id,
        "runner_image_id" => runner.image_id,
        "runner_environment_identity" => runner_environment_identity,
        "active_manifest_version_id" => manifest_deployment.published.manifest_version_id,
        "last_reload_class" => Atom.to_string(change),
        "reloaded_at" => DateTime.utc_now() |> DateTime.to_iso8601()
      })

    with :ok <- State.write_compose_selection(updated, opts) do
      State.write_runtime(updated, opts)
    end
  end

  defp start_result(compose_deployment, runner, manifest_deployment) do
    %{
      runner_release_id: runner.runner_release_id,
      manifest_version_id: manifest_deployment.published.manifest_version_id,
      runner_image_id: runner.image_id,
      view_url: compose_deployment.view_url,
      orchestrator_url: compose_deployment.orchestrator_url
    }
  end

  defp runtime_diagnostics(deployment, opts) do
    client = Keyword.get(opts, :orchestrator_client, OrchestratorClient)

    with {:ok, secrets} <- State.read_secrets(opts),
         token when is_binary(token) <- secrets["service_token"],
         {:ok, diagnostics} <-
           client.diagnostics(
             deployment.orchestrator_url,
             token,
             local_context(deployment)
           ) do
      diagnostics
    else
      _unavailable -> %{"status" => "unavailable"}
    end
  end

  defp runtime_status(services, deployment, opts) do
    service = ComposeDeployment.service!(deployment, :control_plane)

    case Map.get(services, service) do
      %{status: :running} -> runtime_diagnostics(deployment, opts)
      _not_running -> %{"status" => "unavailable"}
    end
  end

  defp local_context(deployment) do
    %{
      "actor_id" => "local-dev-cli",
      "session_id" => "local-dev-cli",
      "local_dev_context" => "trusted",
      "workspace_id" => deployment.workspace_id
    }
  end

  defp parse_compose_ps(output, 0) do
    case decode_compose_services(output) do
      {:ok, services} -> services
      {:error, _reason} -> %{}
    end
  end

  defp parse_compose_ps(_output, _status), do: %{}

  defp decode_compose_ps(output) do
    output = String.trim(output)

    case JSON.decode(output) do
      {:ok, decoded} ->
        {:ok, decoded}

      {:error, _reason} ->
        output
        |> String.split("\n", trim: true)
        |> Enum.reduce_while({:ok, []}, fn line, {:ok, services} ->
          case JSON.decode(line) do
            {:ok, service} when is_map(service) -> {:cont, {:ok, [service | services]}}
            _invalid -> {:halt, {:error, :invalid_compose_ps}}
          end
        end)
        |> case do
          {:ok, services} -> {:ok, Enum.reverse(services)}
          {:error, _reason} = error -> error
        end
    end
  end

  defp decode_compose_services(output) do
    with {:ok, decoded} <- decode_compose_ps(output) do
      decoded
      |> List.wrap()
      |> Enum.reduce_while({:ok, %{}}, fn
        service, {:ok, services} when is_map(service) ->
          name = service["Service"] || service["Name"]

          if is_binary(name) and name != "" do
            details = %{
              status: normalize_service_status(service["State"]),
              health: normalize_health(service["Health"]),
              image: service["Image"]
            }

            {:cont, {:ok, Map.put(services, name, details)}}
          else
            {:halt, {:error, :invalid_compose_service}}
          end

        _invalid, _acc ->
          {:halt, {:error, :invalid_compose_service}}
      end)
    end
  end

  defp normalize_service_status("running"), do: :running
  defp normalize_service_status("exited"), do: :stopped
  defp normalize_service_status("dead"), do: :dead
  defp normalize_service_status(nil), do: :unknown
  defp normalize_service_status(other), do: other

  defp normalize_health("healthy"), do: :healthy
  defp normalize_health("unhealthy"), do: :unhealthy
  defp normalize_health("starting"), do: :starting
  defp normalize_health(nil), do: :none
  defp normalize_health(other), do: other

  defp stack_status(_deployment, services, 0) when map_size(services) == 0, do: :stopped

  defp stack_status(deployment, services, 0) do
    states =
      deployment
      |> runtime_service_names()
      |> Enum.map(&get_in(services, [&1, :status]))

    cond do
      Enum.all?(states, &(&1 == :running)) -> :running
      Enum.any?(states, &(&1 == :running)) -> :partial
      true -> :stopped
    end
  end

  defp stack_status(_deployment, _services, _status), do: :unknown

  defp bounded_runner_state({:ok, state}) do
    Map.take(state, ["runner_release_id", "image_reference", "image_id", "manifest_version_id"])
  end

  defp bounded_runner_state({:error, _reason}), do: nil

  defp active_manifest(opts) do
    case State.read_runtime(opts) do
      {:ok, runtime} -> runtime["active_manifest_version_id"]
      {:error, _reason} -> nil
    end
  end

  defp last_failure(opts) do
    case State.read_last_failure(opts) do
      {:ok, failure} -> OutputRedactor.redact_term(failure, opts)
      {:error, _reason} -> nil
    end
  end

  defp selected_services(deployment, opts) do
    case Keyword.get(opts, :service, :all) do
      :postgres ->
        case Map.fetch(deployment.services, :postgres) do
          {:ok, service} -> {:ok, [service]}
          :error -> {:error, {:invalid_service, :postgres}}
        end

      :runner ->
        {:ok, [ComposeDeployment.service!(deployment, :runner)]}

      :control_plane ->
        {:ok, [ComposeDeployment.service!(deployment, :control_plane)]}

      :all ->
        {:ok, runtime_service_names(deployment)}

      unsupported ->
        {:error, {:invalid_service, unsupported}}
    end
  end

  defp log_tail(opts) do
    case Keyword.get(opts, :tail, @default_log_tail) do
      value when is_integer(value) and value in 1..10_000 -> value
      _invalid -> @default_log_tail
    end
  end

  defp reload_message(:manifest_only, runner, deployment),
    do:
      "Manifest-only reload complete: #{deployment.published.manifest_version_id}; runner #{runner.runner_release_id} unchanged"

  defp reload_message(:runner_image_reused, runner, deployment),
    do:
      "Runner image reused: #{runner.runner_release_id}; manifest #{deployment.published.manifest_version_id} remains aligned"

  defp reload_message(:runner_rebuild, runner, deployment),
    do:
      "Runner reload complete: #{runner.runner_release_id}; activated #{deployment.published.manifest_version_id}"

  defp reload_message(:runner_environment, runner, deployment),
    do:
      "Runner environment reload complete: #{runner.runner_release_id}; activated #{deployment.published.manifest_version_id}"

  defp role_service_statuses(deployment, rendered_services) do
    Map.new(@runtime_roles, fn role ->
      service = ComposeDeployment.service!(deployment, role)

      details =
        Map.get(rendered_services, service, %{status: :stopped, health: :none, image: nil})

      {role, Map.put(details, :service, service)}
    end)
  end

  defp runtime_service_names(deployment) do
    Enum.map(@runtime_roles, &ComposeDeployment.service!(deployment, &1))
  end

  defp maybe_stop_postgres(_deployment, nil, _opts), do: :ok

  defp maybe_stop_postgres(deployment, postgres, opts),
    do: compose(deployment, ["stop", "--timeout", "30", postgres], :postgres, opts)

  defp validate_recorded_compose_file(deployment, opts) do
    case Config.resolve_compose_file(Keyword.put(opts, :compose_file, deployment.compose_file)) do
      {:ok, path} when path == deployment.compose_file -> :ok
      {:ok, _different} -> {:error, :deployment_changed_during_reload}
      {:error, _reason} = error -> error
    end
  end

  defp unchanged_deployment(previous, current) do
    if previous.compose_file == current.compose_file and
         previous.project_name == current.project_name and
         previous.contract_version == current.contract_version and
         previous.profile == current.profile and
         previous.services == current.services and
         previous.control_plane_image == current.control_plane_image do
      :ok
    else
      {:error, :deployment_changed_during_reload}
    end
  end

  defp remove_obsolete_generated_compose(deployment, opts) do
    path = opts |> Favn.Dev.Paths.root_dir() |> Favn.Dev.Paths.compose_path()

    if deployment.compose_file == Path.expand(path) do
      :ok
    else
      case File.rm(path) do
        :ok -> :ok
        {:error, :enoent} -> :ok
        {:error, reason} -> {:error, {:obsolete_compose_cleanup_failed, path, reason}}
      end
    end
  end

  defp profile_name(:local), do: "local"
  defp profile_name(:single_host), do: "single-host"

  defp record_failure(command, reason, opts) do
    State.write_last_failure(
      %{
        "command" => command,
        "error" => reason |> OutputRedactor.redact_term(opts) |> bounded(),
        "at" => DateTime.utc_now() |> DateTime.to_iso8601()
      },
      opts
    )
  end

  defp progress(opts, message), do: Keyword.get(opts, :progress_fun, &IO.puts/1).(message)

  defp bounded(value) when is_binary(value),
    do: value |> String.trim() |> String.slice(-8_192, 8_192)

  defp bounded(value), do: inspect(value, limit: 50, printable_limit: 4_096)
end
