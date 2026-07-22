defmodule Favn.Dev.ComposeLifecycle do
  @moduledoc """
  Owns the project-scoped Docker Compose development lifecycle.

  All container operations use the immutable control-plane image selected by
  install state and the customer runner image selected by its runner release
  ID. Commands never place credentials in process arguments.
  """

  alias Favn.Dev.{
    Activate,
    ComposeProject,
    Config,
    Docker,
    Install,
    LocalHttpClient,
    Lock,
    OrchestratorClient,
    OutputRedactor,
    Publish,
    RunnerImage,
    State
  }

  @runtime_schema_version 5
  @maintenance_schema_version 2
  @maintenance_token ~r/\A[A-Za-z0-9_-]{43}\z/
  @runner_release_id ~r/\Arr_[0-9a-f]{64}\z/
  @default_ready_timeout_ms 120_000
  @default_log_tail 100
  @services ["postgres", "runner", "control-plane"]

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
    with {:ok, install, project} <- installed_project(opts),
         {:ok, preexisting} <- ensure_startable_stack(project, opts) do
      case do_start(install, project, opts) do
        {:ok, _result} = success ->
          success

        {:error, reason} = error ->
          _ = cleanup_failed_start(project, preexisting, opts)
          _ = record_failure("dev", reason, opts)
          error
      end
    else
      {:error, reason} = error ->
        _ = record_failure("dev", reason, opts)
        error
    end
  end

  defp do_start(install, project, opts) do
    with :ok <- State.clear_maintenance(opts),
         :ok <- put_runtime_configuration(project, opts),
         {:ok, runner} <- RunnerImage.ensure(project, opts),
         :ok <- compose(project, ["up", "--detach", "--wait", "postgres"], :postgres, opts),
         :ok <- release_operations(project, opts),
         :ok <-
           compose(project, ["up", "--detach", "--wait", "--no-deps", "runner"], :runner, opts),
         :ok <-
           compose(
             project,
             ["up", "--detach", "--wait", "--no-deps", "control-plane"],
             :control_plane,
             opts
           ),
         :ok <- await_liveness(project, opts),
         {:ok, deployment} <- deploy_manifest(project, runner, opts),
         :ok <- await_readiness(project, opts),
         result <- start_result(project, runner, deployment),
         :ok <- write_runtime(install, project, result, opts) do
      {:ok, result}
    end
  end

  @doc "Starts the local stack, streams Compose logs, and stops it on exit."
  @spec start_foreground(keyword()) :: :ok | {:error, term()}
  def start_foreground(opts \\ []) when is_list(opts) do
    with {:ok, result} <- start(opts) do
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
  end

  @doc "Rebuilds an aligned release and applies the canonical local change class."
  @spec reload(keyword()) :: :ok | {:error, term()}
  def reload(opts \\ []) when is_list(opts) do
    Lock.with_lock(opts, fn -> reload_locked(opts) end)
  end

  defp reload_locked(opts) do
    with {:ok, _install, project} <- installed_project(opts),
         :ok <- put_runtime_configuration(project, opts),
         :ok <- reload_preflight(project, Keyword.put_new(opts, :ready_timeout_ms, 5_000)),
         {:ok, recovery} <- ensure_reload_recovery(project, opts),
         {:ok, runner} <- RunnerImage.ensure(project, opts),
         {:ok, change, deployment} <- apply_runner_change(project, recovery, runner, opts),
         :ok <- update_runtime_after_reload(runner, deployment, change, opts) do
      progress(opts, reload_message(change, runner, deployment))
      :ok
    else
      {:error, reason} = error ->
        _ = record_failure("reload", reason, opts)
        error
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
    case project_from_state(opts) do
      {:ok, project} ->
        with :ok <-
               compose(
                 project,
                 ["stop", "--timeout", "180", "control-plane"],
                 :control_plane,
                 opts
               ),
             :ok <- compose(project, ["stop", "--timeout", "180", "runner"], :runner, opts),
             :ok <- compose(project, ["stop", "--timeout", "30", "postgres"], :postgres, opts),
             :ok <- State.clear_runtime(opts),
             :ok <- State.clear_maintenance(opts) do
          :ok
        end

      {:error, :install_required} ->
        :ok

      {:error, _reason} = error ->
        error
    end
  end

  @doc "Returns bounded Compose and release status without reading secret values."
  @spec status(keyword()) :: map()
  def status(opts \\ []) when is_list(opts) do
    result =
      case project_from_state(opts) do
        {:ok, project} ->
          {output, command_status} = Docker.compose(project, ["ps", "--format", "json"], opts)
          services = parse_compose_ps(output, command_status)
          runtime = runtime_status(services, project, opts)

          %{
            stack_status: stack_status(services, command_status),
            storage: :postgres,
            services: services,
            runner: bounded_runner_state(State.read_runner_latest(opts)),
            active_manifest_version_id: active_manifest(opts),
            user_urls: %{
              web: project["view_url"],
              orchestrator_api: project["orchestrator_url"]
            },
            compose_project: project["project_name"],
            runtime: runtime,
            last_failure: last_failure(opts)
          }

        {:error, reason} ->
          %{
            stack_status: if(reason == :install_required, do: :not_installed, else: :unknown),
            storage: :postgres,
            services: %{},
            runner: nil,
            active_manifest_version_id: active_manifest(opts),
            user_urls: %{},
            compose_project: nil,
            runtime: %{"status" => "unavailable"},
            last_failure: last_failure(opts),
            error: reason
          }
      end

    OutputRedactor.redact_term(result, opts)
  end

  @doc "Reads bounded, prefixed service logs through Docker Compose."
  @spec logs(keyword()) :: :ok | {:error, term()}
  def logs(opts \\ []) when is_list(opts) do
    with {:ok, project} <- project_from_state(opts) do
      args = ["logs", "--tail", Integer.to_string(log_tail(opts)), "--no-color"]
      args = if Keyword.get(opts, :follow, false), do: args ++ ["--follow"], else: args
      args = args ++ selected_services(opts)

      {output, status} =
        Docker.compose(
          project,
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
         {:ok, install, _project} <- installed_project(opts) do
      compose_status = status(opts)

      report = %{
        "status" => "ok",
        "docker" => probe,
        "control_plane" => %{
          "image_reference" => install["image_reference"],
          "image_id" => install["image_id"],
          "build_id" => install["control_plane_build_id"]
        },
        "compose" => compose_status,
        "runtime" => compose_status.runtime
      }

      {:ok, OutputRedactor.redact_term(report, opts)}
    end
  end

  defp installed_project(opts) do
    with :ok <- Install.ensure_ready(opts),
         {:ok, install} <- State.read_install(opts),
         %{} = project <- install["compose"] do
      {:ok, install, project}
    else
      {:error, _reason} = error -> error
      _invalid -> {:error, :install_stale}
    end
  end

  defp project_from_state(opts) do
    case State.read_install(opts) do
      {:ok, %{"compose" => %{} = project}} -> {:ok, project}
      {:error, :not_found} -> {:error, :install_required}
      _invalid -> {:error, :install_stale}
    end
  end

  defp release_operations(project, opts) do
    Enum.reduce_while(
      ["migrate", "grant-runtime", "verify-schema", "provision-workspace"],
      :ok,
      fn operation, :ok ->
        service =
          if operation == "verify-schema", do: "control-plane-verify", else: "control-plane-ops"

        case compose(
               project,
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

  defp deploy_manifest(project, runner, opts) do
    case Keyword.get(opts, :deploy_fun) do
      fun when is_function(fun, 3) ->
        if Mix.env() == :test,
          do: fun.(project, runner, opts),
          else: {:error, :deployment_injection_not_allowed}

      _other ->
        do_deploy_manifest(project, runner, opts)
    end
  end

  defp do_deploy_manifest(project, runner, opts) do
    with {:ok, secrets} <- State.read_secrets(opts),
         token when is_binary(token) <- secrets["service_token"],
         manifest_path <- Path.join(runner.manifest_dir, "manifest-index.json"),
         {:ok, published} <-
           Publish.run(
             manifest_path: manifest_path,
             orchestrator_url: project["orchestrator_url"],
             env: %{"FAVN_ORCHESTRATOR_SERVICE_TOKEN" => token},
             client: Keyword.get(opts, :orchestrator_client, OrchestratorClient),
             maintenance_token: Keyword.get(opts, :maintenance_token)
           ),
         {:ok, activated} <-
           Activate.run(
             orchestrator_url: project["orchestrator_url"],
             manifest_version_id: published.manifest_version_id,
             workspace_id: project["workspace_id"],
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

  defp await_readiness(project, opts) do
    timeout_ms = Keyword.get(opts, :ready_timeout_ms, @default_ready_timeout_ms)
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    readiness_fun = Keyword.get(opts, :readiness_fun, &readiness/1)
    do_await_readiness(project["orchestrator_url"], readiness_fun, deadline, opts)
  end

  defp reload_preflight(project, opts) do
    case State.read_maintenance(opts) do
      {:ok, maintenance} ->
        with {:ok, recovery} <- validate_maintenance(maintenance) do
          case recovery["phase"] do
            "preparing" -> await_readiness(project, opts)
            "active" -> await_liveness(project, opts)
          end
        end

      {:error, :not_found} ->
        await_readiness(project, opts)

      _invalid ->
        {:error, :invalid_local_maintenance_state}
    end
  end

  defp await_liveness(project, opts) do
    timeout_ms = Keyword.get(opts, :ready_timeout_ms, @default_ready_timeout_ms)
    deadline = System.monotonic_time(:millisecond) + timeout_ms

    liveness_fun =
      Keyword.get(
        opts,
        :liveness_fun,
        Keyword.get(opts, :readiness_fun, &liveness/1)
      )

    do_await_liveness(project["orchestrator_url"], liveness_fun, deadline, opts)
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

  defp apply_runner_change(
         project,
         %{
           "phase" => "preparing",
           "previous_runner" => %{"runner_release_id" => release_id}
         },
         %{runner_release_id: release_id} = runner,
         opts
       ) do
    with {:ok, deployment} <- deploy_manifest(project, runner, opts),
         :ok <- await_readiness(project, opts),
         :ok <- State.clear_maintenance(opts) do
      {:ok, :manifest_only, deployment}
    end
  end

  defp apply_runner_change(project, recovery, runner, opts) do
    coordinated_runner_replacement(project, recovery, runner, opts)
  end

  defp coordinated_runner_replacement(project, recovery, runner, opts) do
    previous = previous_runner_from_recovery(recovery)

    with {:ok, active_recovery} <- begin_runner_replacement(project, recovery, opts) do
      maintenance_token = active_recovery["token"]

      case safely_replace_runner(
             project,
             previous,
             runner,
             active_recovery,
             maintenance_token,
             opts
           ) do
        {:ok, deployment} ->
          with :ok <- finish_runner_replacement(project, maintenance_token, opts),
               :ok <- await_readiness(project, opts) do
            {:ok, :runner_replacement, deployment}
          end

        {:error, reason, :rollback_verified} ->
          case finish_runner_replacement(project, maintenance_token, opts) do
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
        _ = restore_runner_selection(project, previous, opts)
        error
    end
  end

  defp safely_replace_runner(
         project,
         previous,
         runner,
         recovery,
         maintenance_token,
         opts
       ) do
    replace_runner(project, previous, runner, recovery, maintenance_token, opts)
  rescue
    exception ->
      replacement_failure(
        {:runner_replacement_exception, Exception.message(exception)},
        restore_previous_runner(project, previous, recovery, maintenance_token, opts)
      )
  catch
    kind, reason ->
      replacement_failure(
        {:runner_replacement_caught, kind, reason},
        restore_previous_runner(project, previous, recovery, maintenance_token, opts)
      )
  end

  defp replace_runner(project, previous, runner, recovery, maintenance_token, opts) do
    case await_runner_drain(project, opts) do
      :ok ->
        do_replace_runner(project, previous, runner, recovery, maintenance_token, opts)

      {:error, reason} ->
        replacement_failure(
          reason,
          verify_unchanged_previous_runner(
            project,
            previous,
            recovery,
            maintenance_token,
            opts
          )
        )
    end
  end

  defp do_replace_runner(project, previous, runner, recovery, maintenance_token, opts) do
    replacement =
      with :ok <-
             compose(project, ["stop", "--timeout", "180", "runner"], :runner_drain, opts),
           :ok <-
             compose(
               project,
               ["up", "--detach", "--wait", "--no-deps", "--force-recreate", "runner"],
               :runner_replacement,
               opts
             ),
           :ok <- verify_replacement_runner(project, maintenance_token, runner, opts),
           {:ok, deployment} <-
             deploy_manifest(
               project,
               runner,
               Keyword.put(opts, :maintenance_token, maintenance_token)
             ) do
        {:ok, deployment}
      end

    case replacement do
      {:ok, _deployment} = success ->
        success

      {:error, reason} ->
        rollback = restore_previous_runner(project, previous, recovery, maintenance_token, opts)
        _ = record_failure("reload_runner", {runner.runner_release_id, reason}, opts)
        replacement_failure(reason, rollback)
    end
  end

  defp await_runner_drain(project, opts) do
    timeout_ms = Keyword.get(opts, :runner_drain_timeout_ms, 120_000)
    deadline = System.monotonic_time(:millisecond) + max(timeout_ms, 0)
    do_await_runner_drain(project, deadline, opts)
  end

  defp do_await_runner_drain(project, deadline, opts) do
    with {:ok, status} <- runner_replacement_status(project, opts),
         true <- maintenance_drained?(status),
         :ok <- ensure_no_in_flight_runs(project, opts) do
      :ok
    else
      false ->
        retry_runner_drain(project, deadline, {:active_admissions, :present}, opts)

      {:error, {:in_flight_runs, _run_ids} = reason} ->
        retry_runner_drain(project, deadline, reason, opts)

      {:error, _reason} = error ->
        error
    end
  end

  defp retry_runner_drain(project, deadline, reason, opts) do
    if System.monotonic_time(:millisecond) >= deadline do
      {:error, reason}
    else
      Process.sleep(Keyword.get(opts, :runner_drain_poll_interval_ms, 250))
      do_await_runner_drain(project, deadline, opts)
    end
  end

  defp ensure_no_in_flight_runs(project, opts) do
    case Keyword.get(opts, :in_flight_fun) do
      fun when is_function(fun, 1) ->
        if Mix.env() == :test,
          do: ensure_empty_run_ids(fun.(project)),
          else: {:error, :in_flight_injection_not_allowed}

      _other ->
        fetch_in_flight_runs(project, opts)
    end
  end

  defp fetch_in_flight_runs(project, opts) do
    client = Keyword.get(opts, :orchestrator_client, OrchestratorClient)

    with {:ok, secrets} <- State.read_secrets(opts),
         token when is_binary(token) <- secrets["service_token"],
         {:ok, run_ids} <-
           client.in_flight_runs(
             project["orchestrator_url"],
             token,
             local_context(project)
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

  defp begin_runner_replacement(project, recovery, opts) do
    with {:ok, active_recovery} <- mark_recovery_active(recovery, opts),
         maintenance_token <- active_recovery["token"],
         {:ok, service_token} <- service_token(opts),
         {:ok, ^maintenance_token} <-
           client(opts).begin_runner_replacement(
             project["orchestrator_url"],
             service_token,
             maintenance_token
           ) do
      {:ok, active_recovery}
    else
      {:ok, _different_token} -> {:error, :maintenance_token_mismatch}
      {:error, _reason} = error -> error
    end
  end

  defp runner_replacement_status(project, opts) do
    with {:ok, token} <- service_token(opts) do
      client(opts).runner_replacement_status(project["orchestrator_url"], token)
    end
  end

  defp finish_runner_replacement(project, maintenance_token, opts) do
    with {:ok, token} <- service_token(opts) do
      with :ok <-
             client(opts).finish_runner_replacement(
               project["orchestrator_url"],
               token,
               maintenance_token
             ),
           :ok <- State.clear_maintenance(opts) do
        :ok
      end
    end
  end

  defp verify_replacement_runner(project, maintenance_token, runner, opts) do
    with {:ok, token} <- service_token(opts),
         {:ok, _verified} <-
           client(opts).verify_replacement_runner(
             project["orchestrator_url"],
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

  defp ensure_reload_recovery(project, opts) do
    case State.read_maintenance(opts) do
      {:ok, maintenance} ->
        validate_maintenance(maintenance)

      {:error, :not_found} ->
        with {:ok, previous} <- load_previous_runner(opts),
             {:ok, active_manifest} <- active_manifest_identity(project, previous, opts),
             recovery <- new_recovery(previous, active_manifest),
             :ok <- State.write_maintenance(recovery, opts) do
          {:ok, recovery}
        end

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

  defp active_manifest_identity(project, previous, opts) do
    with {:ok, identity} <- fetch_active_manifest_identity(project, opts),
         true <- identity["required_runner_release_id"] == previous.runner_release_id do
      {:ok, identity}
    else
      false -> {:error, :active_manifest_runner_mismatch}
      {:error, _reason} = error -> error
    end
  end

  defp fetch_active_manifest_identity(project, opts) do
    result =
      case Keyword.get(opts, :active_manifest_fun) do
        fun when is_function(fun, 1) ->
          if Mix.env() == :test,
            do: fun.(project),
            else: {:error, :active_manifest_injection_not_allowed}

        _other ->
          with {:ok, token} <- service_token(opts) do
            client(opts).bootstrap_active_manifest(
              project["orchestrator_url"],
              token,
              local_context(project)
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

  defp verify_unchanged_previous_runner(_project, nil, _recovery, _maintenance_token, _opts),
    do: {:error, :previous_runner_unavailable}

  defp verify_unchanged_previous_runner(
         project,
         previous,
         recovery,
         maintenance_token,
         opts
       ) do
    with :ok <- restore_runner_selection(project, previous, opts),
         :ok <-
           verify_replacement_runner(
             project,
             maintenance_token,
             %{runner_release_id: previous.runner_release_id},
             opts
           ),
         :ok <- verify_active_manifest_alignment(project, recovery, opts) do
      :ok
    end
  end

  defp restore_previous_runner(_project, nil, _recovery, _maintenance_token, _opts),
    do: {:error, :previous_runner_unavailable}

  defp restore_previous_runner(
         project,
         %{image_reference: image_reference} = previous,
         recovery,
         maintenance_token,
         opts
       )
       when is_binary(image_reference) do
    with :ok <- restore_runner_selection(project, previous, opts),
         :ok <-
           compose(
             project,
             ["up", "--detach", "--wait", "--no-deps", "--force-recreate", "runner"],
             :runner_rollback,
             opts
           ),
         :ok <-
           verify_replacement_runner(
             project,
             maintenance_token,
             %{runner_release_id: previous.runner_release_id},
             opts
           ),
         :ok <- verify_active_manifest_alignment(project, recovery, opts) do
      :ok
    end
  end

  defp restore_previous_runner(_project, _previous, _recovery, _maintenance_token, _opts),
    do: {:error, :invalid_previous_runner}

  defp verify_active_manifest_alignment(project, recovery, opts) do
    expected = recovery["active_manifest"]

    with {:ok, actual} <- fetch_active_manifest_identity(project, opts) do
      if actual == expected,
        do: :ok,
        else: {:error, {:active_manifest_mismatch, expected, actual}}
    end
  end

  defp replacement_failure(reason, :ok), do: {:error, reason, :rollback_verified}

  defp replacement_failure(reason, {:error, rollback_reason}),
    do: {:error, reason, {:rollback_failed, rollback_reason}}

  defp restore_runner_selection(project, %{image_reference: image_reference, state: state}, opts)
       when is_binary(image_reference) and is_map(state) do
    with :ok <- Favn.Dev.ComposeProject.put_runner_image(project, image_reference),
         :ok <- State.write_runner_latest(state, opts) do
      :ok
    end
  end

  defp restore_runner_selection(_project, _previous, _opts), do: :ok

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

  defp compose(project, args, phase, opts) do
    {output, status} = Docker.compose(project, args, opts)

    if status == 0 do
      :ok
    else
      service_logs = failure_service_logs(project, phase, opts)
      service_health = failure_service_health(project, phase, opts)

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

  defp failure_service_logs(project, phase, opts) do
    case failure_service(phase) do
      nil ->
        ""

      service ->
        {logs, _status} =
          Docker.compose(
            project,
            ["logs", "--tail", "200", "--no-color", service],
            Keyword.put(opts, :compose_command_timeout_ms, 30_000)
          )

        "\n#{service} logs:\n" <> logs
    end
  end

  defp failure_service_health(project, phase, opts) do
    with service when is_binary(service) <- failure_service(phase),
         {container, 0} <- Docker.compose(project, ["ps", "--quiet", service], opts),
         container when container != "" <- String.trim(container),
         {:ok, state} <- Docker.inspect_container_state(container, opts) do
      "#{service} state:\n" <> inspect(state, limit: 50, printable_limit: 4_096)
    else
      _unavailable -> ""
    end
  end

  defp failure_service(:postgres), do: "postgres"
  defp failure_service(:runner), do: "runner"
  defp failure_service(:runner_drain), do: "runner"
  defp failure_service(:runner_replacement), do: "runner"
  defp failure_service(:runner_rollback), do: "runner"
  defp failure_service(:control_plane), do: "control-plane"
  defp failure_service({:release_operation, "verify-schema"}), do: "control-plane-verify"
  defp failure_service({:release_operation, _operation}), do: "control-plane-ops"
  defp failure_service(_phase), do: nil

  defp ensure_startable_stack(project, opts) do
    {output, status} = Docker.compose(project, ["ps", "--all", "--format", "json"], opts)

    if status == 0 do
      case decode_compose_services(output) do
        {:ok, services} ->
          running =
            @services
            |> Enum.filter(&match?(%{status: :running}, Map.get(services, &1)))
            |> MapSet.new()

          cond do
            MapSet.size(running) == 0 ->
              {:ok, running}

            MapSet.size(running) == length(@services) ->
              {:error, :stack_already_running}

            true ->
              states =
                Map.new(@services, fn service ->
                  {service, get_in(services, [service, :status]) || :stopped}
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

  defp cleanup_failed_start(project, preexisting, opts) do
    newly_started =
      ["control-plane", "runner", "postgres"]
      |> Enum.reject(&MapSet.member?(preexisting, &1))

    case newly_started do
      [] ->
        :ok

      services ->
        {_output, _status} =
          Docker.compose(project, ["stop", "--timeout", "30" | services], opts)

        :ok
    end
  end

  defp write_runtime(install, project, result, opts) do
    State.write_runtime(
      %{
        "schema_version" => @runtime_schema_version,
        "kind" => "docker_compose",
        "compose_project" => project["project_name"],
        "control_plane_image_reference" => install["image_reference"],
        "runner_release_id" => result.runner_release_id,
        "runner_image_id" => result.runner_image_id,
        "active_manifest_version_id" => result.manifest_version_id,
        "started_at" => DateTime.utc_now() |> DateTime.to_iso8601()
      },
      opts
    )
  end

  defp update_runtime_after_reload(runner, deployment, change, opts) do
    runtime =
      case State.read_runtime(opts) do
        {:ok, state} ->
          state

        {:error, _reason} ->
          %{"schema_version" => @runtime_schema_version, "kind" => "docker_compose"}
      end

    State.write_runtime(
      Map.merge(runtime, %{
        "runner_release_id" => runner.runner_release_id,
        "runner_image_id" => runner.image_id,
        "active_manifest_version_id" => deployment.published.manifest_version_id,
        "last_reload_class" => Atom.to_string(change),
        "reloaded_at" => DateTime.utc_now() |> DateTime.to_iso8601()
      }),
      opts
    )
  end

  defp start_result(project, runner, deployment) do
    %{
      runner_release_id: runner.runner_release_id,
      manifest_version_id: deployment.published.manifest_version_id,
      runner_image_id: runner.image_id,
      view_url: project["view_url"],
      orchestrator_url: project["orchestrator_url"]
    }
  end

  defp runtime_diagnostics(project, opts) do
    client = Keyword.get(opts, :orchestrator_client, OrchestratorClient)

    with {:ok, secrets} <- State.read_secrets(opts),
         token when is_binary(token) <- secrets["service_token"],
         {:ok, diagnostics} <-
           client.diagnostics(project["orchestrator_url"], token, local_context(project)) do
      diagnostics
    else
      _unavailable -> %{"status" => "unavailable"}
    end
  end

  defp runtime_status(services, project, opts) do
    case Map.get(services, "control-plane") do
      %{status: :running} -> runtime_diagnostics(project, opts)
      _not_running -> %{"status" => "unavailable"}
    end
  end

  defp local_context(project) do
    %{
      "actor_id" => "local-dev-cli",
      "session_id" => "local-dev-cli",
      "local_dev_context" => "trusted",
      "workspace_id" => project["workspace_id"]
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

  defp stack_status(services, 0) when map_size(services) == 0, do: :stopped

  defp stack_status(services, 0) do
    states = Enum.map(@services, &get_in(services, [&1, :status]))

    cond do
      Enum.all?(states, &(&1 == :running)) -> :running
      Enum.any?(states, &(&1 == :running)) -> :partial
      true -> :stopped
    end
  end

  defp stack_status(_services, _status), do: :unknown

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

  defp selected_services(opts) do
    case Keyword.get(opts, :service, :all) do
      :postgres ->
        ["postgres"]

      :runner ->
        ["runner"]

      service when service in [:operator, :web, :orchestrator, :control_plane] ->
        ["control-plane"]

      _other ->
        @services
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

  defp reload_message(:runner_replacement, runner, deployment),
    do:
      "Runner reload complete: #{runner.runner_release_id}; activated #{deployment.published.manifest_version_id}"

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
