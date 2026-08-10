defmodule FavnStoragePostgres.Bootstrap.Connection do
  @moduledoc false

  defmodule Probe do
    @moduledoc false
    @behaviour Postgrex.SimpleConnection

    @impl true
    def init(state), do: {:ok, state}

    @impl true
    def notify(_channel, _payload, _state), do: :ok
  end

  alias FavnStoragePostgres.Authentication
  alias FavnStoragePostgres.Bootstrap.{Config, Profile, WorkflowRunner}
  alias FavnStoragePostgres.Config, as: DatabaseConfig
  alias FavnStoragePostgres.Repo

  @type error_code ::
          :authentication_rejected
          | :authentication_unavailable
          | :database_access_rejected
          | :database_missing
          | :dependency_unavailable
          | :invalid_database_configuration
          | :server_unreachable
          | :unknown_outcome

  @spec with_raw(Config.t(), Profile.t(), String.t(), atom(), (pid() -> result)) ::
          result | {:error, error_code()}
        when result: term()
  def with_raw(config, profile, database, lifecycle, function)
      when is_function(function, 1) do
    WorkflowRunner.track_stage(connection_stage(lifecycle), fn ->
      do_with_raw(config, profile, database, lifecycle, function)
    end)
  end

  defp do_with_raw(config, profile, database, lifecycle, function) do
    with {:ok, connection_config} <- connection_config(config, profile, database, lifecycle),
         :ok <- ensure_dependencies(connection_config.authentication),
         {:ok, authentication_state} <- start_authentication(connection_config.authentication) do
      try do
        with :ok <- authentication_ready(connection_config.authentication),
             {:ok, connection, physical_connection} <-
               start_connection(connection_config, profile) do
          try do
            run_while_alive(
              physical_connection,
              connection_config.authentication,
              resource_loss_mode(config.operation),
              fn -> function.(connection) end
            )
          after
            stop_process(connection)
          end
        end
      after
        stop_authentication(authentication_state)
      end
    end
  end

  @spec with_repo(Config.t(), Profile.t(), String.t(), atom(), (module() -> result)) ::
          result | {:error, error_code()}
        when result: term()
  def with_repo(config, profile, database, lifecycle, function)
      when is_function(function, 1) do
    WorkflowRunner.track_stage(connection_stage(lifecycle), fn ->
      do_with_repo(config, profile, database, lifecycle, function)
    end)
  end

  defp do_with_repo(config, profile, database, lifecycle, function) do
    with {:ok, connection_config} <- connection_config(config, profile, database, lifecycle),
         :ok <- ensure_dependencies(connection_config.authentication),
         {:ok, authentication_state} <- start_authentication(connection_config.authentication) do
      try do
        with :ok <- authentication_ready(connection_config.authentication),
             :ok <-
               probe_connection(
                 connection_config.notification_options,
                 connection_config.authentication
               ),
             {:ok, repo} <-
               start_repo(connection_config.repo_options, connection_config.authentication) do
          try do
            run_while_alive(
              repo,
              connection_config.authentication,
              resource_loss_mode(config.operation),
              fn ->
                case verify_repo_connection(connection_config.authentication) do
                  :ok -> function.(Repo)
                  {:error, code} -> {:error, code}
                end
              end
            )
          after
            stop_process(repo)
          end
        end
      after
        stop_authentication(authentication_state)
      end
    end
  end

  defp connection_stage(:bootstrap_maintenance), do: :bootstrap_connection
  defp connection_stage(:bootstrap_target), do: :bootstrap_target
  defp connection_stage(:migrator_lock), do: :migrator_connection
  defp connection_stage(:migrator_operation), do: :migrator_connection
  defp connection_stage(:runtime_operation), do: :runtime_connection
  defp connection_stage(lifecycle), do: lifecycle

  defp connection_config(config, profile, database, lifecycle) do
    env = Config.connection_env(config, profile, database, lifecycle)

    case DatabaseConfig.connection_config_from_env(env) do
      {:ok, connection_config} -> {:ok, connection_config}
      {:error, _reason} -> {:error, :invalid_database_configuration}
    end
  end

  defp ensure_dependencies(authentication) do
    with {:ok, _started} <- Application.ensure_all_started(:ecto_sql),
         {:ok, _started} <- Application.ensure_all_started(:postgrex),
         {:ok, applications} <- Authentication.applications(authentication) do
      Enum.reduce_while(applications, :ok, fn application, :ok ->
        case Application.ensure_all_started(application) do
          {:ok, _started} -> {:cont, :ok}
          {:error, _reason} -> {:halt, {:error, :dependency_unavailable}}
        end
      end)
    else
      {:error, _reason} -> {:error, :dependency_unavailable}
    end
  end

  defp start_authentication(authentication) do
    with {:ok, child_specs} <- Authentication.child_specs(authentication) do
      case child_specs do
        [] ->
          {:ok, nil}

        specs ->
          case Supervisor.start_link(specs, strategy: :one_for_one) do
            {:ok, pid} -> {:ok, pid}
            {:error, _reason} -> {:error, :authentication_unavailable}
          end
      end
    else
      {:error, _reason} -> {:error, :authentication_unavailable}
    end
  end

  defp authentication_ready(:password), do: :ok

  defp authentication_ready(authentication) do
    case Authentication.status(authentication) do
      %{lifecycle_ready?: true} -> :ok
      _status -> {:error, :authentication_unavailable}
    end
  end

  defp start_connection(connection_config, profile) do
    options =
      connection_config.notification_options
      |> Keyword.drop([:auto_reconnect, :sync_connect])
      |> Keyword.put(:backoff_type, :stop)

    with :ok <- probe_connection(options, connection_config.authentication),
         {:ok, connection, physical_connection} <-
           start_postgrex(options, connection_config.authentication) do
      case run_after_connect(
             connection,
             physical_connection,
             profile,
             connection_config.authentication
           ) do
        :ok ->
          {:ok, connection, physical_connection}

        {:error, code} ->
          stop_process(connection)
          {:error, code}
      end
    end
  end

  defp probe_connection(options, authentication) do
    previous_trap_exit? = Process.flag(:trap_exit, true)

    try do
      probe_options =
        options
        |> Keyword.put(:auto_reconnect, false)
        |> Keyword.put(:sync_connect, true)

      case Postgrex.SimpleConnection.start_link(Probe, :ok, probe_options) do
        {:ok, probe} ->
          Process.unlink(probe)
          GenServer.stop(probe)
          :ok

        {:error, reason} ->
          classify_connection_error(reason, authentication)
      end
    after
      Process.flag(:trap_exit, previous_trap_exit?)
    end
  end

  defp start_postgrex(options, authentication) do
    listener_tag = make_ref()

    options =
      options
      |> Keyword.put(:pool_size, 1)
      |> Keyword.put(:connection_listeners, {[self()], listener_tag})

    case Postgrex.start_link(options) do
      {:ok, connection} ->
        Process.unlink(connection)

        case await_physical_connection(
               connection,
               listener_tag,
               authentication,
               Keyword.get(options, :timeout, 15_000)
             ) do
          {:ok, physical_connection} ->
            {:ok, connection, physical_connection}

          {:error, code} ->
            stop_process(connection)
            {:error, code}
        end

      {:error, reason} ->
        classify_connection_error(reason, authentication)
    end
  end

  defp await_physical_connection(connection, listener_tag, authentication, timeout) do
    monitor = Process.monitor(connection)

    receive do
      {:connected, physical_connection, ^listener_tag} ->
        Process.demonitor(monitor, [:flush])
        {:ok, physical_connection}

      {:DOWN, ^monitor, :process, ^connection, reason} ->
        classify_connection_error(reason, authentication)
    after
      timeout ->
        Process.demonitor(monitor, [:flush])
        {:error, :server_unreachable}
    end
  end

  defp run_after_connect(connection, physical_connection, profile, authentication) do
    case run_while_alive(physical_connection, authentication, :classify, fn ->
           try do
             DatabaseConfig.after_connect(connection, "bootstrap:#{profile.purpose}")
           rescue
             exception -> {:after_connect_error, exception}
           catch
             :exit, reason -> {:after_connect_error, reason}
           end
         end) do
      :ok -> :ok
      {:after_connect_error, reason} -> classify_connection_error(reason, authentication)
      {:error, code} -> {:error, code}
    end
  end

  defp start_repo(options, authentication) do
    case Repo.start_link(options) do
      {:ok, pid} ->
        Process.unlink(pid)
        {:ok, pid}

      {:error, {:already_started, _pid}} ->
        {:error, :dependency_unavailable}

      {:error, reason} ->
        classify_connection_error(reason, authentication)
    end
  end

  defp run_while_alive(process, _authentication, :unknown_outcome, function) do
    watcher = start_resource_watcher(process)

    try do
      function.()
    after
      stop_resource_watcher(watcher)
    end
  end

  defp run_while_alive(process, authentication, :classify, function) do
    caller = self()
    reference = make_ref()
    process_monitor = Process.monitor(process)
    workflow_context = WorkflowRunner.current_context()
    workflow_reference = WorkflowRunner.context_reference(workflow_context)

    {worker, worker_monitor} =
      spawn_monitor(fn ->
        case workflow_context do
          nil ->
            send(caller, {reference, :result, function.()})

          workflow_context ->
            case WorkflowRunner.guarded_result(workflow_context, function) do
              {:ok, result} -> send(caller, {reference, :result, result})
              {:error, failure} -> send(caller, {reference, :failure, failure})
            end
        end
      end)

    await_classified_result(
      process,
      authentication,
      reference,
      process_monitor,
      worker,
      worker_monitor,
      workflow_reference
    )
  end

  defp await_classified_result(
         process,
         authentication,
         reference,
         process_monitor,
         worker,
         worker_monitor,
         workflow_reference
       ) do
    receive do
      {^reference, :result, result} ->
        Process.demonitor(worker_monitor, [:flush])
        Process.demonitor(process_monitor, [:flush])
        result

      {^reference, :failure, failure} ->
        Process.demonitor(worker_monitor, [:flush])
        Process.demonitor(process_monitor, [:flush])
        WorkflowRunner.propagate_failure(failure)

      {:favn_workflow_context, ^workflow_reference, event}
      when is_reference(workflow_reference) ->
        WorkflowRunner.absorb_context_event(workflow_reference, event)

        await_classified_result(
          process,
          authentication,
          reference,
          process_monitor,
          worker,
          worker_monitor,
          workflow_reference
        )

      {:DOWN, ^process_monitor, :process, ^process, reason} ->
        Process.exit(worker, :kill)
        await_worker_down(worker_monitor, worker)
        absorb_pending_context_events(workflow_reference)
        resource_loss(:classify, reason, authentication)

      {:DOWN, ^worker_monitor, :process, ^worker, reason} ->
        Process.demonitor(process_monitor, [:flush])
        exit(reason)
    end
  end

  defp absorb_pending_context_events(workflow_reference) do
    receive do
      {:favn_workflow_context, ^workflow_reference, event}
      when is_reference(workflow_reference) ->
        WorkflowRunner.absorb_context_event(workflow_reference, event)
        absorb_pending_context_events(workflow_reference)
    after
      0 -> :ok
    end
  end

  defp start_resource_watcher(process) do
    caller = self()
    ready_reference = make_ref()

    watcher =
      spawn(fn ->
        process_monitor = Process.monitor(process)
        caller_monitor = Process.monitor(caller)

        receive do
          {:DOWN, ^process_monitor, :process, ^process, _reason} ->
            Process.exit(caller, :kill)
        after
          0 ->
            send(caller, {:resource_watcher_ready, ready_reference, self()})
            watch_resource(process, process_monitor, caller, caller_monitor)
        end
      end)

    receive do
      {:resource_watcher_ready, ^ready_reference, ^watcher} -> watcher
    end
  end

  defp watch_resource(process, process_monitor, caller, caller_monitor) do
    receive do
      {:stop_resource_watcher, reference, ^caller} ->
        Process.demonitor(process_monitor, [:flush])
        Process.demonitor(caller_monitor, [:flush])
        send(caller, {:resource_watcher_stopped, reference, self()})

      {:DOWN, ^process_monitor, :process, ^process, _reason} ->
        Process.exit(caller, :kill)

      {:DOWN, ^caller_monitor, :process, ^caller, _reason} ->
        :ok
    end
  end

  defp stop_resource_watcher(watcher) do
    reference = make_ref()
    send(watcher, {:stop_resource_watcher, reference, self()})

    receive do
      {:resource_watcher_stopped, ^reference, ^watcher} -> :ok
    end
  end

  defp resource_loss(:classify, reason, authentication),
    do: classify_connection_error(reason, authentication)

  defp resource_loss_mode(:status), do: :classify

  defp resource_loss_mode(operation) when operation in [:bootstrap, :upgrade],
    do: :unknown_outcome

  defp await_worker_down(monitor, worker) do
    receive do
      {:DOWN, ^monitor, :process, ^worker, _reason} -> :ok
    end
  end

  defp verify_repo_connection(authentication) do
    case Ecto.Adapters.SQL.query(Repo, "SELECT 1", []) do
      {:ok, _result} -> :ok
      {:error, reason} -> classify_connection_error(reason, authentication)
    end
  catch
    :exit, reason -> classify_connection_error(reason, authentication)
  end

  defp classify_connection_error(reason, authentication) do
    case authentication_failure(authentication) do
      nil -> classify_database_error(reason)
      code -> {:error, code}
    end
  end

  defp classify_database_error(%Postgrex.Error{postgres: %{code: code}})
       when code in [:invalid_authorization_specification, :invalid_password] do
    {:error, :authentication_rejected}
  end

  defp classify_database_error(%Postgrex.Error{postgres: %{code: :invalid_catalog_name}}),
    do: {:error, :database_missing}

  defp classify_database_error(%Postgrex.Error{postgres: %{code: :insufficient_privilege}}),
    do: {:error, :database_access_rejected}

  defp classify_database_error(%DBConnection.ConnectionError{}),
    do: {:error, :server_unreachable}

  defp classify_database_error(%Postgrex.Error{}), do: {:error, :server_unreachable}
  defp classify_database_error(_reason), do: {:error, :server_unreachable}

  defp authentication_failure(:password), do: nil

  defp authentication_failure(authentication) do
    case Authentication.status(authentication) do
      %{last_failure_class: class}
      when class in [
             :token_timeout,
             :identity_unavailable,
             :provider_unavailable,
             :insufficient_validity
           ] ->
        :authentication_unavailable

      %{last_failure_class: class} when class in [:identity_rejected, :invalid_config] ->
        :authentication_rejected

      _status ->
        nil
    end
  end

  defp stop_authentication(nil), do: :ok
  defp stop_authentication(pid), do: stop_process(pid)

  defp stop_process(pid) when is_pid(pid) do
    if Process.alive?(pid), do: GenServer.stop(pid)
  catch
    :exit, _reason -> :ok
  end
end
