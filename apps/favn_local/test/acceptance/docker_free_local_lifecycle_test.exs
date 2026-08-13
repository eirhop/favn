defmodule FavnLocal.DockerFreeLocalLifecycleAcceptanceTest do
  use ExUnit.Case, async: false

  alias Favn.Contracts.GenerationCapabilitiesRequest
  alias Favn.Manifest.Publication
  alias FavnLocal.Config
  alias FavnLocal.DevelopmentRuntime
  alias FavnLocal.Locator
  alias FavnLocal.Publication, as: LocalPublication
  alias FavnOrchestrator.ManifestStore
  alias FavnOrchestrator.Persistence.Commands, as: C
  alias FavnOrchestrator.Persistence.SystemContext
  alias FavnOrchestrator.RunnerRegistry
  alias FavnOrchestrator.RunnerTasks
  alias FavnOrchestrator.Runs
  alias FavnOrchestrator.RunState
  alias FavnStoragePostgres.RunnerTasks.Codec
  alias FavnStoragePostgres.Release

  @moduletag :acceptance
  @moduletag timeout: 180_000

  test "source development overlaps releases and preserves the active runner after reload failure" do
    previous_primary_level = Logger.level()
    previous_handler_level = handler_level()

    root_dir =
      Path.join(System.tmp_dir!(), "favn_source_dev_#{System.unique_integer([:positive])}")

    workspace_id = "source-dev-#{System.unique_integer([:positive])}"
    File.mkdir_p!(Path.join(root_dir, "config"))

    File.write!(
      Path.join([root_dir, "config", "config.exs"]),
      """
      import Config
      config :favn, asset_modules: [FavnLocal.TestSupport.DrainAsset]
      """
    )

    previous_asset_modules = Application.get_env(:favn, :asset_modules)
    Application.put_env(:favn, :asset_modules, [FavnLocal.TestSupport.DrainAsset])
    previous_endpoint = Application.get_env(:favn_view, FavnView.Endpoint)
    previous_session = Application.get_env(:favn_view, :session_cookie_options)

    previous_passwordless =
      Application.get_env(:favn_view, :source_development_passwordless_login)

    previous_trusted_local =
      Application.get_env(:favn_orchestrator, :trusted_local_development_auth)

    previous_dev = Application.get_env(:favn, :dev)

    Application.delete_env(:favn_view, FavnView.Endpoint)
    Application.delete_env(:favn_view, :session_cookie_options)

    on_exit(fn ->
      File.rm_rf(root_dir)
      restore_env(FavnView.Endpoint, previous_endpoint)
      restore_env(:session_cookie_options, previous_session)
      restore_env(:source_development_passwordless_login, previous_passwordless)
      restore_orchestrator_env(:trusted_local_development_auth, previous_trusted_local)
      restore_favn_env(:asset_modules, previous_asset_modules)
      restore_favn_env(:dev, previous_dev)
      Logger.configure(level: previous_primary_level)
      restore_handler_level(previous_handler_level)
    end)

    provisioning_env =
      System.get_env()
      |> Map.put("FAVN_OPERATOR_COMMAND_HMAC_SECRET", "local-acceptance-hmac-secret-0001")

    assert {:ok, %{status: :ok}} =
             Release.provision_workspace_administrator(
               %{
                 "operation_id" => "local-acceptance-#{workspace_id}",
                 "workspace" => %{
                   "id" => workspace_id,
                   "slug" => workspace_id,
                   "display_name" => "Source Development Acceptance"
                 },
                 "administrator" => %{
                   "mode" => "password",
                   "username" => "admin-#{workspace_id}",
                   "display_name" => "Source development administrator",
                   "password" => "local-acceptance-password-634"
                 }
               },
               provisioning_env
             )

    Application.put_env(:favn, :dev,
      workspace_id: workspace_id,
      orchestrator_port: free_port(),
      view_port: free_port()
    )

    test_process = self()

    dev_env =
      System.get_env()
      |> Map.delete("FAVN_LOG_LEVEL")
      |> Map.drop([
        "FAVN_LOG_LEVEL",
        "FAVN_VIEW_PORT",
        "FAVN_ORCHESTRATOR_API_PORT"
      ])
      |> Map.put_new("FAVN_RUNTIME_INPUT_PIN_KEY", Base.encode64(String.duplicate("k", 32)))

    assert {:ok, started} =
             FavnLocal.dev(
               root_dir: root_dir,
               startup_timeout_ms: 60_000,
               env: dev_env,
               progress_fun: &send(test_process, {:source_dev_progress, &1})
             )

    assert started.status == :ready
    assert Process.alive?(started.supervisor)
    assert Logger.level() == :info
    assert :erpc.call(started.runner_node, Logger, :level, []) == :info

    assert progress_stages() == [
             :configuration_loaded,
             :postgres_ready,
             :manifest_built,
             :orchestrator_ready,
             :view_ready,
             :runner_starting
           ]

    assert Application.fetch_env!(:favn_view, FavnView.Endpoint)[:adapter] ==
             Bandit.PhoenixAdapter

    view_url = "http://127.0.0.1:#{Application.fetch_env!(:favn, :dev)[:view_port]}"

    assert {:ok, {{_version, 302, _reason}, login_headers, _body}} =
             :httpc.request(
               :get,
               {String.to_charlist(view_url <> "/login"), []},
               [autoredirect: false],
               []
             )

    assert header(login_headers, "location") == "/assets"
    session_cookie = login_headers |> header("set-cookie") |> cookie_pair()

    assert {:ok, {{_version, 200, _reason}, _headers, _body}} =
             :httpc.request(
               :get,
               {String.to_charlist(view_url <> "/assets"),
                [{~c"cookie", String.to_charlist(session_cookie)}]},
               [],
               []
             )

    {workspace_context, pinned_run, pinned_version, pinned_ref} =
      create_release_pinned_run(workspace_id)

    reload_release_id = FavnTestSupport.runner_release_id(:reload)

    reload =
      Task.async(fn ->
        FavnLocal.reload(
          root_dir: root_dir,
          runner_release_id: reload_release_id,
          reload_timeout_ms: 60_000
        )
      end)

    assert_eventually(fn ->
      status = DevelopmentRuntime.status()

      status.status == :reloading and
        status.runner_release_id != started.runner_release_id
    end)

    old_session = runner_session(started.runner_release_id)
    old_runner_node = node(old_session.agent_pid)
    assert Node.ping(old_runner_node) == :pong

    :erpc.cast(old_runner_node, System, :stop, [1])

    assert_eventually(fn ->
      case runner_session(started.runner_release_id, :optional) do
        %{runner_instance_id: runner_id} -> runner_id != old_session.runner_instance_id
        nil -> false
      end
    end)

    restarted_old_session = runner_session(started.runner_release_id)
    assert Node.ping(node(restarted_old_session.agent_pid)) == :pong

    delayed_task =
      enqueue_delayed_release_work(
        workspace_context,
        pinned_run,
        pinned_version,
        pinned_ref,
        started.runner_release_id
      )

    assert_eventually(fn ->
      match?(
        {:ok, %{status: status}} when status in [:succeeded, :failed, :cancelled, :unknown],
        RunnerTasks.fetch(workspace_id, delayed_task.task_id)
      )
    end)

    finish_release_pinned_run(workspace_context, pinned_run)

    assert {:ok, reloaded} = Task.await(reload, 60_000)
    assert reloaded.runner_release_id != started.runner_release_id
    assert reloaded.runner_node != started.runner_node
    assert Node.ping(started.runner_node) == :pang
    assert Node.ping(reloaded.runner_node) == :pong

    failed_release_id = FavnTestSupport.runner_release_id(:alternate)
    assert {:ok, %Publication{} = publication} = LocalPublication.build(failed_release_id)
    invalid_publication = %{publication | execution_packages: [%{}]}

    invalid_reload =
      Task.async(fn ->
        DevelopmentRuntime.reload(invalid_publication, failed_release_id, 60_000)
      end)

    failed_candidate_port =
      await_value(fn ->
        case :sys.get_state(DevelopmentRuntime) do
          %{status: :reloading, candidate: %{port: port}} -> {:ok, port}
          _state -> :retry
        end
      end)

    assert {:error, _reason} = Task.await(invalid_reload, 60_000)

    assert_eventually(fn ->
      state = :sys.get_state(DevelopmentRuntime)

      is_nil(state.candidate) and
        not MapSet.member?(state.ignored_ports, failed_candidate_port)
    end)

    send(DevelopmentRuntime, {:runner_stop_timeout, failed_candidate_port})

    :sys.replace_state(DevelopmentRuntime, fn state ->
      %{state | ignored_ports: MapSet.put(state.ignored_ports, failed_candidate_port)}
    end)

    send(DevelopmentRuntime, {:runner_stop_timeout, failed_candidate_port})

    assert_eventually(fn ->
      state = :sys.get_state(DevelopmentRuntime)
      MapSet.member?(state.ignored_ports, failed_candidate_port) and state.status == :ready
    end)

    send(DevelopmentRuntime, {failed_candidate_port, {:exit_status, 0}})

    assert_eventually(fn ->
      state = :sys.get_state(DevelopmentRuntime)
      not MapSet.member?(state.ignored_ports, failed_candidate_port) and state.status == :ready
    end)

    assert Process.alive?(started.supervisor)
    assert DevelopmentRuntime.status().status == :ready
    assert DevelopmentRuntime.status().runner_release_id == reloaded.runner_release_id
    assert Node.ping(reloaded.runner_node) == :pong
    assert {:ok, locator} = Locator.read(root_dir)
    assert locator.runner_release_id == reloaded.runner_release_id

    ref = Process.monitor(started.supervisor)
    assert :ok = FavnLocal.stop(root_dir: root_dir, stop_timeout_ms: 60_000)
    assert_receive {:DOWN, ^ref, :process, _pid, _reason}, 10_000
    assert {:error, :not_running} = Locator.read(root_dir)

    assert {:ok, restarted_config} =
             Config.load(root_dir: root_dir, workspace_id: workspace_id, env: dev_env)

    assert restarted_config.view_secret_key_base ==
             Application.fetch_env!(:favn_view, FavnView.Endpoint)[:secret_key_base]

    assert :ok = Config.apply(restarted_config)
    assert {:ok, _applications} = Application.ensure_all_started(:favn_orchestrator)
    assert {:ok, _applications} = Application.ensure_all_started(:favn_view)

    assert {:ok, {{_version, 200, _reason}, _restart_headers, _body}} =
             :httpc.request(
               :get,
               {String.to_charlist(view_url <> "/assets"),
                [{~c"cookie", String.to_charlist(session_cookie)}]},
               [],
               []
             )

    assert :ok = Application.stop(:favn_view)
    assert :ok = Application.stop(:favn_orchestrator)
    assert :ok = Config.clear_source_development_auth()
  end

  defp free_port do
    {:ok, socket} = :gen_tcp.listen(0, [:binary, active: false, ip: {127, 0, 0, 1}])
    {:ok, {_address, port}} = :inet.sockname(socket)
    :ok = :gen_tcp.close(socket)
    port
  end

  defp create_release_pinned_run(workspace_id) do
    context = SystemContext.workspace(workspace_id, :local_drain_acceptance)
    {:ok, runtime} = ManifestStore.get_runtime_state(context)
    {:ok, version} = ManifestStore.get_manifest(context, runtime.manifest_version_id)
    ref = version.manifest.assets |> hd() |> Map.fetch!(:ref)
    run_id = "local-drain-#{System.unique_integer([:positive])}"

    run =
      RunState.new(
        id: run_id,
        workspace_id: workspace_id,
        deployment_id: runtime.deployment_id,
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        runner_releases: version.runner_releases,
        asset_ref: ref,
        target_refs: [ref]
      )

    assert {:ok, committed} =
             Runs.create(
               context,
               run,
               %{
                 run_id: run.id,
                 sequence: 1,
                 event_type: :run_submitted,
                 status: :pending,
                 occurred_at: run.inserted_at
               },
               command_id: "create:#{run.id}"
             )

    {context, committed.run, version, ref}
  end

  defp enqueue_delayed_release_work(context, run, version, ref, release_id) do
    request = %GenerationCapabilitiesRequest{manifest: version, asset_ref: ref}
    {:ok, payload, payload_hash} = Codec.encode_payload(:generation_capabilities, request)
    {:ok, orchestration_context} = Codec.encode_orchestration_context(%{kind: :test})
    now = DateTime.utc_now()

    command = %C.EnqueueRunnerTask{
      workspace_context: context,
      command_id: "enqueue:delayed:#{run.id}",
      task_id: "rt_delayed_#{System.unique_integer([:positive])}",
      domain_identity: "local-drain-delayed:#{run.id}",
      task_kind: :generation_capabilities,
      runner_pool: "default",
      required_runner_release_id: release_id,
      required_capability: "generation_capabilities",
      retry_class: :safe_to_retry,
      payload: payload,
      payload_hash: payload_hash,
      orchestration_context: orchestration_context,
      run_id: run.id,
      operation_id: nil,
      asset_step_id: nil,
      deadline_at: DateTime.add(now, 60, :second),
      issued_at: now,
      occurred_at: now
    }

    assert {:ok, task} = RunnerTasks.enqueue(command)
    task
  end

  defp finish_release_pinned_run(context, run) do
    occurred_at = DateTime.utc_now()

    terminal =
      RunState.transition(
        run,
        [
          status: :ok,
          result: %{status: :ok},
          metadata: Map.put(run.metadata, :terminal_event_type, :run_finished)
        ],
        occurred_at
      )

    assert {:ok, _committed} =
             Runs.commit(
               context,
               terminal,
               %{
                 run_id: run.id,
                 sequence: terminal.event_seq,
                 event_type: :run_finished,
                 status: :ok,
                 occurred_at: occurred_at
               },
               command_id: "finish:#{run.id}"
             )
  end

  defp runner_session(release_id, mode \\ :required) do
    session =
      RunnerRegistry.list()
      |> Enum.find(&(&1.required_runner_release_id == release_id))

    case {session, mode} do
      {nil, :required} -> flunk("runner release #{release_id} is not registered")
      _other -> session
    end
  end

  defp assert_eventually(fun, attempts \\ 300)

  defp assert_eventually(fun, attempts) when attempts > 0 do
    if fun.() do
      :ok
    else
      Process.sleep(100)
      assert_eventually(fun, attempts - 1)
    end
  end

  defp assert_eventually(_fun, 0), do: flunk("condition did not become true")

  defp await_value(fun, attempts \\ 300)

  defp await_value(fun, attempts) when attempts > 0 do
    case fun.() do
      {:ok, value} ->
        value

      :retry ->
        Process.sleep(100)
        await_value(fun, attempts - 1)
    end
  end

  defp await_value(_fun, 0), do: flunk("value did not become available")

  defp progress_stages(acc \\ []) do
    receive do
      {:source_dev_progress, {stage, _summary}} -> progress_stages([stage | acc])
      {:source_dev_progress, stage} -> progress_stages([stage | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp header(headers, name) do
    Enum.find_value(headers, fn {key, value} ->
      if String.downcase(to_string(key)) == name, do: to_string(value)
    end)
  end

  defp cookie_pair(set_cookie), do: set_cookie |> String.split(";", parts: 2) |> hd()

  defp restore_env(key, nil), do: Application.delete_env(:favn_view, key)
  defp restore_env(key, value), do: Application.put_env(:favn_view, key, value)

  defp restore_orchestrator_env(key, nil),
    do: Application.delete_env(:favn_orchestrator, key)

  defp restore_orchestrator_env(key, value),
    do: Application.put_env(:favn_orchestrator, key, value)

  defp restore_favn_env(key, nil), do: Application.delete_env(:favn, key)
  defp restore_favn_env(key, value), do: Application.put_env(:favn, key, value)

  defp handler_level do
    case :logger.get_handler_config(:default) do
      {:ok, %{level: level}} -> {:present, level}
      {:error, {:not_found, :default}} -> :missing
    end
  end

  defp restore_handler_level({:present, level}),
    do: :logger.set_handler_config(:default, :level, level)

  defp restore_handler_level(:missing), do: :ok
end
