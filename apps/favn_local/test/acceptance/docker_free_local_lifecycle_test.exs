defmodule FavnLocal.DockerFreeLocalLifecycleAcceptanceTest do
  use ExUnit.Case, async: false

  alias Favn.Manifest.Publication
  alias FavnLocal.Config
  alias FavnLocal.Lifecycle, as: LocalLifecycle
  alias FavnLocal.Locator
  alias FavnLocal.Publication, as: LocalPublication
  alias FavnOrchestrator.Lifecycle, as: OrchestratorLifecycle
  alias FavnStoragePostgres.Release

  @moduletag :acceptance
  @moduletag timeout: 180_000

  test "source development starts, reloads, and shuts down safely after reload failure" do
    previous_primary_level = Logger.level()
    previous_handler_level = handler_level()

    root_dir =
      Path.join(System.tmp_dir!(), "favn_source_dev_#{System.unique_integer([:positive])}")

    workspace_id = "source-dev-#{System.unique_integer([:positive])}"
    File.mkdir_p!(Path.join(root_dir, "config"))
    File.write!(Path.join([root_dir, "config", "config.exs"]), "import Config\n")
    previous_endpoint = Application.get_env(:favn_view, FavnView.Endpoint)
    previous_session = Application.get_env(:favn_view, :session_cookie_options)

    previous_passwordless =
      Application.get_env(:favn_view, :source_development_passwordless_login)

    previous_trusted_local =
      Application.get_env(:favn_orchestrator, :trusted_local_development_auth)

    Application.delete_env(:favn_view, FavnView.Endpoint)
    Application.delete_env(:favn_view, :session_cookie_options)

    on_exit(fn ->
      File.rm_rf(root_dir)
      restore_env(FavnView.Endpoint, previous_endpoint)
      restore_env(:session_cookie_options, previous_session)
      restore_env(:source_development_passwordless_login, previous_passwordless)
      restore_orchestrator_env(:trusted_local_development_auth, previous_trusted_local)
      Logger.configure(level: previous_primary_level)
      restore_handler_level(previous_handler_level)
    end)

    assert {:ok, %{status: :ok}} =
             Release.provision_workspace(
               workspace_id: workspace_id,
               slug: workspace_id,
               display_name: "Source Development Acceptance"
             )

    Application.put_env(:favn, :dev,
      workspace_id: workspace_id,
      orchestrator_port: free_port(),
      view_port: free_port()
    )

    test_process = self()

    dev_env =
      Map.drop(System.get_env(), [
        "FAVN_LOG_LEVEL",
        "FAVN_VIEW_PORT",
        "FAVN_ORCHESTRATOR_API_PORT"
      ])

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

    admission_holder =
      Task.async(fn ->
        {:ok, permit} = OrchestratorLifecycle.acquire_admission()
        send(test_process, :admission_held)

        receive do
          :release_admission -> :ok
        end

        :ok = OrchestratorLifecycle.release_admission(permit)
      end)

    assert_receive :admission_held

    reload =
      Task.async(fn ->
        FavnLocal.reload(root_dir: root_dir, reload_timeout_ms: 60_000)
      end)

    assert_eventually(fn -> LocalLifecycle.status().status == :reloading end)
    assert nil == Task.yield(reload, 100)

    send(admission_holder.pid, :release_admission)
    assert :ok = Task.await(admission_holder)
    assert {:ok, reloaded} = Task.await(reload, 60_000)
    assert reloaded.runner_release_id != started.runner_release_id

    failed_release_id = FavnTestSupport.runner_release_id(:alternate)
    assert {:ok, %Publication{} = publication} = LocalPublication.build(failed_release_id)
    invalid_publication = %{publication | execution_packages: [%{}]}

    assert {:error, _reason} =
             LocalLifecycle.reload(invalid_publication, failed_release_id, 60_000)

    ref = Process.monitor(started.supervisor)
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
