defmodule Favn.Local.ComposeAcceptanceTest do
  use ExUnit.Case, async: false

  alias Favn.Dev.{
    ComposeEnv,
    ComposeDeployment,
    ComposeLifecycle,
    ComposeProject,
    Doctor,
    Docker,
    Install,
    OrchestratorClient,
    Paths,
    Reset,
    State
  }

  alias Favn.Dev.Init.Compose, as: ComposeInit

  @moduletag :integration
  @moduletag :container
  @moduletag timeout: 1_200_000

  @reference_proxy_image "nginx:1.30.4@sha256:5cf90903deda2c5981b8ad05e7617ac010e389f0dde0ac83487c02c509281de6"

  @runner_environment %{
    "FAVN_ACCEPTANCE_DOLLARS" => "$HOME and ${UNSET_VALUE}",
    "FAVN_ACCEPTANCE_QUOTES" => "\"double\" and 'single'",
    "FAVN_ACCEPTANCE_SLASH_HASH" => "C:\\runtime\\path # literal",
    "FAVN_ACCEPTANCE_MULTILINE" => "first line\nsecond line"
  }

  setup do
    candidate =
      System.get_env("FAVN_CONTROL_PLANE_CANDIDATE") ||
        raise "FAVN_CONTROL_PLANE_CANDIDATE must name the repository-built candidate image"

    {:ok, image} = Docker.inspect_image(candidate)

    root_dir =
      Path.join(
        Path.expand("../../../../_build/test-artifacts", __DIR__),
        "favn_local_compose_acceptance_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(root_dir)
    File.mkdir_p!(Path.join(root_dir, "config"))
    File.write!(Path.join(root_dir, "mix.exs"), "defmodule Fixture.MixProject do end\n")
    File.write!(Path.join(root_dir, "config/config.exs"), "import Config\n")
    assert {:ok, _scaffold} = ComposeInit.run(root_dir: root_dir)
    add_consumer_service!(root_dir)

    for app <- ~w(favn_runner favn_orchestrator favn_view) do
      File.mkdir_p!(Path.join(root_dir, "apps/#{app}"))
      File.write!(Path.join(root_dir, "apps/#{app}/mix.exs"), "defmodule Fixture do end")
    end

    File.write!(Path.join(root_dir, "mix.lock"), "lock")

    {:ok, build_state} = Agent.start_link(fn -> [] end)

    opts = [
      root_dir: root_dir,
      favn_version: Favn.RunnerRelease.current_favn_version(),
      candidate_control_plane: %{"reference" => candidate, "image_id" => image.id},
      web_port: free_port(),
      orchestrator_port: free_port(),
      progress_fun: fn _message -> :ok end,
      ready_timeout_ms: 180_000,
      runner_release_build_timeout_ms: 1_200_000,
      docker_build_timeout_ms: 1_200_000,
      compose_command_timeout_ms: 600_000,
      env_file_loaded: @runner_environment,
      runner_build_fun: fn build_opts ->
        build_runner(build_opts, Agent.get(build_state, & &1))
      end,
      foreground: false
    ]

    assert {:ok, :installed} = Install.run(opts)
    project_name = ComposeProject.project_name(root_dir)

    on_exit(fn ->
      _ = ComposeLifecycle.stop(opts)
      _ = Reset.run(Keyword.put(opts, :yes, true))
      cleanup_project_resources(project_name)
      File.rm_rf(root_dir)
    end)

    %{opts: opts, root_dir: root_dir, build_state: build_state}
  end

  test "the production-like local stack starts, deploys, stops, and restores", context do
    assert {:ok, first} = ComposeLifecycle.start(context.opts)
    deployment = deployment!(context.opts)

    assert %{stack_status: :running, services: services} =
             ComposeLifecycle.status(context.opts)

    assert services.postgres.health == :healthy
    assert services.runner.health == :healthy
    assert services.control_plane.health == :healthy

    compose = File.read!(deployment.compose_file)
    refute compose =~ "/home/"
    refute compose =~ "5432:5432"
    refute compose =~ "4369:4369"
    refute compose =~ "9100:9100"

    running = inspect_services(deployment)

    assert Enum.any?(running["runner"]["Mounts"], fn mount ->
             mount["Destination"] == "/var/lib/favn/data" and
               Path.expand(mount["Source"]) == Paths.data_dir(context.root_dir)
           end)

    assert running["control-plane"]["Mounts"] == []
    assert running["runner"]["HostConfig"]["PortBindings"] == %{}
    assert running["postgres"]["HostConfig"]["PortBindings"] == %{}

    owner = File.stat!(context.root_dir)
    assert running["runner"]["Config"]["User"] == "#{owner.uid}:#{owner.gid}"

    for service <- ["runner", "control-plane"] do
      assert running[service]["HostConfig"]["ReadonlyRootfs"] == true
      assert "ALL" in running[service]["HostConfig"]["CapDrop"]
      assert "no-new-privileges:true" in running[service]["HostConfig"]["SecurityOpt"]
    end

    assert running["control-plane"]["Config"]["User"] == "10001:10001"

    assert_control_plane_runtime_contract(running["control-plane"]["Id"])

    assert Map.take(container_environment(running["runner"]), Map.keys(@runner_environment)) ==
             @runner_environment

    assert %{"HostIp" => "127.0.0.1"} =
             hd(running["control-plane"]["HostConfig"]["PortBindings"]["4000/tcp"])

    assert %{"HostIp" => "127.0.0.1"} =
             hd(running["control-plane"]["HostConfig"]["PortBindings"]["4101/tcp"])

    assert Enum.all?(running, fn {_service, inspection} ->
             inspection["HostConfig"]["NetworkMode"] == deployment.project_name <> "-network"
           end)

    assert_liveview_websocket_through_proxy!(deployment, context.root_dir)

    assert {_output, 0} = Docker.compose(deployment, ["up", "--detach", "sentinel"])
    sentinel_id = compose_container_id!(deployment, "sentinel")

    assert :ok = ComposeLifecycle.reload(context.opts)
    reloaded = inspect_services(deployment)

    for service <- ["postgres", "runner", "control-plane"] do
      assert reloaded[service]["Image"] == running[service]["Image"]
      assert reloaded[service]["State"]["StartedAt"] == running[service]["State"]["StartedAt"]
    end

    Agent.update(context.build_state, fn _modules -> [Favn.Dev.Paths] end)
    assert :ok = ComposeLifecycle.reload(context.opts)
    replaced = inspect_services(deployment)

    assert replaced["runner"]["Image"] != reloaded["runner"]["Image"]

    assert replaced["runner"]["State"]["StartedAt"] !=
             reloaded["runner"]["State"]["StartedAt"]

    for service <- ["postgres", "control-plane"] do
      assert replaced[service]["Image"] == reloaded[service]["Image"]
      assert replaced[service]["State"]["StartedAt"] == reloaded[service]["State"]["StartedAt"]
    end

    assert {:ok, replacement_latest} = State.read_runner_latest(root_dir: context.root_dir)
    assert replacement_latest["runner_release_id"] != first.runner_release_id
    assert {:ok, replacement_runtime} = State.read_runtime(root_dir: context.root_dir)

    Agent.update(context.build_state, fn _modules -> [Favn.Dev.Paths, Favn.Dev.Config] end)

    assert {:error, {:in_flight_runs, ["run-blocking-reload"]}} =
             ComposeLifecycle.reload(
               context.opts
               |> Keyword.put(:runner_drain_timeout_ms, 0)
               |> Keyword.put(:in_flight_fun, fn _project ->
                 {:ok, ["run-blocking-reload"]}
               end)
             )

    blocked = inspect_services(deployment)
    assert blocked["runner"]["Image"] == replaced["runner"]["Image"]
    assert blocked["runner"]["State"]["StartedAt"] == replaced["runner"]["State"]["StartedAt"]

    assert {:ok, after_blocked_latest} = State.read_runner_latest(root_dir: context.root_dir)
    assert after_blocked_latest["runner_release_id"] == replacement_latest["runner_release_id"]
    assert {:ok, after_blocked_runtime} = State.read_runtime(root_dir: context.root_dir)

    assert after_blocked_runtime["active_manifest_version_id"] ==
             replacement_runtime["active_manifest_version_id"]

    assert {:ok, diagnostics} = ComposeLifecycle.diagnostics(context.opts)
    assert diagnostics["status"] == "ok"

    assert {:ok, secrets} = State.read_secrets(root_dir: context.root_dir)
    encoded_diagnostics = JSON.encode!(diagnostics)

    refute secrets
           |> Map.values()
           |> Enum.filter(&(is_binary(&1) and &1 != ""))
           |> Enum.any?(&String.contains?(encoded_diagnostics, &1))

    {_doctor_result, doctor_checks} = normalize_doctor_result(Doctor.run(context.opts))

    for check_name <- ["docker", "control-plane install", "compose deployment", "compose runtime"] do
      assert %{status: :ok} = Enum.find(doctor_checks, &(&1.name == check_name))
    end

    logs = ExUnit.CaptureIO.capture_io(fn -> assert :ok = ComposeLifecycle.logs(context.opts) end)

    refute secrets
           |> Map.values()
           |> Enum.filter(&(is_binary(&1) and &1 != ""))
           |> Enum.any?(&String.contains?(logs, &1))

    Agent.update(context.build_state, fn _modules -> [] end)
    assert :ok = ComposeLifecycle.reload(context.opts)
    rolled_back = inspect_services(deployment)

    assert rolled_back["runner"]["Image"] == running["runner"]["Image"]

    assert rolled_back["runner"]["State"]["StartedAt"] !=
             blocked["runner"]["State"]["StartedAt"]

    for service <- ["postgres", "control-plane"] do
      assert rolled_back[service]["Image"] == blocked[service]["Image"]
      assert rolled_back[service]["State"]["StartedAt"] == blocked[service]["State"]["StartedAt"]
    end

    assert {:ok, rollback_latest} = State.read_runner_latest(root_dir: context.root_dir)
    assert rollback_latest["runner_release_id"] == first.runner_release_id
    assert {:ok, rollback_runtime} = State.read_runtime(root_dir: context.root_dir)
    assert rollback_runtime["active_manifest_version_id"] == first.manifest_version_id

    assert :ok = ComposeLifecycle.stop(context.opts)
    assert ComposeLifecycle.status(context.opts).stack_status == :stopped

    assert {:ok, second} = ComposeLifecycle.start(context.opts)
    assert second.runner_release_id == rollback_latest["runner_release_id"]
    assert second.runner_image_id == rollback_latest["image_id"]
    assert second.manifest_version_id == first.manifest_version_id

    deployment = deployment!(context.opts)
    assert_runtime_input_key_rotation!(deployment, context.opts)
    assert_view_session_key_rotation!(deployment, context.opts)
    assert_service_token_rotation!(deployment, context.opts)

    assert %{stack_status: :running, runtime: %{"status" => "ok"}} =
             ComposeLifecycle.status(context.opts)

    assert :ok = ComposeLifecycle.stop(context.opts)
    assert File.exists?(deployment.compose_file)
    assert {_, 0} = System.cmd("docker", ["container", "inspect", sentinel_id])

    sentinel_volume = "favn-unrelated-#{System.unique_integer([:positive])}"
    {_output, 0} = System.cmd("docker", ["volume", "create", sentinel_volume])

    try do
      assert {:error, {:confirmation_required, resources}} = Reset.run(context.opts)
      assert resources.compose_project == deployment.project_name
      assert :ok = Reset.run(Keyword.put(context.opts, :yes, true))
      assert File.dir?(Paths.data_dir(context.root_dir))
      assert File.exists?(deployment.compose_file)
      assert {_, 0} = System.cmd("docker", ["container", "inspect", sentinel_id])
      {_output, 0} = System.cmd("docker", ["volume", "inspect", sentinel_volume])
    after
      _ = System.cmd("docker", ["volume", "rm", "--force", sentinel_volume])
    end
  end

  defp build_runner(opts, extra_modules) do
    Favn.Dev.build_runner(
      Keyword.merge(opts,
        skip_compile: true,
        skip_project_root_check: true,
        allow_non_prod_build: true,
        allow_unpinned_favn: true,
        extra_modules: extra_modules
      )
    )
  end

  defp normalize_doctor_result({:ok, checks}), do: {:ok, checks}
  defp normalize_doctor_result({:error, checks}), do: {:error, checks}

  defp deployment!(opts) do
    assert {:ok, runtime} = State.read_runtime(opts)
    assert {:ok, deployment} = ComposeDeployment.from_runtime(runtime, opts)
    deployment
  end

  defp add_consumer_service!(root_dir) do
    path = Path.join(root_dir, "deploy/compose.local.yml")
    compose = File.read!(path)

    sentinel = """
    services:
      sentinel:
        image: "#{@reference_proxy_image}"
        restart: unless-stopped

    """

    File.write!(path, String.replace(compose, "services:\n", sentinel, global: false))
  end

  defp compose_container_id!(deployment, service) do
    assert {container, 0} = Docker.compose(deployment, ["ps", "--quiet", service])
    container = String.trim(container)
    assert container != ""
    container
  end

  defp cleanup_project_resources(project_name) do
    {containers, 0} =
      System.cmd("docker", [
        "container",
        "ls",
        "--all",
        "--quiet",
        "--filter",
        "label=com.docker.compose.project=#{project_name}"
      ])

    case String.split(containers, "\n", trim: true) do
      [] -> :ok
      ids -> _ = System.cmd("docker", ["container", "rm", "--force" | ids])
    end

    _ = System.cmd("docker", ["network", "rm", project_name <> "-network"])
    _ = System.cmd("docker", ["volume", "rm", "--force", project_name <> "-postgres-data"])
    :ok
  end

  defp free_port do
    {:ok, socket} = :gen_tcp.listen(0, [:binary, active: false, ip: {127, 0, 0, 1}])
    {:ok, port} = :inet.port(socket)
    :ok = :gen_tcp.close(socket)
    port
  end

  defp inspect_services(deployment) do
    Map.new(["postgres", "runner", "control-plane"], fn service ->
      {container, 0} = Docker.compose(deployment, ["ps", "--quiet", service])
      container = String.trim(container)
      assert container != ""

      {encoded, 0} = System.cmd("docker", ["container", "inspect", container])
      {:ok, [inspection]} = JSON.decode(encoded)
      {service, inspection}
    end)
  end

  defp container_environment(inspection) do
    inspection["Config"]["Env"]
    |> Map.new(fn entry ->
      [key, value] = String.split(entry, "=", parts: 2)
      {key, value}
    end)
  end

  defp assert_control_plane_runtime_contract(container_id) do
    expression = """
    loaded = Application.loaded_applications() |> Enum.map(&elem(&1, 0))

    if :favn_runner in loaded or Code.ensure_loaded?(FavnRunner) or Code.ensure_loaded?(Mix) do
      raise "control-plane runtime includes runner or Mix code"
    end
    """

    assert {output, 0} =
             System.cmd(
               "docker",
               [
                 "exec",
                 container_id,
                 "/app/bin/favn_control_plane",
                 "rpc",
                 expression
               ],
               stderr_to_stdout: true
             )

    refute output =~ "control-plane runtime includes"
  end

  defp assert_service_token_rotation!(deployment, opts) do
    assert {:ok, secrets} = State.read_secrets(opts)
    old_token = secrets["service_token"]

    new_token =
      :crypto.hash(:sha256, "favn-container-rotation") |> Base.url_encode64(padding: false)

    roles = "platform_reader+platform_operator+platform_admin"

    put_compose_environment!(deployment, %{
      "FAVN_ORCHESTRATOR_API_SERVICE_TOKENS" =>
        "local-tooling-old|#{roles}:#{old_token},local-tooling-next|#{roles}:#{new_token}"
    })

    recreate_control_plane!(deployment, opts)
    assert_diagnostics_ready!(deployment.orchestrator_url, old_token)
    assert_diagnostics_ready!(deployment.orchestrator_url, new_token)

    put_compose_environment!(deployment, %{
      "FAVN_ORCHESTRATOR_API_SERVICE_TOKENS" => "local-tooling-next|#{roles}:#{new_token}"
    })

    assert :ok = State.write_secrets(Map.put(secrets, "service_token", new_token), opts)
    recreate_control_plane!(deployment, opts)
    assert_diagnostics_ready!(deployment.orchestrator_url, new_token)

    assert {:error, _unauthorized} =
             OrchestratorClient.diagnostics(deployment.orchestrator_url, old_token)
  end

  defp assert_runtime_input_key_rotation!(deployment, opts) do
    assert {:ok, secrets} = State.read_secrets(opts)
    old_key = secrets["runtime_input_pin_key"]
    new_key = :crypto.hash(:sha256, "favn-container-key-rotation") |> Base.encode64()

    put_compose_environment!(deployment, %{
      "FAVN_RUNTIME_INPUT_PIN_KEYS" => JSON.encode!(%{"1" => old_key, "2" => new_key}),
      "FAVN_RUNTIME_INPUT_PIN_KEY_VERSION" => "2"
    })

    recreate_control_plane!(deployment, opts)
    assert_runtime_input_key_inventory!(deployment, opts, 2, [1, 2], [old_key, new_key])

    put_compose_environment!(deployment, %{
      "FAVN_RUNTIME_INPUT_PIN_KEYS" => JSON.encode!(%{"2" => new_key})
    })

    recreate_control_plane!(deployment, opts)
    assert_runtime_input_key_inventory!(deployment, opts, 2, [2], [old_key, new_key])
  end

  defp assert_runtime_input_key_inventory!(deployment, opts, current, retained, secret_keys) do
    assert {output, 0} =
             Docker.compose(
               deployment,
               [
                 "--profile",
                 "operations",
                 "run",
                 "--rm",
                 "control-plane-ops",
                 "runtime-input-key-inventory"
               ],
               Keyword.put(opts, :compose_command_timeout_ms, 300_000)
             )

    assert output =~ "current_version: #{current}"

    for version <- retained do
      assert output =~ Integer.to_string(version)
    end

    refute Enum.any?(secret_keys, &String.contains?(output, &1))
  end

  defp assert_view_session_key_rotation!(deployment, opts) do
    assert {:ok, secrets} = State.read_secrets(opts)
    cookie = login_browser_session!(deployment, secrets)
    assert_browser_session_status!(deployment.view_url, cookie, 200)

    new_secret = :crypto.strong_rand_bytes(48) |> Base.url_encode64(padding: false)

    put_compose_environment!(deployment, %{"FAVN_VIEW_SECRET_KEY_BASE" => new_secret})
    assert :ok = State.write_secrets(Map.put(secrets, "web_session_secret", new_secret), opts)
    recreate_control_plane!(deployment, opts)

    assert_browser_session_redirected_to_login!(deployment.view_url, cookie)
  end

  defp login_browser_session!(deployment, secrets) do
    :ok = ensure_inets_started!()
    login_url = deployment.view_url <> "/login"
    %{status: 200, headers: get_headers, body: body} = http_request!(:get, login_url)
    csrf_token = csrf_token!(body)
    get_cookies = response_cookies(get_headers)

    form =
      URI.encode_query([
        {"_csrf_token", csrf_token},
        {"operator[workspace_id]", deployment.workspace_id},
        {"operator[username]", "admin"},
        {"operator[password]", secrets["bootstrap_password"]}
      ])

    headers = [{"cookie", encode_cookie_header(get_cookies)}]

    %{status: 302, headers: post_headers} =
      http_request!(:post, login_url, headers, form)

    assert response_header(post_headers, "location") == "/assets"

    cookies = Map.merge(get_cookies, response_cookies(post_headers))

    if map_size(cookies) == 0 do
      flunk("operator login did not issue a browser session cookie")
    end

    encode_cookie_header(cookies)
  end

  defp assert_browser_session_status!(view_url, cookie, expected_status) do
    %{status: status} = http_request!(:get, view_url <> "/assets", [{"cookie", cookie}])
    assert status == expected_status
  end

  defp assert_browser_session_redirected_to_login!(view_url, cookie) do
    %{status: status, headers: headers} =
      http_request!(:get, view_url <> "/assets", [{"cookie", cookie}])

    assert status == 302
    assert response_header(headers, "location") == "/login?return_to=%2Fassets"
  end

  defp http_request!(method, url, headers \\ [], body \\ nil) do
    request_headers =
      Enum.map(headers, fn {name, value} -> {to_charlist(name), to_charlist(value)} end)

    request =
      case {method, body} do
        {:get, nil} ->
          {to_charlist(url), request_headers}

        {:post, encoded} when is_binary(encoded) ->
          {to_charlist(url), request_headers, ~c"application/x-www-form-urlencoded", encoded}
      end

    case :httpc.request(method, request, [autoredirect: false, timeout: 15_000],
           body_format: :binary
         ) do
      {:ok, {{_version, status, _reason}, response_headers, response_body}} ->
        %{status: status, headers: response_headers, body: response_body}

      {:error, _reason} ->
        flunk("container browser request failed")
    end
  end

  defp ensure_inets_started! do
    case Application.ensure_all_started(:inets) do
      {:ok, _started} -> :ok
      {:error, _reason} -> flunk("could not start the HTTP client")
    end
  end

  defp csrf_token!(body) do
    case Regex.run(~r/name="_csrf_token"[^>]*value="([^"]+)"/, body) do
      [_, token] -> token
      _missing -> flunk("login form did not include a CSRF token")
    end
  end

  defp response_cookies(headers) do
    headers
    |> Enum.filter(fn {name, _value} -> String.downcase(to_string(name)) == "set-cookie" end)
    |> Map.new(fn {_name, value} ->
      [cookie | _attributes] = value |> to_string() |> String.split(";", trim: true)
      [name, encoded] = String.split(cookie, "=", parts: 2)
      {name, encoded}
    end)
  end

  defp encode_cookie_header(cookies) do
    cookies
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.map_join("; ", fn {name, value} -> name <> "=" <> value end)
  end

  defp response_header(headers, expected_name) do
    Enum.find_value(headers, fn {name, value} ->
      if String.downcase(to_string(name)) == expected_name, do: to_string(value)
    end)
  end

  defp assert_liveview_websocket_through_proxy!(deployment, root_dir) do
    proxy_port = free_port()
    proxy_name = "favn-reference-proxy-#{System.unique_integer([:positive])}"
    config_path = Path.join(root_dir, "reference-proxy.nginx.conf")
    File.write!(config_path, reference_proxy_config())

    try do
      assert {container_id, 0} =
               System.cmd(
                 "docker",
                 [
                   "run",
                   "--detach",
                   "--rm",
                   "--name",
                   proxy_name,
                   "--network",
                   deployment.project_name <> "-network",
                   "--publish",
                   "127.0.0.1:#{proxy_port}:8080",
                   "--volume",
                   "#{config_path}:/etc/nginx/nginx.conf:ro",
                   @reference_proxy_image
                 ],
                 stderr_to_stdout: true
               )

      assert String.trim(container_id) != ""
      assert_websocket_upgrade_ready!(proxy_port, deployment.view_url)
    after
      _ =
        System.cmd(
          "docker",
          ["container", "rm", "--force", proxy_name],
          stderr_to_stdout: true
        )
    end
  end

  defp reference_proxy_config do
    """
    events {}

    http {
      server {
        listen 8080;

        location / {
          proxy_pass http://control-plane.favn.internal:4000;
          proxy_http_version 1.1;
          proxy_set_header Upgrade $http_upgrade;
          proxy_set_header Connection "upgrade";
          proxy_set_header Host $http_host;
          proxy_set_header Origin $http_origin;
          proxy_set_header X-Forwarded-Proto $scheme;
          proxy_set_header X-Forwarded-Host $http_host;
          proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        }
      }
    }
    """
  end

  defp assert_websocket_upgrade_ready!(proxy_port, public_origin, attempts \\ 60)

  defp assert_websocket_upgrade_ready!(proxy_port, public_origin, attempts) when attempts > 0 do
    case websocket_upgrade(proxy_port, public_origin) do
      {:ok, response} ->
        normalized = String.downcase(response)
        assert normalized =~ ~r/^http\/1\.[01] 101 /m
        assert normalized =~ ~r/^upgrade: websocket\r?$/m

      {:error, _reason} ->
        Process.sleep(250)
        assert_websocket_upgrade_ready!(proxy_port, public_origin, attempts - 1)
    end
  end

  defp assert_websocket_upgrade_ready!(_proxy_port, _public_origin, 0),
    do: flunk("reference proxy never completed a LiveView WebSocket upgrade")

  defp websocket_upgrade(proxy_port, public_origin) do
    uri = URI.parse(public_origin)
    public_host = uri.host <> ":" <> Integer.to_string(uri.port)
    websocket_key = :crypto.strong_rand_bytes(16) |> Base.encode64()

    request =
      [
        "GET /live/websocket?vsn=2.0.0 HTTP/1.1\r\n",
        "Host: ",
        public_host,
        "\r\n",
        "Origin: ",
        public_origin,
        "\r\n",
        "Upgrade: websocket\r\n",
        "Connection: Upgrade\r\n",
        "Sec-WebSocket-Key: ",
        websocket_key,
        "\r\n",
        "Sec-WebSocket-Version: 13\r\n\r\n"
      ]

    with {:ok, socket} <-
           :gen_tcp.connect({127, 0, 0, 1}, proxy_port, [:binary, active: false], 2_000) do
      try do
        with :ok <- :gen_tcp.send(socket, request),
             {:ok, response} <- receive_http_headers(socket, "") do
          {:ok, response}
        end
      after
        :gen_tcp.close(socket)
      end
    end
  end

  defp receive_http_headers(_socket, response) when byte_size(response) > 65_536,
    do: {:error, :response_headers_too_large}

  defp receive_http_headers(socket, response) do
    if String.contains?(response, "\r\n\r\n") do
      {:ok, response}
    else
      case :gen_tcp.recv(socket, 0, 2_000) do
        {:ok, chunk} -> receive_http_headers(socket, response <> chunk)
        {:error, reason} -> {:error, reason}
      end
    end
  end

  defp put_compose_environment!(deployment, updates) do
    path = deployment.env_file
    assert {:ok, environment} = ComposeEnv.read(path)
    assert {:ok, encoded} = ComposeEnv.encode(Map.merge(environment, updates))
    assert :ok = File.write(path, encoded)
    assert :ok = File.chmod(path, 0o600)
  end

  defp recreate_control_plane!(deployment, opts) do
    assert {output, 0} =
             Docker.compose(
               deployment,
               ["up", "--detach", "--wait", "--no-deps", "--force-recreate", "control-plane"],
               Keyword.put(opts, :compose_command_timeout_ms, 300_000)
             )

    assert output == "" or is_binary(output)
  end

  defp assert_diagnostics_ready!(url, token, attempts \\ 60)

  defp assert_diagnostics_ready!(url, token, attempts) when attempts > 0 do
    case OrchestratorClient.diagnostics(url, token) do
      {:ok, %{"status" => "ok"}} ->
        :ok

      _not_ready ->
        Process.sleep(250)
        assert_diagnostics_ready!(url, token, attempts - 1)
    end
  end

  defp assert_diagnostics_ready!(_url, _token, 0), do: flunk("rotated service token not ready")
end
