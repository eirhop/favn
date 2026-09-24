Code.require_file("support.exs", __DIR__)

defmodule RegistrationStress.Control do
  @moduledoc false
  alias RegistrationStress.Support, as: S

  def proxy(opts, path \\ "", method \\ :get, body \\ nil) do
    [postgres, proxy] =
      S.docker(["inspect", opts.project <> "-postgres-1", opts.project <> "-database-proxy-1"])
      |> JSON.decode!()

    for {record, service} <- [{postgres, "postgres"}, {proxy, "database-proxy"}] do
      labels = record["Config"]["Labels"]

      S.ensure(
        labels["com.docker.compose.project"] == opts.project and
          labels["com.docker.compose.service"] == service and record["State"]["Running"],
        "Proxy containers do not belong to this running project"
      )
    end

    S.ensure(
      Enum.any?(
        postgres["NetworkSettings"]["Ports"]["8474/tcp"] || [],
        &(&1["HostIp"] == "127.0.0.1" and &1["HostPort"] == to_string(opts.proxy_port))
      ),
      "Proxy port does not belong to this project"
    )

    S.ensure(
      proxy["HostConfig"]["NetworkMode"] == "container:" <> postgres["Id"],
      "Proxy must share PostgreSQL network namespace"
    )

    {:ok, status, response} =
      S.http("http://127.0.0.1:#{opts.proxy_port}/proxies/control_database#{path}", method, body)

    S.ensure(status in 200..299, "Proxy request failed: #{status}")
    response
  end

  def clear_faults(opts) do
    for toxic <- proxy(opts)["toxics"], do: proxy(opts, "/toxics/" <> toxic["name"], :delete)
    proxy(opts, "", :post, %{enabled: true})
  end

  def capacity(opts) do
    env = S.env()
    port = Map.get(env, "FAVN_API_HOST_PORT", "4101")

    ports =
      S.docker([
        "inspect",
        "--format",
        ~s|{{json (index .NetworkSettings.Ports "4101/tcp")}}|,
        opts.project <> "-control-plane-1"
      ])
      |> JSON.decode!()

    if Enum.any?(ports || [], &(&1["HostIp"] == "127.0.0.1" and &1["HostPort"] == port)) do
      case S.http(
             "http://127.0.0.1:#{port}/api/orchestrator/v1/runner-capacity",
             :get,
             nil,
             [{"Authorization", "Bearer " <> Map.fetch!(env, "FAVN_PLATFORM_TOKEN")}],
             3_000
           ) do
        {:ok, 200, %{"data" => data}} -> data
        _ -> %{"unavailable" => "api_request_failed"}
      end
    else
      %{"unavailable" => "project_api_not_published"}
    end
  rescue
    _ -> %{"unavailable" => "capacity_inspection_failed"}
  end

  def snapshot(opts) do
    ids = S.command(S.compose(opts, ["ps", "--all", "--quiet"])) |> String.split()

    template =
      ~s|{"id":{{json .Id}},"name":{{json .Name}},"image_id":{{json .Image}},"status":{{json .State.Status}},"started_at":{{json .State.StartedAt}},"restart_count":{{.RestartCount}},"nano_cpus":{{.HostConfig.NanoCpus}},"health":{{with (index .State "Health")}}{{json .Status}}{{else}}null{{end}}}|

    containers =
      if ids == [],
        do: [],
        else:
          S.docker(["inspect", "--format", template | ids])
          |> String.split("\n", trim: true)
          |> Enum.map(&JSON.decode!/1)

    container = S.command(S.compose(opts, ["ps", "-q", "control-plane"])) |> String.trim()

    counters =
      if container != "",
        do:
          S.docker([
            "exec",
            container,
            "sh",
            "-ec",
            "cat /sys/fs/cgroup/cpu.max /sys/fs/cgroup/cpu.stat /sys/fs/cgroup/memory.current"
          ])
          |> String.split("\n", trim: true)

    opts.project
    |> S.query(File.read!(Path.join(S.here(), "snapshot.sql")))
    |> Map.merge(%{
      "observed_at" => S.now(),
      "proxy" => proxy(opts),
      "containers" => containers,
      "runner_capacity" => capacity(opts),
      "orchestrator_cgroup" => counters
    })
  end

  def start_runners(opts) do
    S.ensure(opts.count in 1..5, "The local profile supports one to five runners")

    for n <- 1..opts.count do
      name = "#{opts.project}-runner-#{n}.favn.local"

      IO.write(
        S.command(
          S.compose(opts, [
            "run",
            "-d",
            "--no-deps",
            "--name",
            name,
            "-e",
            "FAVN_RUNNER_INSTANCE_ID=" <> name,
            "-e",
            "FAVN_RUNNER_NODE_HOST_ALIAS=" <> name,
            "runner"
          ]),
          env: [{"FAVN_RUNNER_NODE_HOST_ALIAS", name}]
        )
      )
    end
  end

  def outage(opts) do
    S.ensure(
      opts.seconds in 1..120 and opts.timeout in 1..900,
      "Outage <= 120s, trigger wait <= 900s"
    )

    S.ensure(proxy(opts)["enabled"], "Restore proxy before arming outage")

    S.ensure(
      opts.generation_state in ["building", "active"] and
        opts.phase in ["receipt", "materialized"],
      "Invalid trigger phase or generation state"
    )

    S.ensure(
      opts[:next_run] == true != is_binary(opts[:run_id]),
      "Choose exactly one of --next-run or --run-id"
    )

    sql =
      File.read!(Path.join(S.here(), "trigger.sql"))
      |> String.replace(":generation_state", "'#{opts.generation_state}'")

    sql =
      if opts[:next_run] do
        S.ensure(
          S.query(opts.project, "SELECT count(*) FROM favn_control.runs;") == 0,
          "--next-run requires fresh project"
        )

        String.replace(sql, "t.run_id = :run_id", "TRUE")
      else
        S.ensure(Regex.match?(~r/^[a-zA-Z0-9_-]{1,128}$/, opts.run_id), "Invalid run ID")
        String.replace(sql, ":run_id", "'#{opts.run_id}'")
      end

    predicate =
      if opts.phase == "materialized",
        do: "m.materialization_id IS NOT NULL",
        else: "m.materialization_id IS NULL"

    sql = String.replace(sql, ":phase_predicate", predicate)
    io = S.evidence(Map.fetch!(opts, :output))

    try do
      S.emit(
        %{
          event: "armed",
          at: S.now(),
          run_id: opts[:run_id],
          phase: opts.phase,
          outage_seconds: opts.seconds
        },
        io
      )

      candidate = await_candidate(opts.project, sql, S.monotonic() + opts.timeout * 1000)
      S.emit(%{event: "durable_trigger", at: S.now(), candidate: candidate}, io)
      # Restore even if the disabling request has an uncertain transport result.
      S.sleep(0)

      try do
        proxy(opts, "", :post, %{enabled: false})
        S.emit(%{event: "outage_started", at: S.now(), proxy: proxy(opts)}, io)
        S.sleep(opts.seconds * 1000)
      after
        restore(opts, io, 3)
      end
    after
      File.close(io)
    end
  end

  defp await_candidate(project, sql, deadline) do
    application = "favn_763_watch_" <> Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)
    [executable | args] = S.query_command(project)
    args = List.update_at(args, -1, &("export PGAPPNAME=" <> application <> "; " <> &1))

    port =
      Port.open(
        {:spawn_executable, System.find_executable(executable)},
        [:binary, :exit_status, {:line, 1_048_576}, {:args, args}]
      )

    try do
      Port.command(port, sql <> "\n\\watch 0.1\n")
      receive_candidate(port, deadline)
    after
      try do
        S.query(
          project,
          "SELECT COALESCE(jsonb_agg(pg_terminate_backend(pid)), '[]'::jsonb) FROM pg_stat_activity WHERE application_name='#{application}' AND pid <> pg_backend_pid();"
        )
      after
        stop_watcher(port)
      end
    end
  end

  defp stop_watcher(port) do
    unless watcher_exited?(port, S.monotonic() + 1000) do
      if info = Port.info(port, :os_pid) do
        {:os_pid, pid} = info
        System.cmd("kill", ["-TERM", to_string(pid)], stderr_to_stdout: true)

        unless watcher_exited?(port, S.monotonic() + 3000) do
          if Port.info(port, :os_pid) == {:os_pid, pid} do
            System.cmd("kill", ["-KILL", to_string(pid)], stderr_to_stdout: true)
            watcher_exited?(port, S.monotonic() + 3000)
          end
        end
      end
    end

    if Port.info(port), do: Port.close(port)
  end

  defp watcher_exited?(port, deadline) do
    remaining = max(deadline - S.monotonic(), 0)

    if remaining == 0 do
      false
    else
      receive do
        {^port, {:exit_status, _}} -> true
        {^port, {:data, _}} -> watcher_exited?(port, deadline)
      after
        remaining -> false
      end
    end
  end

  defp receive_candidate(port, deadline) do
    S.ensure(S.monotonic() < deadline, "No qualifying durable trigger; no outage injected")

    receive do
      {^port, {:data, {:eol, line}}} ->
        line = String.trim(line)

        case if(String.starts_with?(line, "{"), do: JSON.decode!(line)) do
          %{"candidate" => candidate} when not is_nil(candidate) -> candidate
          _ -> receive_candidate(port, deadline)
        end

      {^port, {:exit_status, status}} ->
        raise "Trigger SQL session exited: #{status}"

      :terminate ->
        raise "Interrupted before outage"
    after
      1000 -> receive_candidate(port, deadline)
    end
  end

  defp restore(opts, io, attempts) do
    proxy(opts, "", :post, %{enabled: true})
    restored = proxy(opts)
    S.ensure(restored["enabled"], "Proxy remains disabled")
    S.emit(%{event: "outage_ended", at: S.now(), proxy: restored}, io)
  rescue
    error ->
      if attempts > 1 do
        Process.sleep(1000)
        restore(opts, io, attempts - 1)
      else
        S.emit(
          %{event: "restoration_failed", at: S.now(), error_type: inspect(error.__struct__)},
          io
        )

        reraise error, __STACKTRACE__
      end
  end

  def run("snapshot", opts), do: S.emit(snapshot(opts))
  def run("clear-faults", opts), do: S.emit(clear_faults(opts))
  def run("start-runners", opts), do: start_runners(opts)
  def run("outage-after-receipt", opts), do: outage(opts)

  def run("latency", opts) do
    S.ensure(
      is_integer(opts[:ms]) and opts.jitter >= 0 and opts.jitter <= opts.ms and opts.ms <= 1000,
      "Require 0 <= jitter <= latency <= 1000 ms"
    )

    clear_faults(opts)

    for direction <- ["upstream", "downstream"] do
      proxy(opts, "/toxics", :post, %{
        name: "latency_" <> direction,
        type: "latency",
        stream: direction,
        toxicity: 1,
        attributes: %{latency: opts.ms, jitter: opts.jitter}
      })
    end

    S.emit(proxy(opts))
  end

  def run("observe", opts) do
    S.ensure(opts.interval >= 1 and opts.seconds in 1..3600, "Interval >= 1s, duration <= 1h")
    io = S.evidence(Map.fetch!(opts, :output))

    try do
      observe(opts, io, S.monotonic() + opts.seconds * 1000)
    after
      File.close(io)
    end
  end

  def run("up", opts) do
    execute = fn args -> IO.write(S.command(S.compose(opts, args))) end
    execute.(["up", "-d", "--no-build", "postgres", "database-proxy", "data-init"])
    execute.(["run", "--rm", "database-bootstrap"])
    # CREATE DATABASE cannot run inside a transaction; psql executes its selected statement.
    S.input_command(
      [
        "docker",
        "--context",
        "orbstack",
        "exec",
        "-i",
        opts.project <> "-postgres-1",
        "sh",
        "-ec",
        ~S(export PGPASSWORD="$POSTGRES_PASSWORD" PGSSLROOTCERT=/var/lib/postgresql/certs/ca.crt PGSSLMODE=verify-full; exec psql -h postgres -U favn_bootstrap -d favn -AtX -v ON_ERROR_STOP=1)
      ],
      "SELECT 'CREATE DATABASE stress_lake' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname='stress_lake')\\gexec\n"
    )

    execute.(["run", "--rm", "--no-deps", "catalog-tool", "init"])
    execute.(["up", "-d", "--no-build", "--wait", "--wait-timeout", "120", "control-plane"])
    execute.(["run", "--rm", "--no-deps", "operator", "publish"])
    start_runners(%{opts | count: 5})

    S.ensure(
      Enum.any?(1..60, fn _ ->
        if capacity(opts)["registered_runners"] == 5,
          do: true,
          else:
            (
              S.sleep(1000)
              false
            )
      end),
      "Five live runner sessions did not register"
    )

    execute.(["run", "--rm", "--no-deps", "operator", "activate"])
    status = snapshot(opts)

    S.ensure(
      S.query(
        opts.project,
        "SELECT count(*) FROM favn_control.asset_target_bindings WHERE workspace_id='elastic-simulation' AND compatibility_status NOT IN ('ready','uninitialized');"
      ) == 0,
      "Activation left unresolved targets"
    )

    S.ensure(
      status["runner_capacity"]["registered_runners"] == 5,
      "Activation lost runner presence"
    )

    execute.(["up", "-d", "--no-build", "--wait", "view", "https-proxy"])
    S.emit(status)
  end

  def run(action, _opts), do: raise(ArgumentError, "Unknown action: #{action}")

  defp observe(opts, io, deadline) do
    if S.monotonic() < deadline do
      S.emit(snapshot(opts), io)
      S.sleep(round(opts.interval * 1000))
      observe(opts, io, deadline)
    end
  end
end

alias RegistrationStress.Support, as: S

{opts, actions} =
  S.options(
    System.argv(),
    [
      project: :string,
      proxy_port: :integer,
      compose_override: :string,
      ms: :integer,
      jitter: :integer,
      count: :integer,
      seconds: :integer,
      interval: :float,
      output: :string,
      run_id: :string,
      next_run: :boolean,
      generation_state: :string,
      phase: :string,
      timeout: :integer
    ],
    project: "favn-763-local",
    proxy_port: 8476,
    jitter: 0,
    count: 5,
    interval: 5.0,
    generation_state: "building",
    phase: "materialized",
    timeout: 300
  )

if opts[:help] do
  IO.puts(
    "elixir control.exs [--project favn-763-NAME] [--compose-override FILE] ACTION [options]\nActions: up, snapshot, start-runners, observe, latency, clear-faults, outage-after-receipt"
  )
else
  [action] = actions

  S.ensure(
    Regex.match?(~r/^favn-763-[a-z0-9-]+$/, opts.project),
    "Use isolated favn-763-* project"
  )

  opts = Map.put_new(opts, :seconds, if(action == "observe", do: 600, else: 90))
  S.trap_termination()
  RegistrationStress.Control.run(action, opts)
end
