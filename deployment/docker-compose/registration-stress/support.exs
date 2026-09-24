defmodule RegistrationStress.Support do
  @moduledoc false
  @root Path.expand("../../..", __DIR__)
  def root, do: @root
  def here, do: __DIR__
  def now, do: DateTime.utc_now() |> DateTime.to_iso8601()
  def monotonic, do: System.monotonic_time(:millisecond)
  def hash(data), do: :crypto.hash(:sha256, data) |> Base.encode16(case: :lower)
  def ensure(true, _message), do: :ok
  def ensure(false, message), do: raise(ArgumentError, message)

  def options(argv, switches, defaults \\ []) do
    {opts, rest, invalid} = OptionParser.parse(argv, strict: [help: :boolean] ++ switches)
    ensure(invalid == [], "Invalid options: #{inspect(invalid)}")
    {Map.new(Keyword.merge(defaults, opts)), rest}
  end

  def command([exe | args], opts \\ []) do
    {output, status} = System.cmd(exe, args, cd: @root, env: Keyword.get(opts, :env, []))
    ensure(status == 0, "Command #{exe} failed with status #{status}")
    output
  end

  def input_command(args, input) do
    path =
      Path.join(
        System.tmp_dir!(),
        "favn-stress-#{Base.url_encode64(:crypto.strong_rand_bytes(12))}"
      )

    {:ok, io} = File.open(path, [:write, :exclusive])

    try do
      File.chmod!(path, 0o600)
      IO.binwrite(io, input)
      File.close(io)

      command(["sh", "-c", ~S(exec "$@" < "$FAVN_STRESS_INPUT"), "stress" | args],
        env: [{"FAVN_STRESS_INPUT", path}]
      )
    after
      File.close(io)
      File.rm(path)
    end
  end

  def docker(args), do: command(["docker", "--context", "orbstack" | args])

  def env do
    Path.join(@root, "deployment/docker-compose/.env.local")
    |> File.read!()
    |> String.split("\n", trim: true)
    |> Enum.reject(&String.starts_with?(&1, "#"))
    |> Enum.flat_map(fn line ->
      case String.split(line, "=", parts: 2) do
        [key, value] -> [{key, value}]
        _ -> []
      end
    end)
    |> Map.new()
  end

  def compose(opts, args) do
    override =
      if opts[:compose_override], do: ["-f", Path.expand(opts.compose_override)], else: []

    [
      "docker",
      "--context",
      "orbstack",
      "compose",
      "--env-file",
      Path.join(@root, "deployment/docker-compose/.env.local"),
      "--env-file",
      Path.join(@root, ".favn/registration-stress/build.env"),
      "--project-name",
      opts.project,
      "-f",
      Path.join(@root, "deployment/docker-compose/compose.yml"),
      "-f",
      Path.join(__DIR__, "compose.yml")
    ] ++ override ++ args
  end

  def query(project, sql), do: input_command(query_command(project), sql) |> JSON.decode!()

  def query_command(project) do
    [
      "docker",
      "--context",
      "orbstack",
      "exec",
      "-i",
      project <> "-postgres-1",
      "sh",
      "-ec",
      ~S(export PGPASSWORD="$POSTGRES_PASSWORD" PGSSLROOTCERT=/var/lib/postgresql/certs/ca.crt PGSSLMODE=verify-full; exec psql -h postgres -U favn_bootstrap -d favn -AtX -v ON_ERROR_STOP=1)
    ]
  end

  def http(url, method \\ :get, body \\ nil, headers \\ [], timeout \\ 5_000) do
    :inets.start()
    headers = Enum.map(headers, fn {k, v} -> {String.to_charlist(k), String.to_charlist(v)} end)

    request =
      if is_nil(body),
        do: {String.to_charlist(url), headers},
        else: {String.to_charlist(url), headers, ~c"application/json", JSON.encode!(body)}

    case :httpc.request(
           method,
           request,
           [timeout: timeout, connect_timeout: timeout, autoretry: 0, autoredirect: false],
           body_format: :binary
         ) do
      {:ok, {{_, status, _}, _, response}} ->
        {:ok, status, if(response == "", do: nil, else: JSON.decode!(response))}

      {:error, _reason} ->
        {:error, :transport}
    end
  end

  def json_file(path, data), do: File.write!(path, JSON.encode!(data) <> "\n")

  def evidence(path) do
    File.mkdir_p!(Path.dirname(path))
    {:ok, io} = File.open(path, [:write, :exclusive])
    io
  end

  def emit(record, io \\ nil) do
    line = JSON.encode!(record)

    if io do
      IO.puts(io, line)
      :ok = :file.sync(io)
    end

    IO.puts(line)
  end

  def sleep(ms) do
    receive do
      :terminate -> raise "Interrupted; restoring active fault before exit"
    after
      ms -> :ok
    end
  end

  def trap_termination do
    owner = self()

    System.trap_signal(:sigterm, fn ->
      monitor = Process.monitor(owner)
      send(owner, :terminate)
      # The VM's default SIGTERM handler runs after this callback returns.
      # Let the script unwind its restoration block before allowing shutdown.
      receive do
        {:DOWN, ^monitor, :process, ^owner, _} -> :ok
      after
        60_000 ->
          Process.demonitor(monitor, [:flush])
          :ok
      end
    end)
  end

  def revision(ref) do
    revision = command(["git", "rev-parse", "--verify", ref <> "^{commit}"]) |> String.trim()
    ensure(Regex.match?(~r/^[a-f0-9]{40}$/, revision), "Expected full commit identity")
    revision
  end

  def archive(revision, context) do
    ensure(not File.exists?(context), "Refusing to overwrite source snapshot: #{context}")
    File.mkdir_p!(context)
    archive = context <> ".tar"

    try do
      command(["git", "archive", "--output", archive, revision])
      command(["tar", "-xf", archive, "-C", context])
    after
      File.rm(archive)
    end
  end
end
