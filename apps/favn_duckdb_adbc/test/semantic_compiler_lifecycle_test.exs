defmodule FavnDuckdbADBC.SemanticCompilerLifecycleTest do
  use ExUnit.Case, async: false

  @moduletag :adbc_integration
  alias FavnDuckdbADBC.SemanticCompiler

  test "native worker builds when the application path contains a space" do
    directory =
      Path.join(
        System.tmp_dir!(),
        "favn semantic build #{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm_rf!(directory) end)

    app_dir = Path.expand("..", __DIR__)
    assert {_output, 0} = System.cmd("make", ["MIX_APP_PATH=#{directory}"], cd: app_dir)
    assert File.regular?(Path.join(directory, "priv/semantic_worker"))
  end

  test "native ownership setup failure fails before DuckDB can load" do
    directory =
      Path.join(System.tmp_dir!(), "favn-semantic-arm-#{System.unique_integer([:positive])}")

    File.mkdir_p!(directory)
    worker = build_fault_worker(directory)
    System.put_env("FAVN_SEMANTIC_TEST_FAULT", "arm")
    port = open_worker(worker)
    {:os_pid, supervisor} = Port.info(port, :os_pid)

    on_exit(fn ->
      System.delete_env("FAVN_SEMANTIC_TEST_FAULT")
      if Port.info(port), do: Port.close(port)
      File.rm_rf!(directory)
    end)

    request = [
      <<1>>,
      field(System.fetch_env!("DUCKDB_ADBC_DRIVER")),
      field("SELECT json_serialize_sql('SELECT SUM(1)')"),
      field("DESCRIBE SELECT SUM(1) AS result")
    ]

    assert Port.command(port, request)

    assert {:error, :semantic_worker_ownership_unavailable} =
             SemanticCompiler.await_worker(port, [], nil, 12_000, supervisor)
  end

  test "queued final failure remains readable after the native port has exited" do
    directory =
      Path.join(
        System.tmp_dir!(),
        "favn-semantic-prequeued-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(directory)
    worker = build_fault_worker(directory)
    System.put_env("FAVN_SEMANTIC_TEST_FAULT", "arm")
    port = open_worker(worker)
    {:os_pid, supervisor} = Port.info(port, :os_pid)

    on_exit(fn ->
      System.delete_env("FAVN_SEMANTIC_TEST_FAULT")
      if Port.info(port), do: Port.close(port)
      File.rm_rf!(directory)
    end)

    request = [
      <<1>>,
      field(System.fetch_env!("DUCKDB_ADBC_DRIVER")),
      field("SELECT json_serialize_sql('SELECT SUM(1)')"),
      field("DESCRIBE SELECT SUM(1) AS result")
    ]

    assert Port.command(port, request)
    assert eventually(fn -> is_nil(Port.info(port)) end, 2_000)

    assert {:error, :semantic_worker_ownership_unavailable} =
             SemanticCompiler.await_worker(port, [], nil, 12_000, supervisor)
  end

  if :os.type() == {:unix, :darwin} do
    test "post-readiness Darwin watcher error terminates and reaps blocked native work" do
      directory =
        Path.join(System.tmp_dir!(), "favn-semantic-watch-#{System.unique_integer([:positive])}")

      File.mkdir_p!(directory)
      worker = build_fault_worker(directory)
      marker = Path.join(directory, "entered")
      library = Path.join(directory, "fixture.dylib")
      source = Path.join(__DIR__, "support/semantic_blocked.c")
      assert {_output, 0} = System.cmd("cc", ["-dynamiclib", source, "-o", library])
      System.put_env("FAVN_SEMANTIC_TEST_FAULT", "watch_error")
      System.put_env("FAVN_SEMANTIC_FIXTURE_MODE", "query")
      System.put_env("FAVN_SEMANTIC_FIXTURE_MARKER", marker)
      port = open_worker(worker)
      {:os_pid, supervisor} = Port.info(port, :os_pid)

      on_exit(fn ->
        System.delete_env("FAVN_SEMANTIC_TEST_FAULT")
        System.delete_env("FAVN_SEMANTIC_FIXTURE_MODE")
        System.delete_env("FAVN_SEMANTIC_FIXTURE_MARKER")
        if Port.info(port), do: Port.close(port)
        File.rm_rf!(directory)
      end)

      request = [<<1>>, field(library), field("SELECT 1"), field("DESCRIBE SELECT 1")]
      assert Port.command(port, request)

      assert {:error, :semantic_worker_failed} =
               SemanticCompiler.await_worker(port, [], nil, 12_000, supervisor)

      worker_pid = marker |> File.read!() |> String.to_integer()
      assert eventually(fn -> reaped?(worker_pid) end, 3_000)
    end
  end

  test "outer deadline retains the native supervisor identity before its handshake" do
    port = open_worker()
    {:os_pid, pid} = Port.info(port, :os_pid)

    try do
      assert {:error,
              {:semantic_worker_cleanup_unconfirmed,
               %{supervisor_pid: ^pid, worker_pid: nil, process_group: nil}}} =
               SemanticCompiler.await_worker(port, [], nil, 10)
    after
      Port.close(port)
    end
  end

  test "native request parser rejects a field after its byte budget is exhausted" do
    port = open_worker()

    on_exit(fn ->
      if Port.info(port), do: Port.close(port)
    end)

    max_first_field = 262_144 - 1 - 4
    request = [<<1, max_first_field::32-big>>, :binary.copy("x", max_first_field), <<8::32-big>>]
    assert Port.command(port, request)
    assert_receive {^port, {:exit_status, 1}}, 2_000
  end

  test "native request parser rejects a truncated field" do
    worker = Application.app_dir(:favn_duckdb_adbc, "priv/semantic_worker")

    assert {"", 1} =
             System.cmd("/bin/sh", [
               "-c",
               "printf '\\001\\000\\000\\000\\010ab' | \"$1\"",
               "sh",
               worker
             ])
  end

  test "native request parser accepts delayed fragmented input" do
    port = open_worker()
    {:os_pid, supervisor} = Port.info(port, :os_pid)

    on_exit(fn ->
      if Port.info(port), do: Port.close(port)
    end)

    sql = ~s|SUM("gross")|
    parse = "SELECT json_serialize_sql('SELECT " <> sql <> "')"

    bind =
      "DESCRIBE SELECT " <>
        sql <>
        " AS result FROM " <>
        ~s|(SELECT CAST(NULL AS DECIMAL(18,2)) AS "gross" WHERE FALSE) AS inputs|

    request =
      IO.iodata_to_binary([
        <<1>>,
        field(System.fetch_env!("DUCKDB_ADBC_DRIVER")),
        field(parse),
        field(bind)
      ])

    <<first::binary-size(3), remaining::binary>> = request
    assert Port.command(port, first)
    Process.sleep(20)
    assert Port.command(port, remaining)

    assert {:ok, %{native_type: "DECIMAL(38,2)"}} =
             SemanticCompiler.await_worker(
               port,
               [%{name: "gross", type: :decimal, nullable: false}],
               nil,
               12_000,
               supervisor
             )
  end

  test "Elixir accepts fragmented output and rejects malformed final framing" do
    directory =
      Path.join(System.tmp_dir!(), "favn-semantic-protocol-#{System.unique_integer([:positive])}")

    File.mkdir_p!(directory)
    fixture = Path.join(directory, "protocol")
    source = Path.join(__DIR__, "support/semantic_protocol.c")
    assert {_output, 0} = System.cmd("cc", ["-Wall", "-Wextra", "-Werror", source, "-o", fixture])

    on_exit(fn ->
      System.delete_env("FAVN_SEMANTIC_TEST_PROTOCOL")
      File.rm_rf!(directory)
    end)

    for mode <-
          ~w(fragmented duplicate duplicate_ready duplicate_ast truncated oversized trailing) do
      System.put_env("FAVN_SEMANTIC_TEST_PROTOCOL", mode)
      port = open_worker(fixture)
      {:os_pid, supervisor} = Port.info(port, :os_pid)

      if mode == "fragmented" do
        assert {:error, :semantic_worker_ownership_unavailable} =
                 SemanticCompiler.await_worker(port, [], nil, 3_000, supervisor)
      else
        assert {:error, {:semantic_worker_cleanup_unconfirmed, %{supervisor_pid: ^supervisor}}} =
                 SemanticCompiler.await_worker(port, [], nil, 3_000, supervisor)
      end

      if Port.info(port), do: Port.close(port)
    end
  end

  test "rejected grammar never enters native binding" do
    directory =
      Path.join(System.tmp_dir!(), "favn-semantic-reject-#{System.unique_integer([:positive])}")

    File.mkdir_p!(directory)
    worker = build_fault_worker(directory)
    marker = Path.join(directory, "bind-entered")
    System.put_env("FAVN_SEMANTIC_TEST_BIND_MARKER", marker)
    port = open_worker(worker)
    {:os_pid, supervisor} = Port.info(port, :os_pid)

    on_exit(fn ->
      System.delete_env("FAVN_SEMANTIC_TEST_BIND_MARKER")
      if Port.info(port), do: Port.close(port)
      File.rm_rf!(directory)
    end)

    sql = ~s|SUM("gross") + random()|
    parse = "SELECT json_serialize_sql('SELECT " <> sql <> "')"

    bind =
      "DESCRIBE SELECT " <>
        sql <>
        " AS result FROM " <>
        ~s|(SELECT CAST(NULL AS DECIMAL(18,2)) AS "gross" WHERE FALSE) AS inputs|

    request = [<<1>>, field(System.fetch_env!("DUCKDB_ADBC_DRIVER")), field(parse), field(bind)]
    assert Port.command(port, request)

    assert {:error, :invalid_semantic_expression} =
             SemanticCompiler.await_worker(
               port,
               [%{name: "gross", type: :decimal, nullable: false}],
               nil,
               12_000,
               supervisor
             )

    refute File.exists?(marker)
  end

  test "missing native symbol and unsupported DuckDB version fail explicitly" do
    directory =
      Path.join(System.tmp_dir!(), "favn-semantic-abi-#{System.unique_integer([:positive])}")

    File.mkdir_p!(directory)
    source = Path.join(__DIR__, "support/semantic_blocked.c")
    shared = if :os.type() == {:unix, :darwin}, do: ["-dynamiclib"], else: ["-shared", "-fPIC"]
    suffix = if :os.type() == {:unix, :darwin}, do: ".dylib", else: ".so"
    on_exit(fn -> File.rm_rf!(directory) end)

    for {mode, flag, expected} <- [
          {"missing", "-DFAVN_SEMANTIC_TEST_MISSING_SYMBOL", :semantic_worker_failed},
          {"version", "-DFAVN_SEMANTIC_TEST_UNSUPPORTED_VERSION", :semantic_runtime_unsupported}
        ] do
      library = Path.join(directory, mode <> suffix)
      assert {_output, 0} = System.cmd("cc", shared ++ [flag, source, "-o", library])
      port = open_worker()
      {:os_pid, supervisor} = Port.info(port, :os_pid)

      assert Port.command(port, [
               <<1>>,
               field(library),
               field("SELECT 1"),
               field("DESCRIBE SELECT 1")
             ])

      assert {:error, ^expected} =
               SemanticCompiler.await_worker(port, [], nil, 12_000, supervisor)

      if Port.info(port), do: Port.close(port)
    end
  end

  test "uncertain native cleanup reports identity without claiming completion" do
    directory =
      Path.join(
        System.tmp_dir!(),
        "favn-semantic-uncertain-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(directory)
    worker = build_fault_worker(directory)
    {library, marker} = blocked_fixture(directory)
    System.put_env("FAVN_SEMANTIC_TEST_FAULT", "cleanup_unconfirmed")
    System.put_env("FAVN_SEMANTIC_FIXTURE_MODE", "query")
    System.put_env("FAVN_SEMANTIC_FIXTURE_MARKER", marker)
    port = open_worker(worker)
    {:os_pid, supervisor} = Port.info(port, :os_pid)

    on_exit(fn ->
      for name <-
            ~w(FAVN_SEMANTIC_TEST_FAULT FAVN_SEMANTIC_FIXTURE_MODE FAVN_SEMANTIC_FIXTURE_MARKER),
          do: System.delete_env(name)

      if Port.info(port), do: Port.close(port)
      File.rm_rf!(directory)
    end)

    assert Port.command(port, [
             <<1>>,
             field(library),
             field("SELECT 1"),
             field("DESCRIBE SELECT 1")
           ])

    assert {:error,
            {:semantic_worker_cleanup_unconfirmed,
             %{supervisor_pid: ^supervisor, worker_pid: worker_pid, process_group: worker_pid}}} =
             SemanticCompiler.await_worker(port, [], nil, 12_000, supervisor)

    assert File.read!(marker) == Integer.to_string(worker_pid)
    assert eventually(fn -> stopped?(worker_pid) end, 3_000)
  end

  test "forced cleanup signals once each and never after confirmed reaping" do
    directory =
      Path.join(System.tmp_dir!(), "favn-semantic-signals-#{System.unique_integer([:positive])}")

    File.mkdir_p!(directory)
    worker = build_fault_worker(directory)
    {library, marker} = blocked_fixture(directory)
    signals = Path.join(directory, "signals")
    System.put_env("FAVN_SEMANTIC_FIXTURE_MODE", "query")
    System.put_env("FAVN_SEMANTIC_FIXTURE_MARKER", marker)
    System.put_env("FAVN_SEMANTIC_FIXTURE_IGNORE_TERM", "1")
    System.put_env("FAVN_SEMANTIC_TEST_SIGNAL_LOG", signals)
    port = open_worker(worker)
    {:os_pid, supervisor} = Port.info(port, :os_pid)

    on_exit(fn ->
      for name <-
            ~w(FAVN_SEMANTIC_FIXTURE_MODE FAVN_SEMANTIC_FIXTURE_MARKER FAVN_SEMANTIC_FIXTURE_IGNORE_TERM FAVN_SEMANTIC_TEST_SIGNAL_LOG),
          do: System.delete_env(name)

      if Port.info(port), do: Port.close(port)
      File.rm_rf!(directory)
    end)

    assert Port.command(port, [
             <<1>>,
             field(library),
             field("SELECT 1"),
             field("DESCRIBE SELECT 1")
           ])

    assert {:error, :semantic_validation_timeout} =
             SemanticCompiler.await_worker(port, [], nil, 12_000, supervisor)

    worker_pid = marker |> File.read!() |> String.to_integer()
    assert reaped?(worker_pid)
    assert File.read!(signals) == "TK"
    Process.sleep(50)
    assert File.read!(signals) == "TK"
  end

  for mode <- ~w(constructor query) do
    test "supervisor death stops native work blocked in #{mode}" do
      mode = unquote(mode)

      directory =
        Path.join(
          System.tmp_dir!(),
          "favn-semantic-lifecycle-#{System.unique_integer([:positive])}"
        )

      File.mkdir_p!(directory)
      marker = Path.join(directory, "entered")

      library =
        Path.join(
          directory,
          "fixture" <> if(:os.type() == {:unix, :darwin}, do: ".dylib", else: ".so")
        )

      source = Path.join(__DIR__, "support/semantic_blocked.c")
      flags = if :os.type() == {:unix, :darwin}, do: ["-dynamiclib"], else: ["-shared", "-fPIC"]
      assert {_output, 0} = System.cmd("cc", flags ++ [source, "-o", library])
      System.put_env("FAVN_SEMANTIC_FIXTURE_MODE", mode)
      System.put_env("FAVN_SEMANTIC_FIXTURE_MARKER", marker)
      port = open_worker()
      {:os_pid, supervisor} = Port.info(port, :os_pid)

      on_exit(fn ->
        System.delete_env("FAVN_SEMANTIC_FIXTURE_MODE")
        System.delete_env("FAVN_SEMANTIC_FIXTURE_MARKER")
        if Port.info(port), do: Port.close(port)
        File.rm_rf!(directory)
      end)

      request = [<<1>>, field(library), field("SELECT 1"), field("DESCRIBE SELECT 1")]
      assert Port.command(port, request)
      assert {:ok, <<^supervisor::32-big, worker::32-big>>} = read_frame(port, ?I, <<>>)
      assert worker > 0
      assert eventually(fn -> File.exists?(marker) end, 2_000)
      assert {_output, 0} = System.cmd("kill", ["-KILL", Integer.to_string(supervisor)])
      assert eventually(fn -> stopped?(worker) end, 3_000)
    end
  end

  test "Elixir owner death stops a blocked native query" do
    directory =
      Path.join(System.tmp_dir!(), "favn-semantic-owner-#{System.unique_integer([:positive])}")

    File.mkdir_p!(directory)
    marker = Path.join(directory, "entered")

    library =
      Path.join(
        directory,
        "fixture" <> if(:os.type() == {:unix, :darwin}, do: ".dylib", else: ".so")
      )

    source = Path.join(__DIR__, "support/semantic_blocked.c")
    flags = if :os.type() == {:unix, :darwin}, do: ["-dynamiclib"], else: ["-shared", "-fPIC"]
    assert {_output, 0} = System.cmd("cc", flags ++ [source, "-o", library])
    System.put_env("FAVN_SEMANTIC_FIXTURE_MODE", "query")
    System.put_env("FAVN_SEMANTIC_FIXTURE_MARKER", marker)

    test_pid = self()

    owner =
      spawn(fn ->
        port = open_worker()
        request = [<<1>>, field(library), field("SELECT 1"), field("DESCRIBE SELECT 1")]
        true = Port.command(port, request)
        send(test_pid, {:worker_identity, read_frame(port, ?I, <<>>)})

        receive do
          :stop -> :ok
        end
      end)

    on_exit(fn ->
      if Process.alive?(owner), do: Process.exit(owner, :kill)
      System.delete_env("FAVN_SEMANTIC_FIXTURE_MODE")
      System.delete_env("FAVN_SEMANTIC_FIXTURE_MARKER")
      File.rm_rf!(directory)
    end)

    assert_receive {:worker_identity, {:ok, <<_supervisor::32-big, worker::32-big>>}}, 3_000
    assert eventually(fn -> File.exists?(marker) end, 2_000)
    Process.exit(owner, :kill)
    assert eventually(fn -> stopped?(worker) end, 3_000)
  end

  test "timeout reaps native work even when it ignores TERM" do
    directory =
      Path.join(System.tmp_dir!(), "favn-semantic-timeout-#{System.unique_integer([:positive])}")

    File.mkdir_p!(directory)
    marker = Path.join(directory, "entered")

    library =
      Path.join(
        directory,
        "fixture" <> if(:os.type() == {:unix, :darwin}, do: ".dylib", else: ".so")
      )

    source = Path.join(__DIR__, "support/semantic_blocked.c")
    flags = if :os.type() == {:unix, :darwin}, do: ["-dynamiclib"], else: ["-shared", "-fPIC"]
    assert {_output, 0} = System.cmd("cc", flags ++ [source, "-o", library])

    previous_driver = Application.get_env(:favn, :duckdb_adbc)
    Application.put_env(:favn, :duckdb_adbc, driver: library)
    System.put_env("FAVN_SEMANTIC_FIXTURE_MODE", "query")
    System.put_env("FAVN_SEMANTIC_FIXTURE_MARKER", marker)
    System.put_env("FAVN_SEMANTIC_FIXTURE_IGNORE_TERM", "1")

    on_exit(fn ->
      if previous_driver,
        do: Application.put_env(:favn, :duckdb_adbc, previous_driver),
        else: Application.delete_env(:favn, :duckdb_adbc)

      System.delete_env("FAVN_SEMANTIC_FIXTURE_MODE")
      System.delete_env("FAVN_SEMANTIC_FIXTURE_MARKER")
      System.delete_env("FAVN_SEMANTIC_FIXTURE_IGNORE_TERM")
      File.rm_rf!(directory)
    end)

    assert {:error, :semantic_validation_timeout} =
             SemanticCompiler.validate(~s|SUM("gross")|, [
               %{name: "gross", type: :decimal, nullable: false}
             ])

    worker = marker |> File.read!() |> String.to_integer()
    assert eventually(fn -> reaped?(worker) end, 3_000)
  end

  defp open_worker(path \\ Application.app_dir(:favn_duckdb_adbc, "priv/semantic_worker")) do
    Port.open(
      {:spawn_executable, path},
      [:binary, :exit_status, :use_stdio, :hide]
    )
  end

  defp build_fault_worker(directory) do
    path = Path.join(directory, "semantic_worker")
    source = Path.expand("../c_src/semantic_worker.c", __DIR__)

    flags = [
      "-std=c11",
      "-D_POSIX_C_SOURCE=200809L",
      "-DFAVN_SEMANTIC_TEST_FAULTS",
      "-O2",
      "-Wall",
      "-Wextra",
      "-Werror",
      "-pthread",
      source,
      "-o",
      path,
      "-ldl"
    ]

    assert {_output, 0} = System.cmd("cc", flags)
    path
  end

  defp blocked_fixture(directory) do
    marker = Path.join(directory, "entered")

    library =
      Path.join(
        directory,
        if(:os.type() == {:unix, :darwin}, do: "fixture.dylib", else: "fixture.so")
      )

    source = Path.join(__DIR__, "support/semantic_blocked.c")
    flags = if :os.type() == {:unix, :darwin}, do: ["-dynamiclib"], else: ["-shared", "-fPIC"]
    assert {_output, 0} = System.cmd("cc", flags ++ [source, "-o", library])
    {library, marker}
  end

  defp field(value), do: <<byte_size(value)::32-big, value::binary>>

  defp read_frame(port, tag, buffer) do
    case buffer do
      <<size::32-big, ^tag, payload::binary-size(8), _rest::binary>> when size == 9 ->
        {:ok, payload}

      _ ->
        receive do
          {^port, {:data, bytes}} -> read_frame(port, tag, buffer <> bytes)
          {^port, {:exit_status, status}} -> {:error, status}
        after
          2_000 -> {:error, :timeout}
        end
    end
  end

  defp eventually(fun, timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout
    eventually_until(fun, deadline)
  end

  defp eventually_until(fun, deadline) do
    if fun.() do
      true
    else
      if System.monotonic_time(:millisecond) >= deadline do
        false
      else
        Process.sleep(20)
        eventually_until(fun, deadline)
      end
    end
  end

  defp stopped?(pid) do
    case System.cmd("ps", ["-p", Integer.to_string(pid), "-o", "stat="]) do
      {_, 1} -> true
      {status, 0} -> String.starts_with?(String.trim(status), "Z")
      _ -> false
    end
  end

  defp reaped?(pid) do
    case System.cmd("ps", ["-p", Integer.to_string(pid), "-o", "stat="]) do
      {_, 1} -> true
      _ -> false
    end
  end
end
