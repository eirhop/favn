defmodule FavnRunner.ReadinessReporterTest do
  use ExUnit.Case, async: true

  alias FavnRunner.ReadinessReporter

  @probe Path.expand("../../favn/priv/templates/deployment/runner-healthcheck.sh", __DIR__)
  @env Path.expand("../../favn/priv/templates/deployment/env.sh.eex", __DIR__)

  setup do
    path = Path.join(System.tmp_dir!(), "favn-ready-#{System.unique_integer([:positive])}")

    on_exit(fn ->
      File.rm(path)
      File.rm(path <> ".tmp")
    end)

    %{path: path}
  end

  test "publishes readiness, removes it on disconnect, and publishes reconnection", %{path: path} do
    parent = self()

    check = fn ->
      send(parent, {:checking, self()})
      receive do: (result -> result)
    end

    File.write!(path, "#{System.system_time(:second)}\n")
    start_supervised!({ReadinessReporter, path: path, check: check, interval: 1})
    assert_receive {:checking, worker}
    refute File.exists?(path)
    send(worker, :ok)
    eventually(fn -> probe(path) == 0 end)
    assert_receive {:checking, worker}
    send(worker, {:error, :runner_not_ready})
    eventually(fn -> not File.exists?(path) end)
    assert_receive {:checking, worker}
    send(worker, :ok)
    eventually(fn -> probe(path) == 0 end)
  end

  test "a blocked check times out, kills its worker, and allows another check", %{path: path} do
    parent = self()

    check = fn ->
      send(parent, {:checking, self()})
      receive do: (result -> result)
    end

    start_supervised!({ReadinessReporter, path: path, check: check, timeout: 30, interval: 1})
    assert_receive {:checking, worker}
    monitor = Process.monitor(worker)
    assert_receive {:DOWN, ^monitor, :process, ^worker, :killed}
    refute File.exists?(path)
    assert_receive {:checking, next}
    send(next, :ok)
    eventually(fn -> probe(path) == 0 end)
  end

  test "exceptions fail closed without crashing the reporter", %{path: path} do
    reporter = start_supervised!({ReadinessReporter, path: path, check: fn -> raise "failed" end})
    eventually(fn -> :sys.get_state(reporter).task == nil end)
    assert Process.alive?(reporter)
    assert probe(path) != 0
  end

  test "probe rejects missing, malformed, expired and future snapshots", %{path: path} do
    assert probe(path) != 0
    now = System.system_time(:second)

    for value <- ["", "ready", "1 + 1", "999999999999999999999", now - 10, now + 30] do
      File.write!(path, "#{value}\n")
      assert probe(path) != 0
    end

    File.write!(path, "#{now}\n")
    assert probe(path) == 0
  end

  test "release startup invalidates a snapshot before the application boots", %{path: path} do
    File.write!(path, "#{System.system_time(:second)}\n")

    assert {_, 0} =
             System.cmd("sh", [@env],
               env: [
                 {"RELEASE_COMMAND", "start"},
                 {"FAVN_RUNNER_READINESS_FILE", path},
                 {"FAVN_RUNNER_NODE_HOST_ALIAS", "runner.favn.local"},
                 {"FAVN_DISTRIBUTION_COOKIE", "test-cookie"}
               ]
             )

    refute File.exists?(path)
  end

  defp probe(path) do
    {_output, status} =
      System.cmd("sh", [@probe],
        env: [{"FAVN_RUNNER_READINESS_FILE", path}],
        stderr_to_stdout: true
      )

    status
  end

  defp eventually(check, attempts \\ 100)
  defp eventually(check, 0), do: assert(check.())

  defp eventually(check, attempts) do
    if check.(),
      do: :ok,
      else:
        (
          Process.sleep(5)
          eventually(check, attempts - 1)
        )
  end
end
