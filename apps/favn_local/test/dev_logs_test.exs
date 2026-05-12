defmodule Favn.Dev.LogsTest do
  use ExUnit.Case, async: true

  alias Favn.Dev.Logs
  alias Favn.Dev.Paths
  alias Favn.Dev.State

  setup do
    root_dir =
      Path.join(System.tmp_dir!(), "favn_dev_logs_test_#{System.unique_integer([:positive])}")

    File.mkdir_p!(root_dir)
    :ok = State.ensure_layout(root_dir: root_dir)

    on_exit(fn ->
      File.rm_rf(root_dir)
    end)

    %{root_dir: root_dir}
  end

  test "run/1 prints all service logs with prefixes", %{root_dir: root_dir} do
    File.write!(Paths.web_log_path(root_dir), "web-one\n")
    File.write!(Paths.orchestrator_log_path(root_dir), "orc-one\n")
    File.write!(Paths.runner_log_path(root_dir), "run-one\n")

    output = collect(fn writer -> Logs.run(root_dir: root_dir, writer: writer) end)

    assert output =~ "[web] web-one"
    assert output =~ "[orchestrator] orc-one"
    assert output =~ "[runner] run-one"
  end

  test "run/1 supports service selection and tail", %{root_dir: root_dir} do
    File.write!(Paths.web_log_path(root_dir), "one\ntwo\nthree\n")

    output =
      collect(fn writer ->
        Logs.run(root_dir: root_dir, service: :web, tail: 2, writer: writer)
      end)

    refute output =~ "one"
    assert output =~ "two"
    assert output =~ "three"
  end

  test "run/1 supports follow mode", %{root_dir: root_dir} do
    File.write!(Paths.web_log_path(root_dir), "before\n")

    output =
      collect(fn writer ->
        Task.start(fn ->
          Process.sleep(30)
          File.write!(Paths.web_log_path(root_dir), "before\nafter\n")
        end)

        Logs.run(
          root_dir: root_dir,
          service: :web,
          follow: true,
          follow_ticks: 6,
          follow_sleep_ms: 20,
          writer: writer
        )
      end)

    assert output =~ "before"
    assert output =~ "after"
  end

  test "run/1 defaults to current operator log when operator runtime is active", %{
    root_dir: root_dir
  } do
    File.write!(Paths.web_log_path(root_dir), "stale-web\n")
    File.write!(Paths.orchestrator_log_path(root_dir), "stale-orchestrator\n")
    File.write!(Paths.operator_log_path(root_dir), "operator-current\n")
    File.write!(Paths.runner_log_path(root_dir), "runner-current\n")

    assert :ok =
             State.write_runtime(
               %{
                 "services" => %{
                   "operator" => %{"pid" => 123, "log_path" => Paths.operator_log_path(root_dir)},
                   "runner" => %{"pid" => 456, "log_path" => Paths.runner_log_path(root_dir)}
                 }
               },
               root_dir: root_dir
             )

    output = collect(fn writer -> Logs.run(root_dir: root_dir, writer: writer) end)

    assert output =~ "[operator] operator-current"
    assert output =~ "[runner] runner-current"
    refute output =~ "[web] operator-current"
    refute output =~ "[orchestrator] operator-current"
    refute output =~ "stale-web"
    refute output =~ "stale-orchestrator"
  end

  test "run/1 aliases legacy web and orchestrator selections to active operator log", %{
    root_dir: root_dir
  } do
    File.write!(Paths.web_log_path(root_dir), "stale-web\n")
    File.write!(Paths.orchestrator_log_path(root_dir), "stale-orchestrator\n")
    File.write!(Paths.operator_log_path(root_dir), "operator-current\n")

    assert :ok =
             State.write_runtime(
               %{
                 "services" => %{
                   "operator" => %{"pid" => 123, "log_path" => Paths.operator_log_path(root_dir)}
                 }
               },
               root_dir: root_dir
             )

    web_output =
      collect(fn writer -> Logs.run(root_dir: root_dir, service: :web, writer: writer) end)

    orchestrator_output =
      collect(fn writer ->
        Logs.run(root_dir: root_dir, service: :orchestrator, writer: writer)
      end)

    assert web_output =~ "operator-current"
    assert orchestrator_output =~ "operator-current"
    refute web_output =~ "stale-web"
    refute orchestrator_output =~ "stale-orchestrator"
  end

  defp collect(fun) do
    parent = self()
    writer = fn data -> send(parent, {:log_chunk, IO.iodata_to_binary(data)}) end

    assert :ok = fun.(writer)

    gather_chunks([])
  end

  defp gather_chunks(acc) do
    receive do
      {:log_chunk, chunk} -> gather_chunks([chunk | acc])
    after
      20 ->
        acc
        |> Enum.reverse()
        |> Enum.join("")
    end
  end
end
