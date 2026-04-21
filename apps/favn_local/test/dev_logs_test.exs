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
