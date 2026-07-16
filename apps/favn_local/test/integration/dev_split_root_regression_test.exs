defmodule Favn.DevSplitRootRegressionTest do
  use ExUnit.Case, async: false

  alias Favn.Dev.Paths
  alias Favn.Dev.State

  @moduletag :integration
  @moduletag :slow

  @repo_root Path.expand("../../../..", __DIR__)
  @default_project_dir Path.join(@repo_root, "examples/basic-workflow-tutorial")
  @configured_project_dir (case System.get_env("FAVN_SPLIT_ROOT_PROJECT") do
                             value when is_binary(value) and value != "" -> Path.expand(value)
                             _ -> @default_project_dir
                           end)

  @run_split_root? File.exists?(Path.join(@configured_project_dir, "mix.exs"))

  @tag skip:
         if(@run_split_root?,
           do: false,
           else: "split-root project missing mix.exs at #{@configured_project_dir}"
         )
  test "split-root dev loop recovers from stale runtime beams" do
    repo_root = @repo_root
    project_dir = @configured_project_dir

    unless File.exists?(Path.join(project_dir, "mix.exs")) do
      flunk("split-root project missing mix.exs at #{project_dir}")
    end

    root_arg = ["--root-dir", repo_root]
    inetrc_path = write_loopback_inetrc!()
    on_exit(fn -> File.rm(inetrc_path) end)

    {_deps_output, 0} = run_mix!(project_dir, ["deps.get", "--check-locked"], inetrc_path)
    _ = run_mix!(project_dir, ["favn.stop" | root_arg], inetrc_path, allow_failure: true)
    _ = run_mix!(project_dir, ["favn.reset" | root_arg], inetrc_path, allow_failure: true)

    {install_output, 0} = run_mix!(project_dir, ["favn.install" | root_arg], inetrc_path)
    assert install_output =~ "Favn install"

    assert {:ok, %{"materialized_root" => runtime_root}} =
             State.read_install_runtime(root_dir: repo_root)

    assert {:ok, _} = File.rm_rf(Path.join(runtime_root, "_build/dev/lib/favn_runner"))
    assert {:ok, _} = File.rm_rf(Path.join(runtime_root, "_build/dev/lib/favn_orchestrator"))

    dev_task =
      Task.async(fn ->
        run_mix!(project_dir, ["favn.dev", "--sqlite" | root_arg], inetrc_path)
      end)

    try do
      case wait_until_ready(repo_root, dev_task) do
        :ok -> :ok
        {:error, reason} -> flunk("split-root dev did not become ready: #{inspect(reason)}")
      end

      {stop_output, 0} = run_mix!(project_dir, ["favn.stop" | root_arg], inetrc_path)
      assert stop_output =~ "Favn local stack stopped"

      {dev_output, 0} = Task.await(dev_task, 310_000)
      assert dev_output =~ "Favn local dev stack"
      assert dev_output =~ "storage: sqlite"
      assert dev_output =~ "scheduler: disabled"
    after
      _ = run_mix!(project_dir, ["favn.stop" | root_arg], inetrc_path, allow_failure: true)
      shutdown_dev_task(dev_task)
    end
  end

  defp wait_until_ready(root_dir, dev_task, attempts \\ 360)

  defp wait_until_ready(root_dir, _dev_task, 0) do
    {:error,
     {:timeout,
      %{
        runtime: State.read_runtime(root_dir: root_dir),
        runner_log: log_tail(Paths.runner_log_path(root_dir)),
        operator_log: log_tail(Paths.operator_log_path(root_dir))
      }}}
  end

  defp wait_until_ready(root_dir, dev_task, attempts) do
    case Task.yield(dev_task, 0) do
      nil ->
        case State.read_runtime(root_dir: root_dir) do
          {:ok, %{"active_manifest_version_id" => manifest_version_id}}
          when is_binary(manifest_version_id) and manifest_version_id != "" ->
            :ok

          _other ->
            Process.sleep(500)
            wait_until_ready(root_dir, dev_task, attempts - 1)
        end

      {:ok, {output, status}} ->
        {:error, {:dev_exited_before_readiness, status, tail(output)}}

      {:exit, reason} ->
        {:error, {:dev_task_exit, reason}}
    end
  end

  defp run_mix!(project_dir, args, inetrc_path, opts \\ [])
       when is_binary(project_dir) and is_list(args) and is_binary(inetrc_path) do
    mix = System.find_executable("mix") || "mix"
    allow_failure = Keyword.get(opts, :allow_failure, false)

    cmd_opts = [
      cd: project_dir,
      stderr_to_stdout: true,
      env: %{"MIX_ENV" => "dev", "ERL_INETRC" => inetrc_path}
    ]

    case System.cmd(mix, args, cmd_opts) do
      {output, 0 = status} ->
        {output, status}

      {output, status} when allow_failure ->
        {output, status}

      {output, status} ->
        flunk("mix #{Enum.join(args, " ")} failed (status=#{status}):\n#{output}")
    end
  end

  defp write_loopback_inetrc! do
    short_host =
      :net_adm.localhost()
      |> List.to_string()
      |> String.split(".", parts: 2)
      |> hd()

    unless Regex.match?(~r/^[A-Za-z0-9_-]+$/, short_host) do
      flunk("invalid local short hostname for split-root lifecycle: #{inspect(short_host)}")
    end

    tmp_dir =
      if match?({:unix, _}, :os.type()) and File.dir?("/tmp"),
        do: "/tmp",
        else: System.tmp_dir!()

    path =
      Path.join(
        tmp_dir,
        "favn_split_root_#{System.unique_integer([:positive, :monotonic])}.inetrc"
      )

    File.write!(
      path,
      "{host, {127,0,0,1}, [#{inspect(short_host)}]}.\n{lookup, [file, native]}.\n"
    )

    path
  end

  defp shutdown_dev_task(%Task{} = task) do
    if Process.alive?(task.pid) do
      case Task.yield(task, 10_000) do
        nil -> Task.shutdown(task, :brutal_kill)
        _result -> :ok
      end
    end
  end

  defp log_tail(path) do
    case File.read(path) do
      {:ok, contents} -> tail(contents)
      {:error, reason} -> {:unavailable, reason}
    end
  end

  defp tail(contents, max_bytes \\ 8_000) when is_binary(contents) do
    if byte_size(contents) <= max_bytes do
      contents
    else
      binary_part(contents, byte_size(contents) - max_bytes, max_bytes)
    end
  end
end
