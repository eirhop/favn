defmodule Favn.DevSplitRootRegressionTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  @run_split_root? System.get_env("FAVN_RUN_DEV_SPLIT_ROOT") == "1"
  @split_root_skip_reason "set FAVN_RUN_DEV_SPLIT_ROOT=1 to run split-root dev regression"

  @tag skip: if(@run_split_root?, do: false, else: @split_root_skip_reason)
  test "split-root dev loop recovers from stale runtime beams" do
    repo_root = Path.expand("../../..", __DIR__)
    project_dir = split_root_project_dir(repo_root)

    unless File.exists?(Path.join(project_dir, "mix.exs")) do
      flunk("split-root project missing mix.exs at #{project_dir}")
    end

    root_arg = ["--root-dir", repo_root]

    _ = run_mix!(project_dir, ["favn.stop" | root_arg], allow_failure: true)
    _ = run_mix!(project_dir, ["favn.reset" | root_arg], allow_failure: true)

    assert :ok = File.rm_rf(Path.join(repo_root, "_build/dev/lib/favn_runner"))
    assert :ok = File.rm_rf(Path.join(repo_root, "_build/dev/lib/favn_orchestrator"))

    {install_output, 0} = run_mix!(project_dir, ["favn.install" | root_arg])
    assert install_output =~ "Favn install"

    dev_task =
      Task.async(fn ->
        run_mix!(project_dir, ["favn.dev", "--sqlite" | root_arg], timeout: 300_000)
      end)

    try do
      assert :ok = wait_until_running(project_dir, root_arg)

      {stop_output, 0} = run_mix!(project_dir, ["favn.stop" | root_arg])
      assert stop_output =~ "Favn local stack stopped"

      {dev_output, 0} = Task.await(dev_task, 310_000)
      assert dev_output =~ "Favn local dev stack"
      assert dev_output =~ "storage: sqlite"
    after
      _ = run_mix!(project_dir, ["favn.stop" | root_arg], allow_failure: true)
    end
  end

  defp split_root_project_dir(repo_root) do
    case System.get_env("FAVN_SPLIT_ROOT_PROJECT") do
      value when is_binary(value) and value != "" -> Path.expand(value)
      _ -> Path.join(repo_root, "examples/reference_workload")
    end
  end

  defp wait_until_running(project_dir, root_arg, attempts \\ 120)

  defp wait_until_running(_project_dir, _root_arg, 0), do: {:error, :timeout}

  defp wait_until_running(project_dir, root_arg, attempts) do
    {status_output, status_code} = run_mix!(project_dir, ["favn.status" | root_arg], allow_failure: true)

    if status_code == 0 and String.contains?(status_output, "status: running") do
      :ok
    else
      Process.sleep(500)
      wait_until_running(project_dir, root_arg, attempts - 1)
    end
  end

  defp run_mix!(project_dir, args, opts \\ []) when is_binary(project_dir) and is_list(args) do
    mix = System.find_executable("mix") || "mix"
    timeout = Keyword.get(opts, :timeout, 120_000)
    allow_failure = Keyword.get(opts, :allow_failure, false)

    cmd_opts = [cd: project_dir, stderr_to_stdout: true, env: %{"MIX_ENV" => "dev"}, timeout: timeout]

    case System.cmd(mix, args, cmd_opts) do
      {output, 0 = status} ->
        {output, status}

      {output, status} when allow_failure ->
        {output, status}

      {output, status} ->
        flunk("mix #{Enum.join(args, " ")} failed (status=#{status}):\n#{output}")
    end
  end
end
