defmodule Favn.DevSplitRootRegressionTest do
  use ExUnit.Case, async: false

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

    {_deps_output, 0} = run_mix!(project_dir, ["deps.get", "--check-locked"])
    _ = run_mix!(project_dir, ["favn.stop" | root_arg], allow_failure: true)
    _ = run_mix!(project_dir, ["favn.reset" | root_arg], allow_failure: true)

    {install_output, 0} = run_mix!(project_dir, ["favn.install" | root_arg])
    assert install_output =~ "Favn install"

    assert {:ok, %{"materialized_root" => runtime_root}} =
             State.read_install_runtime(root_dir: repo_root)

    assert {:ok, _} = File.rm_rf(Path.join(runtime_root, "_build/dev/lib/favn_runner"))
    assert {:ok, _} = File.rm_rf(Path.join(runtime_root, "_build/dev/lib/favn_orchestrator"))

    dev_task =
      Task.async(fn ->
        run_mix!(project_dir, ["favn.dev", "--sqlite" | root_arg])
      end)

    try do
      assert :ok = wait_until_ready(repo_root)

      {stop_output, 0} = run_mix!(project_dir, ["favn.stop" | root_arg])
      assert stop_output =~ "Favn local stack stopped"

      {dev_output, 0} = Task.await(dev_task, 310_000)
      assert dev_output =~ "Favn local dev stack"
      assert dev_output =~ "storage: sqlite"
      assert dev_output =~ "scheduler: disabled"
    after
      _ = run_mix!(project_dir, ["favn.stop" | root_arg], allow_failure: true)
    end
  end

  defp wait_until_ready(root_dir, attempts \\ 120)

  defp wait_until_ready(_root_dir, 0), do: {:error, :timeout}

  defp wait_until_ready(root_dir, attempts) do
    case State.read_runtime(root_dir: root_dir) do
      {:ok, %{"active_manifest_version_id" => manifest_version_id}}
      when is_binary(manifest_version_id) and manifest_version_id != "" ->
        :ok

      _other ->
        Process.sleep(500)
        wait_until_ready(root_dir, attempts - 1)
    end
  end

  defp run_mix!(project_dir, args, opts \\ []) when is_binary(project_dir) and is_list(args) do
    mix = System.find_executable("mix") || "mix"
    allow_failure = Keyword.get(opts, :allow_failure, false)

    cmd_opts = [cd: project_dir, stderr_to_stdout: true, env: %{"MIX_ENV" => "dev"}]

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
