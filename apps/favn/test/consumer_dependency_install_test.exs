defmodule Favn.ConsumerDependencyInstallTest do
  use ExUnit.Case, async: false

  setup do
    base_dir =
      Path.join(
        System.tmp_dir!(),
        "favn_consumer_dependency_install_#{System.unique_integer([:positive])}"
      )

    snapshot_dir = Path.join(base_dir, "snapshot")
    consumer_dir = Path.join(base_dir, "consumer")

    on_exit(fn ->
      File.rm_rf(base_dir)
    end)

    %{snapshot_dir: snapshot_dir, consumer_dir: consumer_dir}
  end

  test "public package dependency boundary stays standalone-consumer safe" do
    deps = Favn.MixProject.project()[:deps]
    dep_apps = Enum.map(deps, &elem(&1, 0))

    refute :favn in dep_apps
    refute Enum.any?(deps, fn dep -> Keyword.has_key?(dep_opts(dep), :in_umbrella) end)

    assert runtime_deps(deps) == [:favn_authoring, :favn_local, :favn_sql_runtime]
    assert test_only_deps(deps) == [:favn_orchestrator, :favn_runner, :favn_test_support]

    assert Enum.all?(internal_path_deps(deps), fn dep ->
             opts = dep_opts(dep)
             path = Keyword.fetch!(opts, :path)
             String.starts_with?(path, "../")
           end)
  end

  @tag :slow
  test "fresh consumer can deps.get and compile favn from git umbrella subdir", %{
    snapshot_dir: snapshot_dir,
    consumer_dir: consumer_dir
  } do
    repo_root = Path.expand("../../..", __DIR__)

    File.mkdir_p!(snapshot_dir)

    assert {:ok, _files} =
             File.cp_r(Path.join(repo_root, "apps"), Path.join(snapshot_dir, "apps"))

    assert {:ok, _files} =
             File.cp_r(Path.join(repo_root, "config"), Path.join(snapshot_dir, "config"))

    assert :ok = File.cp(Path.join(repo_root, "mix.lock"), Path.join(snapshot_dir, "mix.lock"))

    assert {_, 0} = System.cmd("git", ["init", "-q"], cd: snapshot_dir)
    assert {_, 0} = System.cmd("git", ["add", "."], cd: snapshot_dir)

    assert {_, 0} =
             System.cmd(
               "git",
               [
                 "-c",
                 "user.name=Test",
                 "-c",
                 "user.email=test@example.com",
                 "commit",
                 "-q",
                 "-m",
                 "snapshot"
               ],
               cd: snapshot_dir
             )

    {ref, 0} = System.cmd("git", ["rev-parse", "HEAD"], cd: snapshot_dir)
    ref = String.trim(ref)

    assert {_, 0} = System.cmd("mix", ["new", consumer_dir, "--sup"])

    File.write!(
      Path.join(consumer_dir, "mix.exs"),
      consumer_mix_exs("file://#{snapshot_dir}", ref)
    )

    assert {output, 0} = System.cmd("mix", ["deps.get"], cd: consumer_dir, stderr_to_stdout: true)
    refute output =~ "App favn lists itself as a dependency"

    assert {_, 0} = System.cmd("mix", ["compile"], cd: consumer_dir, stderr_to_stdout: true)
  end

  @tag :slow
  test "fresh local consumer can resolve favn with plugin path dependencies", %{
    consumer_dir: consumer_dir
  } do
    repo_root = Path.expand("../../..", __DIR__)

    assert {_, 0} = System.cmd("mix", ["new", consumer_dir, "--sup"])

    File.write!(
      Path.join(consumer_dir, "mix.exs"),
      consumer_mix_exs_with_plugin_paths(repo_root)
    )

    {output, status} = System.cmd("mix", ["deps.get"], cd: consumer_dir, stderr_to_stdout: true)

    assert status == 0, String.slice(output, -4_000, 4_000)
    refute output =~ "Dependencies have diverged"

    assert {_, 0} = System.cmd("mix", ["compile"], cd: consumer_dir, stderr_to_stdout: true)

    runner_boot = """
    Application.put_env(:favn, :runner_plugins, [Favn.Azure.RunnerPlugin])
    {:ok, _started} = Application.ensure_all_started(:favn_runner)

    started = Application.started_applications() |> Enum.map(&elem(&1, 0))

    unless :favn_azure in started and :inets in started and :ssl in started and
             is_pid(Process.whereis(Favn.Azure.Credentials.Cache)) do
      raise "Azure runner plugin did not boot its packaged applications and cache"
    end
    """

    assert {_, 0} =
             System.cmd(
               "mix",
               ["run", "--no-start", "--no-compile", "--eval", runner_boot],
               cd: consumer_dir,
               stderr_to_stdout: true
             )
  end

  defp runtime_deps(deps) do
    deps
    |> internal_path_deps()
    |> Enum.reject(fn dep -> Keyword.get(dep_opts(dep), :only) == :test end)
    |> Enum.map(&elem(&1, 0))
  end

  defp test_only_deps(deps) do
    deps
    |> internal_path_deps()
    |> Enum.filter(fn dep -> Keyword.get(dep_opts(dep), :only) == :test end)
    |> Enum.map(&elem(&1, 0))
  end

  defp internal_path_deps(deps) do
    Enum.filter(deps, fn dep -> Keyword.has_key?(dep_opts(dep), :path) end)
  end

  defp dep_opts({_app, opts}) when is_list(opts), do: opts
  defp dep_opts({_app, _requirement, opts}) when is_list(opts), do: opts

  defp consumer_mix_exs(repo_url, ref) do
    """
    defmodule FavnConsumerInstall.MixProject do
      use Mix.Project

      def project do
        [
          app: :favn_consumer_install,
          version: \"0.1.0\",
          elixir: \"~> 1.20\",
          start_permanent: Mix.env() == :prod,
          deps: deps()
        ]
      end

      def application do
        [extra_applications: [:logger]]
      end

      defp deps do
        [
          {:favn, git: \"#{repo_url}\", ref: \"#{ref}\", subdir: \"apps/favn\"}
        ]
      end
    end
    """
  end

  defp consumer_mix_exs_with_plugin_paths(repo_root) do
    """
    defmodule FavnConsumerInstall.MixProject do
      use Mix.Project

      def project do
        [
          app: :favn_consumer_install,
          version: "0.1.0",
          elixir: "~> 1.20",
          start_permanent: Mix.env() == :prod,
          deps: deps()
        ]
      end

      def application do
        [extra_applications: [:logger]]
      end

      defp deps do
        [
          {:favn, path: "#{Path.join(repo_root, "apps/favn")}"},
          {:favn_duckdb, path: "#{Path.join(repo_root, "apps/favn_duckdb")}"},
          {:favn_azure, path: "#{Path.join(repo_root, "apps/favn_azure")}"}
        ]
      end
    end
    """
  end
end
