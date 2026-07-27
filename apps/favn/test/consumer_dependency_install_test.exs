defmodule Favn.ConsumerDependencyInstallTest do
  use ExUnit.Case, async: false

  setup do
    base_dir =
      Path.join([
        Mix.Project.build_path(),
        "test-artifacts",
        "favn_consumer_dependency_install_#{System.unique_integer([:positive])}"
      ])

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

    assert runtime_deps(deps) == [:favn_sql_runtime]
    assert build_only_deps(deps) == [:favn_authoring, :favn_local, :favn_orchestrator]
    assert test_only_deps(deps) == [:favn_test_support]

    local_deps = FavnLocal.MixProject.project()[:deps]
    refute :favn_runner in runtime_deps(local_deps)
    assert :favn_runner in build_only_deps(local_deps)

    app_file = Path.join([Mix.Project.build_path(), "lib/favn_local/ebin/favn_local.app"])
    assert {:ok, [{:application, :favn_local, properties}]} = :file.consult(app_file)
    refute :favn_runner in Keyword.fetch!(properties, :applications)

    assert Enum.all?(internal_path_deps(deps), fn dep ->
             opts = dep_opts(dep)
             path = Keyword.fetch!(opts, :path)
             String.starts_with?(path, "../")
           end)
  end

  @tag :slow
  @tag timeout: 300_000
  test "fresh consumer can deps.get and compile favn from git umbrella subdir", %{
    snapshot_dir: snapshot_dir,
    consumer_dir: consumer_dir
  } do
    repo_root = Path.expand("../../..", __DIR__)

    snapshot_dir = Path.join(snapshot_dir, "repo")
    copy_checkout!(repo_root, snapshot_dir)

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
      consumer_mix_exs(file_url(snapshot_dir), ref)
    )

    assert {output, 0} = System.cmd("mix", ["deps.get"], cd: consumer_dir, stderr_to_stdout: true)
    refute output =~ "App favn lists itself as a dependency"

    assert {compile_output, 0} =
             System.cmd("mix", ["compile"], cd: consumer_dir, stderr_to_stdout: true)

    refute compile_output =~ "Can't resolve css_path"

    {dev_output, dev_status} =
      System.cmd("mix", ["favn.dev"],
        cd: consumer_dir,
        env: dev_env_overrides(),
        stderr_to_stdout: true
      )

    assert dev_status != 0
    assert dev_output =~ "missing required environment variable FAVN_DATABASE_URL"
    refute dev_output =~ "Can't resolve css_path"
    refute dev_output =~ "ENOENT"

    view_root = Path.join([consumer_dir, "deps", "favn", "apps", "favn_view"])
    assert File.stat!(Path.join(view_root, "priv/static/assets/css/app.css")).size > 0
    assert File.stat!(Path.join(view_root, "priv/static/assets/js/app.js")).size > 0

    hash_probe = """
    hash = FavnView.Storybook.asset_hash(:css_path)

    unless is_binary(hash) and byte_size(hash) > 0 do
      raise "Storybook CSS hash is unavailable"
    end
    """

    assert {_, 0} =
             System.cmd(
               "mix",
               ["run", "--no-start", "--no-compile", "--eval", hash_probe],
               cd: consumer_dir,
               stderr_to_stdout: true
             )
  end

  @tag :slow
  @tag timeout: 300_000
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

    inspection_boot = """
    started = Application.started_applications() |> Enum.map(&elem(&1, 0))

    if :favn_runner in started do
      raise "ordinary consumer startup unexpectedly started favn_runner"
    end
    """

    assert {_, 0} =
             System.cmd(
               "mix",
               ["run", "--no-compile", "--eval", inspection_boot],
               cd: consumer_dir,
               stderr_to_stdout: true
             )

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
    |> Enum.reject(fn dep -> Keyword.get(dep_opts(dep), :runtime, true) == false end)
    |> Enum.map(&elem(&1, 0))
  end

  defp build_only_deps(deps) do
    deps
    |> internal_path_deps()
    |> Enum.filter(fn dep -> Keyword.get(dep_opts(dep), :runtime, true) == false end)
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

  defp copy_checkout!(source, destination) do
    {encoded_paths, 0} =
      System.cmd(
        "git",
        ["-C", source, "ls-files", "--cached", "--others", "--exclude-standard", "-z"]
      )

    paths =
      encoded_paths
      |> String.split(<<0>>, trim: true)
      |> Enum.filter(&File.regular?(Path.join(source, &1)))

    File.mkdir_p!(destination)

    Enum.each(paths, fn relative ->
      source_path = Path.join(source, relative)
      destination_path = Path.join(destination, relative)
      File.mkdir_p!(Path.dirname(destination_path))

      case File.lstat!(source_path) do
        %{type: :symlink} ->
          source_path
          |> File.read_link!()
          |> File.ln_s!(destination_path)

        _regular ->
          File.cp!(source_path, destination_path)
      end
    end)
  end

  defp file_url(path) do
    normalized = path |> Path.expand() |> String.replace("\\", "/")

    if Regex.match?(~r/^[A-Za-z]:\//, normalized),
      do: "file:///" <> normalized,
      else: "file://" <> normalized
  end

  defp dev_env_overrides do
    empty_proxies =
      for name <- ~w(HTTP_PROXY HTTPS_PROXY http_proxy https_proxy),
          System.get_env(name) == "",
          do: {name, nil}

    [{"FAVN_DATABASE_URL", nil} | empty_proxies]
  end

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
          {:favn_duckdb_adbc,
           path: "#{Path.join(repo_root, "apps/favn_duckdb_adbc")}"},
          {:favn_azure, path: "#{Path.join(repo_root, "apps/favn_azure")}"}
        ]
      end
    end
    """
  end
end
