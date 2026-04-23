defmodule Favn.Dev.RuntimeLaunchTest do
  use ExUnit.Case, async: true

  alias Favn.Dev.Config
  alias Favn.Dev.ConsumerCodePath
  alias Favn.Dev.RuntimeLaunch

  test "runner, orchestrator, and web specs target installed runtime roots" do
    runtime = %{
      "materialized_root" => "/tmp/favn_runtime",
      "runner_root" => "/tmp/favn_runtime",
      "orchestrator_root" => "/tmp/favn_runtime",
      "web_root" => "/tmp/favn_runtime/web/favn_web"
    }

    config = Config.resolve(storage: :sqlite)

    node_names = %{
      runner_short: "favn_runner_test",
      runner_full: "favn_runner_test@host",
      orchestrator_short: "favn_orchestrator_test",
      orchestrator_full: "favn_orchestrator_test@host"
    }

    secrets = %{
      "rpc_cookie" => "cookie",
      "service_token" => "token",
      "web_session_secret" => "secret"
    }

    runner = RuntimeLaunch.runner_spec(runtime, [], node_names, secrets)
    orchestrator = RuntimeLaunch.orchestrator_spec(runtime, config, [], node_names, secrets)
    web = RuntimeLaunch.web_spec(runtime, config, [], secrets)

    assert runner.cwd == runtime["runner_root"]
    assert orchestrator.cwd == runtime["orchestrator_root"]
    assert web.cwd == runtime["web_root"]
    assert "--no-compile" in runner.args
    assert "mix" in runner.args
    assert "--no-compile" in orchestrator.args
    assert "mix" in orchestrator.args
    assert web.exec == (System.find_executable("node") || "node")
    assert hd(web.args) == Path.join(runtime["web_root"], "node_modules/vite/bin/vite.js")
    assert "preview" in web.args
  end

  test "consumer code path excludes runtime-owned favn apps" do
    build_path =
      Path.join(
        System.tmp_dir!(),
        "favn_consumer_code_path_#{System.unique_integer([:positive])}"
      )

    on_exit(fn ->
      File.rm_rf(build_path)
    end)

    assert :ok = File.mkdir_p(Path.join(build_path, "lib/favn_runner/ebin"))
    assert :ok = File.mkdir_p(Path.join(build_path, "lib/favn_local/ebin"))
    assert :ok = File.mkdir_p(Path.join(build_path, "lib/my_app/ebin"))
    assert :ok = File.mkdir_p(Path.join(build_path, "lib/jason/ebin"))

    assert ConsumerCodePath.ebin_paths(build_path: build_path) == [
             Path.join(build_path, "lib/jason/ebin"),
             Path.join(build_path, "lib/my_app/ebin")
           ]
  end
end
