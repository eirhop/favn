defmodule Favn.Dev.RuntimeLaunchTest do
  use ExUnit.Case, async: true

  alias Favn.Dev.Config
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
    assert "-S" in runner.args
    assert "mix" in orchestrator.args
    assert "preview" in web.args
  end
end
