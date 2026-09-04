defmodule Mix.Tasks.Favn.ReloadTaskTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureIO

  alias Mix.Tasks.Favn.Reload

  test "prints the successful legacy result even when reload_status is absent" do
    result = %{
      status: :ready,
      workspace_id: "local-dev",
      runner_release_id: "rr_example",
      manifest_version_id: "mv_example",
      phases: %{
        manifest_build_ms: 2147,
        execution_packages_ms: 13,
        manifest_publication_ms: 1090,
        manifest_activation_ms: 6275,
        deployment_ms: 7378
      },
      duration_ms: 10826,
      orchestrator_url: "http://127.0.0.1:4101",
      view_url: "http://127.0.0.1:4173",
      operator_node: :operator,
      runner_releases: %{"default" => "rr_example"},
      deployment_id: "deployment:example",
      execution_packages: %{registered: 0, provided: 66},
      runner_node: :runner,
      old_runner: :drained
    }

    output = capture_io(fn -> assert :ok = Reload.print_result(result) end)
    assert output =~ "Favn reload completed"
    assert output =~ "Runner release: rr_example"
    assert output =~ "Manifest: mv_example"
    assert output =~ "Phases: build 2147ms, packages 13ms, publish 1090ms, activate 6275ms"
    assert output =~ "Reload time: 10826ms"
    refute output =~ "Favn source unchanged"
  end
end
