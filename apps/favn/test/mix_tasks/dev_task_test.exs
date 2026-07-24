defmodule Mix.Tasks.Favn.DevTaskTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureIO

  alias Mix.Tasks.Favn.Dev, as: DevTask

  test "prints bounded startup progress and an actionable ready banner" do
    output =
      capture_io(fn ->
        DevTask.print_start()

        DevTask.print_progress(
          {:configuration_loaded,
           %{
             view_url: "http://127.0.0.1:4173",
             orchestrator_url: "http://127.0.0.1:4101",
             workspace_id: "local-dev"
           }}
        )

        DevTask.print_progress(:postgres_ready)
        DevTask.print_progress({:manifest_built, %{manifest_version_id: "mv_example"}})
        DevTask.print_progress({:orchestrator_ready, %{url: "http://127.0.0.1:4101"}})
        DevTask.print_progress({:view_ready, %{url: "http://127.0.0.1:4173"}})
        DevTask.print_progress(:runner_starting)

        DevTask.print_ready(%{
          view_url: "http://127.0.0.1:4173",
          orchestrator_url: "http://127.0.0.1:4101",
          workspace_id: "local-dev"
        })
      end)

    assert output =~ "Starting Favn development"
    assert output =~ "PostgreSQL: ready"
    assert output =~ "View: http://127.0.0.1:4173 (listening)"
    assert output =~ "Runner: ready"
    assert output =~ "Manifest: active"
    assert output =~ "Favn development is ready"
    assert output =~ "Open Favn: http://127.0.0.1:4173"
    assert output =~ "Authentication: automatic (local development)"
    assert output =~ "Press Ctrl+C to stop."
    refute output =~ "Runner release:"
    refute output =~ "mv_example"
    refute output =~ "Password:"
  end
end
