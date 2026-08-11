Code.require_file(Path.expand("../../../rel/control_plane/release.exs", __DIR__))

defmodule FavnOrchestrator.ControlPlaneReleaseTest do
  use ExUnit.Case, async: true

  test "Orchestrator release starts Azure credentials before PostgreSQL and orchestration" do
    applications = FavnControlPlane.Release.orchestrator_applications()

    assert applications == [
             :favn_core,
             :favn_azure,
             :favn_storage_postgres,
             :favn_orchestrator
           ]

    assert Enum.find_index(applications, &(&1 == :favn_azure)) <
             Enum.find_index(applications, &(&1 == :favn_storage_postgres))

    azure_mix = File.read!(Path.expand("../../favn_azure/mix.exs", __DIR__))
    assert azure_mix =~ "extra_applications: [:logger, :inets, :ssl]"
  end

  test "View release loads the public Orchestrator facade without starting its application" do
    assert FavnControlPlane.Release.view_applications() == [
             {:favn_orchestrator, :load},
             {:favn_view, :permanent}
           ]
  end

  test "image entrypoint selects exactly one explicit release role" do
    wrapper =
      File.read!(
        Path.expand("../../../rel/control_plane/overlays/bin/favn_control_plane", __DIR__)
      )

    assert wrapper =~ "FAVN_CONTROL_PLANE_ROLE"
    assert wrapper =~ "/app/releases/orchestrator/bin/favn_orchestrator"
    assert wrapper =~ "/app/releases/view/bin/favn_view"
    refute wrapper =~ "eval"
  end
end
