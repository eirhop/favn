Code.require_file(Path.expand("../../../rel/control_plane/release.exs", __DIR__))

defmodule FavnOrchestrator.ControlPlaneReleaseTest do
  use ExUnit.Case, async: true

  test "release starts Azure credentials before PostgreSQL and orchestration" do
    applications = FavnControlPlane.Release.applications()

    assert applications == [
             :favn_core,
             :favn_azure,
             :favn_storage_postgres,
             :favn_orchestrator,
             :favn_view
           ]

    assert Enum.find_index(applications, &(&1 == :favn_azure)) <
             Enum.find_index(applications, &(&1 == :favn_storage_postgres))

    azure_mix = File.read!(Path.expand("../../favn_azure/mix.exs", __DIR__))
    assert azure_mix =~ "extra_applications: [:logger, :inets, :ssl]"
  end
end
