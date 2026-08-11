defmodule FavnOrchestrator.ReleaseHealthTest do
  use ExUnit.Case, async: true

  test "release health is scoped to the Orchestrator readiness facade" do
    source =
      File.read!(Path.expand("../lib/favn_orchestrator/release_health.ex", __DIR__))

    assert source =~ "FavnOrchestrator.readiness()"
    refute source =~ "FavnView"
    refute source =~ "System.get_env"
    refute source =~ "System.fetch_env"
  end
end
