defmodule FavnView.ApplicationTest do
  use ExUnit.Case, async: true

  test "View shutdown is independent from Orchestrator draining" do
    source = File.read!(Path.expand("../lib/favn_view/application.ex", __DIR__))

    refute source =~ "FavnOrchestrator.drain"
    refute function_exported?(FavnView.Application, :prep_stop, 1)
  end
end
