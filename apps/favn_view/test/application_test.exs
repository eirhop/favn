defmodule FavnView.ApplicationTest do
  use ExUnit.Case, async: true

  test "View shutdown is independent from Orchestrator draining" do
    source = File.read!(Path.expand("../lib/favn_view/application.ex", __DIR__))

    refute source =~ "FavnOrchestrator.drain"
    refute function_exported?(FavnView.Application, :prep_stop, 1)
  end

  test "View and Orchestrator PubSub children have distinct supervisor ids" do
    view = FavnView.Application.pubsub_child_spec(FavnView.PubSub)
    orchestrator = FavnView.Application.pubsub_child_spec(FavnOrchestrator.PubSub)

    assert view.id == FavnView.PubSub
    assert orchestrator.id == FavnOrchestrator.PubSub
    refute view.id == orchestrator.id
  end
end
