defmodule FavnOrchestratorTest do
  use ExUnit.Case
  doctest FavnOrchestrator

  test "greets the world" do
    assert FavnOrchestrator.hello() == :world
  end
end
