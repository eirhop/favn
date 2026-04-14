defmodule FavnViewTest do
  use ExUnit.Case
  doctest FavnView

  test "greets the world" do
    assert FavnView.hello() == :world
  end
end
