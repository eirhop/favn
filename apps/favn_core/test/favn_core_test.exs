defmodule FavnCoreTest do
  use ExUnit.Case
  doctest FavnCore

  test "greets the world" do
    assert FavnCore.hello() == :world
  end
end
