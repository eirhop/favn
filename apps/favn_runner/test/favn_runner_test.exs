defmodule FavnRunnerTest do
  use ExUnit.Case
  doctest FavnRunner

  test "greets the world" do
    assert FavnRunner.hello() == :world
  end
end
