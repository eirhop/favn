defmodule FavnTestSupportTest do
  use ExUnit.Case
  doctest FavnTestSupport

  test "greets the world" do
    assert FavnTestSupport.hello() == :world
  end
end
