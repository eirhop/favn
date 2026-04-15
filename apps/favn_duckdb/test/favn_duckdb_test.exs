defmodule FavnDuckdbTest do
  use ExUnit.Case
  doctest FavnDuckdb

  test "greets the world" do
    assert FavnDuckdb.hello() == :world
  end
end
