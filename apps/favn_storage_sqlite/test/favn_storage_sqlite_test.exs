defmodule FavnStorageSqliteTest do
  use ExUnit.Case
  doctest FavnStorageSqlite

  test "greets the world" do
    assert FavnStorageSqlite.hello() == :world
  end
end
