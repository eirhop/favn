defmodule FavnStoragePostgresTest do
  use ExUnit.Case
  doctest FavnStoragePostgres

  test "greets the world" do
    assert FavnStoragePostgres.hello() == :world
  end
end
