defmodule FavnCoreTest do
  use ExUnit.Case
  doctest FavnCore

  test "keeps the app-level scaffold smoke value" do
    assert FavnCore.hello() == :world
  end
end
