defmodule FavnPublicTest do
  use ExUnit.Case
  doctest Favn.PublicScaffold

  test "public app scaffold compiles" do
    assert Favn.PublicScaffold.status() == :phase_2_in_progress
  end
end
