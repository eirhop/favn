defmodule Favn.SharedFixtureSupportSmokeTest do
  use ExUnit.Case

  alias Favn.Test.Fixtures.Assets.Basic.SampleAssets

  test "legacy tests can consume shared fixture modules from favn_test_support" do
    assert Code.ensure_loaded?(SampleAssets)
    assert :ok = SampleAssets.extract_orders(%{})
  end
end
