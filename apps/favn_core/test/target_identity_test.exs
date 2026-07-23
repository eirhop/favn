defmodule Favn.TargetIdentityTest do
  use ExUnit.Case, async: true

  alias Favn.TargetIdentity

  test "builds canonical asset and pipeline identities" do
    assert TargetIdentity.for_asset({MyApp.Asset, :orders}) ==
             "asset:Elixir.MyApp.Asset:orders"

    assert TargetIdentity.for_pipeline({MyApp.Pipeline, :daily}) ==
             "pipeline:Elixir.MyApp.Pipeline:daily"
  end
end
