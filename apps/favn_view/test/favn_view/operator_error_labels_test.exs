defmodule FavnView.OperatorErrorLabelsTest do
  use ExUnit.Case, async: true

  alias FavnView.OperatorErrorLabels

  test "collection load labels are always render-safe strings" do
    assert OperatorErrorLabels.collection_load(:not_found) ==
             "No active manifest is available."

    assert OperatorErrorLabels.collection_load(:active_manifest_not_set) ==
             "Active manifest not set"

    label = OperatorErrorLabels.collection_load(%{password: "must-not-leak"})

    assert label == "Backend unavailable. Try again later."
    refute label =~ "must-not-leak"
  end
end
