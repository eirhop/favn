defmodule Favn.Manifest.LabelsTest do
  use ExUnit.Case, async: true

  alias Favn.Manifest.Labels

  test "normalizes atoms and strings to string labels" do
    assert Labels.normalize_label(:sales) == {:ok, "sales"}
    assert Labels.normalize_label("sales") == {:ok, "sales"}
    assert Labels.normalize_labels([:daily, "raw"]) == {:ok, ["daily", "raw"]}
  end

  test "matches atom and string labels by normalized string value" do
    assert Labels.match_label?(:sales, "sales")
    assert Labels.match_label?("sales", :sales)
    refute Labels.match_label?(:sales, "finance")
    refute Labels.match_label?(:sales, 123)
  end

  test "rejects non-label values" do
    assert Labels.normalize_label(nil) == {:error, {:invalid_manifest_label, nil}}
    assert Labels.normalize_label(true) == {:error, {:invalid_manifest_label, true}}
    assert Labels.normalize_label(false) == {:error, {:invalid_manifest_label, false}}
    assert Labels.normalize_label(__MODULE__) == {:error, {:invalid_manifest_label, __MODULE__}}
    assert Labels.normalize_label(123) == {:error, {:invalid_manifest_label, 123}}
    assert Labels.normalize_labels(:daily) == {:error, {:invalid_manifest_labels, :daily}}
  end
end
