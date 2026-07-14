defmodule FavnOrchestrator.Operator.WindowSelectionTest do
  use ExUnit.Case, async: true

  alias Favn.Window.Spec
  alias FavnOrchestrator.Operator.WindowSelection

  test "parses timeline ids and resolves the asset timezone" do
    assert {:ok, request} = WindowSelection.data_coverage_request("window:day:2026-07-13")

    assert {:ok, window} =
             WindowSelection.resolve(
               %{window: %Spec{kind: :day, timezone: "Europe/Oslo"}},
               request
             )

    assert window.kind == :day
    assert window.timezone == "Europe/Oslo"

    assert {:ok, refresh_request} =
             WindowSelection.refresh_request("refresh:month:2026-07")

    assert refresh_request.kind == :month
  end

  test "rejects malformed ids and incompatible asset policies" do
    assert {:error, {:invalid_window_id, "window:week:2026-W29"}} =
             WindowSelection.data_coverage_request("window:week:2026-W29")

    assert {:ok, request} = WindowSelection.data_coverage_request("window:day:2026-07-13")

    assert {:error, {:window_kind_mismatch, :month, :day}} =
             WindowSelection.resolve(%{window: :month}, request)
  end
end
