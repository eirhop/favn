defmodule FavnView.Components.RunDetailPage.NotFoundTest do
  use ExUnit.Case, async: true

  import Phoenix.LiveViewTest

  alias FavnView.Components.RunDetailPage.NotFound

  test "distinguishes a missing run from an unreadable persisted snapshot" do
    missing =
      render_component(&NotFound.not_found_panel/1,
        run: %{id: "run_missing", error: "Run not found", not_found?: true}
      )

    assert missing =~ "No persisted run snapshot matches"

    unavailable =
      render_component(&NotFound.not_found_panel/1,
        run: %{
          id: "run_unreadable",
          error: "Backend unavailable. Try again later.",
          not_found?: false
        }
      )

    assert unavailable =~ "persisted run snapshot could not be loaded"
    refute unavailable =~ "No persisted run snapshot matches"
  end

  test "renders projection lag as loading rather than a false failure" do
    loading =
      render_component(&NotFound.not_found_panel/1,
        run: %{
          id: "run_committed",
          error: "Run not found",
          not_found?: true,
          initializing?: true
        }
      )

    assert loading =~ "Loading run"
    assert loading =~ "operator view is becoming available"
    refute loading =~ "No persisted run snapshot matches"
    refute loading =~ "Back to assets"
  end
end
