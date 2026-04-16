defmodule FavnViewWeb.ManifestsSchedulerLiveTest do
  use FavnViewWeb.ConnCase, async: false

  alias FavnView.TestFixtures

  setup do
    {:ok, version: TestFixtures.setup_orchestrator!()}
  end

  test "manifests index lists versions and links to detail", %{conn: conn, version: version} do
    {:ok, _view, html} = live(conn, ~p"/manifests")
    assert html =~ "Manifests"
    assert html =~ version.manifest_version_id
  end

  test "manifest detail shows counts", %{conn: conn, version: version} do
    {:ok, _view, html} = live(conn, ~p"/manifests/#{version.manifest_version_id}")
    assert html =~ "Manifest Detail"
    assert html =~ "Assets:"
    assert html =~ "Pipelines:"
  end

  test "scheduler page renders and supports reload/tick actions", %{conn: conn} do
    {:ok, view, html} = live(conn, ~p"/scheduler")
    assert html =~ "Scheduler Inspection"

    assert render_click(element(view, "button", "Reload")) =~ "scheduler"
    assert render_click(element(view, "button", "Tick")) =~ "scheduler"
  end
end
