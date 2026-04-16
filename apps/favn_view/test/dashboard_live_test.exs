defmodule FavnViewWeb.DashboardLiveTest do
  use FavnViewWeb.ConnCase, async: false

  alias FavnOrchestrator
  alias FavnView.TestFixtures

  setup do
    {:ok, version: TestFixtures.setup_orchestrator!()}
  end

  test "renders dashboard and submits an asset run", %{conn: conn} do
    {:ok, view, html} = live(conn, ~p"/")

    assert html =~ "Favn Dashboard"
    assert html =~ "Submit Asset Run"

    value = first_option_value(render(view), "asset_ref")

    view
    |> form("#asset-submit-form", %{asset_ref: value})
    |> render_submit()

    assert {path, _flash} = assert_redirect(view)
    assert String.starts_with?(path, "/runs/")
  end

  test "renders dashboard and submits a pipeline run", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/")
    value = first_option_value(render(view), "pipeline")

    view
    |> form("#pipeline-submit-form", %{pipeline: value})
    |> render_submit()

    assert {path, _flash} = assert_redirect(view)
    assert String.starts_with?(path, "/runs/")
  end

  test "shows submitted run in recent list after live event", %{conn: conn, version: version} do
    {:ok, view, _html} = live(conn, ~p"/")

    assert {:ok, run_id} =
             FavnOrchestrator.submit_asset_run({MyApp.Assets.Gold, :asset},
               manifest_version_id: version.manifest_version_id
             )

    assert_eventually(fn -> render(view) =~ run_id end)
  end

  defp assert_eventually(fun, attempts \\ 30)

  defp assert_eventually(fun, attempts) when attempts > 0 do
    if fun.() do
      assert true
    else
      Process.sleep(25)
      assert_eventually(fun, attempts - 1)
    end
  end

  defp assert_eventually(_fun, 0), do: flunk("condition not met in time")

  defp first_option_value(html, name) do
    regex = ~r/<select name="#{name}">\s*<option value="([^"]+)"/s
    [_, value] = Regex.run(regex, html)
    value
  end
end
