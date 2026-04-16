defmodule FavnViewWeb.RunsLiveTest do
  use FavnViewWeb.ConnCase, async: false

  alias FavnOrchestrator
  alias FavnView.TestFixtures

  setup do
    {:ok, version: TestFixtures.setup_orchestrator!()}
  end

  test "runs index receives live updates from global run events", %{conn: conn, version: version} do
    {:ok, view, html} = live(conn, ~p"/runs")
    assert html =~ "Runs"

    assert {:ok, run_id} =
             FavnOrchestrator.submit_asset_run({MyApp.Assets.Gold, :asset},
               manifest_version_id: version.manifest_version_id
             )

    assert_eventually(fn -> render(view) =~ run_id end)
  end

  test "run detail renders timeline and allows rerun", %{conn: conn, version: version} do
    assert {:ok, run_id} =
             FavnOrchestrator.submit_asset_run({MyApp.Assets.Gold, :asset},
               manifest_version_id: version.manifest_version_id
             )

    assert {:ok, _terminal} = await_terminal(run_id)

    {:ok, view, html} = live(conn, ~p"/runs/#{run_id}")
    assert html =~ "Timeline"
    assert html =~ "Run created"

    view
    |> element("button", "Rerun")
    |> render_click()

    assert {path, _flash} = assert_redirect(view)
    assert String.starts_with?(path, "/runs/")
    refute path == "/runs/#{run_id}"
  end

  test "run detail can cancel an active run while subscribed", %{conn: conn, version: version} do
    run_id = TestFixtures.insert_running_run!(version)

    {:ok, view, html} = live(conn, ~p"/runs/#{run_id}")
    assert html =~ "Status:"
    assert html =~ "running"

    view
    |> element("button", "Cancel run")
    |> render_click()

    assert_eventually(
      fn ->
        rendered = render(view)
        rendered =~ "Run cancelled" and rendered =~ "status=cancelled"
      end,
      80
    )
  end

  defp await_terminal(run_id, attempts \\ 80)
  defp await_terminal(_run_id, 0), do: {:error, :timeout}

  defp await_terminal(run_id, attempts) do
    case FavnOrchestrator.get_run(run_id) do
      {:ok, run} when run.status in [:ok, :error, :cancelled, :timed_out] ->
        {:ok, run}

      _ ->
        Process.sleep(20)
        await_terminal(run_id, attempts - 1)
    end
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
end
