defmodule FavnViewWeb.OperatorFlowLiveTest do
  use FavnViewWeb.ConnCase, async: false

  alias FavnOrchestrator
  alias FavnView.TestFixtures

  setup do
    version = TestFixtures.setup_orchestrator!()
    run_id = TestFixtures.insert_running_run!(version)
    {:ok, version: version, run_id: run_id}
  end

  test "dashboard to run detail supports cancel and rerun flows", %{
    conn: conn,
    version: version,
    run_id: run_id
  } do
    {:ok, _dashboard, dashboard_html} = live(conn, ~p"/")
    assert dashboard_html =~ run_id

    {:ok, run_view, run_html} = live(conn, ~p"/runs/#{run_id}")
    assert run_html =~ "Run #{run_id}"
    assert run_html =~ "running"

    run_view
    |> element("button", "Cancel run")
    |> render_click()

    assert_eventually(
      fn ->
        rendered = render(run_view)
        rendered =~ "status=cancelled" and rendered =~ "Run cancelled"
      end,
      80
    )

    assert {:ok, terminal_run_id} =
             FavnOrchestrator.submit_asset_run({MyApp.Assets.Gold, :asset},
               manifest_version_id: version.manifest_version_id
             )

    assert {:ok, _terminal} = await_terminal(terminal_run_id)

    {:ok, terminal_view, _terminal_html} = live(conn, ~p"/runs/#{terminal_run_id}")

    terminal_view
    |> element("button", "Rerun")
    |> render_click()

    assert {path, _flash} = assert_redirect(terminal_view)
    assert String.starts_with?(path, "/runs/")
    refute path == "/runs/#{terminal_run_id}"
  end

  defp assert_eventually(fun, attempts) when attempts > 0 do
    if fun.() do
      assert true
    else
      Process.sleep(25)
      assert_eventually(fun, attempts - 1)
    end
  end

  defp assert_eventually(_fun, 0), do: flunk("condition not met in time")

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
end
