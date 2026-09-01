defmodule FavnView.RunnersLiveTest do
  use ExUnit.Case, async: false

  alias FavnView.Auth.Scope
  alias FavnView.RunnersLive

  @env_keys [:operator_runner_overview_fun, :operator_runner_session_tasks_fun]

  setup do
    previous = Map.new(@env_keys, &{&1, Application.get_env(:favn_view, &1)})

    on_exit(fn ->
      Enum.each(previous, fn
        {key, nil} -> Application.delete_env(:favn_view, key)
        {key, value} -> Application.put_env(:favn_view, key, value)
      end)
    end)

    :ok
  end

  test "changing the window reloads with the matching overlap bound" do
    test_pid = self()

    Application.put_env(:favn_view, :operator_runner_overview_fun, fn :operator_context, opts ->
      send(test_pid, {:overview, opts})
      {:ok, overview()}
    end)

    assert {:noreply, socket} =
             RunnersLive.handle_event("set_window", %{"scope" => "month"}, socket())

    assert socket.assigns.window == :month
    assert_received {:overview, opts}
    assert %DateTime{} = opts[:overlapping_after]
    assert DateTime.diff(DateTime.utc_now(), opts[:overlapping_after], :day) >= 29

    assert {:noreply, socket} =
             RunnersLive.handle_event("set_window", %{"scope" => "today"}, socket)

    assert socket.assigns.window == :today
    assert_received {:overview, opts}
    assert %DateTime{} = opts[:overlapping_after]
    assert DateTime.diff(DateTime.utc_now(), opts[:overlapping_after], :hour) <= 24

    assert {:noreply, socket} =
             RunnersLive.handle_event("set_window", %{"scope" => "all"}, socket)

    assert_received {:overview, opts}
    assert is_nil(opts[:overlapping_after])

    assert {:noreply, unchanged} =
             RunnersLive.handle_event("set_window", %{"scope" => "bogus"}, socket)

    assert unchanged.assigns.window == :all
    refute_received {:overview, _opts}
  end

  test "the state filter narrows locally without a reload" do
    Application.put_env(:favn_view, :operator_runner_overview_fun, fn :operator_context, _opts ->
      raise "state filtering must not reload the overview"
    end)

    assert {:noreply, socket} =
             RunnersLive.handle_event("set_state", %{"scope" => "crashed"}, socket())

    assert socket.assigns.state == :crashed

    assert {:noreply, unchanged} =
             RunnersLive.handle_event("set_state", %{"scope" => "bogus"}, socket)

    assert unchanged.assigns.state == :crashed
  end

  test "expanding a session fetches its workspace tasks once and collapses again" do
    test_pid = self()

    Application.put_env(:favn_view, :operator_runner_session_tasks_fun, fn :operator_context,
                                                                           opts ->
      send(test_pid, {:session_tasks, opts})
      {:ok, [%{task_id: "rt_failed"}]}
    end)

    params = %{
      "key" => "runner-a:41:2026-08-20T14:33:24Z",
      "instance" => "runner-a",
      "generation" => "41",
      "registered-at" => "2026-08-20T14:33:24Z",
      "ended-at" => "2026-08-20T14:36:36Z"
    }

    assert {:noreply, expanded} =
             RunnersLive.handle_event("toggle_session_tasks", params, socket())

    assert %{"runner-a:41:2026-08-20T14:33:24Z" => [%{task_id: "rt_failed"}]} =
             expanded.assigns.expanded

    assert_received {:session_tasks, opts}
    assert opts[:runner_instance_id] == "runner-a"
    assert opts[:session_generation] == 41
    assert opts[:registered_at] == ~U[2026-08-20 14:33:24Z]
    assert opts[:ended_at] == ~U[2026-08-20 14:36:36Z]

    assert {:noreply, collapsed} =
             RunnersLive.handle_event("toggle_session_tasks", params, expanded)

    assert collapsed.assigns.expanded == %{}
    refute_received {:session_tasks, _opts}
  end

  test "a failed task fetch renders as unavailable rather than crashing" do
    Application.put_env(:favn_view, :operator_runner_session_tasks_fun, fn :operator_context,
                                                                           _opts ->
      {:error, :forbidden}
    end)

    params = %{
      "key" => "runner-a:41:2026-08-20T14:33:24Z",
      "instance" => "runner-a",
      "generation" => "41",
      "registered-at" => "2026-08-20T14:33:24Z",
      "ended-at" => ""
    }

    assert {:noreply, socket} =
             RunnersLive.handle_event("toggle_session_tasks", params, socket())

    assert socket.assigns.expanded == %{"runner-a:41:2026-08-20T14:33:24Z" => :unavailable}
  end

  defp socket(overrides \\ []) do
    assigns =
      Enum.into(overrides, %{
        __changed__: %{},
        current_scope: %Scope{operator_context: :operator_context},
        overview: nil,
        loading: false,
        error: nil,
        window: :week,
        state: :all,
        expanded: %{},
        poll_ref: nil,
        nav_items: []
      })

    %Phoenix.LiveView.Socket{assigns: assigns}
  end

  defp overview do
    %{
      registry_status: :available,
      runners: [],
      runner_count: 0,
      busy_runner_count: 0,
      capacity: [],
      workspace_tasks: %{
        queued_count: 0,
        active_count: 0,
        failed_count: 0,
        failed_since: DateTime.utc_now(),
        oldest_queued_at: nil
      },
      sessions: [],
      totals: %{
        window_start: DateTime.utc_now(),
        window_end: DateTime.utc_now(),
        session_count: 0,
        awake_ms: 0,
        busy_ms: 0,
        idle_ms: 0
      },
      observed_at: DateTime.utc_now()
    }
  end
end
