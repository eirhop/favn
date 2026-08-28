defmodule FavnView.RunDetailLiveTest do
  use ExUnit.Case, async: false

  import Phoenix.LiveViewTest

  alias FavnOrchestrator.OperatorRunView.Asset
  alias FavnOrchestrator.OperatorRunView.Flow
  alias FavnOrchestrator.OperatorRunView.Header
  alias FavnOrchestrator.Persistence.Results.BackfillWindow
  alias FavnOrchestrator.Persistence.Results.RunWindowChoice
  alias FavnOrchestrator.Persistence.Results.RunWindowChoices
  alias FavnView.Auth.Scope
  alias FavnView.RunDetailLive
  alias FavnView.RunWindowRail

  setup do
    keys = [
      :operator_run_flow_fun,
      :operator_run_events_fun,
      :operator_run_windows_fun,
      :operator_backfill_windows_fun,
      :operator_execution_group_fun,
      :run_subscribe_fun,
      :run_unsubscribe_fun,
      :compare_read_timeout_ms
    ]

    previous = Map.new(keys, &{&1, Application.get_env(:favn_view, &1)})

    Application.put_env(:favn_view, :run_subscribe_fun, fn _context, run_id ->
      send(self(), {:subscribed, run_id})
      :ok
    end)

    on_exit(fn -> Enum.each(previous, fn {key, value} -> restore_env(key, value) end) end)
  end

  test "subscribes to only the selected run before issuing the exact Flow read" do
    caller = self()

    Application.put_env(:favn_view, :operator_run_flow_fun, fn :operator_context, "run-one" ->
      send(caller, :flow_read)
      {:ok, %{kind: :run, detail: flow("run-one", :running)}}
    end)

    assert {:ok, mounted} =
             RunDetailLive.mount(%{"run_id" => "run-one"}, %{}, connected_socket())

    assert_receive {:subscribed, "run-one"}
    assert_receive :flow_read
    assert mounted.assigns.run.found?
    assert mounted.assigns.run.total_asset_attempts == 2
    assert Enum.map(mounted.assigns.run.assets, & &1.asset_ref) == ["crm.orders", "crm.total"]
    assert MapSet.equal?(mounted.assigns.run_event_subscriptions, MapSet.new(["run-one"]))
    assert is_reference(mounted.assigns.fallback_poll_ref)
  end

  test "does not issue a read during disconnected rendering" do
    caller = self()

    Application.put_env(:favn_view, :operator_run_flow_fun, fn _context, _run_id ->
      send(caller, :unexpected_read)
      {:ok, %{kind: :run, detail: flow("run-one", :ok)}}
    end)

    assert {:ok, mounted} =
             RunDetailLive.mount(%{"run_id" => "run-one"}, %{}, disconnected_socket())

    refute_receive :unexpected_read
    assert mounted.assigns.run.initializing?
  end

  test "ignores unrelated persistence publications and run events" do
    caller = self()

    Application.put_env(:favn_view, :operator_run_flow_fun, fn _context, run_id ->
      send(caller, :flow_read)
      {:ok, %{kind: :run, detail: flow(run_id, :running)}}
    end)

    assert {:ok, mounted} =
             RunDetailLive.mount(%{"run_id" => "run-one"}, %{}, connected_socket())

    assert_receive :flow_read
    assert {:noreply, unchanged} = RunDetailLive.handle_info(:favn_persistence_published, mounted)

    assert {:noreply, unrelated} =
             RunDetailLive.handle_info(
               {:favn_run_event, %{run_id: "run-two", sequence: 99}},
               unchanged
             )

    refute_receive :flow_read
    assert is_nil(unrelated.assigns.refresh_timer_ref)
  end

  test "coalesces a burst of selected-run events into one refresh" do
    caller = self()

    Application.put_env(:favn_view, :operator_run_flow_fun, fn _context, run_id ->
      send(caller, :flow_read)
      {:ok, %{kind: :run, detail: flow(run_id, :running)}}
    end)

    assert {:ok, mounted} =
             RunDetailLive.mount(%{"run_id" => "run-one"}, %{}, connected_socket())

    assert_receive :flow_read
    fallback_ref = mounted.assigns.fallback_poll_ref

    {:noreply, first} =
      RunDetailLive.handle_info(
        {:favn_run_event, %{run_id: "run-one", sequence: 3}},
        mounted
      )

    {:noreply, second} =
      RunDetailLive.handle_info(
        {:favn_run_event, %{run_id: "run-one", sequence: 4}},
        first
      )

    assert is_reference(first.assigns.refresh_timer_ref)
    assert is_nil(first.assigns.fallback_poll_ref)
    assert second.assigns.refresh_timer_ref == first.assigns.refresh_timer_ref
    assert second.assigns.pending_run_event_sequences == %{"run-one" => 4}

    assert {:noreply, unchanged} =
             RunDetailLive.handle_info({:poll_run, fallback_ref}, second)

    refute_receive :flow_read
    assert unchanged.assigns.refresh_timer_ref == second.assigns.refresh_timer_ref
  end

  test "keeps the last successful Flow when a live refresh fails" do
    Application.put_env(:favn_view, :operator_run_flow_fun, fn _context, run_id ->
      {:ok, %{kind: :run, detail: flow(run_id, :running)}}
    end)

    assert {:ok, mounted} =
             RunDetailLive.mount(%{"run_id" => "run-one"}, %{}, connected_socket())

    Application.put_env(:favn_view, :operator_run_flow_fun, fn _context, _run_id ->
      {:error, :unavailable}
    end)

    assert {:noreply, refreshed} =
             RunDetailLive.handle_info(
               {:poll_run, mounted.assigns.fallback_poll_ref},
               mounted
             )

    assert refreshed.assigns.run.found?
    assert length(refreshed.assigns.run.assets) == 2
    assert refreshed.assigns.run.refresh_error == "Run could not be loaded"
  end

  test "loads bounded events only after switching to Events" do
    Application.put_env(:favn_view, :operator_run_flow_fun, fn _context, run_id ->
      {:ok, %{kind: :run, detail: flow(run_id, :ok)}}
    end)

    Application.put_env(:favn_view, :operator_run_events_fun, fn _context, run_id ->
      {:ok,
       %{
         kind: :run,
         header: header(run_id, :ok),
         events: [
           %{
             sequence: 2,
             occurred_at: ~U[2026-08-23 10:00:02Z],
             event_type: :step_finished,
             status: :ok,
             asset_ref: "crm.orders",
             summary: "Wrote 42 rows"
           }
         ]
       }}
    end)

    assert {:ok, mounted} =
             RunDetailLive.mount(%{"run_id" => "run-one"}, %{}, connected_socket())

    assert {:noreply, events_socket} =
             RunDetailLive.handle_params(
               %{"run_id" => "run-one", "view" => "events"},
               "",
               mounted
             )

    assert events_socket.assigns.active_mode == :events
    assert [%{sequence: 2, asset: "crm.orders"}] = events_socket.assigns.run.events
    assert events_socket.assigns.run.assets == []
  end

  test "direct Events connection never loads Flow" do
    caller = self()

    Application.put_env(:favn_view, :operator_run_flow_fun, fn _context, _run_id ->
      send(caller, :unexpected_flow_read)
      {:error, :unexpected_flow_read}
    end)

    Application.put_env(:favn_view, :operator_run_events_fun, fn _context, run_id ->
      send(caller, :events_read)
      {:ok, %{kind: :run, header: header(run_id, :ok), events: []}}
    end)

    assert {:ok, mounted} =
             RunDetailLive.mount(
               %{"run_id" => "run-one", "view" => "events"},
               %{},
               connected_socket()
             )

    assert_receive :events_read
    refute_receive :unexpected_flow_read
    assert mounted.assigns.active_mode == :events
    assert mounted.assigns.run.assets == []
  end

  test "failed Events refresh retains the pending sequence and retries" do
    Application.put_env(:favn_view, :operator_run_events_fun, fn _context, run_id ->
      {:ok, %{kind: :run, header: header(run_id, :running), events: []}}
    end)

    assert {:ok, mounted} =
             RunDetailLive.mount(
               %{"run_id" => "run-one", "view" => "events"},
               %{},
               connected_socket()
             )

    assert {:noreply, pending} =
             RunDetailLive.handle_info(
               {:favn_run_event, %{run_id: "run-one", sequence: 3}},
               mounted
             )

    first_timer = pending.assigns.refresh_timer_ref

    Application.put_env(:favn_view, :operator_run_events_fun, fn _context, _run_id ->
      {:error, :unavailable}
    end)

    assert {:noreply, retrying} =
             RunDetailLive.handle_info({:refresh_run, first_timer}, pending)

    assert retrying.assigns.pending_run_event_sequences == %{"run-one" => 3}
    assert is_reference(retrying.assigns.refresh_timer_ref)
    refute retrying.assigns.refresh_timer_ref == first_timer
    assert retrying.assigns.run.found?
    assert retrying.assigns.run.refresh_error == "Run could not be loaded"
  end

  test "a run outside a backfill never reads its windows" do
    caller = self()

    Application.put_env(:favn_view, :operator_run_flow_fun, fn _context, run_id ->
      {:ok, %{kind: :run, detail: flow(run_id, :ok)}}
    end)

    Application.put_env(:favn_view, :operator_run_windows_fun, fn _context, _run_id ->
      send(caller, :unexpected_window_read)
      {:ok, %RunWindowChoices{overflow?: false, items: []}}
    end)

    assert {:ok, mounted} =
             RunDetailLive.mount(%{"run_id" => "run-one"}, %{}, connected_socket())

    refute_receive :unexpected_window_read
    assert is_nil(mounted.assigns.windows)
    assert is_nil(mounted.assigns.rail)
  end

  test "a backfill run loads its rail eagerly and restricts selection to it" do
    Application.put_env(:favn_view, :operator_run_flow_fun, fn _context, run_id ->
      {:ok, %{kind: :run, detail: backfill_flow(run_id, :ok)}}
    end)

    Application.put_env(:favn_view, :operator_run_windows_fun, fn _context, _run_id ->
      {:ok, window_choices()}
    end)

    assert {:ok, mounted} =
             RunDetailLive.mount(%{"run_id" => "run-one"}, %{}, connected_socket())

    # Eager: no button press, no second event.
    assert Enum.map(mounted.assigns.windows, & &1.run_id) == ["run-one", "run-two"]
    assert Enum.map(mounted.assigns.rail.cells, & &1.run_id) == ["run-one", "run-two"]
    assert [%{selected?: true}, %{selected?: false}] = mounted.assigns.rail.cells

    assert {:noreply, rejected} =
             RunDetailLive.handle_event("select_window", %{"run_id" => "run-other"}, mounted)

    assert rejected.assigns.flash["error"] == "That window run is not available"

    # Selecting the open window is a no-op, not an error: the rail is a
    # calendar, so its current cell is still a cell.
    assert {:noreply, same} =
             RunDetailLive.handle_event("select_window", %{"run_id" => "run-one"}, mounted)

    assert is_nil(same.redirected)
    refute same.assigns.flash["error"]

    assert {:noreply, patching} =
             RunDetailLive.handle_event("select_window", %{"run_id" => "run-two"}, mounted)

    # Patch, not navigate: the page keeps its process and its subscription.
    assert {:live, :patch, %{to: "/runs/run-two?view=flow"}} = patching.redirected
  end

  test "arrow keys step between adjacent window runs and stop at the ends" do
    Application.put_env(:favn_view, :operator_run_flow_fun, fn _context, run_id ->
      {:ok, %{kind: :run, detail: backfill_flow(run_id, :ok)}}
    end)

    Application.put_env(:favn_view, :operator_run_windows_fun, fn _context, _run_id ->
      {:ok, window_choices()}
    end)

    assert {:ok, mounted} =
             RunDetailLive.mount(%{"run_id" => "run-one"}, %{}, connected_socket())

    assert {:noreply, forward} =
             RunDetailLive.handle_event("step_window", %{"key" => "ArrowRight"}, mounted)

    assert {:live, :patch, %{to: "/runs/run-two?view=flow"}} = forward.redirected

    # run-one is the first cell, so there is nowhere to step back to.
    assert {:noreply, backward} =
             RunDetailLive.handle_event("step_window", %{"key" => "ArrowLeft"}, mounted)

    assert is_nil(backward.redirected)
    refute backward.assigns.flash["error"]

    # Any other key is ignored rather than treated as a step.
    assert {:noreply, ignored} =
             RunDetailLive.handle_event("step_window", %{"key" => "Enter"}, mounted)

    assert is_nil(ignored.redirected)
  end

  test "selecting a window resets the page and moves the subscription exactly once" do
    caller = self()

    Application.put_env(:favn_view, :run_unsubscribe_fun, fn _context, run_id ->
      send(caller, {:unsubscribed, run_id})
      :ok
    end)

    Application.put_env(:favn_view, :operator_run_flow_fun, fn _context, run_id ->
      {:ok, %{kind: :run, detail: backfill_flow(run_id, :ok)}}
    end)

    Application.put_env(:favn_view, :operator_run_windows_fun, fn _context, _run_id ->
      {:ok, window_choices()}
    end)

    assert {:ok, mounted} =
             RunDetailLive.mount(%{"run_id" => "run-one"}, %{}, connected_socket())

    assert_receive {:subscribed, "run-one"}

    assert {:noreply, switched} =
             RunDetailLive.handle_params(%{"run_id" => "run-two"}, "", mounted)

    assert_receive {:unsubscribed, "run-one"}
    assert_receive {:subscribed, "run-two"}
    refute_receive {:unsubscribed, "run-two"}

    assert switched.assigns.run_id == "run-two"
    assert MapSet.equal?(switched.assigns.run_event_subscriptions, MapSet.new(["run-two"]))

    # The rail follows the newly selected run rather than the old one.
    assert [%{selected?: false}, %{selected?: true}] = switched.assigns.rail.cells
  end

  test "a failed window read hides the rail and leaves the run page working" do
    Application.put_env(:favn_view, :operator_run_flow_fun, fn _context, run_id ->
      {:ok, %{kind: :run, detail: backfill_flow(run_id, :ok)}}
    end)

    Application.put_env(:favn_view, :operator_run_windows_fun, fn _context, _run_id ->
      {:error, :unavailable}
    end)

    assert {:ok, mounted} =
             RunDetailLive.mount(%{"run_id" => "run-one"}, %{}, connected_socket())

    assert is_nil(mounted.assigns.rail)
    assert mounted.assigns.run.found?
    assert mounted.assigns.run.total_asset_attempts == 2
  end

  test "a non-terminal backfill keeps polling even when every loaded run is terminal" do
    Application.put_env(:favn_view, :operator_run_flow_fun, fn _context, run_id ->
      {:ok, %{kind: :run, detail: backfill_flow(run_id, :ok)}}
    end)

    Application.put_env(:favn_view, :operator_run_windows_fun, fn _context, _run_id ->
      {:ok, %{window_choices() | backfill_status: :running}}
    end)

    assert {:ok, running} =
             RunDetailLive.mount(%{"run_id" => "run-one"}, %{}, connected_socket())

    refute running.assigns.run.active?
    assert running.assigns.rail.in_progress?
    assert is_reference(running.assigns.fallback_poll_ref)

    Application.put_env(:favn_view, :operator_run_windows_fun, fn _context, _run_id ->
      {:ok, %{window_choices() | backfill_status: :completed}}
    end)

    assert {:ok, finished} =
             RunDetailLive.mount(%{"run_id" => "run-one"}, %{}, connected_socket())

    refute finished.assigns.rail.in_progress?
    assert is_nil(finished.assigns.fallback_poll_ref)
  end

  test "a terminal backfill parent renders group progress and keeps refreshing while children run" do
    caller = self()

    Application.put_env(:favn_view, :operator_run_windows_fun, fn _context, _run_id ->
      {:ok, %RunWindowChoices{overflow?: false, items: [], backfill_status: :running}}
    end)

    Application.put_env(:favn_view, :operator_run_flow_fun, fn _context, run_id ->
      parent_header = %{
        header(run_id, :ok)
        | submit_kind: :backfill_asset,
          trigger_type: :backfill
      }

      {:ok, %{kind: :run, detail: %Flow{header: parent_header, assets: [], overflow?: false}}}
    end)

    Application.put_env(
      :favn_view,
      :operator_execution_group_fun,
      fn :operator_context, "run-parent", [limit: 1] ->
        send(caller, :group_read)

        {:ok,
         %{
           overview: %{
             status: :running,
             active?: true,
             started_at: DateTime.add(DateTime.utc_now(), -60, :second),
             finished_at: nil,
             total_asset_attempts: 3,
             completed_asset_attempts: 1,
             succeeded_asset_attempts: 1,
             skipped_asset_attempts: 0,
             failed_asset_attempts: 0,
             running_asset_attempts: 1,
             queued_asset_attempts: 1,
             planned_asset_attempts: 0,
             summary_totals: %{
               windows: %{
                 total: 3,
                 completed: 1,
                 succeeded: 1,
                 failed: 0,
                 running: 1,
                 queued: 1,
                 cancelled: 0
               }
             }
           }
         }}
      end
    )

    assert {:ok, mounted} =
             RunDetailLive.mount(%{"run_id" => "run-parent"}, %{}, connected_socket())

    assert_receive :group_read
    assert mounted.assigns.run.backfill_parent?
    assert mounted.assigns.run.status == "Running"
    assert mounted.assigns.run.total_windows == 3
    assert mounted.assigns.run.total_asset_attempts == 3
    assert mounted.assigns.run.elapsed_duration != "4.0 s"
    assert mounted.assigns.run.elapsed_duration != "-"
    assert is_reference(mounted.assigns.fallback_poll_ref)

    html =
      render_component(
        &RunDetailLive.render/1,
        Map.put(mounted.assigns, :operator_workspaces, [])
      )

    assert html =~ ~s(data-testid="backfill-parent-explanation")
    assert html =~ "Asset work runs in the windows"
    refute html =~ "No asset work yet"

    assert {:noreply, refreshed} =
             RunDetailLive.handle_info(
               {:poll_run, mounted.assigns.fallback_poll_ref},
               mounted
             )

    assert_receive :group_read
    assert refreshed.assigns.run.active?
  end

  test "a backfill parent opens its earliest window, once" do
    Application.put_env(:favn_view, :operator_run_windows_fun, fn _context, _run_id ->
      {:ok, window_choices()}
    end)

    Application.put_env(:favn_view, :operator_run_flow_fun, fn _context, run_id ->
      parent_header = %{
        header(run_id, :ok)
        | submit_kind: :backfill_asset,
          trigger_type: :backfill
      }

      {:ok, %{kind: :run, detail: %Flow{header: parent_header, assets: [], overflow?: false}}}
    end)

    Application.put_env(:favn_view, :operator_execution_group_fun, fn _context, _run_id, _opts ->
      {:error, :unavailable}
    end)

    assert {:ok, mounted} =
             RunDetailLive.mount(%{"run_id" => "run-parent"}, %{}, connected_socket())

    # LiveView raises on a live patch issued while mounting, and the rail is
    # built inside the mount's first read, so the mount itself must not redirect.
    assert is_nil(mounted.redirected)

    # The parent runs no asset work of its own, so landing on it and finding
    # nothing drawn is a click the operator always has to make.
    assert {:noreply, patched} =
             RunDetailLive.handle_params(
               %{"run_id" => "run-parent"},
               "http://localhost/runs/run-parent",
               mounted
             )

    assert {:live, :patch, %{to: to}} = patched.redirected
    assert to =~ "/runs/run-one"

    # Only on arrival. A later refresh must not drag the page back off a window
    # the operator chose, and neither must coming back to the parent on purpose.
    # LiveView clears `redirected` between messages, so the test does too.
    settled = %{patched | redirected: nil}

    {:noreply, refreshed} =
      RunDetailLive.handle_info({:poll_run, settled.assigns.fallback_poll_ref}, settled)

    assert is_nil(refreshed.redirected)

    assert {:noreply, again} =
             RunDetailLive.handle_params(
               %{"run_id" => "run-parent"},
               "http://localhost/runs/run-parent",
               settled
             )

    assert is_nil(again.redirected)
  end

  test "a backfill parent opens the first window run as soon as one exists" do
    {:ok, reads} = Agent.start_link(fn -> 0 end)

    Application.put_env(:favn_view, :operator_run_windows_fun, fn _context, _run_id ->
      case Agent.get_and_update(reads, &{&1 + 1, &1 + 1}) do
        1 -> {:ok, %RunWindowChoices{overflow?: false, items: [], backfill_status: :running}}
        _later -> {:ok, window_choices()}
      end
    end)

    parent_flow_fun()

    assert {:ok, mounted} =
             RunDetailLive.mount(%{"run_id" => "run-parent"}, %{}, connected_socket())

    assert {:noreply, arrived} =
             RunDetailLive.handle_params(
               %{"run_id" => "run-parent"},
               "http://localhost/runs/run-parent",
               mounted
             )

    # A backfill submitted a moment ago has planned its windows and started none
    # of them, so there is nothing to open yet.
    assert is_nil(arrived.redirected)

    # The window list is read at most once per fallback interval. Age the last
    # read the way that interval would, so the poll below actually re-reads.
    aged =
      Phoenix.Component.assign(
        %{arrived | redirected: nil},
        :windows_read_at,
        arrived.assigns.windows_read_at - 60_000
      )

    assert {:noreply, followed} =
             RunDetailLive.handle_info({:poll_run, aged.assigns.fallback_poll_ref}, aged)

    # Arrival is not one moment. Waiting on a parent that has just produced its
    # first window used to leave the operator on a page with nothing drawn and a
    # cell to click; now the page follows the run it was already watching.
    assert {:live, :patch, %{to: to}} = followed.redirected
    assert to =~ "/runs/run-one"
  end

  test "a combined run keeps saying what it covered after its window opens" do
    combined =
      Enum.map(1..24, fn index ->
        %RunWindowChoice{
          run_id: "run-child",
          window_start_at: combined_month(index - 1),
          window_end_at: combined_month(index),
          status: :succeeded,
          kind: :month,
          timezone: "Etc/UTC"
        }
      end)

    Application.put_env(:favn_view, :operator_run_windows_fun, fn _context, _run_id ->
      {:ok, %RunWindowChoices{overflow?: false, items: combined, backfill_status: :completed}}
    end)

    parent_flow_fun(fn run_id -> backfill_flow(run_id, :ok).header end)

    assert {:ok, mounted} =
             RunDetailLive.mount(%{"run_id" => "run-parent"}, %{}, connected_socket())

    assert mounted.assigns.run.combined_window == %{
             label: "Jan 2023 – Dec 2024",
             window_count: 24,
             kind: :month
           }

    assert {:noreply, patched} =
             RunDetailLive.handle_params(
               %{"run_id" => "run-parent"},
               "http://localhost/runs/run-parent",
               mounted
             )

    assert {:live, :patch, %{to: to}} = patched.redirected
    assert to =~ "/runs/run-child"

    # Opening the window re-reads the run, and the run map a read returns carries
    # no combined window. The window list is not re-read in the same breath — it
    # has a cadence of its own — so a coverage line derived once at rail-build
    # time vanished exactly when the operator arrived where it mattered.
    assert {:noreply, opened} =
             RunDetailLive.handle_params(
               %{"run_id" => "run-child"},
               "http://localhost/runs/run-child",
               %{patched | redirected: nil}
             )

    assert opened.assigns.run.combined_window.label == "Jan 2023 – Dec 2024"
    assert opened.assigns.run.combined_window.window_count == 24
  end

  test "a transient group read failure keeps the last truthful parent progress" do
    {:ok, reads} = Agent.start_link(fn -> 0 end)

    Application.put_env(:favn_view, :operator_run_windows_fun, fn _context, _run_id ->
      {:ok, %RunWindowChoices{overflow?: false, items: [], backfill_status: :running}}
    end)

    Application.put_env(:favn_view, :operator_run_flow_fun, fn _context, run_id ->
      parent_header = %{
        header(run_id, :ok)
        | submit_kind: :backfill_asset,
          trigger_type: :backfill
      }

      {:ok, %{kind: :run, detail: %Flow{header: parent_header, assets: [], overflow?: false}}}
    end)

    Application.put_env(:favn_view, :operator_execution_group_fun, fn _, _, _ ->
      case Agent.get_and_update(reads, fn count -> {count, count + 1} end) do
        0 ->
          {:ok,
           %{
             overview: %{
               status: :running,
               active?: true,
               total_asset_attempts: 4,
               completed_asset_attempts: 2,
               succeeded_asset_attempts: 2,
               skipped_asset_attempts: 0,
               failed_asset_attempts: 0,
               running_asset_attempts: 1,
               queued_asset_attempts: 1,
               planned_asset_attempts: 0,
               summary_totals: %{
                 windows: %{
                   total: 4,
                   completed: 2,
                   succeeded: 2,
                   failed: 0,
                   running: 1,
                   queued: 1,
                   cancelled: 0
                 }
               }
             }
           }}

        _later ->
          {:error, :temporarily_unavailable}
      end
    end)

    assert {:ok, mounted} =
             RunDetailLive.mount(%{"run_id" => "run-parent"}, %{}, connected_socket())

    assert mounted.assigns.run.status == "Running"
    assert mounted.assigns.run.total_windows == 4

    assert {:noreply, refreshed} =
             RunDetailLive.handle_info(
               {:poll_run, mounted.assigns.fallback_poll_ref},
               mounted
             )

    assert refreshed.assigns.run.status == "Running"
    assert refreshed.assigns.run.total_windows == 4
    assert refreshed.assigns.run.total_asset_attempts == 4
    assert refreshed.assigns.run.group_error =~ "showing the last update"
  end

  test "a one-window backfill parent offers that single child run in its rail" do
    Application.put_env(:favn_view, :operator_run_flow_fun, fn _context, run_id ->
      parent_header = %{
        header(run_id, :ok)
        | submit_kind: :backfill_asset,
          trigger_type: :backfill
      }

      {:ok, %{kind: :run, detail: %Flow{header: parent_header, assets: [], overflow?: false}}}
    end)

    Application.put_env(:favn_view, :operator_execution_group_fun, fn _, _, _ ->
      {:ok,
       %{
         overview: %{
           status: :ok,
           active?: false,
           total_asset_attempts: 1,
           completed_asset_attempts: 1,
           succeeded_asset_attempts: 1,
           skipped_asset_attempts: 0,
           failed_asset_attempts: 0,
           running_asset_attempts: 0,
           queued_asset_attempts: 0,
           planned_asset_attempts: 0,
           summary_totals: %{
             windows: %{
               total: 1,
               completed: 1,
               succeeded: 1,
               failed: 0,
               running: 0,
               queued: 0,
               cancelled: 0
             }
           }
         }
       }}
    end)

    Application.put_env(:favn_view, :operator_run_windows_fun, fn _, "run-parent" ->
      {:ok,
       %RunWindowChoices{
         overflow?: false,
         items: [
           %RunWindowChoice{
             run_id: "run-child",
             window_start_at: ~U[2026-07-01 00:00:00Z],
             window_end_at: ~U[2026-08-01 00:00:00Z]
           }
         ]
       }}
    end)

    assert {:ok, mounted} =
             RunDetailLive.mount(%{"run_id" => "run-parent"}, %{}, connected_socket())

    html =
      render_component(
        &RunDetailLive.render/1,
        Map.put(mounted.assigns, :operator_workspaces, [])
      )

    assert html =~ ~s(data-testid="window-rail")
    assert html =~ ~s(phx-value-run_id="run-child")
    refute html =~ ~s(data-testid="run-window-selector")
  end

  test "keeps a durable pre-admission submission visible" do
    Application.put_env(:favn_view, :operator_run_flow_fun, fn _context, "run-queued" ->
      now = ~U[2026-08-23 10:00:00Z]

      {:ok,
       %{
         kind: :submission,
         submission: %{
           run_id: "run-queued",
           status: :queued,
           status_label: "Queued",
           status_tone: :info,
           active?: true,
           target_kind: "pipeline",
           target_id: "crm_reference",
           attempt: 0,
           enqueued_at: now,
           updated_at: now,
           terminal_at: nil,
           failure: nil
         }
       }}
    end)

    assert {:ok, mounted} =
             RunDetailLive.mount(%{"run_id" => "run-queued"}, %{}, connected_socket())

    assert mounted.assigns.run.submission?
    assert mounted.assigns.run.status == "Queued"
  end

  # A backfill window run, not the parent: its root is the parent's id, so the
  # page takes the rail path rather than the execution-group path.
  defp backfill_flow(run_id, status) do
    detail = flow(run_id, status)

    %{
      detail
      | header: %{
          detail.header
          | submit_kind: :backfill_pipeline,
            root_run_id: "run-parent",
            trigger_type: :backfill
        }
    }
  end

  # A parent whose own read says "backfill", with an optional override for the
  # child runs its rail leads to.
  defp parent_flow_fun(child \\ nil) do
    Application.put_env(:favn_view, :operator_run_flow_fun, fn _context, run_id ->
      header =
        if run_id == "run-parent" or is_nil(child) do
          %{header(run_id, :ok) | submit_kind: :backfill_asset, trigger_type: :backfill}
        else
          child.(run_id)
        end

      {:ok, %{kind: :run, detail: %Flow{header: header, assets: [], overflow?: false}}}
    end)

    Application.put_env(:favn_view, :operator_execution_group_fun, fn _context, _run_id, _opts ->
      {:error, :unavailable}
    end)
  end

  defp combined_month(offset) do
    DateTime.new!(
      Date.new!(2023 + div(offset, 12), rem(offset, 12) + 1, 1),
      ~T[00:00:00],
      "Etc/UTC"
    )
  end

  defp window_choices do
    %RunWindowChoices{
      overflow?: false,
      backfill_status: :completed,
      items: [
        %RunWindowChoice{
          run_id: "run-one",
          window_start_at: ~U[2026-07-01 00:00:00Z],
          window_end_at: ~U[2026-08-01 00:00:00Z],
          status: :succeeded,
          kind: :month,
          timezone: "Etc/UTC"
        },
        %RunWindowChoice{
          run_id: "run-two",
          window_start_at: ~U[2026-08-01 00:00:00Z],
          window_end_at: ~U[2026-09-01 00:00:00Z],
          status: :succeeded,
          kind: :month,
          timezone: "Etc/UTC"
        }
      ]
    }
  end

  describe "why a backfill's windows failed" do
    setup do
      Application.put_env(:favn_view, :operator_run_flow_fun, fn _context, run_id ->
        parent_header = %{
          header(run_id, :error)
          | submit_kind: :backfill_pipeline,
            trigger_type: :backfill
        }

        {:ok, %{kind: :run, detail: %Flow{header: parent_header, assets: [], overflow?: false}}}
      end)

      Application.put_env(:favn_view, :operator_execution_group_fun, fn _, _, _ ->
        {:ok, %{overview: failed_overview(31)}}
      end)

      :ok
    end

    test "reads the ledger for a parent whose windows failed, and groups by reason" do
      caller = self()

      Application.put_env(:favn_view, :operator_run_windows_fun, fn _context, _run_id ->
        {:ok, runless_choices("bf-one")}
      end)

      Application.put_env(:favn_view, :operator_backfill_windows_fun, fn _context,
                                                                         backfill_id,
                                                                         opts ->
        send(caller, {:ledger_read, backfill_id, Keyword.get(opts, :status)})
        {:ok, failed_window_page(31, "invalid_backfill_pipeline_identity")}
      end)

      assert {:ok, mounted} =
               RunDetailLive.mount(%{"run_id" => "run-parent"}, %{}, connected_socket())

      assert_receive {:ledger_read, "bf-one", :failed}

      # Thirty-one windows failing for one cause is one fact, not thirty-one.
      assert [group] = mounted.assigns.window_failures
      assert group.reason == "invalid_backfill_pipeline_identity"
      assert group.window_count == 31
      assert group.run_count == 0
      refute mounted.assigns.window_failures_overflow?
      assert is_nil(mounted.assigns.window_failures_error)
    end

    test "does not read the ledger when no window failed" do
      caller = self()

      Application.put_env(:favn_view, :operator_execution_group_fun, fn _, _, _ ->
        {:ok, %{overview: failed_overview(0)}}
      end)

      Application.put_env(:favn_view, :operator_run_windows_fun, fn _context, _run_id ->
        {:ok, runless_choices("bf-one")}
      end)

      Application.put_env(:favn_view, :operator_backfill_windows_fun, fn _, _, _ ->
        send(caller, :unexpected_ledger_read)
        {:ok, failed_window_page(0, "never")}
      end)

      assert {:ok, mounted} =
               RunDetailLive.mount(%{"run_id" => "run-parent"}, %{}, connected_socket())

      refute_receive :unexpected_ledger_read
      assert is_nil(mounted.assigns.window_failures)
    end

    test "does not read the ledger when the window read yielded no backfill id" do
      caller = self()

      Application.put_env(:favn_view, :operator_run_windows_fun, fn _context, _run_id ->
        {:ok, %RunWindowChoices{overflow?: false, items: [], backfill_status: :failed}}
      end)

      Application.put_env(:favn_view, :operator_backfill_windows_fun, fn _, _, _ ->
        send(caller, :unexpected_ledger_read)
        {:ok, failed_window_page(1, "never")}
      end)

      assert {:ok, mounted} =
               RunDetailLive.mount(%{"run_id" => "run-parent"}, %{}, connected_socket())

      refute_receive :unexpected_ledger_read
      assert is_nil(mounted.assigns.window_failures)
    end

    test "a failed ledger read states what is missing and keeps polling for it" do
      Application.put_env(:favn_view, :operator_run_windows_fun, fn _context, _run_id ->
        {:ok, runless_choices("bf-one")}
      end)

      Application.put_env(:favn_view, :operator_backfill_windows_fun, fn _, _, _ ->
        {:error, :temporarily_unavailable}
      end)

      assert {:ok, mounted} =
               RunDetailLive.mount(%{"run_id" => "run-parent"}, %{}, connected_socket())

      assert is_nil(mounted.assigns.window_failures)
      assert mounted.assigns.window_failures_error =~ "could not be loaded"

      # The run and the backfill are both terminal, so without the owed read this
      # page would have no cycle left in which to try again.
      assert is_reference(mounted.assigns.fallback_poll_ref)
    end

    test "a failed re-read drops the truncation marker with the rows it described" do
      {:ok, reads} = Agent.start_link(fn -> 0 end)

      Application.put_env(:favn_view, :operator_run_windows_fun, fn _context, _run_id ->
        {:ok, runless_choices("bf-one")}
      end)

      Application.put_env(:favn_view, :operator_backfill_windows_fun, fn _, _, _ ->
        case Agent.get_and_update(reads, fn count -> {count, count + 1} end) do
          0 -> {:ok, %{failed_window_page(2, "no_runner") | has_more?: true}}
          _later -> {:error, :temporarily_unavailable}
        end
      end)

      assert {:ok, mounted} =
               RunDetailLive.mount(%{"run_id" => "run-parent"}, %{}, connected_socket())

      assert mounted.assigns.window_failures_overflow?

      aged = put_in(mounted.assigns.windows_read_at, mounted.assigns.windows_read_at - 60_000)

      {:noreply, refreshed} =
        RunDetailLive.handle_info({:poll_run, aged.assigns.fallback_poll_ref}, aged)

      # The marker describes a page of rows. Keeping it after dropping them
      # would leave the panel calling a list it no longer holds truncated.
      assert is_nil(refreshed.assigns.window_failures)
      refute refreshed.assigns.window_failures_overflow?
      assert refreshed.assigns.window_failures_error
    end

    test "a ledger error does not outlive the reason to read the ledger" do
      {:ok, reads} = Agent.start_link(fn -> 0 end)

      Application.put_env(:favn_view, :operator_run_windows_fun, fn _context, _run_id ->
        {:ok, runless_choices("bf-one")}
      end)

      # The backfill's failed count drops to zero after the first cycle, which
      # closes the gate. Without clearing, the error from cycle one would keep
      # the fallback poll alive forever for a read that will never be issued.
      Application.put_env(:favn_view, :operator_execution_group_fun, fn _, _, _ ->
        case Agent.get_and_update(reads, fn count -> {count, count + 1} end) do
          0 -> {:ok, %{overview: failed_overview(31)}}
          _later -> {:ok, %{overview: failed_overview(0)}}
        end
      end)

      Application.put_env(:favn_view, :operator_backfill_windows_fun, fn _, _, _ ->
        {:error, :temporarily_unavailable}
      end)

      assert {:ok, mounted} =
               RunDetailLive.mount(%{"run_id" => "run-parent"}, %{}, connected_socket())

      assert mounted.assigns.window_failures_error

      aged = put_in(mounted.assigns.windows_read_at, mounted.assigns.windows_read_at - 60_000)

      {:noreply, refreshed} =
        RunDetailLive.handle_info({:poll_run, aged.assigns.fallback_poll_ref}, aged)

      assert is_nil(refreshed.assigns.window_failures_error)
      assert is_nil(refreshed.assigns.window_failures)
      assert is_nil(refreshed.assigns.fallback_poll_ref)
    end

    test "a ledger read that recovers replaces the warning with the reasons" do
      {:ok, reads} = Agent.start_link(fn -> 0 end)

      Application.put_env(:favn_view, :operator_run_windows_fun, fn _context, _run_id ->
        {:ok, runless_choices("bf-one")}
      end)

      Application.put_env(:favn_view, :operator_backfill_windows_fun, fn _, _, _ ->
        case Agent.get_and_update(reads, fn count -> {count, count + 1} end) do
          0 -> {:error, :temporarily_unavailable}
          _later -> {:ok, failed_window_page(2, "no_runner_available")}
        end
      end)

      assert {:ok, mounted} =
               RunDetailLive.mount(%{"run_id" => "run-parent"}, %{}, connected_socket())

      assert mounted.assigns.window_failures_error

      # The ledger read rides the window read's cadence, so the retry happens on
      # the first poll after the fallback interval rather than on the next event.
      aged = put_in(mounted.assigns.windows_read_at, mounted.assigns.windows_read_at - 60_000)

      {:noreply, refreshed} =
        RunDetailLive.handle_info({:poll_run, aged.assigns.fallback_poll_ref}, aged)

      assert is_nil(refreshed.assigns.window_failures_error)

      assert [%{reason: "no_runner_available", window_count: 2}] =
               refreshed.assigns.window_failures
    end
  end

  describe "flow reading controls" do
    setup do
      Application.put_env(:favn_view, :operator_run_flow_fun, fn _context, run_id ->
        {:ok, %{kind: :run, detail: flow(run_id, :running)}}
      end)

      {:ok, mounted} = RunDetailLive.mount(%{"run_id" => "run-one"}, %{}, connected_socket())
      %{socket: mounted}
    end

    test "the chart is built from the loaded rows and keeps its raw timing", %{socket: socket} do
      chart = socket.assigns.run.chart

      assert chart.lane_count == 2
      assert chart.ghost_count == 1
      assert Enum.map(chart.bands, & &1.id) == ["stage-0", "stage-1"]

      # The row keeps both, so the table can print and the chart can measure.
      assert [%{started_at: %DateTime{}, started_label: label} | _] = socket.assigns.run.assets
      assert is_binary(label)
    end

    test "a status filter narrows the chart and nothing else", %{socket: socket} do
      {:noreply, filtered} =
        RunDetailLive.handle_event("toggle_flow_filter", %{"outcome" => "running"}, socket)

      assert filtered.assigns.flow_filter == [:running]
      assert filtered.assigns.run.chart.lane_count == 1

      # The run's own rows and counts are untouched: only the drawing narrowed.
      assert length(filtered.assigns.run.assets) == 2
      assert filtered.assigns.run.total_asset_attempts == 2

      {:noreply, cleared} =
        RunDetailLive.handle_event("toggle_flow_filter", %{"outcome" => "running"}, filtered)

      assert cleared.assigns.flow_filter == []
      assert cleared.assigns.run.chart.lane_count == 2
    end

    test "an unknown filter or sort is ignored rather than crashing", %{socket: socket} do
      assert {:noreply, ^socket} =
               RunDetailLive.handle_event("toggle_flow_filter", %{"outcome" => "nope"}, socket)

      assert {:noreply, ^socket} =
               RunDetailLive.handle_event("set_flow_sort", %{"sort" => "nope"}, socket)
    end

    test "sorting reorders the chart without issuing a read", %{socket: socket} do
      {:noreply, sorted} =
        RunDetailLive.handle_event("set_flow_sort", %{"sort" => "name"}, socket)

      assert sorted.assigns.flow_sort == :name
      assert sorted.assigns.run.chart.lane_count == 2
    end

    test "expanding a band leaves the others as they were", %{socket: socket} do
      {:noreply, expanded} =
        RunDetailLive.handle_event("toggle_flow_band", %{"band" => "stage-1"}, socket)

      assert expanded.assigns.expanded_bands == ["stage-1"]

      {:noreply, collapsed} =
        RunDetailLive.handle_event("toggle_flow_band", %{"band" => "stage-1"}, expanded)

      assert collapsed.assigns.expanded_bands == []
    end

    test "chart or table is a reading preference that issues no read", %{socket: socket} do
      {:noreply, table} =
        RunDetailLive.handle_event("set_flow_view", %{"view" => "table"}, socket)

      assert table.assigns.flow_view == :table
      assert table.assigns.run.assets == socket.assigns.run.assets
    end
  end

  describe "window comparison" do
    setup do
      Application.put_env(:favn_view, :operator_run_flow_fun, fn _context, run_id ->
        {:ok, %{kind: :run, detail: backfill_flow(run_id, :ok)}}
      end)

      Application.put_env(:favn_view, :operator_run_windows_fun, fn _context, _run_id ->
        {:ok, %{window_choices() | items: month_choices(6)}}
      end)

      {:ok, mounted} = RunDetailLive.mount(%{"run_id" => "run-1"}, %{}, connected_socket())
      %{socket: mounted}
    end

    test "opens on the run the page already shows and empties on exit", %{socket: socket} do
      refute socket.assigns.compare?
      assert socket.assigns.compare_run_ids == []

      {:noreply, on} = RunDetailLive.handle_event("toggle_compare", %{}, socket)

      assert on.assigns.compare?
      assert on.assigns.compare_run_ids == ["run-1"]
      assert [%{run_id: "run-1", compared?: true, track: 1} | _rest] = on.assigns.rail.cells

      {:noreply, off} = RunDetailLive.handle_event("toggle_compare", %{}, on)

      # Leaving returns the page to exactly its single-window behaviour.
      refute off.assigns.compare?
      assert off.assigns.compare_run_ids == []
      assert Enum.all?(off.assigns.rail.cells, &(&1.compared? == false))
    end

    test "adds and removes windows, ordered by the calendar rather than by click", %{
      socket: socket
    } do
      compared = compare(socket, ["run-4", "run-2"])

      assert compared.assigns.compare_run_ids == ["run-1", "run-2", "run-4"]

      {:noreply, removed} =
        RunDetailLive.handle_event("toggle_compare_window", %{"run_id" => "run-2"}, compared)

      assert removed.assigns.compare_run_ids == ["run-1", "run-4"]
      assert %{track: 2} = Enum.find(removed.assigns.rail.cells, &(&1.run_id == "run-4"))
    end

    test "refuses past the limit instead of quietly dropping the click", %{socket: socket} do
      limit = RunWindowRail.compare_limit()
      full = compare(socket, ["run-2", "run-3", "run-4"])

      assert length(full.assigns.compare_run_ids) == limit
      assert full.assigns.rail.compare_full?
      refute full.assigns.compare_limit_reached?

      {:noreply, refused} =
        RunDetailLive.handle_event("toggle_compare_window", %{"run_id" => "run-5"}, full)

      assert refused.assigns.compare_limit_reached?
      assert refused.assigns.compare_run_ids == full.assigns.compare_run_ids

      # Removing one clears the refusal and makes room again.
      {:noreply, freed} =
        RunDetailLive.handle_event("toggle_compare_window", %{"run_id" => "run-2"}, refused)

      refute freed.assigns.compare_limit_reached?
      assert length(freed.assigns.compare_run_ids) == limit - 1
    end

    test "the open run anchors its own comparison", %{socket: socket} do
      compared = compare(socket, ["run-2"])

      {:noreply, kept} =
        RunDetailLive.handle_event("toggle_compare_window", %{"run_id" => "run-1"}, compared)

      assert kept.assigns.compare_run_ids == ["run-1", "run-2"]
    end

    test "an unknown window is refused rather than compared", %{socket: socket} do
      {:noreply, on} = RunDetailLive.handle_event("toggle_compare", %{}, socket)

      {:noreply, refused} =
        RunDetailLive.handle_event("toggle_compare_window", %{"run_id" => "run-elsewhere"}, on)

      assert refused.assigns.flash["error"] == "That window run is not available"
      assert refused.assigns.compare_run_ids == ["run-1"]
    end

    test "a cell click outside compare mode changes no selection", %{socket: socket} do
      assert {:noreply, ^socket} =
               RunDetailLive.handle_event("toggle_compare_window", %{"run_id" => "run-2"}, socket)
    end

    test "switching runs resets the comparison to the newly opened window", %{socket: socket} do
      compared = compare(socket, ["run-2", "run-3"])

      {:noreply, switched} =
        RunDetailLive.handle_params(%{"run_id" => "run-4"}, "/runs/run-4", compared)

      # The previous run's comparison must never render under the new run's URL.
      assert switched.assigns.compare?
      assert switched.assigns.compare_run_ids == ["run-4"]
    end
  end

  describe "comparison reads" do
    setup do
      caller = self()

      Application.put_env(:favn_view, :operator_run_windows_fun, fn _context, _run_id ->
        {:ok, %{window_choices() | items: month_choices(6)}}
      end)

      Application.put_env(:favn_view, :run_unsubscribe_fun, fn _context, run_id ->
        send(caller, {:unsubscribed, run_id})
        :ok
      end)

      %{caller: caller}
    end

    test "reads the windows the selection added and never the open run", %{caller: caller} do
      socket = mount_comparison(caller)
      assert_receive {:flow_read, "run-1"}

      compared = compare(socket, ["run-3", "run-5"])

      assert_receive {:flow_read, "run-3"}
      assert_receive {:flow_read, "run-5"}

      # The open run's track comes from what the page already holds.
      refute_receive {:flow_read, "run-1"}

      assert Enum.map(Map.values(compared.assigns.compare_windows), & &1.state) == [
               :loaded,
               :loaded,
               :loaded
             ]

      assert %{track: 1, selected?: true} = compared.assigns.compare_windows["run-1"]
      assert %{track: 2, label: label} = compared.assigns.compare_windows["run-3"]

      # The track names its own window, read from that window's own header.
      assert label =~ "Mar"
    end

    test "a cycle re-reads only the windows whose events moved", %{caller: caller} do
      socket = mount_comparison(caller)
      compared = compare(socket, ["run-2", "run-3"])
      flush()

      {:noreply, evented} =
        RunDetailLive.handle_info({:favn_run_event, %{run_id: "run-3", sequence: 9}}, compared)

      assert evented.assigns.pending_run_event_sequences == %{"run-3" => 9}

      {:noreply, refreshed} =
        RunDetailLive.handle_info({:refresh_run, evented.assigns.refresh_timer_ref}, evented)

      # The open run is always re-read; among the compared windows only the one
      # with a pending sequence is.
      assert_receive {:flow_read, "run-1"}
      assert_receive {:flow_read, "run-3"}
      refute_receive {:flow_read, "run-2"}

      # The cycle consumed the pending sequences before marking itself done.
      assert refreshed.assigns.pending_run_event_sequences == %{}
      assert refreshed.assigns.run_event_sequences["run-3"] == 9
    end

    test "a burst over the coalesce interval is one cycle, not one per window", %{caller: caller} do
      socket = mount_comparison(caller)
      compared = compare(socket, ["run-2", "run-3"])
      flush()

      bursted =
        Enum.reduce([{"run-2", 5}, {"run-3", 6}, {"run-2", 7}], compared, fn {run_id, seq}, acc ->
          {:noreply, next} =
            RunDetailLive.handle_info({:favn_run_event, %{run_id: run_id, sequence: seq}}, acc)

          next
        end)

      # One coalesced timer for the whole burst, whatever it touched.
      assert is_reference(bursted.assigns.refresh_timer_ref)
      assert bursted.assigns.pending_run_event_sequences == %{"run-2" => 7, "run-3" => 6}
      refute_receive {:flow_read, _run_id}

      {:noreply, _refreshed} =
        RunDetailLive.handle_info({:refresh_run, bursted.assigns.refresh_timer_ref}, bursted)

      reads = drain_reads()
      assert Enum.sort(reads) == ["run-1", "run-2", "run-3"]
      assert length(reads) == length(Enum.uniq(reads))
    end

    test "one failed window is unavailable and the others still draw", %{caller: caller} do
      socket = mount_comparison(caller, fn "run-3" -> {:error, :unavailable} end)
      compared = compare(socket, ["run-3", "run-5"])

      assert %{state: :unavailable, reason: :unavailable, assets: []} =
               compared.assigns.compare_windows["run-3"]

      assert %{state: :loaded} = compared.assigns.compare_windows["run-5"]
      assert compared.assigns.compare?
      refute compared.assigns.compare_error
    end

    test "a window that fails as it is added is unavailable, not a collapse", %{caller: caller} do
      socket = mount_comparison(caller, fn "run-3" -> {:error, :unavailable} end)
      compared = compare(socket, ["run-3"])

      # Nothing was lost: the operator can retry it or pick another window.
      assert compared.assigns.compare?
      assert %{state: :unavailable} = compared.assigns.compare_windows["run-3"]
      refute compared.assigns.compare_error
    end

    test "a comparison that loses every window falls back with a warning", %{caller: caller} do
      failing = :counters.new(1, [])

      socket =
        mount_comparison(caller, fn
          "run-1" ->
            nil

          run_id ->
            if :counters.get(failing, 1) == 0,
              do: {:ok, %{kind: :run, detail: window_flow(run_id, 3)}},
              else: {:error, :unavailable}
        end)

      compared = compare(socket, ["run-3"])
      assert %{state: :loaded} = compared.assigns.compare_windows["run-3"]

      :counters.add(failing, 1, 1)

      {:noreply, evented} =
        RunDetailLive.handle_info({:favn_run_event, %{run_id: "run-3", sequence: 9}}, compared)

      {:noreply, lost} =
        RunDetailLive.handle_info({:refresh_run, evented.assigns.refresh_timer_ref}, evented)

      refute lost.assigns.compare?
      assert lost.assigns.compare_run_ids == []
      assert lost.assigns.compare_windows == %{}
      assert lost.assigns.compare_error =~ "No compared window could be read"

      # The open run is untouched: the page is exactly the single-window view.
      assert lost.assigns.run.found?
      assert length(lost.assigns.run.assets) == 2
    end

    test "an unavailable window is tried again on the next cycle", %{caller: caller} do
      failing = :counters.new(1, [])

      socket =
        mount_comparison(caller, fn "run-3" ->
          if :counters.get(failing, 1) == 0 do
            :counters.add(failing, 1, 1)
            {:error, :unavailable}
          else
            {:ok, %{kind: :run, detail: window_flow("run-3", 3)}}
          end
        end)

      compared = compare(socket, ["run-5", "run-3"])
      assert %{state: :unavailable} = compared.assigns.compare_windows["run-3"]
      flush()

      {:noreply, retried} =
        RunDetailLive.handle_info({:poll_run, compared.assigns.fallback_poll_ref}, compared)

      assert_receive {:flow_read, "run-3"}
      assert %{state: :loaded} = retried.assigns.compare_windows["run-3"]

      # A loaded window is not re-read merely because a cycle ran.
      refute_receive {:flow_read, "run-5"}
    end

    test "a read that never answers keeps the last good result and stays owed", %{caller: caller} do
      Application.put_env(:favn_view, :compare_read_timeout_ms, 50)
      hang = :counters.new(1, [])

      socket =
        mount_comparison(caller, fn
          "run-3" ->
            if :counters.get(hang, 1) == 1 do
              Process.sleep(:infinity)
            else
              {:ok, %{kind: :run, detail: window_flow("run-3", 3)}}
            end

          _run_id ->
            nil
        end)

      compared = compare(socket, ["run-3"])
      assert %{state: :loaded, retry?: false} = compared.assigns.compare_windows["run-3"]

      # The window's own event moves, and the read for it hangs past the bound.
      :counters.add(hang, 1, 1)

      {:noreply, evented} =
        RunDetailLive.handle_info({:favn_run_event, %{run_id: "run-3", sequence: 9}}, compared)

      {:noreply, timed_out} =
        RunDetailLive.handle_info({:refresh_run, evented.assigns.refresh_timer_ref}, evented)

      # The last good result still stands rather than the track blanking...
      assert %{state: :loaded, retry?: true} = timed_out.assigns.compare_windows["run-3"]

      # ...but marking the refresh done cleared the pending sequence that would
      # otherwise have been the only reason to read it again, so the window is
      # explicitly owed a read and the page keeps a cycle alive to make it.
      assert timed_out.assigns.pending_run_event_sequences == %{}
      assert is_reference(timed_out.assigns.fallback_poll_ref)
      flush()

      :counters.sub(hang, 1, 1)

      {:noreply, recovered} =
        RunDetailLive.handle_info({:poll_run, timed_out.assigns.fallback_poll_ref}, timed_out)

      assert_receive {:flow_read, "run-3"}
      assert %{state: :loaded, retry?: false} = recovered.assigns.compare_windows["run-3"]
    end

    test "a failed window read closes compare mode rather than trapping the operator", %{
      caller: caller
    } do
      socket = mount_comparison(caller)
      compared = compare(socket, ["run-3"])
      assert compared.assigns.compare?

      Application.put_env(:favn_view, :operator_run_windows_fun, fn _context, _run_id ->
        {:error, :unavailable}
      end)

      # The window list is read at most once per fallback interval; clearing the
      # last-read stamp is how this test reaches the next due read without
      # waiting one out.
      compared = put_in(compared.assigns.windows_read_at, nil)

      {:noreply, failed} =
        RunDetailLive.handle_info({:poll_run, compared.assigns.fallback_poll_ref}, compared)

      assert failed.assigns.windows_error =~ "Window runs could not be loaded"
      assert is_nil(failed.assigns.rail)

      # The toggle that leaves compare mode lives on the rail, so compare mode
      # cannot outlive it.
      refute failed.assigns.compare?
      assert failed.assigns.compare_windows == %{}
      assert is_nil(failed.assigns.run.comparison)

      # A read the page still owes keeps a cycle alive to make it.
      assert is_reference(failed.assigns.fallback_poll_ref)
    end

    test "the comparison subscribes to its windows and releases them on exit", %{caller: caller} do
      socket = mount_comparison(caller)
      compared = compare(socket, ["run-3", "run-5"])

      assert MapSet.equal?(
               compared.assigns.run_event_subscriptions,
               MapSet.new(["run-1", "run-3", "run-5"])
             )

      {:noreply, off} = RunDetailLive.handle_event("toggle_compare", %{}, compared)

      assert_receive {:unsubscribed, "run-3"}
      assert_receive {:unsubscribed, "run-5"}
      refute_received {:unsubscribed, "run-1"}
      assert MapSet.equal?(off.assigns.run_event_subscriptions, MapSet.new(["run-1"]))
      assert off.assigns.compare_windows == %{}
    end

    test "the chart becomes a comparison only once a second window is there", %{caller: caller} do
      socket = mount_comparison(caller)

      {:noreply, alone} = RunDetailLive.handle_event("toggle_compare", %{}, socket)

      # One window is not a comparison, so the single-run chart still stands.
      assert is_nil(alone.assigns.run.comparison)
      assert alone.assigns.run.chart

      compared = compare(socket, ["run-3"])
      comparison = compared.assigns.run.comparison

      assert comparison.track_count == comparison.lane_count * 2

      # Every lane carries both windows in the same order, which is what makes a
      # track position mean one window across the whole chart.
      tracks = Enum.flat_map(comparison.bands, fn band -> Enum.map(band.lanes, & &1.tracks) end)

      assert tracks != []
      assert Enum.all?(tracks, &(Enum.map(&1, fn track -> track.track end) == [1, 2]))
      assert Enum.all?(tracks, &match?([%{run_id: "run-1"}, %{run_id: "run-3"}], &1))

      # Leaving compare mode puts the single-run chart back.
      {:noreply, off} = RunDetailLive.handle_event("toggle_compare", %{}, compared)
      assert is_nil(off.assigns.run.comparison)
    end

    test "alignment is view state that issues no read", %{caller: caller} do
      socket = mount_comparison(caller)
      compared = compare(socket, ["run-2"])
      flush()

      assert compared.assigns.run.comparison.alignment == :window

      {:noreply, wall_clock} =
        RunDetailLive.handle_event("set_flow_alignment", %{"alignment" => "wall_clock"}, compared)

      assert wall_clock.assigns.flow_alignment == :wall_clock
      assert drain_reads() == []

      assert {:noreply, ^wall_clock} =
               RunDetailLive.handle_event(
                 "set_flow_alignment",
                 %{"alignment" => "nope"},
                 wall_clock
               )
    end

    test "a cycle issues at most one read per selected window", %{caller: caller} do
      socket = mount_comparison(caller)
      compared = compare(socket, ["run-2", "run-3", "run-4"])
      flush()

      {:noreply, _refreshed} =
        RunDetailLive.handle_info({:poll_run, compared.assigns.fallback_poll_ref}, compared)

      # Everything is loaded and nothing has pending events, so the cycle costs
      # the open run's read alone.
      assert drain_reads() == ["run-1"]
      assert length(compared.assigns.compare_run_ids) == RunWindowRail.compare_limit()
    end
  end

  defp mount_comparison(caller, overrides \\ fn _run_id -> nil end) do
    Application.put_env(:favn_view, :operator_run_flow_fun, fn _context, run_id ->
      send(caller, {:flow_read, run_id})
      index = run_id |> String.split("-") |> List.last() |> String.to_integer()

      case safe_override(overrides, run_id) do
        nil -> {:ok, %{kind: :run, detail: window_flow(run_id, index)}}
        result -> result
      end
    end)

    {:ok, mounted} = RunDetailLive.mount(%{"run_id" => "run-1"}, %{}, connected_socket())
    mounted
  end

  defp safe_override(overrides, run_id) do
    overrides.(run_id)
  rescue
    FunctionClauseError -> nil
  end

  # A window run inside a backfill, carrying the window its cell names.
  defp window_flow(run_id, index) do
    detail = backfill_flow(run_id, :ok)
    start_at = DateTime.add(~U[2026-01-01 00:00:00Z], (index - 1) * 31, :day)

    %{
      detail
      | header: %{
          detail.header
          | window_start_at: start_at,
            window_end_at: DateTime.add(start_at, 31, :day)
        }
    }
  end

  defp flush, do: drain_reads()

  defp drain_reads(acc \\ []) do
    receive do
      {:flow_read, run_id} -> drain_reads([run_id | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp compare(socket, run_ids) do
    {:noreply, on} = RunDetailLive.handle_event("toggle_compare", %{}, socket)

    Enum.reduce(run_ids, on, fn run_id, acc ->
      {:noreply, next} =
        RunDetailLive.handle_event("toggle_compare_window", %{"run_id" => run_id}, acc)

      next
    end)
  end

  defp month_choices(count) do
    Enum.map(1..count, fn index ->
      start_at = DateTime.add(~U[2026-01-01 00:00:00Z], (index - 1) * 31, :day)

      %RunWindowChoice{
        run_id: "run-#{index}",
        window_start_at: start_at,
        window_end_at: DateTime.add(start_at, 31, :day),
        status: :succeeded,
        kind: :month,
        timezone: "Etc/UTC"
      }
    end)
  end

  defp flow(run_id, status) do
    %Flow{
      header: header(run_id, status),
      overflow?: false,
      assets: [
        %Asset{
          id: "step-orders",
          run_id: run_id,
          name: "Orders",
          asset_ref: "crm.orders",
          state: status,
          stage: 0,
          started_at: ~U[2026-08-23 10:00:00Z],
          finished_at: if(status == :running, do: nil, else: ~U[2026-08-23 10:00:04Z])
        },
        %Asset{
          id: "step-total",
          run_id: run_id,
          name: "Total",
          asset_ref: "crm.total",
          state: :queued,
          stage: 1,
          started_at: nil,
          finished_at: nil
        }
      ]
    }
  end

  defp header(run_id, status) do
    active? = status in [:pending, :running]

    %Header{
      run_id: run_id,
      root_run_id: run_id,
      status: status,
      active?: active?,
      cancellable?: active?,
      retry_remaining?: false,
      trigger_type: :schedule,
      event_sequence: 2,
      started_at: ~U[2026-08-23 10:00:00Z],
      updated_at: ~U[2026-08-23 10:00:04Z],
      finished_at: if(active?, do: nil, else: ~U[2026-08-23 10:00:04Z]),
      target_id: "crm.orders",
      target_label: "crm.orders",
      counts: %{
        total: 2,
        completed: if(active?, do: 0, else: 1),
        succeeded: if(active?, do: 0, else: 1),
        skipped: 0,
        failed: 0,
        running: if(active?, do: 1, else: 0),
        queued: 1,
        planned: 0
      }
    }
  end

  # A backfill whose windows all failed before a child run existed. The window
  # read returns no choices, because there is no window run to navigate to, and
  # the backfill id is the only route left to the reasons.
  defp runless_choices(backfill_id) do
    %RunWindowChoices{
      overflow?: false,
      items: [],
      backfill_status: :failed,
      backfill_id: backfill_id
    }
  end

  defp failed_overview(failed) do
    %{
      status: :failed,
      active?: false,
      total_asset_attempts: 0,
      completed_asset_attempts: 0,
      succeeded_asset_attempts: 0,
      skipped_asset_attempts: 0,
      failed_asset_attempts: 0,
      running_asset_attempts: 0,
      queued_asset_attempts: 0,
      planned_asset_attempts: 0,
      summary_totals: %{
        windows: %{
          total: failed,
          completed: failed,
          succeeded: 0,
          failed: failed,
          running: 0,
          queued: 0,
          cancelled: 0
        }
      }
    }
  end

  defp failed_window_page(count, reason) do
    items =
      for index <- 1..count//1 do
        %BackfillWindow{
          workspace_id: "workspace-one",
          backfill_id: "bf-one",
          window_id: "bfw-#{index}",
          window_key: "day:Etc/UTC:2026-01-#{String.pad_leading("#{index}", 2, "0")}",
          window_start: DateTime.new!(Date.new!(2026, 1, index), ~T[00:00:00], "Etc/UTC"),
          window_end:
            DateTime.new!(Date.add(Date.new!(2026, 1, index), 1), ~T[00:00:00], "Etc/UTC"),
          status: :failed,
          run_id: nil,
          attempt_count: 1,
          last_error: %{"reason" => ~s(%{"reason" => "#{reason}"})},
          fencing_token: 1,
          payload: %{},
          version: 3
        }
      end

    %{items: items, limit: 500, has_more?: false, next_cursor: nil}
  end

  defp connected_socket do
    %Phoenix.LiveView.Socket{
      transport_pid: self(),
      assigns: %{__changed__: %{}, current_scope: oslo_scope(), flash: %{}}
    }
  end

  defp disconnected_socket do
    %Phoenix.LiveView.Socket{
      assigns: %{__changed__: %{}, current_scope: oslo_scope(), flash: %{}}
    }
  end

  defp oslo_scope do
    %Scope{
      operator_context: :operator_context,
      actor: %{id: "actor-one", username: "operator", display_name: "Operator", roles: [:viewer]},
      workspace_configuration: %FavnOrchestrator.WorkspaceConfiguration{
        workspace_id: "workspace-one",
        deployment_id: "deployment-one",
        default_timezone: "Europe/Oslo",
        default_timezone_source: :application_default
      }
    }
  end

  defp restore_env(key, nil), do: Application.delete_env(:favn_view, key)
  defp restore_env(key, value), do: Application.put_env(:favn_view, key, value)
end
