defmodule FavnView.RunDetailLiveTest do
  use ExUnit.Case, async: false

  import Phoenix.LiveViewTest

  alias FavnOrchestrator.OperatorRunView.Asset
  alias FavnOrchestrator.OperatorRunView.Flow
  alias FavnOrchestrator.OperatorRunView.Header
  alias FavnOrchestrator.Persistence.Results.RunWindowChoice
  alias FavnOrchestrator.Persistence.Results.RunWindowChoices
  alias FavnView.Auth.Scope
  alias FavnView.RunDetailLive

  setup do
    keys = [
      :operator_run_flow_fun,
      :operator_run_events_fun,
      :operator_run_windows_fun,
      :operator_execution_group_fun,
      :run_subscribe_fun,
      :run_unsubscribe_fun
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
