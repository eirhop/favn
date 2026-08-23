defmodule FavnView.RunDetailFlowPagingTest do
  use ExUnit.Case, async: false

  alias FavnOrchestrator.OperatorRunFlow.Header
  alias FavnOrchestrator.OperatorRunFlow.Page
  alias FavnOrchestrator.OperatorRunFlow.Step
  alias FavnOrchestrator.OperatorRunPages.Page, as: SummaryPage
  alias FavnView.Auth.Scope
  alias FavnView.RunDetailLive

  setup do
    previous_flow = Application.get_env(:favn_view, :operator_run_flow_fun)
    previous_activity = Application.get_env(:favn_view, :operator_run_activity_fun)
    previous_subscribe = Application.get_env(:favn_view, :run_subscribe_fun)
    previous_stream = Application.get_env(:favn_view, :run_stream_events_fun)
    previous_windows = Application.get_env(:favn_view, :operator_run_windows_fun)
    previous_events = Application.get_env(:favn_view, :operator_run_events_fun)
    Application.delete_env(:favn_view, :operator_run_activity_fun)
    Application.put_env(:favn_view, :run_subscribe_fun, fn _context, _run_id -> :ok end)

    Application.put_env(:favn_view, :run_stream_events_fun, fn _context, _run_id, _opts ->
      {:ok, []}
    end)

    on_exit(fn ->
      restore_env(:operator_run_flow_fun, previous_flow)
      restore_env(:operator_run_activity_fun, previous_activity)
      restore_env(:run_subscribe_fun, previous_subscribe)
      restore_env(:run_stream_events_fun, previous_stream)
      restore_env(:operator_run_windows_fun, previous_windows)
      restore_env(:operator_run_events_fun, previous_events)
    end)
  end

  test "loads 200 + 200 + 100 rows and retains at most 500" do
    test_pid = self()

    Application.put_env(:favn_view, :operator_run_flow_fun, fn _context, "run", opts ->
      send(test_pid, {:flow_read, opts})

      case Keyword.get(opts, :after) do
        nil ->
          {:ok, page(1, 200, "cursor-200", "cursor-1", true, false)}

        "cursor-200" ->
          {:ok, page(201, 200, "cursor-400", "cursor-201", true, true)}

        "cursor-400" ->
          {:ok, page(401, Keyword.fetch!(opts, :limit), "cursor-500", "cursor-401", true, true)}
      end
    end)

    {:ok, mounted} = RunDetailLive.mount(%{"run_id" => "run"}, %{}, connected_socket())
    assert length(mounted.assigns.run.attempts) == 200
    assert_received {:flow_read, initial_opts}
    assert initial_opts[:limit] == 200

    {:noreply, pending_four_hundred} =
      RunDetailLive.handle_event("load_more_flow", %{}, mounted)

    {:noreply, four_hundred} =
      complete_flow_page(
        pending_four_hundred,
        :append,
        "cursor-200",
        {:ok, page(201, 200, "cursor-400", "cursor-201", true, true)}
      )

    assert length(four_hundred.assigns.run.attempts) == 400
    assert_received {:flow_read, second_opts}
    assert second_opts[:limit] == 200

    {:noreply, pending_five_hundred} =
      RunDetailLive.handle_event("load_more_flow", %{}, four_hundred)

    {:noreply, five_hundred} =
      complete_flow_page(
        pending_five_hundred,
        :append,
        "cursor-400",
        {:ok, page(401, 100, "cursor-500", "cursor-401", true, true)}
      )

    assert length(five_hundred.assigns.run.attempts) == 500
    assert_received {:flow_read, final_opts}
    assert final_opts[:limit] == 100
    assert five_hundred.assigns.run.flow_has_next?
  end

  test "Next and Previous preserve a contiguous 500-row retained range beyond the cap" do
    Application.put_env(:favn_view, :operator_run_flow_fun, fn _context, "run", opts ->
      case {Keyword.get(opts, :after), Keyword.get(opts, :before)} do
        {nil, nil} -> {:ok, page(1, 200, "cursor-200", "cursor-1", true, false)}
        {"cursor-200", nil} -> {:ok, page(201, 200, "cursor-400", "cursor-201", true, true)}
        {"cursor-400", nil} -> {:ok, page(401, 100, "cursor-500", "cursor-401", true, true)}
        {"cursor-500", nil} -> {:ok, page(501, 200, "cursor-700", "cursor-501", true, true)}
        {nil, "cursor-201"} -> {:ok, page(1, 200, "cursor-200", "cursor-1", true, false)}
      end
    end)

    {:ok, mounted} = RunDetailLive.mount(%{"run_id" => "run"}, %{}, connected_socket())

    {:noreply, pending_four_hundred} =
      RunDetailLive.handle_event("load_more_flow", %{}, mounted)

    {:noreply, four_hundred} =
      complete_flow_page(
        pending_four_hundred,
        :append,
        "cursor-200",
        {:ok, page(201, 200, "cursor-400", "cursor-201", true, true)}
      )

    {:noreply, pending_five_hundred} =
      RunDetailLive.handle_event("load_more_flow", %{}, four_hundred)

    {:noreply, five_hundred} =
      complete_flow_page(
        pending_five_hundred,
        :append,
        "cursor-400",
        {:ok, page(401, 100, "cursor-500", "cursor-401", true, true)}
      )

    {:noreply, pending_seven_hundred} =
      RunDetailLive.handle_event("next_flow", %{}, five_hundred)

    {:noreply, seven_hundred} =
      complete_flow_page(
        pending_seven_hundred,
        :next,
        "cursor-500",
        {:ok, page(501, 200, "cursor-700", "cursor-501", true, true)}
      )

    assert Enum.map(seven_hundred.assigns.run.attempts, & &1.asset_step_id) ==
             Enum.map(201..700, &step(&1).asset_step_id)

    assert seven_hundred.assigns.run.flow_previous_cursor == "cursor-201"

    {:noreply, pending_back_to_five_hundred} =
      RunDetailLive.handle_event("previous_flow", %{}, seven_hundred)

    {:noreply, back_to_five_hundred} =
      complete_flow_page(
        pending_back_to_five_hundred,
        :previous,
        "cursor-201",
        {:ok, page(1, 200, "cursor-200", "cursor-1", true, false)}
      )

    assert Enum.map(back_to_five_hundred.assigns.run.attempts, & &1.asset_step_id) ==
             Enum.map(1..500, &step(&1).asset_step_id)

    assert back_to_five_hundred.assigns.run.flow_next_cursor == "cursor-500"
  end

  test "disconnected mount performs no run-detail read" do
    test_pid = self()

    Application.put_env(:favn_view, :operator_run_flow_fun, fn _context, _run_id, _opts ->
      send(test_pid, :unexpected_flow_read)
      {:error, :unavailable}
    end)

    {:ok, mounted} = RunDetailLive.mount(%{"run_id" => "run"}, %{}, disconnected_socket())
    refute_received :unexpected_flow_read
    assert mounted.assigns.run.initializing?
  end

  test "90-row connected render stays within payload, wire, and usability budgets" do
    test_pid = self()
    flow_page = page(1, 90, nil, "cursor-1", false, false)

    Application.put_env(:favn_view, :operator_run_flow_fun, fn _context, "run", _opts ->
      {:ok, flow_page}
    end)

    handler_id = {__MODULE__, :run_detail_render, make_ref()}

    :ok =
      :telemetry.attach(
        handler_id,
        [:favn, :view, :run_detail, :render],
        fn _event, measurements, metadata, pid ->
          send(pid, {:run_detail_render, measurements, metadata})
        end,
        test_pid
      )

    try do
      started = System.monotonic_time(:microsecond)
      {:ok, mounted} = RunDetailLive.mount(%{"run_id" => "run"}, %{}, connected_socket())

      mounted = %{
        mounted
        | assigns:
            mounted.assigns
            |> Map.put(:flash, %{})
            |> Map.put(:operator_workspaces, [])
      }

      html =
        mounted.assigns
        |> RunDetailLive.render()
        |> Phoenix.HTML.Safe.to_iodata()
        |> IO.iodata_to_binary()

      message = %Phoenix.Socket.Message{
        join_ref: "1",
        ref: nil,
        topic: "lv:qualification",
        event: "diff",
        payload: %{rendered: html}
      }

      assert {:socket_push, :text, encoded} = FavnView.LiveViewSerializer.encode!(message)

      connected_usable_us = System.monotonic_time(:microsecond) - started
      wire_bytes = IO.iodata_length(encoded)

      dto_bytes =
        flow_page
        |> FavnOrchestrator.Storage.JsonSafe.data()
        |> Jason.encode!()
        |> byte_size()

      assert length(mounted.assigns.run.attempts) == 90
      assert dto_bytes <= 524_288
      assert wire_bytes <= 1_048_576
      assert connected_usable_us <= 1_000_000
      assert html =~ "lg:hidden"

      assert_receive {:run_detail_render, measurements, %{mode: :flow, mount_kind: :connected}}

      assert measurements.diff_bytes == wire_bytes
      assert measurements.step_count == 90
      assert measurements.duration >= 0

      IO.puts(
        "Run detail render qualification: connected_usable_us=#{connected_usable_us} " <>
          "dto_bytes=#{dto_bytes} wire_bytes=#{wire_bytes}"
      )
    after
      :telemetry.detach(handler_id)
    end
  end

  test "one page task rejects duplicate actions and ignores a stale generation" do
    test_pid = self()

    Application.put_env(:favn_view, :operator_run_flow_fun, fn _context, "run", opts ->
      send(test_pid, {:flow_read, opts})

      if opts[:after],
        do: {:ok, page(201, 200, "cursor-400", "cursor-201", true, true)},
        else: {:ok, page(1, 200, "cursor-200", "cursor-1", true, false)}
    end)

    {:ok, mounted} = RunDetailLive.mount(%{"run_id" => "run"}, %{}, connected_socket())
    assert_receive {:flow_read, _initial}

    {:noreply, pending} = RunDetailLive.handle_event("load_more_flow", %{}, mounted)
    {:noreply, duplicate} = RunDetailLive.handle_event("load_more_flow", %{}, pending)
    assert duplicate.assigns.flow_task_name == pending.assigns.flow_task_name
    assert_receive {:flow_read, _continuation}
    refute_receive {:flow_read, _duplicate}, 25

    stale_name = pending.assigns.flow_task_name

    newer =
      pending
      |> Phoenix.Component.assign(:flow_task_generation, pending.assigns.flow_task_generation + 1)
      |> Phoenix.Component.assign(
        :flow_task_name,
        {:page_mutation, pending.assigns.flow_task_generation + 1}
      )

    assert {:noreply, unchanged} =
             RunDetailLive.handle_async(
               stale_name,
               {:ok,
                {:flow, :append, "cursor-200",
                 {:ok, page(201, 200, "cursor-400", "cursor-201", true, true)}}},
               newer
             )

    assert length(unchanged.assigns.run.attempts) == 200
    assert unchanged.assigns.flow_task_name == newer.assigns.flow_task_name
  end

  test "repair membership wakes bypass stale publication cursors and remain pending until readable" do
    Application.put_env(:favn_view, :operator_run_flow_fun, fn _context, "run", _opts ->
      {:ok, page(1, 200, "cursor-200", "cursor-1", true, false)}
    end)

    {:ok, mounted} = RunDetailLive.mount(%{"run_id" => "run"}, %{}, connected_socket())

    assert {:noreply, reconciling} =
             RunDetailLive.handle_info(
               {:favn_run_projected,
                %{
                  "publication_id" => 1,
                  "repair_generation" => 2,
                  "change" => "membership"
                }},
               mounted
             )

    assert reconciling.assigns.flow_reconcile_required?
    assert {:flow_reconcile, _generation} = reconciling.assigns.flow_task_name

    reconciling = %{reconciling | assigns: Map.put(reconciling.assigns, :flash, %{})}

    assert {:noreply, unavailable} =
             RunDetailLive.handle_async(
               reconciling.assigns.flow_task_name,
               {:ok, {:error, :unavailable}},
               reconciling
             )

    assert unavailable.assigns.flow_reconcile_required?
    assert unavailable.assigns.fallback_poll_ref
  end

  test "membership insertion and deletion rebuild anchors before Next and Previous" do
    Application.put_env(:favn_view, :operator_run_flow_fun, fn _context, "run", _opts ->
      {:ok, page(1, 200, "cursor-200", "cursor-1", true, false)}
    end)

    {:ok, mounted} = RunDetailLive.mount(%{"run_id" => "run"}, %{}, connected_socket())

    insertion_pages = [
      page(0, 200, "cursor-199", "cursor-0", true, false),
      page(200, 200, "cursor-399", "cursor-200", true, true),
      page(400, 100, "cursor-499", "cursor-400", true, true)
    ]

    insertion_boundaries = [
      %{first: "cursor-0", lower: nil},
      %{first: "cursor-200", lower: "cursor-199"},
      %{first: "cursor-400", lower: "cursor-399"}
    ]

    insertion_socket =
      mounted
      |> Phoenix.Component.assign(:flow_task_name, {:flow_reconcile, 20})
      |> Phoenix.Component.assign(:flow_reconcile_required?, true)

    assert {:noreply, inserted} =
             RunDetailLive.handle_async(
               {:flow_reconcile, 20},
               {:ok, reconciliation(insertion_pages, insertion_boundaries)},
               insertion_socket
             )

    assert step_ids(inserted) == Enum.map(0..499, &step(&1).asset_step_id)
    assert inserted.assigns.run.flow_boundaries == insertion_boundaries
    assert inserted.assigns.run.flow_anchor_history == []

    inserted_next =
      inserted
      |> Phoenix.Component.assign(:flow_task_name, {:page_mutation, 21})

    assert {:noreply, inserted_forward} =
             RunDetailLive.handle_async(
               {:page_mutation, 21},
               {:ok,
                {:flow, :next, "cursor-499",
                 {:ok, page(500, 200, "cursor-699", "cursor-500", true, true)}}},
               inserted_next
             )

    assert step_ids(inserted_forward) == Enum.map(200..699, &step(&1).asset_step_id)

    inserted_previous =
      inserted_forward
      |> Phoenix.Component.assign(:flow_task_name, {:page_mutation, 22})

    assert {:noreply, inserted_back} =
             RunDetailLive.handle_async(
               {:page_mutation, 22},
               {:ok,
                {:flow, :previous, "cursor-200",
                 {:ok, page(0, 200, "cursor-199", "cursor-0", true, false)}}},
               inserted_previous
             )

    assert step_ids(inserted_back) == Enum.map(0..499, &step(&1).asset_step_id)

    deletion_steps = Enum.map(0..99, &step/1) ++ Enum.map(101..500, &step/1)

    deletion_pages = [
      page_from_steps(Enum.take(deletion_steps, 200), "cursor-200", "cursor-0", true, false),
      page_from_steps(
        Enum.slice(deletion_steps, 200, 200),
        "cursor-400",
        "cursor-201",
        true,
        true
      ),
      page_from_steps(
        Enum.slice(deletion_steps, 400, 100),
        "cursor-500",
        "cursor-401",
        true,
        true
      )
    ]

    deletion_boundaries = [
      %{first: "cursor-0", lower: nil},
      %{first: "cursor-201", lower: "cursor-200"},
      %{first: "cursor-401", lower: "cursor-400"}
    ]

    deletion_socket =
      inserted_back
      |> Phoenix.Component.assign(:flow_task_name, {:flow_reconcile, 23})
      |> Phoenix.Component.assign(:flow_reconcile_required?, true)

    assert {:noreply, deleted} =
             RunDetailLive.handle_async(
               {:flow_reconcile, 23},
               {:ok, reconciliation(deletion_pages, deletion_boundaries)},
               deletion_socket
             )

    assert step_ids(deleted) == Enum.map(deletion_steps, & &1.asset_step_id)
    assert deleted.assigns.run.flow_boundaries == deletion_boundaries

    deleted_next = Phoenix.Component.assign(deleted, :flow_task_name, {:page_mutation, 24})

    assert {:noreply, deleted_forward} =
             RunDetailLive.handle_async(
               {:page_mutation, 24},
               {:ok,
                {:flow, :next, "cursor-500",
                 {:ok, page(501, 200, "cursor-700", "cursor-501", true, true)}}},
               deleted_next
             )

    assert step_ids(deleted_forward) == Enum.map(201..700, &step(&1).asset_step_id)

    deleted_previous =
      Phoenix.Component.assign(deleted_forward, :flow_task_name, {:page_mutation, 25})

    previous_steps = Enum.map(0..99, &step/1) ++ Enum.map(101..200, &step/1)

    assert {:noreply, deleted_back} =
             RunDetailLive.handle_async(
               {:page_mutation, 25},
               {:ok,
                {:flow, :previous, "cursor-201",
                 {:ok, page_from_steps(previous_steps, "cursor-200", "cursor-0", true, false)}}},
               deleted_previous
             )

    assert step_ids(deleted_back) == Enum.map(deletion_steps, & &1.asset_step_id)
  end

  test "Window and Event continuations replace bounded state and expose exclusive errors" do
    Application.put_env(:favn_view, :operator_run_flow_fun, fn _context, "run", _opts ->
      {:ok, page(1, 200, "cursor-200", "cursor-1", true, false)}
    end)

    {:ok, mounted} = RunDetailLive.mount(%{"run_id" => "run"}, %{}, connected_socket())

    window_page = %SummaryPage{
      items: [window_summary("window-new")],
      total: 75,
      has_more?: false,
      next_cursor: nil
    }

    window_socket =
      mounted
      |> Phoenix.Component.assign(:run, Map.put(mounted.assigns.run, :child_runs, [%{id: "old"}]))
      |> Phoenix.Component.assign(:flow_task_name, {:page_mutation, 10})

    assert {:noreply, windowed} =
             RunDetailLive.handle_async(
               {:page_mutation, 10},
               {:ok, {:windows, nil, {:ok, window_page}}},
               window_socket
             )

    assert Enum.map(windowed.assigns.run.child_runs, & &1.id) == ["window-new"]
    assert windowed.assigns.run.total_windows == 75

    event_page = %SummaryPage{
      items: [event_summary(51)],
      total: 120,
      has_more?: true,
      next_cursor: "event-next"
    }

    event_socket =
      windowed
      |> Phoenix.Component.assign(:run, Map.put(windowed.assigns.run, :events, [%{sequence: 1}]))
      |> Phoenix.Component.assign(:flow_task_name, {:page_mutation, 11})

    assert {:noreply, events} =
             RunDetailLive.handle_async(
               {:page_mutation, 11},
               {:ok, {:events, nil, {:ok, event_page}}},
               event_socket
             )

    assert Enum.map(events.assigns.run.events, & &1.sequence) == [51]
    assert events.assigns.run.event_total == 120

    error_socket = Phoenix.Component.assign(events, :flow_task_name, {:page_mutation, 12})

    assert {:noreply, failed} =
             RunDetailLive.handle_async(
               {:page_mutation, 12},
               {:ok, {:events, nil, {:error, :timeout}}},
               error_socket
             )

    assert failed.assigns.run.mode_error

    refute FavnView.Components.RunDetailPage.execution_group_page(%{
             run: failed.assigns.run,
             flow: nil,
             active_mode: :events,
             selected_child_run_id: nil,
             selected_attempt: nil,
             selected_attempt_id: nil,
             flow_filter_form: nil
           })
           |> Phoenix.HTML.Safe.to_iodata()
           |> IO.iodata_to_binary()
           |> String.contains?("data-testid=\"run-event-timeline\"")
  end

  test "relevant Window and Event refreshes use only their bounded current-page reads" do
    test_pid = self()

    Application.put_env(:favn_view, :operator_run_flow_fun, fn _context, "run", _opts ->
      {:ok, page(1, 2, nil, "cursor-1", false, false)}
    end)

    Application.put_env(:favn_view, :operator_run_activity_fun, fn _context, _run_id, _opts ->
      send(test_pid, :unexpected_broad_activity_read)
      {:error, :unavailable}
    end)

    window_page = %SummaryPage{
      items: [window_summary("window-current")],
      total: 75,
      has_more?: true,
      next_cursor: "window-next"
    }

    Application.put_env(:favn_view, :operator_run_windows_fun, fn _context, "run", opts ->
      send(test_pid, {:window_refresh, opts})
      {:ok, window_page}
    end)

    {:ok, mounted} = RunDetailLive.mount(%{"run_id" => "run"}, %{}, connected_socket())

    window_socket =
      mounted
      |> Phoenix.Component.assign(:active_mode, :windows)
      |> Phoenix.Component.assign(
        :run,
        Map.put(mounted.assigns.run, :window_current_cursor, "window-current")
      )

    assert {:noreply, refreshing_windows} = RunDetailLive.handle_info(:refresh_run, window_socket)
    assert_receive {:window_refresh, opts}
    assert opts[:after] == "window-current"
    refute_received :unexpected_broad_activity_read

    assert {:noreply, refreshed_windows} =
             RunDetailLive.handle_async(
               refreshing_windows.assigns.flow_task_name,
               {:ok, {:summary_refresh, :windows, "window-current", {:ok, window_page}}},
               refreshing_windows
             )

    assert refreshed_windows.assigns.run.window_current_cursor == "window-current"

    event_page = %SummaryPage{
      items: [event_summary(51)],
      total: 120,
      has_more?: true,
      next_cursor: 50
    }

    Application.put_env(:favn_view, :operator_run_events_fun, fn _context, "run", opts ->
      send(test_pid, {:event_refresh, opts})
      {:ok, event_page}
    end)

    event_socket =
      refreshed_windows
      |> Phoenix.Component.assign(:active_mode, :events)
      |> Phoenix.Component.assign(
        :run,
        Map.put(refreshed_windows.assigns.run, :event_current_cursor, 51)
      )

    assert {:noreply, refreshing_events} =
             RunDetailLive.handle_info(:favn_projection_listener_resumed, event_socket)

    assert_receive {:event_refresh, opts}
    assert opts[:after] == 51
    refute_received :unexpected_broad_activity_read

    assert {:noreply, refreshed_events} =
             RunDetailLive.handle_async(
               refreshing_events.assigns.flow_task_name,
               {:ok, {:summary_refresh, :events, 51, {:ok, event_page}}},
               refreshing_events
             )

    assert refreshed_events.assigns.run.event_current_cursor == 51
    refute_received :unexpected_broad_activity_read
  end

  test "a projected wake during a Window refresh is retained without perpetual polling" do
    test_pid = self()

    Application.put_env(:favn_view, :operator_run_flow_fun, fn _context, "run", _opts ->
      {:ok, page(1, 2, nil, "cursor-1", false, false)}
    end)

    window_page = %SummaryPage{
      items: [window_summary("window-current")],
      total: 75,
      has_more?: true,
      next_cursor: "window-next"
    }

    Application.put_env(:favn_view, :operator_run_windows_fun, fn _context, "run", opts ->
      send(test_pid, {:window_refresh, opts})
      {:ok, window_page}
    end)

    {:ok, mounted} = RunDetailLive.mount(%{"run_id" => "run"}, %{}, connected_socket())

    window_socket =
      mounted
      |> Phoenix.Component.assign(:active_mode, :windows)
      |> Phoenix.Component.assign(:run_events_live?, true)
      |> Phoenix.Component.assign(:fallback_poll_ref, nil)
      |> Phoenix.Component.assign(
        :run,
        Map.put(mounted.assigns.run, :window_current_cursor, "window-current")
      )

    assert {:noreply, first_refresh} = RunDetailLive.handle_info(:refresh_run, window_socket)
    first_task = first_refresh.assigns.flow_task_name
    assert_receive {:window_refresh, _opts}

    assert {:noreply, wake_retained} =
             RunDetailLive.handle_info(
               {:favn_run_projected, %{"publication_id" => 2, "change" => "header"}},
               first_refresh
             )

    assert wake_retained.assigns.summary_refresh_pending?
    assert is_nil(wake_retained.assigns.flow_pending_watermark)

    assert {:noreply, second_refresh} =
             RunDetailLive.handle_async(
               first_task,
               {:ok, {:summary_refresh, :windows, "window-current", {:ok, window_page}}},
               wake_retained
             )

    refute second_refresh.assigns.summary_refresh_pending?
    assert second_refresh.assigns.flow_task_name != first_task
    assert_receive {:window_refresh, _opts}

    assert {:noreply, settled} =
             RunDetailLive.handle_async(
               second_refresh.assigns.flow_task_name,
               {:ok, {:summary_refresh, :windows, "window-current", {:ok, window_page}}},
               second_refresh
             )

    assert settled.assigns.run.total_windows == 75
    assert length(settled.assigns.run.child_runs) == 1
    refute settled.assigns.summary_refresh_pending?
    refute settled.assigns.summary_repair_required?
    refute settled.assigns.fallback_poll_ref

    assert {:noreply, duplicate_ignored} =
             RunDetailLive.handle_info(
               {:favn_run_projected, %{"publication_id" => 2, "change" => "header"}},
               settled
             )

    refute duplicate_ignored.assigns.flow_task_name
    refute_receive {:window_refresh, _opts}, 25
  end

  test "a Window repair remains retryable until its bounded refresh succeeds" do
    Application.put_env(:favn_view, :operator_run_flow_fun, fn _context, "run", _opts ->
      {:ok, page(1, 2, nil, "cursor-1", false, false)}
    end)

    Application.put_env(:favn_view, :operator_run_windows_fun, fn _context, "run", _opts ->
      {:error, :timeout}
    end)

    {:ok, mounted} = RunDetailLive.mount(%{"run_id" => "run"}, %{}, connected_socket())

    window_socket =
      mounted
      |> Phoenix.Component.assign(:active_mode, :windows)
      |> Phoenix.Component.assign(:run_events_live?, true)
      |> Phoenix.Component.assign(:fallback_poll_ref, nil)

    assert {:noreply, refreshing} =
             RunDetailLive.handle_info(
               {:favn_run_projected, %{"repair_generation" => 2, "change" => "membership"}},
               window_socket
             )

    assert refreshing.assigns.summary_repair_required?
    refute refreshing.assigns.flow_reconcile_required?

    assert {:noreply, failed} =
             RunDetailLive.handle_async(
               refreshing.assigns.flow_task_name,
               {:ok, {:summary_refresh, :windows, nil, {:error, :timeout}}},
               refreshing
             )

    assert failed.assigns.summary_repair_required?
    assert is_reference(failed.assigns.fallback_poll_ref)
  end

  test "a repair racing a Window refresh is acknowledged only by its follow-up read" do
    Application.put_env(:favn_view, :operator_run_flow_fun, fn _context, "run", _opts ->
      {:ok, page(1, 2, nil, "cursor-1", false, false)}
    end)

    window_page = %SummaryPage{
      items: [window_summary("window-current")],
      total: 75,
      has_more?: true,
      next_cursor: "window-next"
    }

    Application.put_env(:favn_view, :operator_run_windows_fun, fn _context, "run", _opts ->
      {:ok, window_page}
    end)

    {:ok, mounted} = RunDetailLive.mount(%{"run_id" => "run"}, %{}, connected_socket())

    window_socket =
      mounted
      |> Phoenix.Component.assign(:active_mode, :windows)
      |> Phoenix.Component.assign(:run_events_live?, true)
      |> Phoenix.Component.assign(:fallback_poll_ref, nil)

    assert {:noreply, first_refresh} = RunDetailLive.handle_info(:refresh_run, window_socket)
    first_task = first_refresh.assigns.flow_task_name

    assert {:noreply, repair_pending} =
             RunDetailLive.handle_info(
               {:favn_run_projected, %{"repair_generation" => 3, "change" => "membership"}},
               first_refresh
             )

    assert repair_pending.assigns.summary_repair_required?

    assert {:noreply, repair_refresh} =
             RunDetailLive.handle_async(
               first_task,
               {:ok, {:summary_refresh, :windows, nil, {:ok, window_page}}},
               repair_pending
             )

    assert repair_refresh.assigns.summary_repair_required?
    assert repair_refresh.assigns.summary_task_repair_generation == 3

    assert {:noreply, retryable} =
             RunDetailLive.handle_async(
               repair_refresh.assigns.flow_task_name,
               {:ok, {:summary_refresh, :windows, nil, {:error, :timeout}}},
               repair_refresh
             )

    assert retryable.assigns.summary_repair_required?
    assert is_reference(retryable.assigns.fallback_poll_ref)
  end

  test "a filter change invalidates a coalesced Flow wake and its old timer" do
    test_pid = self()

    Application.put_env(:favn_view, :operator_run_flow_fun, fn _context, "run", opts ->
      send(test_pid, {:flow_read, opts})
      {:ok, page(1, 2, nil, "cursor-1", false, false)}
    end)

    {:ok, mounted} = RunDetailLive.mount(%{"run_id" => "run"}, %{}, connected_socket())
    assert_receive {:flow_read, _initial}

    assert {:noreply, scheduled} =
             RunDetailLive.handle_info(
               {:favn_run_projected, %{"publication_id" => 2, "change" => "header"}},
               mounted
             )

    old_token = scheduled.assigns.refresh_timer_ref
    assert is_reference(old_token)
    assert scheduled.assigns.flow_pending_watermark == 2

    assert {:noreply, filtered} =
             RunDetailLive.handle_params(%{"asset_prefix" => "asset_"}, "", scheduled)

    assert_receive {:flow_read, filtered_opts}
    assert filtered_opts[:asset_prefix] == "asset_"
    refute filtered.assigns.refresh_timer_ref
    refute filtered.assigns.flow_pending_watermark
    refute filtered.assigns.flow_reconcile_pending?

    assert {:noreply, unchanged} =
             RunDetailLive.handle_info({:refresh_run, old_token}, filtered)

    assert unchanged.assigns.run == filtered.assigns.run
    refute_received {:flow_read, _stale}
  end

  defp complete_flow_page(socket, direction, cursor, result) do
    RunDetailLive.handle_async(
      socket.assigns.flow_task_name,
      {:ok, {:flow, direction, cursor, result}},
      socket
    )
  end

  defp page(first, count, next_cursor, previous_cursor, has_next?, has_previous?) do
    %Page{
      header: header(),
      items: Enum.map(first..(first + count - 1), &step/1),
      next_cursor: next_cursor,
      previous_cursor: previous_cursor,
      asset_prefix: nil,
      has_next?: has_next?,
      has_previous?: has_previous?
    }
  end

  defp page_from_steps(items, next_cursor, previous_cursor, has_next?, has_previous?) do
    %Page{
      header: header(),
      items: items,
      next_cursor: next_cursor,
      previous_cursor: previous_cursor,
      asset_prefix: nil,
      has_next?: has_next?,
      has_previous?: has_previous?
    }
  end

  defp reconciliation(pages, boundaries) do
    {:ok, List.first(pages), Enum.flat_map(pages, & &1.items), List.last(pages), boundaries}
  end

  defp step_ids(socket), do: Enum.map(socket.assigns.run.attempts, & &1.asset_step_id)

  defp header do
    %Header{
      run_id: "run",
      root_run_id: "run",
      status: :running,
      counts: %{
        total: 10_000,
        completed: 0,
        succeeded: 0,
        skipped: 0,
        failed: 0,
        running: 0,
        queued: 0,
        planned: 10_000
      },
      filtered_total: 10_000,
      unfiltered_total: 10_000,
      projection_cursor: 1,
      trigger_type: :manual,
      started_at: ~U[2026-08-23 10:00:00Z],
      updated_at: ~U[2026-08-23 10:00:00Z],
      finished_at: nil,
      target_id: "pipeline",
      target_label: "Example:Pipeline",
      manifest_version_id: "manifest"
    }
  end

  defp step(index) do
    id = index |> Integer.to_string() |> String.pad_leading(5, "0")

    %Step{
      run_id: "run",
      asset_step_id: "step-#{id}",
      asset_ref: "Example.Asset:asset_#{id}",
      display_name: "asset_#{id}",
      status: :planned,
      stage: 0
    }
  end

  defp window_summary(run_id) do
    %{
      run_id: run_id,
      window_id: "window-id",
      status: :running,
      window_start_at: ~U[2026-08-01 00:00:00Z],
      window_end_at: ~U[2026-08-02 00:00:00Z],
      duration_ms: nil,
      counts: %{total: 1, succeeded: 0, skipped: 0, failed: 0, running: 1, queued: 0, planned: 0}
    }
  end

  defp event_summary(sequence) do
    %{
      sequence: sequence,
      status: :running,
      occurred_at: ~U[2026-08-01 00:00:00Z],
      event_type: :step_running,
      asset_step_id: "step-00051",
      summary: "step running"
    }
  end

  defp connected_socket do
    %Phoenix.LiveView.Socket{
      transport_pid: self(),
      assigns: %{__changed__: %{}, current_scope: oslo_scope()}
    }
  end

  defp disconnected_socket do
    %Phoenix.LiveView.Socket{
      transport_pid: nil,
      assigns: %{__changed__: %{}, current_scope: oslo_scope()}
    }
  end

  defp oslo_scope do
    %Scope{
      operator_context: :operator_context,
      actor: %{id: "actor", username: "viewer", display_name: "Viewer", roles: [:viewer]},
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
