defmodule FavnView.RunDetailLiveTest do
  use ExUnit.Case, async: false

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
      :run_subscribe_fun
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
    Application.put_env(:favn_view, :operator_run_flow_fun, fn _context, run_id ->
      {:ok, %{kind: :run, detail: flow(run_id, :running)}}
    end)

    assert {:ok, mounted} =
             RunDetailLive.mount(%{"run_id" => "run-one"}, %{}, connected_socket())

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
    assert second.assigns.refresh_timer_ref == first.assigns.refresh_timer_ref
    assert second.assigns.pending_run_event_sequences == %{"run-one" => 4}
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
             RunDetailLive.handle_params(%{"view" => "events"}, "", mounted)

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

  test "loads window choices only on request and restricts navigation to those choices" do
    Application.put_env(:favn_view, :operator_run_flow_fun, fn _context, run_id ->
      {:ok, %{kind: :run, detail: flow(run_id, :ok)}}
    end)

    Application.put_env(:favn_view, :operator_run_windows_fun, fn _context, "run-one" ->
      {:ok,
       %RunWindowChoices{
         overflow?: false,
         items: [
           %RunWindowChoice{
             run_id: "run-one",
             window_start_at: ~U[2026-07-01 00:00:00Z],
             window_end_at: ~U[2026-08-01 00:00:00Z]
           },
           %RunWindowChoice{
             run_id: "run-two",
             window_start_at: ~U[2026-08-01 00:00:00Z],
             window_end_at: ~U[2026-09-01 00:00:00Z]
           }
         ]
       }}
    end)

    assert {:ok, mounted} =
             RunDetailLive.mount(%{"run_id" => "run-one"}, %{}, connected_socket())

    assert is_nil(mounted.assigns.windows)
    assert {:noreply, loaded} = RunDetailLive.handle_event("load_windows", %{}, mounted)
    assert Enum.map(loaded.assigns.windows, & &1.run_id) == ["run-one", "run-two"]

    assert {:noreply, rejected} =
             RunDetailLive.handle_event("switch_window", %{"run_id" => "run-other"}, loaded)

    assert rejected.assigns.flash["error"] == "That window run is not available"

    assert {:noreply, navigating} =
             RunDetailLive.handle_event("switch_window", %{"run_id" => "run-two"}, loaded)

    assert {:live, :redirect, %{to: "/runs/run-two"}} = navigating.redirected
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
          detail?: true,
          started_at: ~U[2026-08-23 10:00:00Z],
          finished_at: if(status == :running, do: nil, else: ~U[2026-08-23 10:00:04Z])
        },
        %Asset{
          id: "planned-total",
          run_id: run_id,
          name: "Total",
          asset_ref: "crm.total",
          state: :planned,
          detail?: false,
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
        queued: 0,
        planned: 1
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
