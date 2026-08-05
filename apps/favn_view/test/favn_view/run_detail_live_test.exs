defmodule FavnView.RunDetailLiveTest do
  use ExUnit.Case, async: false

  alias FavnView.Auth.Scope
  alias FavnView.RunDetailLive

  setup do
    previous = Application.get_env(:favn_view, :operator_run_activity_fun)
    previous_subscribe = Application.get_env(:favn_view, :run_subscribe_fun)
    previous_stream = Application.get_env(:favn_view, :run_stream_events_fun)
    previous_timezone = Application.get_env(:favn, :default_timezone)
    Application.put_env(:favn_view, :run_subscribe_fun, fn _context, _run_id -> :ok end)
    Application.put_env(:favn, :default_timezone, "Europe/Oslo")

    Application.put_env(:favn_view, :run_stream_events_fun, fn _context, _run_id, _opts ->
      {:ok, []}
    end)

    on_exit(fn ->
      restore_env(:operator_run_activity_fun, previous)
      restore_env(:run_subscribe_fun, previous_subscribe)
      restore_env(:run_stream_events_fun, previous_stream)
      restore_app_env(:favn, :default_timezone, previous_timezone)
    end)
  end

  test "retries a committed run while its operator projection is unavailable" do
    Application.put_env(:favn_view, :operator_run_activity_fun, fn
      :operator_context, "run-committed", _opts -> {:error, :not_found}
    end)

    socket = %Phoenix.LiveView.Socket{
      transport_pid: self(),
      assigns: %{
        __changed__: %{},
        current_scope: %Scope{operator_context: :operator_context}
      }
    }

    assert {:ok, mounted} = RunDetailLive.mount(%{"run_id" => "run-committed"}, %{}, socket)
    assert mounted.assigns.run.initializing?
    assert mounted.assigns.detail_load_attempts_remaining == 10
    assert is_reference(mounted.assigns.fallback_poll_ref)

    assert {:noreply, retried} =
             RunDetailLive.handle_info({:poll_run, mounted.assigns.fallback_poll_ref}, mounted)

    assert retried.assigns.run.initializing?
    assert retried.assigns.detail_load_attempts_remaining == 9
    assert is_reference(retried.assigns.fallback_poll_ref)
  end

  test "keeps a durable queued submission visible and polling" do
    now = DateTime.utc_now()

    Application.put_env(:favn_view, :operator_run_activity_fun, fn
      :operator_context, "run-queued", _opts ->
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
             target_id: "crm_Reference",
             attempt: 0,
             enqueued_at: now,
             updated_at: now,
             terminal_at: nil,
             failure: nil
           }
         }}
    end)

    socket = %Phoenix.LiveView.Socket{
      transport_pid: self(),
      assigns: %{
        __changed__: %{},
        current_scope: %Scope{operator_context: :operator_context}
      }
    }

    assert {:ok, mounted} = RunDetailLive.mount(%{"run_id" => "run-queued"}, %{}, socket)
    assert mounted.assigns.run.submission?
    assert mounted.assigns.run.raw_status == :queued
    refute mounted.assigns.run.initializing?
    assert mounted.assigns.detail_load_attempts_remaining == 0
    assert is_reference(mounted.assigns.fallback_poll_ref)

    assert {:noreply, selected} =
             RunDetailLive.handle_params(%{"attempt" => "attempt-1"}, "", mounted)

    assert selected.assigns.selected_attempt_id == "attempt-1"

    assert {:noreply, refreshed} = RunDetailLive.handle_info(:refresh_run, selected)
    assert refreshed.assigns.selected_attempt_id == "attempt-1"
  end

  test "uses requested windows for child rows and names mixed execution scope" do
    started_at = ~U[2026-07-01 08:00:00Z]
    finished_at = ~U[2026-07-01 08:05:00Z]
    june = window("month:Europe/Oslo:2026-06", ~U[2026-05-31 22:00:00Z])
    july = window("month:Europe/Oslo:2026-07", ~U[2026-06-30 22:00:00Z])

    Application.put_env(:favn_view, :operator_run_activity_fun, fn
      :operator_context, "root", _opts ->
        {:ok,
         %{
           kind: :run,
           detail: %{
             summary: %{
               id: "root",
               status: :ok,
               root_status: :ok,
               active?: false,
               trigger_type: :backfill,
               target_assets: [],
               started_at: started_at,
               finished_at: finished_at,
               duration_ms: 300_000,
               total_windows: 1,
               completed_windows: 1,
               failed_windows: 0,
               total_asset_attempts: 3,
               completed_asset_attempts: 3,
               failed_asset_attempts: 0,
               running_asset_attempts: 0,
               queued_asset_attempts: 0,
               progress: nil
             },
             root_run: %{
               id: "root",
               status: :ok,
               submit_kind: :backfill_pipeline,
               manifest_version_id: "manifest",
               event_seq: 2
             },
             child_runs: [
               %{
                 id: "child-july",
                 status: :ok,
                 window: nil,
                 started_at: started_at,
                 finished_at: finished_at,
                 duration_ms: 300_000,
                 event_seq: 4
               }
             ],
             windows: [june, july],
             requested_windows: [
               july
               |> Map.put(:child_run_id, "child-july")
               |> Map.put(:status, :succeeded)
             ],
             has_non_windowed_assets?: true,
             asset_attempts: [
               attempt("none", nil, :ok, started_at, finished_at),
               attempt("june", june, :ok, started_at, finished_at),
               attempt("july", july, :skipped_fresh, started_at, finished_at)
             ],
             backfill_failures: [],
             backfill_failure_count: 0,
             root_event_sequence: 2
           }
         }}
    end)

    socket = %Phoenix.LiveView.Socket{
      transport_pid: self(),
      assigns: %{
        __changed__: %{},
        current_scope: %Scope{operator_context: :operator_context}
      }
    }

    assert {:ok, mounted} = RunDetailLive.mount(%{"run_id" => "root"}, %{}, socket)
    assert mounted.assigns.run.subtitle == "No window & Jun 2026 -> Jul 2026"
    assert mounted.assigns.run.window == "No window & Jun 2026 -> Jul 2026"

    assert [%{window_label: "Jul 2026", progress: "3 / 3"}] =
             mounted.assigns.run.child_runs
  end

  defp window(key, start_at) do
    %{
      key: key,
      label: nil,
      kind: :month,
      start_at: start_at,
      end_at: DateTime.add(start_at, 30, :day),
      timezone: "Europe/Oslo"
    }
  end

  defp attempt(id, window, status, started_at, finished_at) do
    %{
      id: id,
      asset_step_id: id,
      root_execution_group_id: "root",
      child_run_id: "child-july",
      run_id: "child-july",
      status: status,
      asset_key: "Asset:#{id}",
      asset_ref: "Asset:#{id}",
      stage: 0,
      execution_pool: nil,
      queue_reason: nil,
      attempt_number: 1,
      started_at: started_at,
      finished_at: finished_at,
      duration_ms: 300_000,
      error_summary: nil,
      output_metadata: nil,
      window: window
    }
  end

  defp restore_env(key, nil), do: Application.delete_env(:favn_view, key)
  defp restore_env(key, value), do: Application.put_env(:favn_view, key, value)
  defp restore_app_env(app, key, nil), do: Application.delete_env(app, key)
  defp restore_app_env(app, key, value), do: Application.put_env(app, key, value)
end
