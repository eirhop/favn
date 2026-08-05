defmodule FavnView.RunDetailLiveTest do
  use ExUnit.Case, async: false

  alias FavnView.Auth.Scope
  alias FavnView.RunDetailLive

  setup do
    previous = Application.get_env(:favn_view, :operator_run_activity_fun)
    on_exit(fn -> restore_env(:operator_run_activity_fun, previous) end)
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

  defp restore_env(key, nil), do: Application.delete_env(:favn_view, key)
  defp restore_env(key, value), do: Application.put_env(:favn_view, key, value)
end
