defmodule FavnView.Components.SelectedWindowActions do
  @moduledoc """
  Compact action strip for the selected asset timeline window.
  """

  use FavnView, :html

  attr :selected_window, :map, default: nil
  attr :submitting_window_run?, :boolean, default: false
  attr :selected_window_error, :string, default: nil
  attr :submitted_run_id, :string, default: nil

  def selected_window_actions(assigns) do
    ~H"""
    <div
      :if={@selected_window}
      class="flex flex-col gap-3 rounded-box border border-base-content/10 bg-base-content/[0.04] p-4 sm:flex-row sm:items-center sm:justify-between"
      data-testid="selected-window-actions"
    >
      <div class="min-w-0">
        <p class="text-xs uppercase tracking-[0.18em] text-base-content/45">Selected window</p>
        <p class="mt-1 text-sm font-medium text-base-content">{@selected_window.range_label}</p>
        <p class="mt-0.5 text-xs text-base-content/55">{status_label(@selected_window.status)}</p>
        <p :if={!@selected_window.run_enabled?} class="mt-1 text-xs text-base-content/45">
          {run_disabled_reason_label(@selected_window.run_disabled_reason)}
        </p>
        <p
          :if={@selected_window_error}
          class="mt-1 text-xs text-error"
          data-testid="selected-window-error"
        >
          {@selected_window_error}
        </p>
        <p :if={@submitted_run_id} class="mt-1 text-xs text-success" data-testid="submitted-run-id">
          Submitted {@submitted_run_id}
        </p>
      </div>

      <div class="flex w-full shrink-0 justify-end gap-2 sm:w-auto">
        <button
          type="button"
          class="btn btn-primary btn-soft btn-sm"
          phx-click="run_selected_window"
          phx-disable-with="Submitting..."
          disabled={!@selected_window.run_enabled? || @submitting_window_run?}
          data-testid="run-selected-window"
        >
          <span :if={@submitting_window_run?} class="loading loading-spinner loading-xs"></span>
          {@selected_window.run_label || "Run this window"}
        </button>
      </div>
    </div>
    """
  end

  defp status_label(:success), do: "healthy"
  defp status_label(:warning), do: "late"
  defp status_label(:error), do: "failed"
  defp status_label(:muted), do: "pending"
  defp status_label(_status), do: "unknown"

  defp run_disabled_reason_label(:asset_has_no_window_policy), do: "No window policy"
  defp run_disabled_reason_label(:invalid_window), do: "Invalid window"
  defp run_disabled_reason_label(_reason), do: "Not runnable"
end
