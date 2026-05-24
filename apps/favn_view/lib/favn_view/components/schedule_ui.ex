defmodule FavnView.Components.ScheduleUi do
  @moduledoc """
  Reusable scheduler UI components shared by schedule pages and stories.
  """

  use FavnView, :html

  attr :state, :atom, required: true
  attr :label, :string, required: true

  def activation_badge(assigns) do
    ~H"""
    <span class={["badge badge-sm badge-soft gap-2", activation_class(@state)]}>
      <span class={["status status-xs", activation_dot(@state)]}></span>
      {@label}
    </span>
    """
  end

  attr :state, :atom, required: true
  attr :label, :string, required: true

  def runtime_badge(assigns) do
    ~H"""
    <span class={["badge badge-sm badge-soft gap-2", runtime_class(@state)]}>
      <span class={["status status-xs", runtime_dot(@state)]}></span>
      {@label}
    </span>
    """
  end

  attr :error, :map, default: nil

  def scheduler_error_badge(assigns) do
    ~H"""
    <span :if={!@error}>-</span>
    <span :if={@error} class="badge badge-sm badge-soft badge-warning gap-1" title={@error.message}>
      <.icon name="hero-exclamation-triangle" class="size-3" /> {@error.phase_label}
    </span>
    """
  end

  attr :status, :atom, required: true
  attr :label, :string, required: true

  def occurrence_status_badge(assigns) do
    ~H"""
    <span class={["badge badge-sm badge-soft", occurrence_status_class(@status)]}>{@label}</span>
    """
  end

  attr :occurrences, :list, required: true

  def occurrence_preview_table(assigns) do
    ~H"""
    <div class="overflow-auto">
      <table class="table table-sm" data-testid="schedule-occurrence-table">
        <thead>
          <tr class="border-base-content/10 text-xs text-base-content/55">
            <th class="font-medium">Due at</th>
            <th class="font-medium">Window</th>
            <th class="font-medium">Status</th>
            <th class="font-medium">Notes</th>
          </tr>
        </thead>
        <tbody>
          <tr
            :for={occurrence <- @occurrences}
            class="border-base-content/10 bg-base-100/5 text-sm"
            data-testid="schedule-occurrence-row"
          >
            <td class="whitespace-nowrap">
              <p class="font-medium text-base-content">{occurrence.due_label}</p>
              <p class="text-xs text-base-content/45">{occurrence.timezone}</p>
            </td>
            <td class="min-w-48 text-xs text-base-content/70">{occurrence.window_label}</td>
            <td>
              <.occurrence_status_badge status={occurrence.status} label={occurrence.status_label} />
            </td>
            <td class="min-w-48 text-xs text-base-content/65">
              <span :if={occurrence.notes == []}>-</span>
              <span :for={note <- occurrence.notes} class="mr-2 inline-block">{note}</span>
            </td>
          </tr>
        </tbody>
      </table>
    </div>
    """
  end

  def sample_occurrences do
    [
      %{
        due_at: ~U[2026-05-25 06:00:00Z],
        due_label: "May 25 06:00",
        timezone: "Europe/Oslo",
        window_label: "May 24 00:00 -> May 25 00:00",
        status: :upcoming,
        status_label: "Upcoming",
        notes: []
      },
      %{
        due_at: ~U[2026-05-26 06:00:00Z],
        due_label: "May 26 06:00",
        timezone: "Europe/Oslo",
        window_label: "May 25 00:00 -> May 26 00:00",
        status: :queued,
        status_label: "Queued",
        notes: ["Queued due to overlap policy"]
      },
      %{
        due_at: ~U[2026-05-27 06:00:00Z],
        due_label: "May 27 06:00",
        timezone: "Europe/Oslo",
        window_label: "May 26 00:00 -> May 27 00:00",
        status: :disabled,
        status_label: "Disabled",
        notes: ["Will not submit until enabled"]
      }
    ]
  end

  defp activation_class(:enabled), do: "badge-success"
  defp activation_class(:pending_activation), do: "badge-warning"
  defp activation_class(:needs_review), do: "badge-warning"
  defp activation_class(:disabled), do: "badge-error"
  defp activation_class(:retired), do: "badge-neutral"
  defp activation_class(_state), do: "badge-neutral"

  defp activation_dot(:enabled), do: "status-success"
  defp activation_dot(:pending_activation), do: "status-warning"
  defp activation_dot(:needs_review), do: "status-warning"
  defp activation_dot(:disabled), do: "status-error"
  defp activation_dot(_state), do: "status-neutral"

  defp runtime_class(:running), do: "badge-info"
  defp runtime_class(:queued), do: "badge-warning"
  defp runtime_class(:idle), do: "badge-neutral"
  defp runtime_class(:inactive), do: "badge-neutral"
  defp runtime_class(_state), do: "badge-neutral"

  defp runtime_dot(:running), do: "status-info"
  defp runtime_dot(:queued), do: "status-warning"
  defp runtime_dot(:idle), do: "status-neutral"
  defp runtime_dot(:inactive), do: "status-neutral"
  defp runtime_dot(_state), do: "status-neutral"

  defp occurrence_status_class(:upcoming), do: "badge-info"
  defp occurrence_status_class(:queued), do: "badge-warning"
  defp occurrence_status_class(:running), do: "badge-info"
  defp occurrence_status_class(:blocked), do: "badge-error"
  defp occurrence_status_class(:disabled), do: "badge-neutral"
  defp occurrence_status_class(_status), do: "badge-neutral"
end
