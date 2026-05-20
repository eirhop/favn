defmodule FavnView.Components.RunDetailPage.Ui do
  @moduledoc false
  def status_label(:ok), do: "Succeeded"
  def status_label(:running), do: "Running"
  def status_label(:pending), do: "Queued"
  def status_label(:queued), do: "Queued"
  def status_label(:error), do: "Failed"
  def status_label(:partial), do: "Partial"
  def status_label(:cancelled), do: "Cancelled"
  def status_label(:timed_out), do: "Timed out"
  def status_label(:skipped), do: "Skipped"
  def status_label(nil), do: "Pending"

  def status_label(status),
    do: status |> to_string() |> String.replace("_", " ") |> String.capitalize()

  def status_tone(:ok), do: :success
  def status_tone(:running), do: :info
  def status_tone(:pending), do: :warning
  def status_tone(:queued), do: :warning
  def status_tone(:error), do: :error
  def status_tone(:timed_out), do: :error
  def status_tone(:partial), do: :warning
  def status_tone(_status), do: :neutral
  def status_badge_class(:success), do: "badge badge-success badge-soft"
  def status_badge_class(:info), do: "badge badge-info badge-soft"
  def status_badge_class(:warning), do: "badge badge-warning badge-soft"
  def status_badge_class(:error), do: "badge badge-error badge-soft"
  def status_badge_class(_tone), do: "badge badge-ghost"
  def icon_shell_class(:success), do: "bg-success/15 text-success"
  def icon_shell_class(:info), do: "bg-info/15 text-info"
  def icon_shell_class(:primary), do: "bg-primary/15 text-primary"
  def icon_shell_class(:warning), do: "bg-warning/15 text-warning"
  def icon_shell_class(:error), do: "bg-error/15 text-error"
  def icon_shell_class(_tone), do: "bg-base-content/10 text-base-content/60"
  def status_icon(:success), do: "hero-check-circle"
  def status_icon(:info), do: "hero-arrow-path"
  def status_icon(:warning), do: "hero-clock"
  def status_icon(:error), do: "hero-x-circle"
  def status_icon(_tone), do: "hero-minus-circle"

  def matrix_cell_class(:success),
    do:
      "border-b border-r border-success/20 bg-success/15 p-3 text-center text-success transition hover:bg-success/25 disabled:cursor-not-allowed"

  def matrix_cell_class(:error),
    do:
      "border-b border-r border-error/30 bg-error/15 p-3 text-center text-error transition hover:bg-error/25 disabled:cursor-not-allowed"

  def matrix_cell_class(:info),
    do:
      "border-b border-r border-info/30 bg-info/20 p-3 text-center text-info transition hover:bg-info/30 disabled:cursor-not-allowed"

  def matrix_cell_class(:warning),
    do:
      "border-b border-r border-warning/25 bg-warning/15 p-3 text-center text-warning transition hover:bg-warning/25 disabled:cursor-not-allowed"

  def matrix_cell_class(_tone),
    do:
      "border-b border-r border-base-content/10 bg-base-content/[0.03] p-3 text-center text-base-content/50 transition disabled:cursor-not-allowed"

  def legend_class(:success), do: "bg-success/15 text-success"
  def legend_class(:error), do: "bg-error/15 text-error"
  def legend_class(:info), do: "bg-info/15 text-info"
  def legend_class(:warning), do: "bg-warning/15 text-warning"
  def legend_class(_tone), do: "bg-base-content/10 text-base-content/60"
end
