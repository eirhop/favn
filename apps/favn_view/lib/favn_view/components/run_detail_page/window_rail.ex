defmodule FavnView.Components.RunDetailPage.WindowRail do
  @moduledoc """
  Calendar rail of a backfill's window runs.

  The rail is navigation, never a source of run state: every value below it
  comes from the exact-run read. Cells are window runs, so a window the
  backfill has planned but not yet started is absent by design and the rail
  says so while the backfill is still producing.

  Layout comes from `FavnView.RunWindowRail`, which is pure and tested apart
  from this markup.
  """

  use FavnView, :html

  alias FavnView.RunWindowRail
  alias FavnView.UI.Tokens

  attr :rail, RunWindowRail, required: true
  attr :on_select, :string, default: "select_window"
  attr :on_open_bucket, :string, default: "open_window_bucket"
  attr :on_step, :string, default: "step_window"

  def window_rail(assigns) do
    ~H"""
    <.panel :if={@rail.cells != [] or @rail.buckets != []} padding={:none} data-testid="window-rail">
      <:header title="Window runs" subtitle={subtitle(@rail)} />
      <:actions>
        <.status_badge
          :if={@rail.in_progress?}
          tone={:info}
          label="Still running"
          size={:sm}
          data-testid="window-rail-in-progress"
        />
        <.count_badge count={length(@rail.cells)} label="shown" />
      </:actions>

      <%!-- Keydown is scoped to the rail, not the window: arrow keys move between
      window runs only while focus is inside it. --%>
      <div class="flex flex-col gap-2 p-3" phx-keydown={@on_step}>
        <div
          :if={@rail.layout == :banded}
          class="flex flex-wrap gap-1"
          data-testid="window-rail-buckets"
        >
          <button
            :for={bucket <- @rail.buckets}
            type="button"
            phx-click={@on_open_bucket}
            phx-value-bucket={bucket.id}
            aria-pressed={to_string(bucket.selected?)}
            data-testid="window-rail-bucket"
            class={[
              "rounded-md border px-2 py-1 text-xs transition-colors",
              "focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-primary",
              bucket.selected? && "border-primary/60 bg-primary/10 font-medium",
              !bucket.selected? && "border-base-content/15 hover:bg-base-content/5"
            ]}
          >
            {bucket.label}
            <span class="favn-text-subtle">{bucket.count}</span>
          </button>
        </div>

        <div class="flex flex-wrap gap-1" role="group" aria-label="Window runs">
          <button
            :for={cell <- @rail.cells}
            type="button"
            phx-click={@on_select}
            phx-value-run_id={cell.run_id}
            aria-current={cell.selected? && "true"}
            title={cell_title(cell)}
            data-testid="window-rail-cell"
            data-window-status={cell.status}
            data-window-count={cell.window_count}
            class={[
              "min-w-9 rounded-md border px-2 py-1 text-center text-xs tabular-nums transition-colors",
              "focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-primary",
              Tokens.border_class(tone(cell.status)),
              Tokens.surface_class(tone(cell.status)),
              Tokens.text_class(tone(cell.status)),
              cell.selected? && "ring-2 ring-primary ring-offset-1 ring-offset-base-100 font-semibold"
            ]}
          >
            {cell.label}
            <span :if={cell.window_count > 1} class="ml-1 opacity-70">
              +{cell.window_count - 1}
            </span>
          </button>
        </div>

        <p
          :if={@rail.truncated?}
          class="text-xs favn-text-subtle"
          data-testid="window-rail-truncated"
        >
          Showing the latest {length(@rail.cells)} window runs; older windows exist.
        </p>
      </div>
    </.panel>
    """
  end

  defp subtitle(%RunWindowRail{in_progress?: true}),
    do: "The backfill is still creating window runs."

  defp subtitle(%RunWindowRail{layout: :banded}),
    do: "Pick a period, then a window run."

  defp subtitle(_rail), do: "Select a window run to open it."

  defp cell_title(%{window_count: count} = cell) when count > 1,
    do: "#{cell.label} · #{status_label(cell.status)} · #{count} windows in one run"

  defp cell_title(cell), do: "#{cell.label} · #{status_label(cell.status)}"

  # Window run statuses are not run statuses. `ready` means waiting to start
  # rather than finished well, so the shared token mapping is not used here.
  defp tone(:succeeded), do: :success
  defp tone(:failed), do: :error
  defp tone(:running), do: :info
  defp tone(:claimed), do: :info
  defp tone(:ready), do: :warning
  defp tone(:planned), do: :warning
  defp tone(_status), do: :neutral

  defp status_label(nil), do: "Unknown"

  defp status_label(status),
    do: status |> to_string() |> String.replace("_", " ") |> String.capitalize()
end
