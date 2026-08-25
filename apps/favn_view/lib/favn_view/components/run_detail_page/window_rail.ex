defmodule FavnView.Components.RunDetailPage.WindowRail do
  @moduledoc """
  Calendar rail of a backfill's window runs.

  The rail is navigation, never a source of run state: every value below it
  comes from the exact-run read. Cells are window runs, so a window the
  backfill has planned but not yet started is absent by design and the rail
  says so while the backfill is still producing.

  Layout comes from `FavnView.RunWindowRail`, which is pure and tested apart
  from this markup.

  In compare mode a cell picks a window to draw rather than a window to open, so
  the rail stops being navigation for the duration. Arrow keys still move
  between window runs, which is how the operator changes the open run without
  leaving the comparison.
  """

  use FavnView, :html

  alias FavnView.RunWindowRail
  alias FavnView.UI.Tokens

  attr :rail, RunWindowRail, required: true
  attr :compare?, :boolean, default: false
  attr :limit_reached?, :boolean, default: false
  attr :on_select, :string, default: "select_window"
  attr :on_open_bucket, :string, default: "open_window_bucket"
  attr :on_step, :string, default: "step_window"
  attr :on_toggle_compare, :string, default: "toggle_compare"
  attr :on_toggle_compare_window, :string, default: "toggle_compare_window"

  def window_rail(assigns) do
    ~H"""
    <.panel :if={@rail.cells != [] or @rail.buckets != []} padding={:none} data-testid="window-rail">
      <:header title="Window runs" subtitle={subtitle(@rail, @compare?)} />
      <:actions>
        <.status_badge
          :if={@rail.in_progress?}
          tone={:info}
          label="Still running"
          size={:sm}
          data-testid="window-rail-in-progress"
        />
        <.count_badge count={length(@rail.cells)} label="shown" />
        <.button
          size={:sm}
          variant={if(@compare?, do: :secondary, else: :ghost)}
          icon="hero-square-2-stack"
          phx-click={@on_toggle_compare}
          aria-pressed={to_string(@compare?)}
          data-testid="window-rail-compare-toggle"
        >
          Compare
        </.button>
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

        <div
          class="flex flex-wrap gap-1"
          role="group"
          aria-label={if(@compare?, do: "Windows to compare", else: "Window runs")}
        >
          <button
            :for={cell <- @rail.cells}
            type="button"
            phx-click={if(@compare?, do: @on_toggle_compare_window, else: @on_select)}
            phx-value-run_id={cell.run_id}
            aria-current={cell.selected? && "true"}
            aria-pressed={@compare? && to_string(cell.compared?)}
            title={cell_title(cell, @compare?)}
            data-testid="window-rail-cell"
            data-window-status={cell.status}
            data-window-count={cell.window_count}
            data-compared={@compare? && to_string(cell.compared?)}
            data-track={cell.track}
            class={[
              "min-w-9 rounded-md border px-2 py-1 text-center text-xs tabular-nums transition-colors",
              "focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-primary",
              Tokens.border_class(tone(cell.status)),
              Tokens.surface_class(tone(cell.status)),
              Tokens.text_class(tone(cell.status)),
              cell.selected? && "ring-2 ring-primary ring-offset-1 ring-offset-base-100 font-semibold",
              @compare? && cell.compared? && "outline-2 outline-offset-1 outline-primary"
            ]}
          >
            {cell.label}
            <span :if={cell.window_count > 1} class="ml-1 opacity-70">
              +{cell.window_count - 1}
            </span>
            <span :if={@compare? && cell.compared?} class="ml-1 font-semibold" aria-hidden="true">
              T{cell.track}
            </span>
          </button>
        </div>

        <p
          :if={@compare? && @limit_reached?}
          class={["text-xs", Tokens.text_class(:warning)]}
          data-testid="window-rail-compare-limit"
        >
          A comparison holds at most {RunWindowRail.compare_limit()} windows. Remove one to add
          another.
        </p>

        <p
          :if={@rail.truncated?}
          class="text-xs favn-text-subtle"
          data-testid="window-rail-truncated"
        >
          Showing the latest {@rail.loaded_count} window runs; older windows exist.
        </p>
      </div>
    </.panel>
    """
  end

  defp subtitle(_rail, true),
    do: "Pick windows to compare; arrow keys still move between them."

  defp subtitle(%RunWindowRail{in_progress?: true}, _compare?),
    do: "The backfill is still creating window runs."

  defp subtitle(%RunWindowRail{layout: :banded}, _compare?),
    do: "Pick a period, then a window run."

  defp subtitle(_rail, _compare?), do: "Select a window run to open it."

  # The open run is always part of its own comparison, so its cell says why it
  # cannot be removed rather than looking like a control that failed.
  defp cell_title(%{selected?: true} = cell, true),
    do: "#{window_title(cell)} · Track #{cell.track} · The open window always compares"

  defp cell_title(%{compared?: true} = cell, true),
    do: "#{window_title(cell)} · Track #{cell.track} · Click to remove from the comparison"

  defp cell_title(cell, true), do: "#{window_title(cell)} · Click to add to the comparison"

  defp cell_title(cell, _compare?), do: window_title(cell)

  defp window_title(%{window_count: count} = cell) when count > 1,
    do: "#{cell.label} · #{status_label(cell.status)} · #{count} windows in one run"

  defp window_title(cell), do: "#{cell.label} · #{status_label(cell.status)}"

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
