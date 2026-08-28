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
  alias FavnView.Time
  alias FavnView.UI.Tokens
  alias FavnView.WindowLabel

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
    <.panel :if={show?(@rail)} padding={:sm} data-testid="window-rail">
      <:header title="Window runs" subtitle={subtitle(@rail, @compare?)} />
      <:actions>
        <.status_badge
          :if={@rail.in_progress?}
          tone={:info}
          label="Still running"
          size={:sm}
          data-testid="window-rail-in-progress"
        />
        <.badge tone={:neutral} variant={:outline}>{length(@rail.cells)} shown</.badge>
        <%!-- The comparison needs the chart, and the chart is a desktop reading:
        below `lg` a phone shows the card list, so a compare toggle there would
        be a control that changes nothing. --%>
        <.button
          class="hidden lg:inline-flex"
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
      <div class="flex flex-col gap-2" phx-keydown={@on_step}>
        <%!-- One continuous rail whose items light up inside it, not a row of
        separate buttons: a window run is a position on a calendar, and the
        calendar is the thing on screen. Status rides a dot rather than tinting
        the whole cell, so six statuses cannot fight the selected state for the
        operator's attention. --%>
        <nav
          :if={@rail.layout == :banded}
          class="favn-surface-rail flex w-fit max-w-full flex-wrap gap-0.5 rounded-box p-1"
          aria-label="Window periods"
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
              "favn-mode-item h-9 gap-1.5 rounded-field px-2.5 text-sm font-medium",
              "focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-primary",
              (bucket.selected? && "favn-mode-item-active") || "favn-text-muted"
            ]}
          >
            {bucket.label}
            <.count_badge count={bucket.count} label="window runs" />
          </button>
        </nav>

        <nav
          class="favn-surface-rail flex w-fit max-w-full flex-wrap gap-0.5 rounded-box p-1"
          aria-label={if(@compare?, do: "Windows to compare", else: "Window runs")}
        >
          <button
            :for={cell <- @rail.cells}
            type="button"
            phx-click={if(@compare?, do: @on_toggle_compare_window, else: @on_select)}
            phx-value-run_id={cell.run_id}
            aria-current={cell.selected? && "true"}
            aria-pressed={@compare? && to_string(cell.compared?)}
            title={cell_title(cell, @rail.timezone, @compare?)}
            data-testid="window-rail-cell"
            data-window-status={cell.status}
            data-window-count={cell.window_count}
            data-compared={@compare? && to_string(cell.compared?)}
            data-track={cell.track}
            class={[
              "favn-mode-item h-9 gap-1.5 rounded-field px-2.5 text-sm font-medium tabular-nums",
              "focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-primary",
              (highlighted?(cell, @compare?) && "favn-mode-item-active") || "favn-text-muted"
            ]}
          >
            <span
              class={["status status-xs", Tokens.dot_class(tone(cell.status))]}
              aria-hidden="true"
            ></span>
            {cell.label}
            <span :if={cell.window_count > 1} class="favn-text-subtle">
              +{cell.window_count - 1}
            </span>
            <span :if={@compare? && cell.compared?} class="favn-track-index" aria-hidden="true">
              {cell.track}
            </span>
          </button>
        </nav>

        <p
          :if={@compare? && @limit_reached?}
          class={["text-sm", Tokens.text_class(:warning)]}
          data-testid="window-rail-compare-limit"
        >
          A comparison holds at most {RunWindowRail.compare_limit()} windows. Remove one to add
          another.
        </p>

        <p
          :if={@rail.truncated?}
          class="text-sm favn-text-subtle"
          data-testid="window-rail-truncated"
        >
          Showing the latest {@rail.loaded_count} window runs; older windows exist.
        </p>
      </div>
    </.panel>
    """
  end

  # A combined backfill runs every window it covers as one run, so the rail
  # would offer one cell leading to the page the operator is already on. The
  # coverage span is stated in the run's own header instead.
  defp show?(%RunWindowRail{combined: combined}) when not is_nil(combined), do: false
  defp show?(%RunWindowRail{cells: cells, buckets: buckets}), do: cells != [] or buckets != []

  defp subtitle(rail, true), do: with_coverage(rail, "Pick the windows to compare.")

  defp subtitle(%RunWindowRail{in_progress?: true} = rail, _compare?),
    do: with_coverage(rail, "The backfill is still creating window runs.")

  # A banded rail names the period in its band header, so the cells under it read
  # as positions inside a period the operator can already see.
  defp subtitle(%RunWindowRail{layout: :banded}, _compare?),
    do: "Pick a period, then a window run."

  defp subtitle(rail, _compare?), do: with_coverage(rail, "Select a window run to open it.")

  # A flat rail has no band header, and a cell is labelled by its calendar
  # position alone: three day windows in March read "1 2 3", with the month
  # nowhere on screen. The shared context is stated once here rather than
  # repeated into every cell, which is what the short labels exist to avoid.
  defp with_coverage(%RunWindowRail{layout: :banded}, sentence), do: sentence

  defp with_coverage(%RunWindowRail{cells: cells, kind: kind}, sentence) do
    case coverage(cells, kind) do
      nil -> sentence
      span -> "#{sentence} Covering #{span}."
    end
  end

  defp coverage([], _kind), do: nil

  # Named in the windows' own timezone, because what this states is a stretch of
  # calendar periods rather than a pair of instants.
  defp coverage(cells, kind) do
    first = Enum.min_by(cells, & &1.start_at, DateTime)
    last = Enum.max_by(cells, & &1.end_at, DateTime)

    WindowLabel.span(first.start_at, last.end_at, kind, first.timezone)
  end

  # Out of compare mode the lit cell is the open window. In it, every compared
  # window is lit: the open one is one of them, and singling it out would say
  # that one of the tracks on screen is more selected than the others.
  defp highlighted?(cell, true), do: cell.compared?
  defp highlighted?(cell, _compare?), do: cell.selected?

  # The open run is always part of its own comparison, so its cell says why it
  # cannot be removed rather than looking like a control that failed.
  defp cell_title(%{selected?: true} = cell, timezone, true),
    do: "#{window_title(cell, timezone)} · Track #{cell.track} · The open window always compares"

  defp cell_title(%{compared?: true} = cell, timezone, true),
    do:
      "#{window_title(cell, timezone)} · Track #{cell.track} · Click to remove from the comparison"

  defp cell_title(cell, timezone, true),
    do: "#{window_title(cell, timezone)} · Click to add to the comparison"

  defp cell_title(cell, timezone, _compare?), do: window_title(cell, timezone)

  # A cell is labelled by its calendar position — often a bare day number — so
  # the tooltip is where the window it covers is actually stated.
  defp window_title(%{window_count: count} = cell, timezone) when count > 1,
    do:
      "#{window_span(cell, timezone)} · #{status_label(cell.status)} · #{count} windows in one run"

  defp window_title(cell, timezone),
    do: "#{window_span(cell, timezone)} · #{status_label(cell.status)}"

  # The bounds are two instants, so they are stated on the operator's clock like
  # every other timestamp on this page. The cell's *label* is a calendar period
  # and stays in the window's own timezone, which is what makes a window keyed in
  # one zone and read in another look wrong — a December window opening at 01:00.
  # So when the two differ, the tooltip names the zone the window was keyed in
  # and the mismatch stops being a contradiction.
  defp window_span(cell, timezone) do
    case WindowLabel.full(cell.start_at, cell.end_at, timezone) do
      nil -> cell.label
      bounds -> bounds <> window_timezone_note(cell, timezone)
    end
  end

  defp window_timezone_note(cell, timezone) do
    if same_clock?(cell, timezone), do: "", else: " · Window timezone #{cell.timezone}"
  end

  # Two zone names can be one clock — `UTC` and `Etc/UTC` are, and a workspace
  # may well spell its default the other way round from the window policy. The
  # note exists to explain bounds that do not match the label beside them, so it
  # is worth saying only when the two zones actually render this window
  # differently.
  defp same_clock?(%{timezone: zone}, zone), do: true

  defp same_clock?(cell, timezone) do
    Enum.all?([cell.start_at, cell.end_at], fn at ->
      wall_clock(at, cell.timezone) == wall_clock(at, timezone)
    end)
  end

  defp wall_clock(at, zone), do: at |> Time.shift(zone) |> DateTime.to_naive()

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
