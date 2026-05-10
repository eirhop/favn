defmodule FavnView.Components.AssetDetailPage do
  @moduledoc """
  Static asset detail page foundation for the Favn HUD shell.
  """

  use FavnView, :html

  alias FavnView.Components.AppShell
  alias FavnView.Components.GlassPanel
  alias FavnView.Components.ModeRail

  attr :title, :string, required: true
  attr :status, :string, required: true
  attr :window_range, :string, required: true
  attr :nav_items, :list, required: true
  attr :timeline, :list, required: true

  def asset_detail_page(assigns) do
    ~H"""
    <AppShell.app_shell title={@title} status={@status} nav_items={@nav_items}>
      <.window_timeline_panel window_range={@window_range} timeline={@timeline} />

      <:mode_rail>
        <ModeRail.mode_rail>
          <:item label="Overview" icon="hero-play-circle" active>Overview</:item>
          <:item label="Docs" icon="hero-book-open">Docs</:item>
          <:item label="Definition" icon="hero-code-bracket">Definition</:item>
          <:item label="Lineage" icon="hero-share">Lineage</:item>
          <:item label="Notes" icon="hero-document-text">Notes</:item>
          <:item label="Settings" icon="hero-cog-6-tooth">Settings</:item>
        </ModeRail.mode_rail>
      </:mode_rail>
    </AppShell.app_shell>
    """
  end

  attr :window_range, :string, required: true
  attr :timeline, :list, required: true

  def window_timeline_panel(assigns) do
    ~H"""
    <GlassPanel.glass_panel
      id="window-timeline"
      class="mx-auto w-full max-w-6xl p-6 sm:p-8 lg:p-10"
      data-testid="window-timeline-panel"
    >
      <div class="flex flex-col gap-10">
        <div class="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
          <div>
            <h2 class="text-xl font-medium tracking-tight">Window timeline</h2>
            <p class="mt-2 text-sm text-base-content/60">Daily windows</p>
          </div>

          <div class="join self-start text-sm text-base-content/70">
            <button
              type="button"
              class="btn btn-ghost btn-sm join-item"
              aria-label="Previous window range"
            >
              <.icon name="hero-chevron-left" class="size-4" />
            </button>
            <span class="btn btn-ghost btn-sm join-item pointer-events-none normal-case">
              {@window_range}
            </span>
            <button
              type="button"
              class="btn btn-ghost btn-sm join-item"
              aria-label="Next window range"
            >
              <.icon name="hero-chevron-right" class="size-4" />
            </button>
            <button
              type="button"
              class="btn btn-ghost btn-square btn-sm join-item"
              aria-label="Calendar placeholder"
            >
              <.icon name="hero-calendar-days" class="size-4" />
            </button>
          </div>
        </div>

        <div class="overflow-x-auto pb-2">
          <div class="flex min-w-[58rem] items-end justify-between gap-3 pt-3">
            <.timeline_window :for={window <- @timeline} window={window} />
          </div>
        </div>
      </div>
    </GlassPanel.glass_panel>
    """
  end

  attr :window, :map, required: true

  def timeline_window(assigns) do
    ~H"""
    <div class="flex flex-col items-center gap-4">
      <div
        tabindex="0"
        class={[
          "flex h-32 w-9 items-center justify-center rounded-box border backdrop-blur-sm transition focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-4 focus-visible:outline-primary",
          timeline_window_class(@window),
          @window[:current] && "ring-2 ring-primary shadow-primary/40 shadow-lg"
        ]}
        aria-label={"#{@window.month} #{@window.day}: #{timeline_label(@window.status)}"}
      >
        <.icon name={timeline_icon(@window.status)} class="size-4" />
      </div>
      <div class={[
        "text-center text-xs leading-tight text-base-content/60",
        @window[:current] && "text-primary"
      ]}>
        <div>{@window.day}</div>
        <div :if={@window.day in ["24", "1"]} class="uppercase tracking-wide">{@window.month}</div>
      </div>
      <span :if={@window[:current]} class="status status-primary favn-status-glow"></span>
    </div>
    """
  end

  def sample_nav_items do
    [
      %{label: "Assets", icon: "hero-sparkles", href: "/", active: true},
      %{label: "Lineage", icon: "hero-share", href: "#"},
      %{label: "Storage", icon: "hero-circle-stack", href: "#"},
      %{label: "Runs", icon: "hero-rocket-launch", href: "#"},
      %{label: "Alerts", icon: "hero-bell", href: "#"},
      %{label: "Settings", icon: "hero-cog-6-tooth", href: "#"}
    ]
  end

  def sample_timeline do
    [
      %{day: "24", month: "May", status: :success},
      %{day: "25", month: "May", status: :success},
      %{day: "26", month: "May", status: :success},
      %{day: "27", month: "May", status: :success},
      %{day: "28", month: "May", status: :warning},
      %{day: "29", month: "May", status: :muted},
      %{day: "30", month: "May", status: :muted},
      %{day: "31", month: "May", status: :success},
      %{day: "1", month: "Jun", status: :success},
      %{day: "2", month: "Jun", status: :warning},
      %{day: "3", month: "Jun", status: :success},
      %{day: "4", month: "Jun", status: :success},
      %{day: "5", month: "Jun", status: :success},
      %{day: "6", month: "Jun", status: :success},
      %{day: "7", month: "Jun", status: :muted},
      %{day: "8", month: "Jun", status: :muted},
      %{day: "9", month: "Jun", status: :success},
      %{day: "10", month: "Jun", status: :success},
      %{day: "11", month: "Jun", status: :success},
      %{day: "12", month: "Jun", status: :success, current: true},
      %{day: "13", month: "Jun", status: :success},
      %{day: "14", month: "Jun", status: :success},
      %{day: "15", month: "Jun", status: :success},
      %{day: "16", month: "Jun", status: :success},
      %{day: "17", month: "Jun", status: :success},
      %{day: "18", month: "Jun", status: :success},
      %{day: "19", month: "Jun", status: :success},
      %{day: "20", month: "Jun", status: :muted},
      %{day: "21", month: "Jun", status: :muted},
      %{day: "22", month: "Jun", status: :success}
    ]
  end

  defp timeline_window_class(%{status: :success}) do
    "border-success/40 bg-success/15 text-success"
  end

  defp timeline_window_class(%{status: :warning}) do
    "border-warning/45 bg-warning/15 text-warning"
  end

  defp timeline_window_class(%{status: :muted}) do
    "border-base-content/15 bg-base-content/10 text-base-content/45"
  end

  defp timeline_icon(:success), do: "hero-check-circle"
  defp timeline_icon(:warning), do: "hero-clock"
  defp timeline_icon(:muted), do: "hero-minus-circle"

  defp timeline_label(:success), do: "healthy"
  defp timeline_label(:warning), do: "late"
  defp timeline_label(:muted), do: "pending"
end
