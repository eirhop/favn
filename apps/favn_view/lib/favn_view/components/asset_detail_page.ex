defmodule FavnView.Components.AssetDetailPage do
  @moduledoc """
  Static asset detail page foundation for the Favn HUD shell.
  """

  use FavnView, :html

  alias FavnView.Components.AppShell
  alias FavnView.Components.GlassPanel
  alias FavnView.Components.ModeRail
  alias FavnView.Components.SelectedWindowActions

  attr :title, :string, required: true
  attr :status, :string, required: true
  attr :status_tone, :atom, default: :success
  attr :window_range, :string, required: true
  attr :nav_items, :list, required: true
  attr :timeline, :list, required: true
  attr :active_mode, :atom, default: :timeline
  attr :selected_window, :map, default: nil
  attr :submitting_window_run?, :boolean, default: false
  attr :selected_window_error, :string, default: nil
  attr :submitted_run_id, :string, default: nil

  def asset_detail_page(assigns) do
    ~H"""
    <AppShell.app_shell
      title={@title}
      status={@status}
      status_tone={@status_tone}
      nav_items={@nav_items}
    >
      <.central_view
        active_mode={@active_mode}
        window_range={@window_range}
        timeline={@timeline}
        selected_window={@selected_window}
        submitting_window_run?={@submitting_window_run?}
        selected_window_error={@selected_window_error}
        submitted_run_id={@submitted_run_id}
      />

      <:mode_rail>
        <ModeRail.mode_rail active={@active_mode} modes={detail_modes()} on_select="set_mode" />
      </:mode_rail>
    </AppShell.app_shell>
    """
  end

  attr :active_mode, :atom, required: true
  attr :window_range, :string, required: true
  attr :timeline, :list, required: true
  attr :selected_window, :map, default: nil
  attr :submitting_window_run?, :boolean, default: false
  attr :selected_window_error, :string, default: nil
  attr :submitted_run_id, :string, default: nil

  def central_view(assigns) do
    ~H"""
    <.window_timeline_panel
      :if={@active_mode == :timeline}
      window_range={@window_range}
      timeline={@timeline}
      selected_window={@selected_window}
      submitting_window_run?={@submitting_window_run?}
      selected_window_error={@selected_window_error}
      submitted_run_id={@submitted_run_id}
    />

    <.placeholder_panel :if={@active_mode == :runs} title="Runs coming soon" />
    <.placeholder_panel :if={@active_mode == :lineage} title="Lineage coming soon" />
    <.placeholder_panel :if={@active_mode == :docs} title="Docs coming soon" />
    <.placeholder_panel :if={@active_mode == :code} title="Code coming soon" />
    <.placeholder_panel :if={@active_mode == :details} title="Details coming soon" />
    """
  end

  attr :window_range, :string, required: true
  attr :timeline, :list, required: true
  attr :selected_window, :map, default: nil
  attr :submitting_window_run?, :boolean, default: false
  attr :selected_window_error, :string, default: nil
  attr :submitted_run_id, :string, default: nil

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
            <.timeline_window
              :for={window <- @timeline}
              window={window}
              selected={selected_window?(@selected_window, window)}
            />
          </div>
        </div>

        <SelectedWindowActions.selected_window_actions
          selected_window={@selected_window}
          submitting_window_run?={@submitting_window_run?}
          selected_window_error={@selected_window_error}
          submitted_run_id={@submitted_run_id}
        />
      </div>
    </GlassPanel.glass_panel>
    """
  end

  attr :window, :map, required: true
  attr :selected, :boolean, default: false

  def timeline_window(assigns) do
    ~H"""
    <div class="flex flex-col items-center gap-4">
      <button
        type="button"
        phx-click="select_window"
        phx-value-window-id={@window.id}
        data-testid={"timeline-window-#{@window.id}"}
        class={[
          "flex h-32 w-9 items-center justify-center rounded-box border backdrop-blur-sm transition hover:border-primary/50 focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-4 focus-visible:outline-primary",
          timeline_window_class(@window),
          @selected && "ring-2 ring-primary shadow-primary/40 shadow-lg"
        ]}
        aria-label={"#{@window.date_label}: #{timeline_label(@window.status)}"}
        aria-pressed={to_string(@selected)}
      >
        <.icon name={timeline_icon(@window.status)} class="size-4" />
      </button>
      <div class={[
        "text-center text-xs leading-tight text-base-content/60",
        @selected && "text-primary"
      ]}>
        <div>{@window.day}</div>
        <div :if={@window.day in ["24", "1"]} class="uppercase tracking-wide">{@window.month}</div>
      </div>
      <span :if={@selected} class="status status-primary favn-status-glow"></span>
    </div>
    """
  end

  attr :title, :string, required: true

  def placeholder_panel(assigns) do
    ~H"""
    <GlassPanel.glass_panel
      class="mx-auto w-full max-w-3xl p-8 text-center"
      data-testid="asset-mode-placeholder"
    >
      <h2 class="text-xl font-medium tracking-tight">{@title}</h2>
      <p class="mt-2 text-sm text-base-content/60">This view will be wired in a later iteration.</p>
    </GlassPanel.glass_panel>
    """
  end

  def sample_nav_items do
    [
      %{label: "Assets", icon: "hero-sparkles", href: "/assets", active: true},
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
    |> Enum.map(&sample_window/1)
  end

  def muted_timeline do
    sample_timeline()
    |> Enum.map(&(&1 |> Map.put(:status, :muted) |> Map.delete(:current)))
    |> List.update_at(20, &Map.put(&1, :current, true))
  end

  def selected_sample_window do
    Enum.find(sample_timeline(), & &1[:current])
  end

  def selected_muted_window do
    Enum.find(muted_timeline(), & &1[:current])
  end

  def non_runnable_timeline do
    Enum.map(sample_timeline(), fn window ->
      window
      |> Map.put(:run_enabled?, false)
      |> Map.put(:run_disabled_reason, :asset_has_no_window_policy)
    end)
  end

  def selected_non_runnable_window do
    Enum.find(non_runnable_timeline(), & &1[:current])
  end

  def detail_modes do
    [
      %{id: :timeline, label: "Timeline", icon: "hero-calendar-days"},
      %{id: :runs, label: "Runs", icon: "hero-rocket-launch"},
      %{id: :lineage, label: "Lineage", icon: "hero-share"},
      %{id: :docs, label: "Docs", icon: "hero-book-open"},
      %{id: :code, label: "Code", icon: "hero-code-bracket"},
      %{id: :details, label: "Details", icon: "hero-document-text"}
    ]
  end

  defp selected_window?(nil, _window), do: false
  defp selected_window?(selected_window, window), do: selected_window.id == window.id

  defp sample_window(%{month: month, day: day} = window) do
    year = if month == "May", do: 2026, else: 2026
    date_label = "#{month} #{day}, #{year}"

    window
    |> Map.put(:id, "#{String.downcase(month)}-#{day}-#{year}")
    |> Map.put(:date_label, date_label)
    |> Map.put(:range_label, date_label)
    |> Map.put(:run_enabled?, true)
    |> Map.put(:run_disabled_reason, nil)
    |> Map.put(:run_label, "Run this window")
  end

  defp timeline_window_class(%{status: :success}) do
    "border-success/40 bg-success/15 text-success"
  end

  defp timeline_window_class(%{status: :warning}) do
    "border-warning/45 bg-warning/15 text-warning"
  end

  defp timeline_window_class(%{status: :error}) do
    "border-error/45 bg-error/15 text-error"
  end

  defp timeline_window_class(%{status: :muted}) do
    "border-base-content/15 bg-base-content/10 text-base-content/45"
  end

  defp timeline_icon(:success), do: "hero-check-circle"
  defp timeline_icon(:warning), do: "hero-clock"
  defp timeline_icon(:error), do: "hero-x-circle"
  defp timeline_icon(:muted), do: "hero-minus-circle"

  defp timeline_label(:success), do: "healthy"
  defp timeline_label(:warning), do: "late"
  defp timeline_label(:error), do: "failed"
  defp timeline_label(:muted), do: "pending"
end
