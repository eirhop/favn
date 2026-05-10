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
  attr :freshness, :map, default: nil
  attr :selected_window, :map, default: nil
  attr :run_config_open?, :boolean, default: false
  attr :run_config, :map, default: %{dependencies: "all", refresh: "auto"}
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
        freshness={@freshness}
        selected_window={@selected_window}
        run_config_open?={@run_config_open?}
        run_config={@run_config}
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
  attr :freshness, :map, default: nil
  attr :selected_window, :map, default: nil
  attr :run_config_open?, :boolean, default: false
  attr :run_config, :map, default: %{dependencies: "all", refresh: "auto"}
  attr :submitting_window_run?, :boolean, default: false
  attr :selected_window_error, :string, default: nil
  attr :submitted_run_id, :string, default: nil

  def central_view(assigns) do
    ~H"""
    <.window_timeline_panel
      :if={@active_mode == :timeline}
      window_range={@window_range}
      timeline={@timeline}
      freshness={@freshness}
      selected_window={@selected_window}
      run_config_open?={@run_config_open?}
      run_config={@run_config}
      submitting_window_run?={@submitting_window_run?}
      selected_window_error={@selected_window_error}
      submitted_run_id={@submitted_run_id}
    />

    <.placeholder_panel :if={@active_mode == :runs} title="Runs coming soon" />
    <.placeholder_panel :if={@active_mode == :lineage} title="Lineage coming soon" />
    <.placeholder_panel :if={@active_mode == :docs} title="Docs coming soon" />
    <.placeholder_panel :if={@active_mode == :code} title="Code coming soon" />
    <.freshness_detail_panel :if={@active_mode == :details} freshness={@freshness} />
    """
  end

  attr :window_range, :string, required: true
  attr :timeline, :list, required: true
  attr :freshness, :map, default: nil
  attr :selected_window, :map, default: nil
  attr :run_config_open?, :boolean, default: false
  attr :run_config, :map, default: %{dependencies: "all", refresh: "auto"}
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

        <.freshness_summary freshness={@freshness} />

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
          run_config_open?={@run_config_open?}
          run_config={@run_config}
          submitting_window_run?={@submitting_window_run?}
          selected_window_error={@selected_window_error}
          submitted_run_id={@submitted_run_id}
        />
      </div>
    </GlassPanel.glass_panel>
    """
  end

  attr :freshness, :map, default: nil

  def freshness_summary(assigns) do
    ~H"""
    <div
      :if={@freshness}
      class={[
        "rounded-box border p-4",
        freshness_panel_class(@freshness[:state])
      ]}
      data-testid="asset-freshness-summary"
    >
      <div class="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
        <div class="min-w-0">
          <p class="text-xs uppercase tracking-[0.18em] text-base-content/45">Freshness</p>
          <div class="mt-1 flex flex-wrap items-center gap-2">
            <span class={freshness_badge_class(@freshness[:state])}>
              {freshness_state_label(@freshness[:state])}
            </span>
            <span class="text-xs text-base-content/55">{freshness_policy_label(@freshness)}</span>
          </div>
          <p class="mt-2 text-sm text-base-content/70">{@freshness[:explanation]}</p>
        </div>
      </div>
    </div>
    """
  end

  attr :freshness, :map, default: nil

  def freshness_detail_panel(assigns) do
    ~H"""
    <GlassPanel.glass_panel
      class="mx-auto w-full max-w-4xl p-6 sm:p-8"
      data-testid="asset-freshness-detail-panel"
    >
      <div :if={!@freshness} class="text-sm text-base-content/60">
        Freshness detail is not available from the backend.
      </div>

      <div :if={@freshness} class="space-y-6">
        <div>
          <p class="text-xs uppercase tracking-[0.18em] text-base-content/45">Freshness detail</p>
          <div class="mt-2 flex flex-wrap items-center gap-2">
            <span class={freshness_badge_class(@freshness[:state])}>
              {freshness_state_label(@freshness[:state])}
            </span>
            <span class="badge badge-ghost badge-sm">{freshness_policy_label(@freshness)}</span>
          </div>
          <p class="mt-3 text-sm text-base-content/70">{@freshness[:explanation]}</p>
        </div>

        <dl :if={freshness_latest_success(@freshness)} class="grid gap-3 sm:grid-cols-3">
          <div class="rounded-box border border-base-content/10 bg-base-content/[0.03] p-3">
            <dt class="text-xs uppercase tracking-[0.16em] text-base-content/45">Latest success</dt>
            <dd class="mt-1 break-words font-mono text-xs text-base-content/75">
              {freshness_latest_success(@freshness)[:run_id]}
            </dd>
          </div>
          <div class="rounded-box border border-base-content/10 bg-base-content/[0.03] p-3">
            <dt class="text-xs uppercase tracking-[0.16em] text-base-content/45">Freshness key</dt>
            <dd class="mt-1 break-words font-mono text-xs text-base-content/75">
              {freshness_latest_success(@freshness)[:freshness_key]}
            </dd>
          </div>
          <div class="rounded-box border border-base-content/10 bg-base-content/[0.03] p-3">
            <dt class="text-xs uppercase tracking-[0.16em] text-base-content/45">At</dt>
            <dd class="mt-1 text-xs text-base-content/75">
              {freshness_time(freshness_latest_success(@freshness)[:at])}
            </dd>
          </div>
        </dl>

        <div>
          <h3 class="text-sm font-medium text-base-content">Backend reasons</h3>
          <div class="mt-3 space-y-3" data-testid="asset-freshness-reasons">
            <div
              :for={reason <- freshness_reasons(@freshness)}
              class="rounded-box border border-base-content/10 bg-base-content/[0.03] p-4"
            >
              <div class="flex flex-wrap items-center gap-2">
                <span class="badge badge-ghost badge-sm">{reason[:kind]}</span>
                <span :if={reason[:upstream_ref]} class="font-mono text-xs text-base-content/45">
                  {reason[:upstream_ref]}
                </span>
              </div>
              <p class="mt-2 text-sm text-base-content/75">{reason[:message]}</p>
              <dl class="mt-3 grid gap-2 text-xs text-base-content/60 sm:grid-cols-3">
                <div :if={reason[:previous_version]}>
                  <dt class="uppercase tracking-[0.14em] text-base-content/40">Previous</dt>
                  <dd class="mt-0.5 break-words font-mono">{reason[:previous_version]}</dd>
                </div>
                <div :if={reason[:current_version]}>
                  <dt class="uppercase tracking-[0.14em] text-base-content/40">Current</dt>
                  <dd class="mt-0.5 break-words font-mono">{reason[:current_version]}</dd>
                </div>
                <div :if={reason[:run_id]}>
                  <dt class="uppercase tracking-[0.14em] text-base-content/40">Run</dt>
                  <dd class="mt-0.5 break-words font-mono">{reason[:run_id]}</dd>
                </div>
              </dl>
            </div>
          </div>
        </div>
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

  def sample_freshness(:fresh) do
    %{
      state: :fresh,
      policy: %{kind: :daily, label: "daily Europe/Oslo"},
      latest_success: %{
        run_id: "run_fresh_customer_orders",
        at: ~U[2026-06-12 08:00:00Z],
        freshness_key: "latest"
      },
      explanation: "Backend freshness state currently satisfies this asset's policy.",
      reasons: [
        %{kind: :policy_fresh, message: "Backend freshness state satisfies the declared policy."}
      ]
    }
  end

  def sample_freshness(:stale) do
    %{
      state: :stale,
      policy: %{kind: :daily, label: "daily Europe/Oslo"},
      latest_success: %{
        run_id: "run_old_gold_orders",
        at: ~U[2026-06-11 08:00:00Z],
        freshness_key: "latest"
      },
      explanation:
        "GoldOrders.asset is stale because rawOrders.asset refreshed after this asset last consumed it.",
      reasons: [
        %{
          kind: :upstream_version_changed,
          message: "RawOrders.asset refreshed after this asset last consumed it.",
          upstream_ref: "Elixir.FavnView.Assets.RawOrders:asset",
          previous_version: "raw:v1",
          current_version: "raw:v2",
          run_id: "run_raw_v2"
        }
      ]
    }
  end

  def sample_freshness(:unknown) do
    %{
      state: :unknown,
      policy: %{kind: :window_success, label: "window success"},
      latest_success: nil,
      explanation: "No successful freshness evidence exists for this asset yet.",
      reasons: [
        %{kind: :never_run, message: "No successful freshness-producing run has been recorded."}
      ]
    }
  end

  def sample_freshness(:always_run) do
    %{
      state: :always_run,
      policy: %{kind: :always, label: "always run"},
      latest_success: nil,
      explanation: "Freshness is intentionally bypassed; this asset runs whenever it is planned.",
      reasons: [%{kind: :always_run, message: "Manifest policy is always run."}]
    }
  end

  def sample_freshness(:failed_unknown) do
    %{
      state: :unknown,
      policy: %{kind: :daily, label: "daily Europe/Oslo"},
      latest_success: nil,
      explanation: "Freshness state exists, but backend could not explain whether it is stale.",
      reasons: [
        %{
          kind: :insufficient_state,
          message: "Backend could not build a staleness explanation from available state."
        }
      ]
    }
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

  defp timeline_label(:success), do: "fresh"
  defp timeline_label(:warning), do: "running"
  defp timeline_label(:error), do: "failed"
  defp timeline_label(:muted), do: "unknown"

  defp freshness_state_label(:fresh), do: "Fresh"
  defp freshness_state_label(:stale), do: "Stale"
  defp freshness_state_label(:always_run), do: "Always run"
  defp freshness_state_label(_state), do: "Unknown"

  defp freshness_badge_class(:fresh), do: "badge badge-success badge-soft badge-sm"
  defp freshness_badge_class(:stale), do: "badge badge-warning badge-soft badge-sm"
  defp freshness_badge_class(:always_run), do: "badge badge-info badge-soft badge-sm"
  defp freshness_badge_class(_state), do: "badge badge-neutral badge-soft badge-sm"

  defp freshness_panel_class(:fresh), do: "border-success/20 bg-success/10"
  defp freshness_panel_class(:stale), do: "border-warning/25 bg-warning/10"
  defp freshness_panel_class(:always_run), do: "border-info/20 bg-info/10"
  defp freshness_panel_class(_state), do: "border-base-content/10 bg-base-content/[0.035]"

  defp freshness_policy_label(%{policy: %{label: label}}) when is_binary(label), do: label
  defp freshness_policy_label(%{"policy" => %{"label" => label}}) when is_binary(label), do: label
  defp freshness_policy_label(_freshness), do: "policy unavailable"

  defp freshness_latest_success(%{latest_success: latest_success}) when is_map(latest_success),
    do: latest_success

  defp freshness_latest_success(%{"latest_success" => latest_success})
       when is_map(latest_success),
       do: latest_success

  defp freshness_latest_success(_freshness), do: nil

  defp freshness_reasons(%{reasons: reasons}) when is_list(reasons), do: reasons
  defp freshness_reasons(%{"reasons" => reasons}) when is_list(reasons), do: reasons
  defp freshness_reasons(_freshness), do: []

  defp freshness_time(%DateTime{} = value),
    do: Calendar.strftime(value, "%b %-d, %Y %H:%M:%S UTC")

  defp freshness_time(_value), do: "-"
end
