defmodule FavnView.Components.RunDetailPage.AttemptDrawer do
  @moduledoc false
  use FavnView, :html
  import FavnView.Components.RunDetailPage.Ui

  attr :attempt, :map, required: true

  def attempt_drawer(assigns) do
    ~H"""
    <div class="fixed inset-0 z-[80] bg-base-300/20 backdrop-blur-[1px]" phx-click="close_attempt" />
    <aside
      class="fixed inset-y-0 right-0 z-[90] flex w-full max-w-[30rem] flex-col border-l border-base-content/10 bg-base-100/95 shadow-2xl shadow-primary/20 backdrop-blur-xl lg:max-w-[34rem]"
      data-testid="asset-attempt-drawer"
    >
      <header class="flex items-center justify-between gap-3 border-b border-base-content/10 px-5 py-4 lg:pr-24">
        <div class="flex min-w-0 items-center gap-3">
          <span class="flex size-7 shrink-0 items-center justify-center rounded-full bg-primary/15 text-primary">
            <.icon name="hero-chart-pie" class="size-4" />
          </span>
          <p class="truncate text-sm font-medium text-base-content/75">Asset attempt</p>
        </div>
        <button
          type="button"
          phx-click="close_attempt"
          class="btn btn-ghost btn-sm btn-circle"
          aria-label="Close attempt drawer"
        >
          <.icon name="hero-x-mark" class="size-5" />
        </button>
      </header>

      <div class="min-h-0 flex-1 overflow-y-auto px-5 py-5 lg:pr-24">
        <div class="min-w-0">
          <div class="flex flex-wrap items-center gap-2">
            <h2 class="truncate text-2xl font-semibold tracking-tight">
              {@attempt.short_asset_name}
            </h2>
            <span class={status_badge_class(@attempt.status_tone)}>{@attempt.status}</span>
          </div>
          <p class="mt-1 text-sm text-base-content/65">
            {@attempt.stage_label || "Stage unknown"} · Attempt {@attempt.attempt_number || "-"}
          </p>
        </div>

        <section class="mt-5 rounded-box border border-base-content/10 bg-base-content/[0.025] p-4">
          <dl class="grid gap-4 text-sm">
            <.drawer_fact label="Window" value={@attempt.window_label} />
            <.drawer_fact label="Status" value={@attempt.status} />
            <.drawer_fact label="Started" value={@attempt.started_at} />
            <.drawer_fact label="Finished" value={@attempt.finished_at} />
            <.drawer_fact label="Duration" value={@attempt.duration} />
          </dl>
        </section>

        <section class="mt-3 rounded-box border border-base-content/10 bg-base-content/[0.025] p-4">
          <h3 class="text-sm font-medium">Context</h3>
          <dl class="mt-4 grid gap-4 text-sm">
            <.drawer_fact label="Backfill run" value={@attempt.root_execution_group_id} mono />
            <.drawer_fact label="Window run" value={@attempt.child_run_id || @attempt.run_id} mono />
            <.drawer_fact label="Asset key" value={@attempt.asset_key} mono />
          </dl>
        </section>

        <div
          :if={@attempt.error_summary}
          class="mt-3 rounded-box border border-error/30 bg-error/10 p-3 text-sm text-error"
        >
          {@attempt.error_summary}
        </div>
      </div>

      <footer class="border-t border-base-content/10 bg-base-200/40 p-5 lg:pr-24">
        <.link
          :if={@attempt.logs_href}
          navigate={@attempt.logs_href}
          class="btn btn-info btn-outline w-full rounded-field"
          data-testid="attempt-logs-link"
        >
          Open logs/events <.icon name="hero-arrow-top-right-on-square" class="size-4" />
        </.link>
      </footer>
    </aside>
    """
  end

  attr :label, :string, required: true
  attr :value, :any, required: true
  attr :mono, :boolean, default: false

  def drawer_fact(assigns) do
    ~H"""
    <div>
      <dt class="text-xs text-base-content/45">{@label}</dt>
      <dd class={["mt-1 break-words font-medium", @mono && "font-mono text-xs"]}>{@value || "-"}</dd>
    </div>
    """
  end
end
