defmodule FavnView.Components.RunDetailPage.Stats do
  @moduledoc false
  use FavnView, :html
  import FavnView.Components.RunDetailPage.Ui

  attr :run, :map, required: true

  def execution_group_stats(assigns) do
    ~H"""
    <section data-testid="execution-group-header" class="space-y-3">
      <span class="sr-only" data-testid="execution-group-id">{@run.id}</span>
      <div class="grid gap-2 sm:grid-cols-2 lg:grid-cols-6" data-testid="execution-group-stat-cards">
        <.stat_card
          icon="hero-table-cells"
          label="Windows"
          value={@run.completed_windows}
          suffix={"/ #{@run.total_windows}"}
          detail="completed"
          tone={:info}
        />
        <.stat_card
          icon="hero-square-3-stack-3d"
          label="Asset attempts"
          value={@run.completed_asset_attempts}
          suffix={"/ #{@run.total_asset_attempts}"}
          detail="completed"
          tone={:primary}
        />
        <.stat_card
          icon="hero-check-circle"
          label="Succeeded"
          value={@run.succeeded_asset_attempts}
          tone={:success}
        />
        <.stat_card
          icon="hero-x-circle"
          label="Failed"
          value={@run.failed_asset_attempts}
          tone={:error}
        />
        <.stat_card
          icon="hero-arrow-path"
          label="Running"
          value={@run.running_asset_attempts}
          tone={:info}
        />
        <.stat_card
          icon="hero-clock"
          label="Queued"
          value={@run.queued_asset_attempts}
          tone={:warning}
        />
      </div>
    </section>
    """
  end

  attr :icon, :string, required: true
  attr :label, :string, required: true
  attr :value, :any, required: true
  attr :suffix, :string, default: nil
  attr :detail, :string, default: nil
  attr :tone, :atom, default: :neutral

  def stat_card(assigns) do
    ~H"""
    <div class="favn-surface-list rounded-box p-4">
      <div class="flex items-center gap-3">
        <span class={[
          "flex size-10 items-center justify-center rounded-full",
          icon_shell_class(@tone)
        ]}>
          <.icon name={@icon} class="size-5" />
        </span>
        <div>
          <p class="text-xs text-base-content/55">{@label}</p>
          <p class="text-2xl font-light tracking-tight">
            {@value}<span :if={@suffix} class="text-base-content/45"> {@suffix}</span>
          </p>
          <p :if={@detail} class="text-xs text-base-content/45">{@detail}</p>
        </div>
      </div>
    </div>
    """
  end
end
