defmodule FavnView.Components.PipelineDetailPage do
  @moduledoc """
  Pipeline detail page components for run history and manual operations.
  """

  use FavnView, :html

  alias FavnView.Components.AppShell
  alias FavnView.Components.GlassPanel
  alias FavnView.Components.ModeRail
  alias FavnView.Components.PipelinesPage

  attr :pipeline, :map, required: true
  attr :nav_items, :list, required: true
  attr :active_mode, :atom, default: :runs
  attr :run_error, :string, default: nil
  attr :backfill_error, :string, default: nil
  attr :backfill_config, :map, required: true

  def pipeline_detail_page(assigns) do
    ~H"""
    <AppShell.app_shell
      title={@pipeline.name}
      subtitle={@pipeline.label}
      status={@pipeline.status_label}
      status_tone={status_tone(@pipeline.status)}
      nav_items={@nav_items}
    >
      <div class="mx-auto w-full max-w-6xl space-y-4 pb-24 lg:pb-0" data-testid="pipeline-detail-page">
        <.summary_panel pipeline={@pipeline} />
        <.actions_panel
          pipeline={@pipeline}
          run_error={@run_error}
          backfill_error={@backfill_error}
          backfill_config={@backfill_config}
        />
        <.history_panel pipeline={@pipeline} />
      </div>

      <:mode_rail>
        <ModeRail.mode_rail active={@active_mode} modes={detail_modes()} on_select="set_mode" />
      </:mode_rail>
    </AppShell.app_shell>
    """
  end

  attr :pipeline, :map, required: true

  def summary_panel(assigns) do
    ~H"""
    <GlassPanel.glass_panel class="p-6 sm:p-8" data-testid="pipeline-summary-panel">
      <div class="flex flex-col gap-5 lg:flex-row lg:items-start lg:justify-between">
        <div class="min-w-0">
          <p class="text-xs uppercase tracking-[0.2em] text-base-content/45">Pipeline</p>
          <h2 class="mt-2 text-2xl font-medium tracking-tight">{@pipeline.name}</h2>
          <p class="mt-2 break-words font-mono text-xs text-base-content/55">{@pipeline.label}</p>
        </div>
        <div class="flex flex-wrap gap-2">
          <PipelinesPage.status_badge status={@pipeline.status} />
          <span class="badge badge-ghost badge-sm">{@pipeline.dependencies_label}</span>
          <span class="badge badge-ghost badge-sm">{@pipeline.window_label}</span>
        </div>
      </div>

      <div class="mt-6 grid gap-3 sm:grid-cols-3">
        <.summary_stat label="Selected assets" value={to_string(@pipeline.asset_count)} />
        <.summary_stat label="Last run" value={@pipeline.last_run_label} />
        <.summary_stat label="Runtime" value={@pipeline.runtime_label} />
      </div>

      <div class="mt-6">
        <p class="text-xs uppercase tracking-[0.18em] text-base-content/45">Selected assets</p>
        <div class="mt-3 flex flex-wrap gap-2">
          <span :for={asset <- @pipeline.selected_assets} class="badge badge-soft badge-info">
            {asset}
          </span>
          <span :if={@pipeline.selected_assets == []} class="text-sm text-base-content/55">
            No resolved assets
          </span>
        </div>
      </div>
    </GlassPanel.glass_panel>
    """
  end

  attr :label, :string, required: true
  attr :value, :string, required: true

  def summary_stat(assigns) do
    ~H"""
    <div class="rounded-box border border-base-content/10 bg-base-content/[0.03] p-4">
      <p class="text-xs uppercase tracking-[0.16em] text-base-content/45">{@label}</p>
      <p class="mt-1 text-sm font-medium text-base-content">{@value}</p>
    </div>
    """
  end

  attr :pipeline, :map, required: true
  attr :run_error, :string, default: nil
  attr :backfill_error, :string, default: nil
  attr :backfill_config, :map, required: true

  def actions_panel(assigns) do
    ~H"""
    <GlassPanel.glass_panel class="p-6 sm:p-8" data-testid="pipeline-actions-panel">
      <div class="grid gap-6 lg:grid-cols-[1fr_1.4fr]">
        <div>
          <p class="text-xs uppercase tracking-[0.2em] text-base-content/45">Run pipeline</p>
          <p class="mt-2 text-sm text-base-content/65">
            Submit the active manifest pipeline, equivalent to <code class="font-mono">mix favn.run</code>.
          </p>
          <form phx-submit="run_pipeline" class="mt-4" data-testid="run-pipeline-form">
            <button
              type="submit"
              class="btn btn-primary"
              disabled={!@pipeline.can_run_without_window?}
              data-testid="run-pipeline-button"
            >
              <.icon name="hero-play" class="size-4" /> Run pipeline
            </button>
          </form>
          <p
            :if={!@pipeline.can_run_without_window?}
            class="mt-3 text-sm text-base-content/60"
            data-testid="pipeline-run-disabled-help"
          >
            This pipeline requires an explicit window. Use backfill or choose a specific window.
          </p>
          <p :if={@run_error} class="mt-3 text-sm text-error" data-testid="pipeline-run-error">
            {@run_error}
          </p>
        </div>

        <div>
          <p class="text-xs uppercase tracking-[0.2em] text-base-content/45">Backfill</p>
          <p class="mt-2 text-sm text-base-content/65">
            Submit an explicit range, equivalent to <code class="font-mono">mix favn.backfill submit</code>.
          </p>
          <form
            phx-submit="submit_backfill"
            class="mt-4 grid gap-3 sm:grid-cols-[1fr_1fr_8rem_auto]"
            data-testid="pipeline-backfill-form"
          >
            <input type="hidden" name="backfill[timezone]" value={@backfill_config.timezone} />
            <label class="form-control">
              <span class="label-text text-xs">From</span>
              <input
                name="backfill[from]"
                value={@backfill_config.from}
                class="input input-sm favn-surface-control"
                placeholder={backfill_placeholder(@backfill_config.kind)}
                disabled={!@pipeline.can_backfill?}
              />
            </label>
            <label class="form-control">
              <span class="label-text text-xs">To</span>
              <input
                name="backfill[to]"
                value={@backfill_config.to}
                class="input input-sm favn-surface-control"
                placeholder={backfill_placeholder(@backfill_config.kind)}
                disabled={!@pipeline.can_backfill?}
              />
            </label>
            <label class="form-control">
              <span class="label-text text-xs">Kind</span>
              <select
                name="backfill[kind]"
                class="select select-sm favn-surface-control"
                disabled={!@pipeline.can_backfill?}
              >
                <option
                  :for={kind <- ~w(month day hour year)}
                  value={kind}
                  selected={@backfill_config.kind == kind}
                >
                  {kind}
                </option>
              </select>
            </label>
            <button
              type="submit"
              class="btn btn-primary btn-soft self-end"
              disabled={!@pipeline.can_backfill?}
              data-testid="submit-backfill-button"
            >
              Backfill
            </button>
          </form>
          <p
            :if={@pipeline.can_backfill?}
            class="mt-2 text-xs text-base-content/55"
            data-testid="pipeline-backfill-defaults"
          >
            Defaults to {@backfill_config.kind} windows in {@backfill_config.timezone}.
          </p>
          <p
            :if={!@pipeline.can_backfill?}
            class="mt-2 text-xs text-base-content/55"
            data-testid="pipeline-backfill-disabled-help"
          >
            Backfill requires a windowed pipeline.
          </p>
          <p
            :if={@backfill_error}
            class="mt-3 text-sm text-error"
            data-testid="pipeline-backfill-error"
          >
            {@backfill_error}
          </p>
        </div>
      </div>
    </GlassPanel.glass_panel>
    """
  end

  attr :pipeline, :map, required: true

  def history_panel(assigns) do
    ~H"""
    <GlassPanel.glass_panel class="overflow-hidden" data-testid="pipeline-history-panel">
      <div class="border-b border-base-content/10 p-5 sm:p-6">
        <h2 class="text-lg font-medium">Run history</h2>
        <p class="mt-1 text-sm text-base-content/60">
          Pipeline and backfill runs matched to this pipeline.
        </p>
      </div>

      <div :if={@pipeline.runs == []} class="p-8 text-center text-sm text-base-content/60">
        No runs have been recorded for this pipeline yet.
      </div>

      <div :if={@pipeline.runs != []} class="overflow-x-auto">
        <table class="table" data-testid="pipeline-runs-table">
          <thead>
            <tr class="border-base-content/10 text-base-content/65">
              <th>Run</th>
              <th>Status</th>
              <th>Kind</th>
              <th>Window</th>
              <th>Started</th>
              <th>Duration</th>
            </tr>
          </thead>
          <tbody>
            <tr
              :for={run <- @pipeline.runs}
              class="border-base-content/10"
              data-testid="pipeline-run-row"
            >
              <td>
                <.link navigate={~p"/runs/#{run.id}"} class="link link-hover font-mono text-xs">
                  {run.short_id}
                </.link>
              </td>
              <td><PipelinesPage.status_badge status={run.status} /></td>
              <td>{run.kind_label}</td>
              <td>{run.window_label}</td>
              <td>{run.started_at_label}</td>
              <td>{run.duration_label}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </GlassPanel.glass_panel>
    """
  end

  def sample_pipeline do
    %{
      id: "pipeline:Elixir.Example.Pipelines.SourceRawFullRefresh",
      manifest_version_id: "mv_sample",
      name: "source_raw_full_refresh",
      label: "Example.Pipelines.SourceRawFullRefresh",
      selected_assets: ["source_raw", "source_monthly"],
      asset_count: 2,
      dependencies: :all,
      dependencies_label: "Include deps",
      window_label: "Month Etc/UTC",
      can_run_without_window?: false,
      can_backfill?: true,
      status: :healthy,
      status_label: "Healthy",
      last_run_label: "12m ago",
      runtime_label: "34.5 s",
      runs: [
        %{
          id: "run_source_full_refresh",
          short_id: "run_source...fresh",
          status: :healthy,
          kind_label: "Pipeline",
          window_label: "-",
          started_at_label: "May 13 10:00",
          duration_label: "34.5 s"
        }
      ]
    }
  end

  def detail_modes do
    [
      %{id: :runs, label: "Runs", icon: "hero-clock"},
      %{id: :assets, label: "Assets", icon: "hero-cube", disabled: true},
      %{id: :more, label: "More", icon: "hero-ellipsis-vertical", disabled: true}
    ]
  end

  defp status_tone(:healthy), do: :success
  defp status_tone(:running), do: :info
  defp status_tone(:failed), do: :error
  defp status_tone(_status), do: :neutral

  defp backfill_placeholder("hour"), do: "2026-01-31T13"
  defp backfill_placeholder("day"), do: "2026-01-31"
  defp backfill_placeholder("month"), do: "2026-01"
  defp backfill_placeholder("year"), do: "2026"
  defp backfill_placeholder(_kind), do: "2026-01"
end
