defmodule FavnView.Components.PipelineDetailPage do
  @moduledoc """
  Pipeline detail page components for run history and manual operations.
  """

  use FavnView, :html

  alias FavnView.Components.AppShell

  attr :pipeline, :map, required: true
  attr :nav_items, :list, required: true
  attr :current_scope, :any, default: nil
  attr :operator_workspaces, :list, default: []
  attr :run_error, :string, default: nil
  attr :backfill_error, :string, default: nil
  attr :backfill_config, :map, required: true
  attr :can_submit_runs?, :boolean, default: false
  attr :flash, :map, default: %{}

  def pipeline_detail_page(assigns) do
    ~H"""
    <AppShell.app_shell
      title={@pipeline.name}
      subtitle={@pipeline.label}
      status={@pipeline.status_label}
      status_tone={status_tone(@pipeline.status)}
      nav_items={@nav_items}
      current_scope={@current_scope}
      operator_workspaces={@operator_workspaces}
      flash={@flash}
    >
      <div
        class="mx-auto w-full max-w-[120rem] space-y-4 pb-24 lg:pb-0"
        data-testid="pipeline-detail-page"
      >
        <.summary_panel pipeline={@pipeline} />
        <.actions_panel
          pipeline={@pipeline}
          run_error={@run_error}
          backfill_error={@backfill_error}
          backfill_config={@backfill_config}
          can_submit_runs?={@can_submit_runs?}
        /> <.history_panel pipeline={@pipeline} />
      </div>
    </AppShell.app_shell>
    """
  end

  attr :pipeline, :map, required: true

  def summary_panel(assigns) do
    ~H"""
    <.panel padding={:lg} data-testid="pipeline-summary-panel">
      <div class="flex flex-col gap-5 lg:flex-row lg:items-start lg:justify-between">
        <div class="min-w-0">
          <.eyebrow>Pipeline</.eyebrow>

          <h2 class="mt-2 text-2xl font-medium tracking-tight">{@pipeline.name}</h2>

          <p class="mt-2 break-words font-mono text-sm favn-text-muted">{@pipeline.label}</p>
        </div>

        <div class="flex flex-wrap gap-2">
          <.status_badge tone={status_tone(@pipeline.status)} label={@pipeline.status_label} />
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
        <.eyebrow>Selected assets</.eyebrow>

        <div class="mt-3 flex flex-wrap gap-2">
          <span :for={asset <- @pipeline.selected_assets} class="badge badge-soft badge-info">
            {asset}
          </span>

          <span :if={@pipeline.selected_assets == []} class="text-sm favn-text-muted">
            No resolved assets
          </span>
        </div>
      </div>
    </.panel>
    """
  end

  attr :label, :string, required: true
  attr :value, :string, required: true

  def summary_stat(assigns) do
    ~H"""
    <div class="rounded-box border border-base-content/10 bg-base-content/[0.03] p-4">
      <.eyebrow>{@label}</.eyebrow>

      <p class="mt-1 text-sm font-medium text-base-content">{@value}</p>
    </div>
    """
  end

  attr :pipeline, :map, required: true
  attr :run_error, :string, default: nil
  attr :backfill_error, :string, default: nil
  attr :backfill_config, :map, required: true
  attr :can_submit_runs?, :boolean, default: false

  def actions_panel(assigns) do
    ~H"""
    <.panel padding={:lg} data-testid="pipeline-actions-panel">
      <div class="grid gap-6 lg:grid-cols-[1fr_1.4fr]">
        <div>
          <.eyebrow>Run pipeline</.eyebrow>

          <p class="mt-2 text-sm favn-text-muted">
            Submit the active manifest pipeline, equivalent to <code class="font-mono">mix favn.run</code>.
          </p>

          <form
            phx-submit="run_pipeline"
            class="mt-4"
            data-command-operation="pipeline_run_submit"
            data-command-resource={@pipeline.id}
            data-testid="run-pipeline-form"
          >
            <button
              type="submit"
              class="btn btn-primary"
              disabled={!@can_submit_runs?}
              data-testid="run-pipeline-button"
            >
              <.icon name="hero-play" class="size-4" /> Run pipeline
            </button>
          </form>

          <p
            :if={!@can_submit_runs?}
            class="mt-3 text-sm favn-text-muted"
            data-testid="pipeline-operator-required-help"
          >
            Operator role required to submit runs.
          </p>

          <p
            :if={!is_nil(Map.get(@pipeline, :window))}
            class="mt-3 text-sm favn-text-muted"
            data-testid="pipeline-latest-window-help"
          >
            No window selected: this submits the latest complete window. Use backfill for a range.
          </p>

          <p :if={@run_error} class="mt-3 text-sm text-error" data-testid="pipeline-run-error">
            {@run_error}
          </p>
        </div>

        <div>
          <.eyebrow>Backfill</.eyebrow>

          <p class="mt-2 text-sm favn-text-muted">
            Submit an explicit range, equivalent to <code class="font-mono">mix favn.backfill submit</code>.
          </p>

          <form
            phx-submit="submit_backfill"
            data-command-operation="pipeline_backfill_submit"
            data-command-resource={@pipeline.id}
            class="mt-4 grid gap-3 sm:grid-cols-2"
            data-testid="pipeline-backfill-form"
          >
            <input type="hidden" name="backfill[timezone]" value={@backfill_config.timezone} />

            <.input
              id="pipeline-backfill-from"
              label="From"
              name="backfill[from]"
              value={@backfill_config.from}
              class="input input-sm favn-surface-control"
              placeholder={backfill_placeholder(@backfill_config.kind)}
              disabled={!@can_submit_runs? || !@pipeline.can_backfill?}
            />

            <.input
              id="pipeline-backfill-to"
              label="To"
              name="backfill[to]"
              value={@backfill_config.to}
              class="input input-sm favn-surface-control"
              placeholder={backfill_placeholder(@backfill_config.kind)}
              disabled={!@can_submit_runs? || !@pipeline.can_backfill?}
            />

            <.input
              id="pipeline-backfill-kind"
              type="select"
              label="Window"
              name="backfill[kind]"
              value={@backfill_config.kind}
              options={Enum.map(~w(month day hour year), &{String.capitalize(&1), &1})}
              class="select select-sm favn-surface-control"
              disabled={!@can_submit_runs? || !@pipeline.can_backfill?}
            />

            <.input
              id="pipeline-backfill-refresh"
              type="select"
              label="Refresh"
              name="backfill[refresh]"
              value={@backfill_config.refresh}
              options={[{"Missing only", "missing"}, {"Force", "force"}, {"Auto", "auto"}]}
              class="select select-sm favn-surface-control"
              disabled={!@can_submit_runs? || !@pipeline.can_backfill?}
              data-testid="pipeline-backfill-refresh"
            />

            <.input
              id="pipeline-backfill-combine-windows"
              type="checkbox"
              name="backfill[combine_windows]"
              label="Combine windows"
              tooltip="Run all selected windows in one child run instead of creating one child run per window."
              checked={@backfill_config.combine_windows}
              disabled={!@can_submit_runs? || !@pipeline.can_backfill?}
              data-testid="pipeline-backfill-combine-windows"
            />

            <.button
              type="submit"
              class="self-end justify-self-start"
              disabled={!@can_submit_runs? || !@pipeline.can_backfill?}
              data-testid="submit-backfill-button"
            >
              Backfill
            </.button>
          </form>

          <p
            :if={@pipeline.can_backfill?}
            class="mt-2 text-sm favn-text-muted"
            data-testid="pipeline-backfill-defaults"
          >
            Defaults to {@backfill_config.kind} windows in {@backfill_config.timezone} with {@backfill_config.refresh} refresh.
          </p>

          <p
            :if={!@pipeline.can_backfill?}
            class="mt-2 text-sm favn-text-muted"
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
    </.panel>
    """
  end

  attr :pipeline, :map, required: true

  def history_panel(assigns) do
    ~H"""
    <.panel padding={:none} class="overflow-hidden" data-testid="pipeline-history-panel">
      <div class="border-b border-base-content/10 p-5 sm:p-6">
        <h2 class="text-lg font-medium">Run history</h2>

        <p class="mt-1 text-sm favn-text-muted">
          Pipeline and backfill runs matched to this pipeline.
        </p>
      </div>

      <div :if={@pipeline.runs == []} class="p-8 text-center text-sm favn-text-muted">
        No runs have been recorded for this pipeline yet.
      </div>

      <div :if={@pipeline.runs != []} class="overflow-x-auto">
        <table class="table" data-testid="pipeline-runs-table">
          <thead>
            <tr class="border-base-content/10 favn-text-muted">
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
                <.link navigate={~p"/runs/#{run.id}"} class="link link-hover font-mono text-sm">
                  {run.short_id}
                </.link>
              </td>

              <td>
                <.status_badge tone={status_tone(run.status)} label={status_label(run.status)} />
              </td>

              <td>{run.kind_label}</td>

              <td>{run.window_label}</td>

              <td>{run.started_at_label}</td>

              <td>{run.duration_label}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </.panel>
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

  defp status_label(:healthy), do: "Healthy"
  defp status_label(:running), do: "Running"
  defp status_label(:succeeded), do: "Succeeded"
  defp status_label(:queued), do: "Queued"
  defp status_label(:failed), do: "Failed"
  defp status_label(:cancelled), do: "Cancelled"
  defp status_label(_status), do: "Unknown"

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
