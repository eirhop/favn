defmodule FavnView.Components.PipelineDetailPage do
  @moduledoc """
  One pipeline: what it is declared to do, and what it has done.

  The shell already names the pipeline and carries its health, so this page does
  not repeat either. What it adds is the declaration — the period the pipeline
  runs, in which timezone, with which dependencies and concurrency — because
  those values live in the manifest and appear nowhere else an operator can read
  them.

  Submitting a run is `FavnView.Components.PipelineRunDialog`, not a form on the
  page. Configuration that sits open on a page competes with the page for
  attention and invites a reflexive click on something that costs an hour of
  compute; a dialog makes the moment deliberate and is also where the declared
  values are restated as the intent for this submission.
  """

  use FavnView, :html

  alias FavnView.Components.AppShell
  alias FavnView.Components.PipelineRunDialog

  @visible_assets 12

  attr :pipeline, :map, required: true
  attr :nav_items, :list, required: true
  attr :current_scope, :any, default: nil
  attr :operator_workspaces, :list, default: []
  attr :run_dialog_open?, :boolean, default: false
  attr :run_config, :map, required: true, doc: "see `FavnView.PipelineRunConfig`"
  attr :run_config_defaults, :map, required: true, doc: "what the pipeline declares"
  attr :run_advanced_open?, :boolean, default: false
  attr :run_config_valid?, :boolean, default: true
  attr :run_error, :string, default: nil
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
      facts={facts(@pipeline)}
      flash={@flash}
    >
      <:actions>
        <.button
          variant={:primary}
          icon="hero-play"
          phx-click="open_run_dialog"
          data-testid="open-run-dialog"
        >
          Run pipeline…
        </.button>
      </:actions>

      <div
        class="mx-auto w-full max-w-[120rem] space-y-4 pb-24 lg:pb-0"
        data-testid="pipeline-detail-page"
      >
        <.assets_panel pipeline={@pipeline} /> <.history_panel pipeline={@pipeline} />
      </div>

      <:overlay>
        <PipelineRunDialog.pipeline_run_dialog
          :if={@run_dialog_open?}
          pipeline={@pipeline}
          run_config={@run_config}
          defaults={@run_config_defaults}
          advanced_open?={@run_advanced_open?}
          valid?={@run_config_valid?}
          error={@run_error}
          can_submit_runs?={@can_submit_runs?}
        />
      </:overlay>
    </AppShell.app_shell>
    """
  end

  @doc """
  What the pipeline declares, for the shell's toolbar.

  The default period is stated in words rather than as a resolved date. The
  control plane resolves it at submission, after subtracting the availability
  delay the selected assets declare, so a date computed here would be a second
  answer to a question that already has one — and would be wrong on exactly the
  pipelines whose assets wait for late-arriving data.
  """
  @spec facts(map()) :: [map()]
  def facts(pipeline) do
    [
      %{label: "Assets", value: to_string(pipeline.asset_count)},
      %{label: "Window", value: pipeline.window_label},
      %{label: "Default run", value: pipeline.default_run_label},
      %{label: "Dependencies", value: pipeline.dependencies_label},
      %{label: "Last run", value: pipeline.last_run_label},
      %{label: "Runtime", value: pipeline.runtime_label}
    ]
  end

  @doc """
  The assets the pipeline's selectors resolve to.

  A long selection used to render as one wall of badges taller than the run
  history it pushed off screen. The first #{@visible_assets} answer "is this the
  pipeline I meant"; the rest are one disclosure away for the rarer question of
  whether a particular asset is in it.
  """
  attr :pipeline, :map, required: true

  def assets_panel(assigns) do
    assigns =
      assigns
      |> assign(:visible, Enum.take(assigns.pipeline.selected_assets, @visible_assets))
      |> assign(:hidden, Enum.drop(assigns.pipeline.selected_assets, @visible_assets))

    ~H"""
    <.panel data-testid="pipeline-assets-panel">
      <:header title="Selected assets" subtitle="What this pipeline's selectors resolve to" />

      <div class="flex flex-wrap gap-2">
        <.badge :for={asset <- @visible} tone={:info}>{asset}</.badge>

        <span :if={@pipeline.selected_assets == []} class="text-sm favn-text-muted">
          No resolved assets
        </span>
      </div>

      <details :if={@hidden != []} class="mt-3" data-testid="pipeline-assets-disclosure">
        <summary class="cursor-pointer text-sm favn-text-muted">
          Show all {@pipeline.asset_count}
        </summary>

        <div class="mt-3 flex flex-wrap gap-2">
          <.badge :for={asset <- @hidden} tone={:info}>{asset}</.badge>
        </div>
      </details>
    </.panel>
    """
  end

  attr :pipeline, :map, required: true

  def history_panel(assigns) do
    ~H"""
    <.table_panel
      count={length(@pipeline.runs)}
      count_label={run_count_label(@pipeline.runs)}
      data-testid="pipeline-history-panel"
    >
      <.empty_state
        :if={@pipeline.runs == []}
        title="No runs yet"
        description="This pipeline has never been submitted. Run it to see its history here."
        icon="hero-play"
        data-testid="pipeline-history-empty-state"
      />
      <.runs_table :if={@pipeline.runs != []} runs={@pipeline.runs} />
      <.runs_card_list :if={@pipeline.runs != []} runs={@pipeline.runs} />

      <:footer>
        <.button variant={:link} navigate={~p"/runs"} data-testid="pipeline-all-runs-link">
          All runs
        </.button>
      </:footer>
    </.table_panel>
    """
  end

  attr :runs, :list, required: true

  def runs_table(assigns) do
    ~H"""
    <.data_table
      id="pipeline-runs-table"
      rows={@runs}
      row_testid="pipeline-run-row"
      row_navigate={&~p"/runs/#{&1.id}"}
      fill?
      desktop_only?
    >
      <:col :let={run} label="Run" class="w-64">
        <.stacked_cell
          primary={run.short_id}
          secondary={run.kind_label}
          mono={:primary}
          navigate={~p"/runs/#{run.id}"}
        />
      </:col>

      <:col :let={run} label="Status" class="w-28">
        <.status_badge tone={status_tone(run.status)} label={status_label(run.status)} />
      </:col>

      <:col :let={run} label="Window" class="w-48">
        <span class="text-sm favn-text-muted">{run.window_label}</span>
      </:col>

      <:col :let={run} label="Started" class="w-40">
        <.stacked_cell primary={run.started_at_label} secondary={run.duration_label} tone={:muted} />
      </:col>
    </.data_table>
    """
  end

  attr :runs, :list, required: true

  def runs_card_list(assigns) do
    ~H"""
    <div class="space-y-2.5 p-3 lg:hidden" data-testid="pipeline-runs-card-list">
      <.list_card :for={run <- @runs} navigate={~p"/runs/#{run.id}"} data-testid="pipeline-run-card">
        <div class="flex items-start justify-between gap-3">
          <div class="min-w-0 flex-1">
            <.mono value={run.short_id} truncate />
            <.meta>{run.kind_label} · {run.window_label}</.meta>
          </div>

          <.status_badge tone={status_tone(run.status)} label={status_label(run.status)} />
        </div>

        <.meta class="mt-2">{run.started_at_label} · {run.duration_label}</.meta>
      </.list_card>
    </div>
    """
  end

  @doc """
  A pipeline view model for the design-system browser and component tests.
  """
  @spec sample_pipeline() :: map()
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
      window: %{"kind" => "month", "timezone" => "Etc/UTC", "combine_windows" => false},
      window_label: "Month · Etc/UTC",
      default_run_label: "The last complete month",
      max_concurrency: 4,
      execution_pool: "default",
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

  @doc """
  A pipeline that replaces its whole relation, so no period can be asked for.

  The unwindowed case is a different page, not a disabled version of this one:
  no period facts, no period controls in the dialog, and no way to reach a
  backfill.
  """
  @spec sample_unwindowed_pipeline() :: map()
  def sample_unwindowed_pipeline do
    %{
      sample_pipeline()
      | name: "marketing_refresh",
        label: "Example.Pipelines.MarketingRefresh",
        dependencies: :none,
        dependencies_label: "Selected only",
        window: nil,
        window_label: "Not windowed",
        default_run_label: "The whole relation",
        can_run_without_window?: true,
        can_backfill?: false
    }
  end

  defp run_count_label([_run]), do: "run"
  defp run_count_label(_runs), do: "runs"

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
end
