defmodule FavnView.Components.AssetDetailPage do
  @moduledoc """
  Static asset detail page foundation for the Favn HUD shell.
  """

  use FavnView, :html

  alias FavnView.AssetRoute
  alias FavnView.Components.AppShell
  alias FavnView.Components.ModeRail
  alias FavnView.Components.Navigation
  alias FavnView.Components.OutputMetadata
  alias FavnView.Components.SelectedWindowActions
  alias FavnView.LogsViewModel

  attr :title, :string, required: true
  attr :status, :string, required: true
  attr :status_tone, :atom, default: :success
  attr :window_kind_label, :string, default: "Windows"
  attr :refresh_timeline_label, :string, default: "Refresh periods"
  attr :refresh_cadence_label, :string, default: "Refresh cadence"
  attr :freshness_timeline_label, :string, default: "Freshness periods"
  attr :freshness_cadence_label, :string, default: "Freshness cadence"
  attr :data_coverage_timeline_label, :string, default: "Data windows"
  attr :window_range, :string, required: true
  attr :refresh_window_range, :string, default: "No windows"
  attr :freshness_window_range, :string, default: "No windows"
  attr :data_coverage_window_range, :string, default: "No windows"
  attr :active_timeline, :atom, default: :refresh
  attr :has_freshness_timeline?, :boolean, default: false
  attr :has_data_windows?, :boolean, default: false
  attr :can_run_asset?, :boolean, default: true
  attr :run_contexts, :list, default: []
  attr :selected_run_context, :map, default: nil
  attr :run_context_status, :atom, default: :unavailable
  attr :nav_items, :list, required: true
  attr :current_scope, :any, default: nil
  attr :operator_workspaces, :list, default: []
  attr :timeline, :list, default: []
  attr :refresh_timeline, :list, default: nil
  attr :freshness_timeline, :list, default: nil
  attr :data_coverage_timeline, :list, default: nil
  attr :active_mode, :atom, default: :overview
  attr :freshness, :map, default: nil
  attr :coverage, :any, default: nil
  attr :coverage_policy, :map, default: nil
  attr :coverage_gaps, :list, default: []
  attr :coverage_pagination, :map, default: %{limit: 100, has_more: false, next_cursor: nil}

  attr :coverage_page_cursor, :string, default: nil
  attr :compatibility, :map, default: nil
  attr :rebuild_target_id, :string, default: nil
  attr :assurance, :map, default: nil

  attr :asset_id, :string, required: true, doc: "route id; the rail patches relative to it"
  attr :runs, :list, default: [], doc: "newest first; see `FavnView.UI.Data.run_timeline/1`"
  attr :relation, :map, default: nil, doc: "the four address levels the asset declares"
  attr :cadence_label, :string, default: nil, doc: "how often it runs, in plain words"
  attr :type, :string, default: nil, doc: "`\"sql\"`, `\"elixir\"`, or `\"source\"`"
  attr :upstream, :list, default: [], doc: "assets this one reads"
  attr :downstream, :list, default: [], doc: "assets that read this one"
  attr :selected_run_id, :string, default: nil

  attr :selected_run, :any,
    default: nil,
    doc: "`{:ok, detail}`, `{:not_found, id}`, `{:error, reason}`, or nil when none is selected"

  attr :coverage_plan, :map, default: nil
  attr :coverage_action_error, :string, default: nil
  attr :planning_coverage?, :boolean, default: false
  attr :submitting_coverage?, :boolean, default: false
  attr :selected_window, :map, default: nil
  attr :run_config_open?, :boolean, default: false
  attr :run_config, :map, default: %{dependencies: "all", refresh: "auto"}
  attr :run_config_valid?, :boolean, default: true
  attr :submitting_window_run?, :boolean, default: false
  attr :selected_window_error, :string, default: nil
  attr :submitted_run_id, :string, default: nil
  attr :can_submit_runs?, :boolean, default: false
  attr :flash, :map, default: %{}

  def asset_detail_page(assigns) do
    assigns = assign(assigns, :refresh_timeline, assigns.refresh_timeline || assigns.timeline)

    ~H"""
    <AppShell.app_shell
      title={@title}
      status={@status}
      status_tone={@status_tone}
      nav_items={@nav_items}
      current_scope={@current_scope}
      operator_workspaces={@operator_workspaces}
      flash={@flash}
      content_scroll?={@active_mode != :runs}
    >
      <.central_view
        active_mode={@active_mode}
        window_kind_label={@window_kind_label}
        refresh_timeline_label={@refresh_timeline_label}
        refresh_cadence_label={@refresh_cadence_label}
        freshness_timeline_label={@freshness_timeline_label}
        freshness_cadence_label={@freshness_cadence_label}
        data_coverage_timeline_label={@data_coverage_timeline_label}
        window_range={@window_range}
        refresh_window_range={@refresh_window_range}
        freshness_window_range={@freshness_window_range}
        data_coverage_window_range={@data_coverage_window_range}
        active_timeline={@active_timeline}
        has_freshness_timeline?={@has_freshness_timeline?}
        has_data_windows?={@has_data_windows?}
        can_run_asset?={@can_run_asset?}
        run_contexts={@run_contexts}
        selected_run_context={@selected_run_context}
        run_context_status={@run_context_status}
        refresh_timeline={@refresh_timeline}
        freshness_timeline={@freshness_timeline}
        data_coverage_timeline={@data_coverage_timeline}
        freshness={@freshness}
        coverage={@coverage}
        coverage_policy={@coverage_policy}
        coverage_gaps={@coverage_gaps}
        coverage_pagination={@coverage_pagination}
        coverage_page_cursor={@coverage_page_cursor}
        compatibility={@compatibility}
        rebuild_target_id={@rebuild_target_id}
        assurance={@assurance}
        asset_id={@asset_id}
        runs={@runs}
        relation={@relation}
        cadence_label={@cadence_label}
        type={@type}
        upstream={@upstream}
        downstream={@downstream}
        selected_run_id={@selected_run_id}
        selected_run={@selected_run}
        title={@title}
        coverage_plan={@coverage_plan}
        coverage_action_error={@coverage_action_error}
        planning_coverage?={@planning_coverage?}
        submitting_coverage?={@submitting_coverage?}
        selected_window={@selected_window}
        run_config_open?={@run_config_open?}
        run_config={@run_config}
        run_config_valid?={@run_config_valid?}
        submitting_window_run?={@submitting_window_run?}
        selected_window_error={@selected_window_error}
        submitted_run_id={@submitted_run_id}
        can_submit_runs?={@can_submit_runs?}
      />
      <:mode_rail>
        <ModeRail.mode_rail
          active={@active_mode}
          modes={detail_modes(@asset_id, @has_data_windows?)}
        />
      </:mode_rail>
    </AppShell.app_shell>
    """
  end

  attr :active_mode, :atom, required: true
  attr :title, :string, required: true
  attr :window_kind_label, :string, default: "Windows"
  attr :refresh_timeline_label, :string, default: "Refresh periods"
  attr :refresh_cadence_label, :string, default: "Refresh cadence"
  attr :freshness_timeline_label, :string, default: "Freshness periods"
  attr :freshness_cadence_label, :string, default: "Freshness cadence"
  attr :data_coverage_timeline_label, :string, default: "Data windows"
  attr :window_range, :string, required: true
  attr :refresh_window_range, :string, default: "No windows"
  attr :freshness_window_range, :string, default: "No windows"
  attr :data_coverage_window_range, :string, default: "No windows"
  attr :active_timeline, :atom, default: :refresh
  attr :has_freshness_timeline?, :boolean, default: false
  attr :has_data_windows?, :boolean, default: false
  attr :can_run_asset?, :boolean, default: true
  attr :run_contexts, :list, default: []
  attr :selected_run_context, :map, default: nil
  attr :run_context_status, :atom, default: :unavailable
  attr :refresh_timeline, :list, default: []
  attr :freshness_timeline, :list, default: nil
  attr :data_coverage_timeline, :list, default: nil
  attr :freshness, :map, default: nil
  attr :coverage, :any, default: nil
  attr :coverage_policy, :map, default: nil
  attr :coverage_gaps, :list, default: []
  attr :coverage_pagination, :map, default: %{limit: 100, has_more: false, next_cursor: nil}

  attr :coverage_page_cursor, :string, default: nil
  attr :compatibility, :map, default: nil
  attr :rebuild_target_id, :string, default: nil
  attr :assurance, :map, default: nil

  attr :asset_id, :string, required: true, doc: "route id; the rail patches relative to it"
  attr :runs, :list, default: [], doc: "newest first; see `FavnView.UI.Data.run_timeline/1`"
  attr :relation, :map, default: nil, doc: "the four address levels the asset declares"
  attr :cadence_label, :string, default: nil, doc: "how often it runs, in plain words"
  attr :type, :string, default: nil, doc: "`\"sql\"`, `\"elixir\"`, or `\"source\"`"
  attr :upstream, :list, default: [], doc: "assets this one reads"
  attr :downstream, :list, default: [], doc: "assets that read this one"
  attr :selected_run_id, :string, default: nil

  attr :selected_run, :any,
    default: nil,
    doc: "`{:ok, detail}`, `{:not_found, id}`, `{:error, reason}`, or nil when none is selected"

  attr :coverage_plan, :map, default: nil
  attr :coverage_action_error, :string, default: nil
  attr :planning_coverage?, :boolean, default: false
  attr :submitting_coverage?, :boolean, default: false
  attr :selected_window, :map, default: nil
  attr :run_config_open?, :boolean, default: false
  attr :run_config, :map, default: %{dependencies: "all", refresh: "auto"}
  attr :run_config_valid?, :boolean, default: true
  attr :submitting_window_run?, :boolean, default: false
  attr :selected_window_error, :string, default: nil
  attr :submitted_run_id, :string, default: nil
  attr :can_submit_runs?, :boolean, default: false

  def central_view(assigns) do
    ~H"""
    <.asset_overview
      :if={@active_mode == :overview}
      title={@title}
      asset_id={@asset_id}
      relation={@relation}
      freshness={@freshness}
      runs={@runs}
      upstream={@upstream}
      downstream={@downstream}
      cadence_label={@cadence_label}
      type={@type}
      problems={asset_problems(assigns)}
    />

    <div
      :if={@active_mode == :runs}
      class="flex min-h-0 w-full flex-1 flex-col-reverse gap-4 lg:flex-row"
    >
      <div class="min-h-0 flex-1 overflow-y-auto lg:pr-2">
        <.run_detail_panel
          :if={match?({:ok, _run}, @selected_run)}
          run={elem(@selected_run, 1)}
          asset_id={@asset_id}
        />

        <.error_state
          :if={match?({:error, _reason}, @selected_run)}
          title="This run cannot be read"
          description="The control plane did not return the run. Try again, or open it from the runs screen."
          data-testid="asset-run-error-state"
        />

        <.empty_state
          :if={match?({:not_found, _id}, @selected_run)}
          title="Run not found"
          description="No run of this asset matches that id."
          icon="hero-question-mark-circle"
          data-testid="asset-run-not-found-state"
        />

        <div :if={is_nil(@selected_run)} class="space-y-6">
          <.notice tone={:info} icon="hero-cursor-arrow-rays">
            Pick a run to see what it observed against the contract below.
          </.notice>

          <.assurance_panel :if={@assurance} assurance={@assurance} />

          <.empty_state
            :if={is_nil(@assurance)}
            title="This asset declares no contract"
            description="Add a contract or checks to compare what a run produced against what it promised."
            icon="hero-document-text"
          />
        </div>
      </div>

      <.run_timeline
        runs={@runs}
        selected_id={@selected_run_id}
        empty_label="This asset has not run yet."
        class="favn-surface-list rounded-box max-h-72 shrink-0 p-3 lg:max-h-none lg:w-80"
        data-testid="asset-run-timeline"
      />
    </div>

    <.compatibility_panel
      :if={@active_mode == :diagnostics && @compatibility}
      compatibility={@compatibility}
      rebuild_target_id={@rebuild_target_id}
    />
    <.coverage_summary_panel
      :if={@active_mode == :coverage && @coverage}
      coverage={@coverage}
      policy={@coverage_policy}
      gaps={@coverage_gaps}
      pagination={@coverage_pagination}
      page_cursor={@coverage_page_cursor}
      plan={@coverage_plan}
      action_error={@coverage_action_error}
      planning?={@planning_coverage?}
      submitting?={@submitting_coverage?}
      can_plan?={@can_submit_runs? && @can_run_asset?}
      command_resource={@rebuild_target_id}
    />
    <.window_timeline_panel
      :if={@active_mode == :coverage}
      timelines={[:data_coverage]}
      window_kind_label={@window_kind_label}
      refresh_timeline_label={@refresh_timeline_label}
      refresh_cadence_label={@refresh_cadence_label}
      freshness_timeline_label={@freshness_timeline_label}
      freshness_cadence_label={@freshness_cadence_label}
      data_coverage_timeline_label={@data_coverage_timeline_label}
      window_range={@window_range}
      refresh_window_range={@refresh_window_range}
      freshness_window_range={@freshness_window_range}
      data_coverage_window_range={@data_coverage_window_range}
      active_timeline={@active_timeline}
      has_freshness_timeline?={@has_freshness_timeline?}
      has_data_windows?={@has_data_windows?}
      can_run_asset?={@can_run_asset?}
      run_contexts={@run_contexts}
      selected_run_context={@selected_run_context}
      run_context_status={@run_context_status}
      refresh_timeline={@refresh_timeline}
      freshness_timeline={@freshness_timeline}
      data_coverage_timeline={@data_coverage_timeline}
      freshness={@freshness}
      selected_window={@selected_window}
      run_config_open?={@run_config_open?}
      run_config={@run_config}
      run_config_valid?={@run_config_valid?}
      submitting_window_run?={@submitting_window_run?}
      selected_window_error={@selected_window_error}
      submitted_run_id={@submitted_run_id}
      can_submit_runs?={@can_submit_runs?}
      command_resource={@rebuild_target_id}
    />
    """
  end

  @doc """
  Everything wrong with this asset, worst first.

  Built here rather than in each panel so the overview can say nothing at all when
  nothing is wrong. A page that renders a panel per topic has to fill each one, which
  is how the old screen ended up explaining that freshness was fine twice.
  """
  @spec asset_problems(map()) :: [map()]
  def asset_problems(assigns) do
    [
      compatibility_problem(assigns),
      run_context_problem(assigns),
      failed_run_problem(assigns),
      stale_problem(assigns),
      coverage_problem(assigns)
    ]
    |> Enum.reject(&is_nil/1)
  end

  defp compatibility_problem(%{compatibility: compatibility, asset_id: asset_id})
       when is_map(compatibility) do
    if field(compatibility, :blocks_writes?, false) do
      %{
        tone: :error,
        title: "Runs are blocked",
        detail: blocked_reason(field(compatibility, :status)),
        action: "See what to do",
        href: ~p"/assets/#{asset_id}/diagnostics"
      }
    end
  end

  defp compatibility_problem(_assigns), do: nil

  defp blocked_reason(:rebuild_required),
    do: "The table Favn writes to no longer matches what this asset produces."

  defp blocked_reason(:unexpected_drift),
    do: "The table changed outside Favn, so writing to it could lose data."

  defp blocked_reason(:operator_decision),
    do: "Favn cannot tell whether it owns this table, so it will not write to it."

  defp blocked_reason(_status), do: "Favn cannot write to this asset's table right now."

  defp run_context_problem(%{run_context_status: :ambiguous, asset_id: asset_id}) do
    %{
      tone: :error,
      title: "More than one pipeline could run this",
      detail: "Favn will not guess which one owns it. Choose one before running it.",
      action: "Choose a pipeline",
      href: ~p"/assets/#{asset_id}/diagnostics"
    }
  end

  defp run_context_problem(_assigns), do: nil

  defp failed_run_problem(%{runs: [%{status_tone: :error} = run | _rest], asset_id: asset_id}) do
    %{
      tone: :error,
      title: "The last run failed",
      detail: "Nothing new was written.",
      action: "See what broke",
      href: ~p"/assets/#{asset_id}/runs/#{run.id}"
    }
  end

  defp failed_run_problem(_assigns), do: nil

  defp stale_problem(%{freshness: %{state: :stale} = freshness}) do
    %{
      tone: :warning,
      title: "The data is out of date",
      detail: freshness[:explanation] || "This asset has not run recently enough.",
      action: nil,
      href: nil
    }
  end

  defp stale_problem(_assigns), do: nil

  defp coverage_problem(%{coverage: coverage, has_data_windows?: true, asset_id: asset_id})
       when is_map(coverage) do
    missing = field(coverage, :missing_count, 0)

    if field(coverage, :status) == :incomplete and missing > 0 do
      %{
        tone: :warning,
        title: "#{missing} #{(missing == 1 && "period has") || "periods have"} no data",
        detail: "Some periods this asset should cover were never written.",
        action: "See which ones",
        href: ~p"/assets/#{asset_id}/coverage"
      }
    end
  end

  defp coverage_problem(_assigns), do: nil

  attr :compatibility, :map, required: true
  attr :rebuild_target_id, :string, default: nil

  def compatibility_panel(assigns) do
    ~H"""
    <.panel
      :if={field(@compatibility, :persisted?, false)}
      padding={:none}
      class={[
        "mx-auto mb-6 w-full max-w-[120rem] border p-5 sm:p-6",
        compatibility_panel_class(field(@compatibility, :status))
      ]}
      data-testid="asset-compatibility-panel"
    >
      <div class="flex flex-col gap-4 xl:flex-row xl:items-start xl:justify-between">
        <div class="min-w-0">
          <div class="flex flex-wrap items-center gap-2">
            <p class="text-sm uppercase tracking-[0.18em] favn-text-subtle">
              Target compatibility
            </p>

            <span class={compatibility_badge_class(field(@compatibility, :status))}>
              {compatibility_status_label(field(@compatibility, :status))}
            </span>
          </div>

          <p class="mt-2 text-sm favn-text-muted">
            {compatibility_explanation(@compatibility)}
          </p>
        </div>

        <dl class="grid min-w-0 gap-x-6 gap-y-2 text-sm favn-text-muted sm:grid-cols-2 xl:max-w-3xl">
          <div>
            <dt class="favn-text-subtle">Active generation</dt>

            <dd class="break-all font-mono">
              {coverage_generation_label(field(@compatibility, :active_generation_id))}
            </dd>
          </div>

          <div>
            <dt class="favn-text-subtle">Reason</dt>

            <dd>{humanize(field(@compatibility, :reason_code))}</dd>
          </div>

          <div>
            <dt class="favn-text-subtle">Desired descriptor</dt>

            <dd class="break-all font-mono">
              {field(@compatibility, :desired_descriptor_hash) || "-"}
            </dd>
          </div>

          <div>
            <dt class="favn-text-subtle">Physical fingerprint</dt>

            <dd class="break-all font-mono">
              {field(@compatibility, :physical_fingerprint) || "-"}
            </dd>
          </div>
        </dl>
      </div>

      <div
        :if={compatibility_diff_entries(field(@compatibility, :diff, %{})) != []}
        class="mt-4 rounded-box border border-base-content/10 bg-base-content/[0.03] p-4"
      >
        <p class="text-sm font-medium uppercase tracking-[0.14em] favn-text-subtle">
          Compatibility differences
        </p>

        <dl class="mt-2 grid gap-2 text-sm sm:grid-cols-2">
          <div :for={{name, change} <- compatibility_diff_entries(field(@compatibility, :diff, %{}))}>
            <dt class="favn-text-subtle">{humanize(name)}</dt>

            <dd class="break-all font-mono favn-text-muted">
              {compatibility_change_label(change)}
            </dd>
          </div>
        </dl>
      </div>

      <div
        :if={field(@compatibility, :blocks_writes?, false)}
        class="mt-4 flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between"
        data-testid="asset-compatibility-blocked"
      >
        <p class="text-sm font-medium text-error">
          Runs and backfills are blocked until this target is resolved.
        </p>

        <div :if={@rebuild_target_id} class="flex flex-wrap gap-2">
          <.link
            :if={field(@compatibility, :reason_code) == "unmanaged_physical_relation"}
            navigate={~p"/recoveries?#{[target_id: @rebuild_target_id]}"}
            class="btn btn-warning btn-sm"
            data-testid="recover-asset-ownership"
          >
            Recover ownership
          </.link>

          <.link
            navigate={~p"/rebuilds?#{[target_id: @rebuild_target_id]}"}
            class="btn btn-outline btn-sm"
            data-testid="plan-asset-rebuild"
          >
            Plan rebuild
          </.link>
        </div>
      </div>
    </.panel>
    """
  end

  attr :coverage, :any, required: true
  attr :policy, :map, default: nil
  attr :gaps, :list, default: []
  attr :pagination, :map, default: %{limit: 100, has_more: false, next_cursor: nil}
  attr :page_cursor, :string, default: nil
  attr :plan, :map, default: nil
  attr :action_error, :string, default: nil
  attr :planning?, :boolean, default: false
  attr :submitting?, :boolean, default: false
  attr :can_plan?, :boolean, default: false
  attr :command_resource, :string, required: true

  def coverage_summary_panel(assigns) do
    ~H"""
    <.panel
      padding={:none}
      class="mx-auto mb-6 w-full max-w-[120rem] p-6 sm:p-8"
      data-testid="asset-coverage-summary"
    >
      <div class="flex flex-col gap-5 xl:flex-row xl:items-start xl:justify-between">
        <div class="min-w-0 space-y-3">
          <div class="flex flex-wrap items-center gap-2">
            <p class="text-sm uppercase tracking-[0.18em] favn-text-subtle">Coverage</p>

            <span class={coverage_badge_class(field(@coverage, :status))}>
              {coverage_status_label(field(@coverage, :status))}
            </span>
          </div>

          <p class="text-sm favn-text-muted">
            {coverage_explanation(@coverage)}
          </p>

          <dl :if={field(@coverage, :status) != :unknown} class="grid gap-3 sm:grid-cols-3">
            <.coverage_metric label="Expected" value={field(@coverage, :expected_count, 0)} />
            <.coverage_metric label="Covered" value={field(@coverage, :covered_count, 0)} />
            <.coverage_metric label="Missing" value={field(@coverage, :missing_count, 0)} />
          </dl>

          <dl class="grid gap-x-6 gap-y-2 text-sm favn-text-muted sm:grid-cols-2">
            <div>
              <dt class="favn-text-subtle">Evaluated at</dt>

              <dd>{coverage_time(field(@coverage, :evaluated_at))}</dd>
            </div>

            <div>
              <dt class="favn-text-subtle">Active target generation</dt>

              <dd class="break-all font-mono">
                {coverage_generation_label(field(@coverage, :active_target_generation_id))}
              </dd>
            </div>
          </dl>

          <dl :if={@policy} class="grid gap-x-6 gap-y-2 text-sm favn-text-muted sm:grid-cols-2">
            <div>
              <dt class="favn-text-subtle">Timezone</dt>

              <dd class="font-mono">
                {field(@policy, :timezone)} ({humanize(field(@policy, :timezone_source))})
              </dd>
            </div>

            <div>
              <dt class="favn-text-subtle">Coverage starts</dt>

              <dd>
                {coverage_time(field(@policy, :declared_from))} declared · {coverage_time(
                  field(@policy, :effective_from)
                )} effective
              </dd>
            </div>

            <div>
              <dt class="favn-text-subtle">Expected through</dt>

              <dd>{coverage_window_label(field(@coverage, :last_expected_window))}</dd>
            </div>

            <div>
              <dt class="favn-text-subtle">Availability</dt>

              <dd>{availability_label(field(@policy, :availability_delay_seconds, 0))}</dd>
            </div>
          </dl>
        </div>

        <div class="w-full space-y-3 xl:max-w-xl">
          <div
            :if={field(@coverage, :status) == :incomplete}
            class="rounded-box border border-warning/20 bg-warning/5 p-4"
          >
            <p class="text-sm font-medium uppercase tracking-[0.16em] text-warning">Exact gaps</p>

            <div class="mt-2 max-h-32 space-y-1 overflow-y-auto font-mono text-sm favn-text-muted">
              <p :for={gap <- @gaps}>{field(gap, :window_key)}</p>

              <p :if={@gaps == []} class="font-sans favn-text-subtle">
                No missing windows in this page.
              </p>
            </div>

            <div
              :if={@page_cursor || field(@pagination, :has_more, false)}
              class="mt-3 flex items-center justify-between gap-3"
              data-testid="coverage-gap-pagination"
            >
              <button
                type="button"
                class="btn btn-ghost btn-xs"
                phx-click="page_missing_coverage"
                phx-value-direction="previous"
                disabled={is_nil(@page_cursor)}
                data-testid="previous-coverage-gap-page"
              >
                Previous
              </button>

              <span class="text-center font-sans text-sm favn-text-subtle">
                {length(@gaps)} gaps on this page
              </span>

              <button
                type="button"
                class="btn btn-ghost btn-xs"
                phx-click="page_missing_coverage"
                phx-value-direction="next"
                disabled={!field(@pagination, :has_more, false)}
                data-testid="next-coverage-gap-page"
              >
                Next
              </button>
            </div>
          </div>

          <button
            :if={field(@coverage, :status) == :incomplete && @gaps != [] && is_nil(@plan)}
            type="button"
            class="btn btn-warning btn-soft btn-sm"
            phx-click="plan_missing_coverage"
            disabled={!@can_plan? || @planning?}
            data-testid="plan-missing-coverage"
          >
            <span :if={@planning?} class="loading loading-spinner loading-xs"></span>
            Review missing-window backfill
          </button>

          <div
            :if={@plan}
            class="rounded-box border border-primary/25 bg-primary/5 p-4"
            data-testid="coverage-plan-review"
          >
            <p class="text-sm font-medium">Review immutable plan</p>

            <p class="mt-1 break-all font-mono text-sm favn-text-muted">
              {field(@plan, :plan_hash)}
            </p>

            <div class="mt-3 max-h-36 space-y-1 overflow-y-auto font-mono text-sm favn-text-muted">
              <p :for={window <- field(@plan, :windows, [])}>{field(window, :window_key)}</p>
            </div>

            <button
              type="button"
              class="btn btn-primary btn-sm mt-4"
              phx-click="submit_missing_coverage"
              data-command-operation="coverage_backfill_submit"
              data-command-resource={@command_resource}
              disabled={@submitting?}
              data-testid="submit-missing-coverage"
            >
              <span :if={@submitting?} class="loading loading-spinner loading-xs"></span>
              Submit exact {field(@plan, :window_count, 0)} windows
            </button>
          </div>

          <p
            :if={!@can_plan? && field(@coverage, :status) == :incomplete}
            class="text-sm text-warning"
          >
            Select a valid run context and use an operator account to plan this backfill.
          </p>

          <p :if={@action_error} class="text-sm text-error" data-testid="coverage-action-error">
            {@action_error}
          </p>
        </div>
      </div>
    </.panel>
    """
  end

  attr :label, :string, required: true
  attr :value, :integer, required: true

  defp coverage_metric(assigns) do
    ~H"""
    <div class="rounded-box border border-base-content/10 bg-base-content/[0.03] p-3">
      <dt class="text-sm uppercase tracking-[0.14em] favn-text-subtle">{@label}</dt>

      <dd class="mt-1 text-xl font-medium">{@value}</dd>
    </div>
    """
  end

  attr :window_range, :string, required: true
  attr :window_kind_label, :string, default: "Windows"
  attr :refresh_timeline_label, :string, default: "Refresh periods"
  attr :refresh_cadence_label, :string, default: "Refresh cadence"
  attr :freshness_timeline_label, :string, default: "Freshness periods"
  attr :freshness_cadence_label, :string, default: "Freshness cadence"
  attr :data_coverage_timeline_label, :string, default: "Data windows"
  attr :refresh_window_range, :string, default: "No windows"
  attr :freshness_window_range, :string, default: "No windows"
  attr :data_coverage_window_range, :string, default: "No windows"
  attr :active_timeline, :atom, default: :refresh
  attr :has_freshness_timeline?, :boolean, default: false
  attr :has_data_windows?, :boolean, default: false
  attr :can_run_asset?, :boolean, default: true
  attr :run_contexts, :list, default: []
  attr :selected_run_context, :map, default: nil
  attr :run_context_status, :atom, default: :unavailable
  attr :refresh_timeline, :list, default: []
  attr :freshness_timeline, :list, default: nil
  attr :data_coverage_timeline, :list, default: nil
  attr :freshness, :map, default: nil
  attr :selected_window, :map, default: nil
  attr :run_config_open?, :boolean, default: false
  attr :run_config, :map, default: %{dependencies: "all", refresh: "auto"}
  attr :run_config_valid?, :boolean, default: true
  attr :submitting_window_run?, :boolean, default: false
  attr :selected_window_error, :string, default: nil
  attr :submitted_run_id, :string, default: nil
  attr :can_submit_runs?, :boolean, default: false
  attr :command_resource, :string, required: true

  attr :timelines, :list,
    default: [:refresh, :freshness, :data_coverage],
    doc: "which timelines this page may show; a page scoped to one renders no toggle"

  def window_timeline_panel(assigns) do
    assigns = assign(assigns, :toggles, timeline_toggles(assigns))
    assigns = assign(assigns, :active_timeline, resolved_timeline(assigns))
    assigns = assign(assigns, :timeline, active_timeline(assigns))
    assigns = assign(assigns, :timeline_range, active_timeline_range(assigns))
    assigns = assign(assigns, :timeline_label, active_timeline_label(assigns))
    assigns = assign(assigns, :timeline_kind_label, active_timeline_kind_label(assigns))

    ~H"""
    <.panel
      padding={:none}
      id="window-timeline"
      class="mx-auto w-full max-w-[120rem] p-6 sm:p-8 lg:p-10"
      data-testid="window-timeline-panel"
    >
      <div class="flex flex-col gap-10">
        <.run_context_selector
          :if={@run_contexts != []}
          contexts={@run_contexts}
          selected={@selected_run_context}
          status={@run_context_status}
        />
        <div class="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
          <div>
            <h2 class="text-xl font-medium tracking-tight">{@timeline_label}</h2>

            <p class="mt-2 text-sm favn-text-muted">{@timeline_kind_label}</p>
          </div>

          <div class="flex flex-wrap items-center gap-3 self-start text-sm favn-text-muted">
            <div :if={length(@toggles) > 1} class="join">
              <button
                :for={toggle <- @toggles}
                type="button"
                class={[
                  "btn btn-sm join-item",
                  (@active_timeline == toggle.id && "btn-primary btn-soft") || "btn-ghost"
                ]}
                phx-click="set_timeline"
                phx-value-timeline={toggle.id}
                data-testid={toggle.testid}
              >
                {toggle.label}
              </button>
            </div>

            <span data-testid="timeline-range">{@timeline_range}</span>
          </div>
        </div>
        <.freshness_summary freshness={@freshness} />
        <div class="overflow-x-auto pb-2">
          <div class="flex min-w-[58rem] items-end justify-between gap-3 pt-3">
            <.timeline_window
              :for={window <- @timeline}
              window={window}
              selected={selected_window?(@selected_window, window)}
              selectable?={@active_timeline != :freshness}
            />
          </div>
        </div>

        <SelectedWindowActions.selected_window_actions
          :if={@active_timeline != :freshness}
          selected_window={@selected_window}
          can_run_asset?={@can_run_asset?}
          has_data_windows?={@has_data_windows?}
          active_timeline={@active_timeline}
          run_config_open?={@run_config_open?}
          run_config={@run_config}
          run_config_valid?={@run_config_valid?}
          submitting_window_run?={@submitting_window_run?}
          selected_window_error={@selected_window_error}
          submitted_run_id={@submitted_run_id}
          can_submit_runs?={@can_submit_runs?}
          command_resource={@command_resource}
        />
      </div>
    </.panel>
    """
  end

  attr :contexts, :list, required: true
  attr :selected, :map, default: nil
  attr :status, :atom, required: true

  def run_context_selector(assigns) do
    ~H"""
    <div
      class="rounded-box border border-base-content/10 bg-base-content/[0.03] p-4"
      data-testid="asset-run-context-selector"
    >
      <div class="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
        <div>
          <p class="text-sm uppercase tracking-[0.18em] favn-text-subtle">Run context</p>

          <p class="mt-1 text-sm favn-text-muted">
            Choose the pipeline policy used for run anchors and freshness evaluation.
          </p>
        </div>

        <div class="flex flex-wrap gap-2">
          <.link
            :for={context <- @contexts}
            patch={context.href}
            class={[
              "btn btn-sm",
              selected_run_context?(@selected, context) && "btn-primary btn-soft",
              !selected_run_context?(@selected, context) && "btn-ghost"
            ]}
            data-testid={"asset-run-context-#{context.id}"}
          >
            {context.label}
            <span class="text-sm opacity-60">{run_context_policy_label(context)}</span>
          </.link>
        </div>
      </div>

      <p
        :if={@status == :ambiguous}
        class="mt-3 text-sm text-warning"
        data-testid="asset-run-context-required"
      >
        This asset belongs to multiple pipelines. Select one before running it.
      </p>
    </div>
    """
  end

  @doc """
  The asset in one screen: what state it is in, what is wrong, and what feeds it.

  Facts first, then problems, then lineage, then the one action. Nothing here is
  configuration — this page answers "is it working", and every panel that answered
  "how is it set up" moved to Documentation or Diagnostics. A healthy asset shows no
  problem panel at all rather than a green one saying nothing is wrong.
  """
  attr :freshness, :map, default: nil
  attr :relation, :map, default: nil
  attr :runs, :list, default: []
  attr :upstream, :list, default: []
  attr :downstream, :list, default: []
  attr :title, :string, required: true
  attr :asset_id, :string, required: true
  attr :type, :string, default: nil
  attr :cadence_label, :string, default: nil
  attr :problems, :list, default: []

  def asset_overview(assigns) do
    assigns = assign(assigns, :latest, List.first(assigns.runs))

    ~H"""
    <div class="mx-auto w-full max-w-[120rem] space-y-6" data-testid="asset-overview">
      <.panel padding={:none} class="p-6 sm:p-8">
        <.fact_list columns={3} facts={overview_facts(assigns)} />

        <div class="mt-5 flex flex-wrap items-baseline gap-x-3 gap-y-1 border-t border-base-content/10 pt-4">
          <span class="text-sm favn-text-subtle">Lands in</span>
          <.mono value={relation_address(@relation)} class="min-w-0 flex-1" />

          <.link
            :if={@latest}
            patch={~p"/assets/#{@asset_id}/runs/#{@latest.id}"}
            class="shrink-0 text-sm underline decoration-dotted hover:text-primary"
          >
            Open the last run
          </.link>
        </div>
      </.panel>

      <.notice
        :for={problem <- @problems}
        tone={problem.tone}
        class="mx-auto w-full"
        data-testid="asset-overview-problem"
      >
        <p class="font-medium">{problem.title}</p>
        <p class="mt-0.5">{problem.detail}</p>
        <.link
          :if={problem[:href]}
          patch={problem.href}
          class="mt-1 inline-flex text-sm underline decoration-dotted"
        >
          {problem.action}
        </.link>
      </.notice>

      <.panel padding={:none} class="p-6 sm:p-8" data-testid="asset-lineage">
        <h2 class="text-sm uppercase tracking-[0.18em] favn-text-subtle">Lineage</h2>

        <.lineage_graph
          class="mt-4"
          centre={%{label: @title, icon: asset_type_icon(@type)}}
          inputs={Enum.map(@upstream, &lineage_node/1)}
          outputs={Enum.map(@downstream, &lineage_node/1)}
          inputs_empty="Nothing. This asset reads its source directly."
          outputs_empty="Nothing yet. No other asset reads this one."
        />
      </.panel>
    </div>
    """
  end

  # A dependency this deployment does not carry gets no link and says so, rather than
  # being dropped: a silently shorter list reads as a complete one.
  defp lineage_node(%{target_id: target_id} = asset) when is_binary(target_id) do
    %{
      label: asset[:name] || asset[:asset_ref],
      title: asset[:asset_ref],
      icon: asset_type_icon(asset[:type]),
      navigate: ~p"/assets/#{AssetRoute.to_param(target_id)}"
    }
  end

  defp lineage_node(asset) do
    %{
      label: asset[:name] || asset[:asset_ref],
      title: asset[:asset_ref],
      icon: asset_type_icon(asset[:type]),
      note: "not in this deployment"
    }
  end

  defp asset_type_icon(type) when type in ["sql", :sql], do: "hero-table-cells"
  defp asset_type_icon(type) when type in ["elixir", :elixir], do: "hero-code-bracket"
  defp asset_type_icon(type) when type in ["source", :source], do: "hero-cloud-arrow-down"
  defp asset_type_icon(_type), do: "hero-cube"

  defp overview_facts(assigns) do
    [
      %{
        label: "Freshness",
        value: freshness_state_label(assigns.freshness[:state]),
        tone: freshness_tone(assigns.freshness[:state])
      },
      %{
        label: "Last run",
        value: last_run_value(assigns.latest),
        tone: last_run_tone(assigns.latest)
      },
      %{label: "Runs", value: assigns.cadence_label || "On request"}
    ]
  end

  defp last_run_value(nil), do: "Never"

  defp last_run_value(run) do
    [run[:day_label], run[:time_label]]
    |> Enum.reject(&is_nil/1)
    |> Enum.join(" ")
    |> case do
      "" -> run[:status_label] || "Unknown"
      when_label -> "#{when_label} · #{run[:status_label]}"
    end
  end

  defp last_run_tone(nil), do: :neutral
  defp last_run_tone(run), do: run[:status_tone] || :neutral

  defp freshness_tone(:fresh), do: :success
  defp freshness_tone(:stale), do: :warning
  defp freshness_tone(:always_run), do: :info
  defp freshness_tone(_state), do: :neutral

  # The address someone would type into a query, so the connection is left off: that
  # names how Favn reaches the database, not where the table is inside it. The full
  # four levels belong on Documentation. Declared levels are reported as they are and
  # missing ones are never padded, so this cannot imply a depth the asset lacks.
  defp relation_address(nil), do: "Not declared"

  defp relation_address(relation) do
    [:catalog, :schema, :name]
    |> Enum.map(&field(relation, &1))
    |> Enum.reject(&(&1 in [nil, ""]))
    |> case do
      [] -> "Not declared"
      levels -> Enum.join(levels, ".")
    end
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
          <p class="text-sm uppercase tracking-[0.18em] favn-text-subtle">Freshness</p>

          <div class="mt-1 flex flex-wrap items-center gap-2">
            <span class={freshness_badge_class(@freshness[:state])}>
              {freshness_state_label(@freshness[:state])}
            </span>
            <span class="text-sm favn-text-muted">{freshness_policy_label(@freshness)}</span>
          </div>

          <p class="mt-2 text-sm favn-text-muted">{@freshness[:explanation]}</p>
        </div>
      </div>
    </div>
    """
  end

  attr :freshness, :map, default: nil

  def freshness_detail_panel(assigns) do
    ~H"""
    <.panel
      padding={:none}
      class="mx-auto w-full max-w-4xl p-6 sm:p-8"
      data-testid="asset-freshness-detail-panel"
    >
      <div :if={!@freshness} class="text-sm favn-text-muted">
        Freshness detail is not available from the backend.
      </div>

      <div :if={@freshness} class="space-y-6">
        <div>
          <p class="text-sm uppercase tracking-[0.18em] favn-text-subtle">Freshness detail</p>

          <div class="mt-2 flex flex-wrap items-center gap-2">
            <span class={freshness_badge_class(@freshness[:state])}>
              {freshness_state_label(@freshness[:state])}
            </span>
            <span class="badge badge-ghost badge-sm">{freshness_policy_label(@freshness)}</span>
          </div>

          <p class="mt-3 text-sm favn-text-muted">{@freshness[:explanation]}</p>
        </div>

        <dl :if={freshness_latest_success(@freshness)} class="grid gap-3 sm:grid-cols-3">
          <div class="rounded-box border border-base-content/10 bg-base-content/[0.03] p-3">
            <dt class="text-sm uppercase tracking-[0.16em] favn-text-subtle">Latest success</dt>

            <dd class="mt-1 break-words font-mono text-sm favn-text-muted">
              {freshness_latest_success(@freshness)[:run_id]}
            </dd>
          </div>

          <div class="rounded-box border border-base-content/10 bg-base-content/[0.03] p-3">
            <dt class="text-sm uppercase tracking-[0.16em] favn-text-subtle">Freshness key</dt>

            <dd class="mt-1 break-words font-mono text-sm favn-text-muted">
              {freshness_latest_success(@freshness)[:freshness_key]}
            </dd>
          </div>

          <div class="rounded-box border border-base-content/10 bg-base-content/[0.03] p-3">
            <dt class="text-sm uppercase tracking-[0.16em] favn-text-subtle">At</dt>

            <dd class="mt-1 text-sm favn-text-muted">
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
                <span :if={reason[:upstream_ref]} class="font-mono text-sm favn-text-subtle">
                  {reason[:upstream_ref]}
                </span>
              </div>

              <p class="mt-2 text-sm favn-text-muted">{reason[:message]}</p>

              <dl class="mt-3 grid gap-2 text-sm favn-text-muted sm:grid-cols-3">
                <div :if={reason[:previous_version]}>
                  <dt class="uppercase tracking-[0.14em] favn-text-subtle">Previous</dt>

                  <dd class="mt-0.5 break-words font-mono">{reason[:previous_version]}</dd>
                </div>

                <div :if={reason[:current_version]}>
                  <dt class="uppercase tracking-[0.14em] favn-text-subtle">Current</dt>

                  <dd class="mt-0.5 break-words font-mono">{reason[:current_version]}</dd>
                </div>

                <div :if={reason[:run_id]}>
                  <dt class="uppercase tracking-[0.14em] favn-text-subtle">Run</dt>

                  <dd class="mt-0.5 break-words font-mono">{reason[:run_id]}</dd>
                </div>
              </dl>
            </div>
          </div>
        </div>
      </div>
    </.panel>
    """
  end

  @doc """
  What one run promised to produce, and what it actually produced.

  Two tables rather than a wall of cards. Every check is a row in one table —
  contract row-count claims and hand-written checks alike — because a card each
  stops working at the third check and an asset can declare a dozen. Columns are a
  second table that stays shut when every column matched, so the reader opens it
  only when there is a difference to look at.
  """
  attr :assurance, :map, required: true

  def assurance_panel(assigns) do
    validation = assigns.assurance[:contract_validation]
    contract = assigns.assurance[:contract]
    columns = column_rows(contract, validation)

    assigns =
      assigns
      |> assign(:contract, contract)
      |> assign(:validation, validation)
      |> assign(:check_rows, check_rows(assigns.assurance))
      |> assign(:columns, columns)
      |> assign(:observed?, is_map(validation))
      |> assign(:mismatched, Enum.count(columns, & &1.mismatch))

    ~H"""
    <.panel padding={:none} class="p-6 sm:p-8" data-testid="asset-assurance-panel">
      <div class="flex flex-wrap items-start justify-between gap-3">
        <h2 class="text-xl font-medium tracking-tight">Data quality</h2>

        <div class="flex flex-wrap gap-2">
          <.status_badge
            :if={@assurance[:quality_status]}
            tone={quality_tone(@assurance[:quality_status])}
            label={quality_label(@assurance[:quality_status])}
          />
          <.badge :if={@assurance[:write_outcome]} variant={:outline}>
            {write_label(@assurance[:write_outcome])}
          </.badge>
        </div>
      </div>

      <.fact_list
        :if={@contract}
        class="mt-5"
        columns={2}
        facts={[
          %{label: "One row per", value: grain_label(@contract[:grain])},
          %{label: "Must be unique", value: unique_keys_label(@contract[:unique_keys])}
        ]}
      />

      <section :if={@check_rows != []} class="mt-8" data-testid="asset-quality-checks">
        <div class="flex flex-wrap items-baseline justify-between gap-2">
          <h3 class="font-medium">Checks</h3>
          <span class={["text-sm", Tokens.text_class(checks_tone(@check_rows))]}>
            {checks_summary(@check_rows)}
          </span>
        </div>

        <div class="mt-3 overflow-x-auto">
          <table class="table w-full min-w-[44rem]">
            <thead>
              <tr class="border-base-content/10 favn-text-muted">
                <th>Check</th>

                <th>When</th>

                <th>Expects</th>

                <th>Found</th>

                <th>Result</th>
              </tr>
            </thead>

            <tbody>
              <tr
                :for={row <- @check_rows}
                class="border-base-content/10"
                data-testid="asset-quality-check"
              >
                <td>
                  <.stacked_cell
                    primary={row.name}
                    secondary={row.detail}
                    mono={:primary}
                    class="max-w-[16rem]"
                  />
                </td>

                <td class="favn-text-muted">{row.when}</td>

                <td class="favn-text-muted">{row.expects}</td>

                <td class={row.found_tone && Tokens.text_class(row.found_tone)}>{row.found}</td>

                <td>
                  <.status_badge tone={row.tone} label={row.result} />
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </section>

      <section :if={@columns != []} class="mt-8" data-testid="asset-output-contract">
        <p :if={!@observed?} class="text-sm favn-text-muted">
          This run did not record the table's actual columns, so only what the asset
          promises is shown.
        </p>

        <details open={@mismatched > 0}>
          <summary class="flex cursor-pointer flex-wrap items-baseline justify-between gap-2">
            <span class="font-medium">Columns</span>
            <span class={["text-sm", Tokens.text_class(columns_tone(@mismatched, @observed?))]}>
              {columns_summary(@columns, @mismatched, @observed?)}
            </span>
          </summary>

          <div class="mt-3 overflow-x-auto">
            <table class="table w-full min-w-[40rem]">
              <thead>
                <tr class="border-base-content/10 favn-text-muted">
                  <th>Column</th>

                  <th>Type</th>

                  <th :if={@observed?}>Found</th>

                  <th>Comes from</th>
                </tr>
              </thead>

              <tbody>
                <tr
                  :for={column <- @columns}
                  class="border-base-content/10"
                  data-testid="contract-column"
                >
                  <td class="font-mono">{column.name}</td>

                  <td class="favn-text-muted">{column.expected}</td>

                  <td
                    :if={@observed?}
                    class={(column.mismatch && Tokens.text_class(:error)) || "favn-text-muted"}
                  >
                    {column.found}
                  </td>

                  <td class="favn-text-subtle">{column.source}</td>
                </tr>
              </tbody>
            </table>
          </div>
        </details>
      </section>

      <.notice
        :if={@validation && @validation[:differences] != []}
        tone={:error}
        class="mt-6"
        data-testid="contract-schema-differences"
      >
        <p class="font-medium">The table does not match what the asset promises</p>
        <ul class="mt-1 list-disc space-y-0.5 pl-4">
          <li :for={difference <- @validation[:differences]}>{difference_label(difference)}</li>
        </ul>
      </.notice>

      <p :if={@validation && @validation[:observed_truncated?]} class="mt-4 text-sm text-warning">
        Only the first {length(@validation[:observed_columns])} of {@validation[
          :observed_column_count
        ]} columns were read.
      </p>
    </.panel>
    """
  end

  # Row-count claims and hand-written checks are the same thing to a reader: a rule
  # that either held or did not. They were two different card layouts, so a contract
  # claim looked unlike a check even when both had just passed.
  defp check_rows(assurance) do
    claim_rows =
      assurance
      |> get_in([Access.key(:contract, %{}), Access.key(:row_counts, [])])
      |> List.wrap()
      |> Enum.map(&row_count_check_row/1)

    check_rows =
      assurance
      |> Map.get(:checks, [])
      |> List.wrap()
      |> Enum.reject(&(&1[:origin] == :contract and is_binary(&1[:claim_id])))
      |> Enum.map(&declared_check_row/1)

    claim_rows ++ check_rows
  end

  defp row_count_check_row(row_count) do
    result = row_count[:latest_result]

    %{
      name: "Row count",
      detail: row_count[:claim_id],
      when: "After writing",
      expects: row_count_constraint_label(row_count),
      found: metric_value(result, [:actual, :row_count, :count]),
      found_tone: nil,
      result: check_result_label(result),
      tone: check_result_tone(result),
      on_violation: row_count[:on_violation]
    }
  end

  defp declared_check_row(check) do
    result = check[:latest_result]

    %{
      name: to_string(check[:name]),
      detail: check[:claim_id] || violation_label(check[:on_violation]),
      when: phase_label(check[:phase]),
      expects: check[:message] || violation_label(check[:on_violation]),
      found: metric_value(result, [:actual, :count, :rows]),
      found_tone: nil,
      result: check_result_label(result),
      tone: check_result_tone(result),
      on_violation: check[:on_violation]
    }
  end

  # Contract claims already appear as their own rows, so a generated check that
  # restates one would be the same rule twice under two names.
  defp column_rows(nil, _validation), do: []

  defp column_rows(contract, validation) do
    observed = observed_by_name(validation)

    contract
    |> Map.get(:columns, [])
    |> List.wrap()
    |> Enum.map(fn column ->
      found = Map.get(observed, to_string(column[:name]))

      %{
        name: to_string(column[:name]),
        expected: column_type_label(column),
        found: (found && observed_column_label(found)) || "not found",
        mismatch: is_map(validation) and column_mismatch?(column, found),
        source: column_source_label(column)
      }
    end)
  end

  defp column_mismatch?(_column, nil), do: true

  defp column_mismatch?(column, found) do
    to_string(value(found, :type)) != to_string(column[:type])
  end

  defp column_type_label(column) do
    "#{column[:type]} · #{requirement_label(column[:nullable?])}"
  end

  defp observed_column_label(found) do
    "#{observed_type(found)} · #{observed_requirement_label(found)}"
  end

  defp requirement_label(true), do: "optional"
  defp requirement_label(_nullable), do: "required"

  defp observed_requirement_label(column) do
    if value(column, :nullability_observed?) in [true, "true"] do
      requirement_label(value(column, :nullable?))
    else
      "requirement not read"
    end
  end

  # The fragment a column came from is context, not the answer, so it reads as a
  # quiet source rather than a badge shouting over the column name.
  defp column_source_label(column) do
    case column[:origin] do
      %{kind: :fragment, module: module} -> "shared: #{inspect(module)}"
      _other -> lineage_source_label(column[:sources])
    end
  end

  defp lineage_source_label([]), do: "this asset"
  defp lineage_source_label(nil), do: "this asset"
  defp lineage_source_label(sources), do: Enum.map_join(sources, ", ", &lineage_label/1)

  defp phase_label(phase) when phase in [:after_materialize, "after_materialize"],
    do: "After writing"

  defp phase_label(phase) when phase in [:before_materialize, "before_materialize"],
    do: "Before writing"

  defp phase_label(nil), do: "-"
  defp phase_label(phase), do: humanize(phase)

  defp violation_label(violation) when violation in [:fail, "fail"], do: "fails the run"
  defp violation_label(violation) when violation in [:warn, "warn"], do: "warns only"

  defp violation_label(violation)
       when violation in [:skip_materialization, "skip_materialization"],
       do: "skips writing"

  defp violation_label(nil), do: nil
  defp violation_label(violation), do: humanize(violation)

  defp metric_value(nil, _keys), do: "-"

  defp metric_value(result, keys) do
    metrics = value(result, :metrics, %{})

    Enum.find_value(keys, "-", fn key ->
      case value(metrics, key) do
        nil -> nil
        found -> format_metric(found)
      end
    end)
  end

  defp format_metric(found) when is_integer(found), do: delimited(found)
  defp format_metric(found) when is_binary(found) or is_atom(found), do: to_string(found)
  defp format_metric(found), do: inspect(found)

  defp delimited(value) do
    value
    |> Integer.to_string()
    |> String.reverse()
    |> String.replace(~r/(\d{3})(?=\d)/, "\\1,")
    |> String.reverse()
  end

  defp checks_summary(rows) do
    passed = Enum.count(rows, &(&1.tone == :success))
    "#{passed} of #{length(rows)} passed"
  end

  defp checks_tone(rows) do
    cond do
      Enum.any?(rows, &(&1.tone == :error)) -> :error
      Enum.any?(rows, &(&1.tone == :warning)) -> :warning
      Enum.all?(rows, &(&1.tone == :success)) -> :success
      true -> :neutral
    end
  end

  defp columns_summary(columns, _mismatched, false), do: "#{length(columns)} promised"
  defp columns_summary(columns, 0, true), do: "#{length(columns)} of #{length(columns)} matched"

  defp columns_summary(columns, mismatched, true),
    do: "#{length(columns) - mismatched} of #{length(columns)} matched"

  defp columns_tone(_mismatched, false), do: :neutral
  defp columns_tone(0, true), do: :success
  defp columns_tone(_mismatched, true), do: :error

  defp quality_tone(status) when status in [:passed, "passed"], do: :success
  defp quality_tone(status) when status in [:warning, "warning"], do: :warning
  defp quality_tone(_status), do: :error

  defp quality_label(status) when status in [:passed, "passed"], do: "All checks passed"
  defp quality_label(status) when status in [:warning, "warning"], do: "Checks warned"
  defp quality_label(_status), do: "Checks failed"

  defp write_label(outcome) when outcome in [:written, "written"], do: "Table written"
  defp write_label(outcome) when outcome in [:no_op, "no_op"], do: "Nothing to write"
  defp write_label(outcome) when outcome in [:rolled_back, "rolled_back"], do: "Rolled back"
  defp write_label(outcome) when outcome in [:not_started, "not_started"], do: "Never started"
  defp write_label(outcome), do: humanize(outcome)

  @doc """
  One run's result for one asset, ordered by how likely each part is to be the answer.

  The outcome comes first, then the failure if there was one, then the contract and
  check evidence, and only then the inputs and metadata. A run that succeeded renders
  short: the sections that hold nothing surprising are `details` the reader opens,
  not panels they scroll past.
  """
  attr :run, :map, required: true, doc: "see `FavnOrchestrator.asset_run_detail/0`"
  attr :asset_id, :string, required: true

  def run_detail_panel(assigns) do
    result = assigns.run[:asset_result]
    meta = (result && result[:meta]) || %{}

    assigns =
      assigns
      |> assign(:failed?, run_failed?(assigns.run))
      |> assign(:result, result)
      |> assign(:meta, meta)
      |> assign(:write, OutputMetadata.outcome(meta, result && result[:status]))
      |> assign(:inputs, List.wrap(assigns.run[:runtime_inputs]))

    ~H"""
    <div class="space-y-6" data-testid="asset-run-detail">
      <.panel padding={:none} class="p-6 sm:p-8">
        <div class="flex flex-wrap items-start justify-between gap-3">
          <div class="min-w-0">
            <p class="text-sm uppercase tracking-[0.18em] favn-text-subtle">Run</p>
            <.mono value={@run.run_id} class="mt-1 block text-base" />
          </div>

          <.status_badge
            tone={LogsViewModel.status_tone(@run.status)}
            label={LogsViewModel.status_label(@run.status)}
          />
        </div>

        <p :if={@write} class={["mt-4 text-lg font-light", Tokens.text_class(@write.tone)]}>
          {@write.headline}
          <span :if={@write.target} class="font-mono text-sm favn-text-subtle">
            → {@write.target}
          </span>
        </p>

        <.fact_list class="mt-6" columns={3} facts={run_facts(@run, @result)} />

        <.notice
          :if={@failed?}
          tone={:error}
          icon="hero-exclamation-triangle"
          class="mt-6"
          data-testid="asset-run-failure"
        >
          <p class="font-medium">{run_error_label(@run, @result)}</p>
          <.link
            navigate={~p"/runs/#{@run.run_id}"}
            class="mt-1 inline-flex text-sm underline decoration-dotted"
          >
            Open the full run
          </.link>
        </.notice>
      </.panel>

      <.assurance_panel :if={@run[:assurance]} assurance={@run.assurance} />

      <.panel :if={@inputs != []} padding={:none} class="p-6 sm:p-8">
        <details>
          <summary class="cursor-pointer text-sm font-medium">
            Resolved inputs <span class="favn-text-subtle">({length(@inputs)})</span>
          </summary>

          <p class="mt-2 max-w-3xl text-sm favn-text-muted">
            Which payload each resolver selected for this run. Values are not shown; a
            resolver can declare its parameters sensitive, so the identity and the
            fingerprint are what can be reported.
          </p>

          <div class="mt-4 space-y-3" data-testid="asset-run-inputs">
            <div
              :for={input <- @inputs}
              class="rounded-box border border-base-content/10 bg-base-content/[0.03] p-4"
            >
              <p class="font-mono text-sm font-semibold">{inspect(input[:resolver])}</p>

              <.field_row label="Input identity">
                <.mono value={to_string(input[:input_identity])} />
              </.field_row>
              <.field_row label="Payload fingerprint">
                <.mono value={to_string(input[:payload_fingerprint])} />
              </.field_row>
              <.field_row :if={input[:source_run_id]} label="Inherited from run">
                <.link navigate={~p"/assets/#{@asset_id}/runs/#{input[:source_run_id]}"}>
                  <.mono value={input[:source_run_id]} />
                </.link>
              </.field_row>
            </div>
          </div>
        </details>
      </.panel>

      <OutputMetadata.output_metadata
        :if={@meta != %{}}
        id={"asset-run-metadata-#{@run.run_id}"}
        metadata={@meta}
        status={@result && @result[:status]}
      />
    </div>
    """
  end

  defp run_facts(run, result) do
    [
      %{label: "Started", value: LogsViewModel.timestamp_label(run[:started_at])},
      %{label: "Duration", value: duration_fact(run[:duration_ms])},
      %{label: "Window", value: run_window_label(run[:window])},
      %{label: "Trigger", value: LogsViewModel.trigger_label(run[:submit_kind]) || "Unknown"},
      %{label: "Attempts", value: attempts_label(result)}
    ]
  end

  defp run_failed?(run) do
    LogsViewModel.status_tone(run[:status]) == :error or
      (run[:asset_result] && LogsViewModel.status_tone(run.asset_result[:status]) == :error)
  end

  defp run_error_label(run, result) do
    error = (result && result[:error]) || run[:error]

    case error do
      nil -> "This run failed after its asset step finished."
      %{message: message} when is_binary(message) -> message
      error when is_binary(error) -> error
      error -> inspect(error)
    end
  end

  defp duration_fact(nil), do: "Not finished"
  defp duration_fact(ms) when ms < 1_000, do: "#{ms} ms"

  defp duration_fact(ms) when ms < 60_000,
    do: "#{:erlang.float_to_binary(ms / 1_000, decimals: 1)} s"

  defp duration_fact(ms), do: "#{div(ms, 60_000)}m #{rem(div(ms, 1_000), 60)}s"

  defp run_window_label(%{label: label}) when is_binary(label), do: label
  defp run_window_label(%{value: value}) when is_binary(value), do: value
  defp run_window_label(_window), do: "Full refresh"

  defp attempts_label(%{attempt_count: count, max_attempts: max})
       when is_integer(count) and is_integer(max),
       do: "#{count} of #{max}"

  defp attempts_label(%{attempt_count: count}) when is_integer(count), do: to_string(count)
  defp attempts_label(_result), do: "Not reported"

  attr :window, :map, required: true
  attr :selected, :boolean, default: false
  attr :selectable?, :boolean, default: true

  def timeline_window(assigns) do
    ~H"""
    <div class="flex flex-col items-center gap-4">
      <button
        type="button"
        phx-click={@selectable? && "select_window"}
        phx-value-window-id={@window.id}
        disabled={!@selectable?}
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
        "text-center text-sm leading-tight favn-text-muted",
        @selected && "text-primary"
      ]}>
        <div class="max-w-16 text-balance">{@window.label}</div>
      </div>
      <span :if={@selected} class="status status-primary favn-status-glow"></span>
    </div>
    """
  end

  def sample_nav_items, do: Navigation.items(:assets)

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

  @doc """
  Returns the rail destinations for one asset, newest question first.

  Coverage answers whether data exists for every period the asset is expected to
  cover, which is only a question for an asset that runs per window. A full-refresh
  asset replaces its whole relation on every run, so the rail leaves the destination
  out rather than offering a page with nothing to say.
  """
  @spec detail_modes(String.t(), boolean()) :: [map()]
  def detail_modes(asset_id, has_data_windows?) do
    [
      %{
        id: :overview,
        label: "Overview",
        icon: "hero-squares-2x2",
        patch: ~p"/assets/#{asset_id}"
      },
      %{id: :runs, label: "Runs", icon: "hero-clock", patch: ~p"/assets/#{asset_id}/runs"},
      has_data_windows? &&
        %{
          id: :coverage,
          label: "Coverage",
          icon: "hero-calendar-days",
          patch: ~p"/assets/#{asset_id}/coverage"
        },
      %{
        id: :diagnostics,
        label: "Diagnostics",
        icon: "hero-wrench-screwdriver",
        patch: ~p"/assets/#{asset_id}/diagnostics"
      }
    ]
    |> Enum.filter(& &1)
  end

  defp observed_by_name(%{observed_columns: columns}) when is_list(columns),
    do: Map.new(columns, &{to_string(value(&1, :name)), &1})

  defp observed_by_name(_validation), do: %{}

  defp grain_label(nil), do: "Not declared"

  defp grain_label(%{by: [], description: description}), do: description || "Descriptive grain"

  defp grain_label(%{by: columns, description: description}) do
    names = Enum.map_join(columns, ", ", &to_string/1)
    if description, do: "#{names} · #{description}", else: names
  end

  defp unique_keys_label([]), do: "None"

  defp unique_keys_label(keys),
    do: Enum.map_join(keys, " · ", &Enum.map_join(&1, ", ", fn name -> to_string(name) end))

  defp row_count_constraint_label(%{equals: %{source: :param, name: name}}),
    do: "Exactly @#{name}"

  defp row_count_constraint_label(%{equals: %{source: :literal, value: value}}),
    do: "Exactly #{value}"

  defp row_count_constraint_label(%{min: min, max: max})
       when is_integer(min) and is_integer(max),
       do: "Between #{min} and #{max}"

  defp row_count_constraint_label(%{min: min}) when is_integer(min), do: "At least #{min}"
  defp row_count_constraint_label(%{max: max}) when is_integer(max), do: "At most #{max}"
  defp row_count_constraint_label(_row_count), do: "Constraint unavailable"

  defp observed_type(column),
    do: value(column, :native_type) || value(column, :type) || "unknown"

  defp lineage_label(%{kind: :asset, asset_ref: {module, name}, column: column}),
    do: "#{inspect(module)}.#{name}.#{column}"

  defp lineage_label(%{kind: :external, dataset: dataset, column: column}),
    do: "#{dataset}.#{column}"

  defp lineage_label(source), do: inspect(source)

  defp difference_label(difference) do
    kind = difference |> value(:kind) |> humanize()
    column = value(difference, :column)
    expected = value(difference, :expected)
    observed = value(difference, :observed)

    [
      kind,
      column && to_string(column),
      expected && "expected #{inspect(expected)}",
      observed && "observed #{inspect(observed)}"
    ]
    |> Enum.reject(&is_nil/1)
    |> Enum.join(" · ")
  end

  defp check_result_label(nil), do: "not run"

  defp check_result_label(result) do
    case value(result, :outcome) do
      outcome when outcome in [:passed, "passed"] ->
        "passed"

      outcome when outcome in [:warned, "warned"] ->
        "warned"

      outcome when outcome in [:failed, "failed"] ->
        "failed"

      outcome when outcome in [:errored, "errored"] ->
        "could not run"

      outcome when outcome in [:not_run, "not_run"] ->
        "not run"

      # The check's own condition was not met, so there was nothing to check. That is
      # not a pass and not a failure, and "condition skipped" said neither.
      outcome when outcome in [:condition_skipped, "condition_skipped"] ->
        "not needed"

      outcome when outcome in [:materialization_skipped, "materialization_skipped"] ->
        "blocked the write"

      outcome ->
        humanize(outcome)
    end
  end

  # A skipped check is not a pass. Both used to render green, so a run whose checks
  # never executed looked exactly like a run whose checks all held.
  defp check_result_tone(nil), do: :neutral

  defp check_result_tone(result) do
    case value(result, :outcome) do
      outcome when outcome in [:passed, "passed"] ->
        :success

      outcome
      when outcome in [
             :warned,
             "warned",
             :materialization_skipped,
             "materialization_skipped"
           ] ->
        :warning

      outcome
      when outcome in [
             :not_run,
             "not_run",
             :condition_skipped,
             "condition_skipped"
           ] ->
        :neutral

      _outcome ->
        :error
    end
  end

  defp humanize(nil), do: "unknown"

  defp humanize(value) do
    value
    |> to_string()
    |> String.replace("_", " ")
  end

  defp coverage_badge_class(:complete), do: "badge badge-success badge-soft badge-sm"
  defp coverage_badge_class(:incomplete), do: "badge badge-warning badge-soft badge-sm"
  defp coverage_badge_class(_status), do: "badge badge-neutral badge-soft badge-sm"

  defp coverage_status_label(:complete), do: "Complete"
  defp coverage_status_label(:incomplete), do: "Incomplete"
  defp coverage_status_label(_status), do: "Unknown"

  defp coverage_explanation(coverage) do
    case field(coverage, :status) do
      :complete ->
        "Every window expected at this evaluation time has successful evidence."

      :incomplete ->
        "Some expected windows do not have successful evidence in the active generation."

      :unknown ->
        "Coverage is unavailable: #{humanize(field(coverage, :unknown_reason))}."

      _other ->
        "Coverage is unavailable."
    end
  end

  defp coverage_time(%DateTime{} = value),
    do: Calendar.strftime(value, "%b %-d, %Y %H:%M %Z")

  defp coverage_time(value) when is_binary(value), do: value
  defp coverage_time(_value), do: "-"

  defp coverage_window_label(nil), do: "No windows expected yet"

  defp coverage_window_label(window) do
    case field(window, :start_at) do
      %DateTime{} = start_at -> coverage_time(start_at)
      value when is_binary(value) -> value
      _other -> "-"
    end
  end

  defp coverage_generation_label(value) when is_binary(value), do: value
  defp coverage_generation_label(_value), do: "No persisted generation"

  defp compatibility_panel_class(status)
       when status in [:rebuild_required, :unexpected_drift, :operator_decision],
       do: "border-error/35 bg-error/5"

  defp compatibility_panel_class(:rebuild_available), do: "border-info/30 bg-info/5"
  defp compatibility_panel_class(_status), do: "border-base-content/10"

  defp compatibility_badge_class(status)
       when status in [:rebuild_required, :unexpected_drift, :operator_decision],
       do: "badge badge-error badge-soft badge-sm"

  defp compatibility_badge_class(:rebuild_available),
    do: "badge badge-info badge-soft badge-sm"

  defp compatibility_badge_class(:ready), do: "badge badge-success badge-soft badge-sm"
  defp compatibility_badge_class(_status), do: "badge badge-neutral badge-soft badge-sm"

  defp compatibility_status_label(:ready), do: "Compatible"
  defp compatibility_status_label(:uninitialized), do: "Not initialized"
  defp compatibility_status_label(:rebuild_available), do: "Rebuild available"
  defp compatibility_status_label(:rebuild_required), do: "Rebuild required"
  defp compatibility_status_label(:unexpected_drift), do: "Target drift"
  defp compatibility_status_label(:operator_decision), do: "Operator decision"
  defp compatibility_status_label(_status), do: "Unknown"

  defp compatibility_explanation(compatibility) do
    case field(compatibility, :status) do
      :ready -> "The desired descriptor and active physical target are compatible."
      :uninitialized -> "The first successful materialization will initialize this target."
      :rebuild_available -> "Transformation semantics changed; ordinary writes remain allowed."
      :rebuild_required -> "The desired target is incompatible with the active generation."
      :unexpected_drift -> "The physical target changed outside the recorded generation."
      :operator_decision -> "Favn cannot prove target ownership or safe compatibility."
      _other -> "Target compatibility is unavailable."
    end
  end

  defp compatibility_diff_entries(diff) when is_map(diff) do
    diff
    |> Enum.sort_by(fn {name, _change} -> to_string(name) end)
    |> Enum.take(50)
  end

  defp compatibility_diff_entries(_diff), do: []

  defp compatibility_change_label(change) when is_map(change) do
    previous = field(change, :previous, field(change, :active))
    desired = field(change, :desired, field(change, :observed))

    cond do
      !is_nil(previous) or !is_nil(desired) ->
        "#{bounded_value(previous)} → #{bounded_value(desired)}"

      true ->
        bounded_value(change)
    end
  end

  defp compatibility_change_label(change), do: bounded_value(change)

  defp bounded_value(nil), do: "-"
  defp bounded_value(value) when is_binary(value), do: String.slice(value, 0, 200)
  defp bounded_value(value) when is_atom(value) or is_number(value), do: to_string(value)
  defp bounded_value(value), do: inspect(value, limit: 10, printable_limit: 200)

  defp availability_label(0), do: "Available at the window boundary"

  defp availability_label(seconds) when is_integer(seconds) and rem(seconds, 3_600) == 0,
    do: "Expected #{div(seconds, 3_600)} hours after the window closes"

  defp availability_label(seconds) when is_integer(seconds),
    do: "Expected #{seconds} seconds after the window closes"

  defp availability_label(_value), do: "Availability unknown"

  defp field(value, key, default \\ nil)

  defp field(value, key, default) when is_map(value),
    do: Map.get(value, key, Map.get(value, Atom.to_string(key), default))

  defp field(_value, _key, default), do: default

  defp value(map, key, default \\ nil) when is_map(map),
    do: Map.get(map, key, Map.get(map, Atom.to_string(key), default))

  defp selected_window?(nil, _window), do: false
  defp selected_window?(selected_window, window), do: selected_window.id == window.id

  defp selected_run_context?(%{id: id}, %{id: id}), do: true
  defp selected_run_context?(_selected, _context), do: false

  defp run_context_policy_label(%{policy: %{kind: kind, anchor: anchor}, timezone: timezone}) do
    "#{humanize(kind)} / #{humanize(anchor)} / #{timezone}"
  end

  defp run_context_policy_label(%{timezone: timezone}), do: timezone

  # A single toggle is not a choice, so a page scoped to one timeline shows none.
  # Testids stay literal rather than derived from the id, because they are the
  # contract the tests and the browser checks address these buttons by.
  defp timeline_toggles(assigns) do
    [
      %{id: :refresh, label: "Run", testid: "refresh-timeline-toggle", available?: true},
      %{
        id: :freshness,
        label: "Freshness",
        testid: "freshness-timeline-toggle",
        available?: assigns.has_freshness_timeline?
      },
      %{
        id: :data_coverage,
        label: "Data",
        testid: "data-coverage-timeline-toggle",
        available?: assigns.has_data_windows?
      }
    ]
    |> Enum.filter(&(&1.available? and &1.id in assigns.timelines))
    |> Enum.map(&Map.delete(&1, :available?))
  end

  # The selection lives on the LiveView and is shared by every page, so a page that
  # does not offer the selected timeline falls back to its own first one rather than
  # rendering a strip its toggle cannot reach.
  defp resolved_timeline(%{active_timeline: active, toggles: toggles}) do
    if Enum.any?(toggles, &(&1.id == active)) do
      active
    else
      case toggles do
        [%{id: id} | _rest] -> id
        [] -> active
      end
    end
  end

  defp active_timeline(%{active_timeline: :data_coverage, data_coverage_timeline: timeline})
       when is_list(timeline), do: timeline

  defp active_timeline(%{active_timeline: :freshness, freshness_timeline: timeline})
       when is_list(timeline), do: timeline

  defp active_timeline(%{refresh_timeline: timeline}), do: timeline

  defp active_timeline_range(%{
         active_timeline: :data_coverage,
         data_coverage_window_range: range
       }),
       do: range

  defp active_timeline_range(%{
         active_timeline: :freshness,
         freshness_window_range: range
       }),
       do: range

  defp active_timeline_range(%{refresh_window_range: range}), do: range

  defp active_timeline_label(%{active_timeline: :data_coverage}), do: "Data coverage timeline"
  defp active_timeline_label(%{active_timeline: :freshness}), do: "Freshness timeline"
  defp active_timeline_label(_assigns), do: "Run anchor timeline"

  defp active_timeline_kind_label(%{
         active_timeline: :data_coverage,
         data_coverage_timeline_label: label
       }),
       do: label

  defp active_timeline_kind_label(%{
         active_timeline: :freshness,
         freshness_cadence_label: label
       }),
       do: label

  defp active_timeline_kind_label(%{refresh_cadence_label: label}), do: label

  defp sample_window(%{month: month, day: day} = window) do
    year = if month == "May", do: 2026, else: 2026
    date_label = "#{month} #{day}, #{year}"

    window
    |> Map.put(:id, "#{String.downcase(month)}-#{day}-#{year}")
    |> Map.put(:label, day)
    |> Map.put(:date_label, date_label)
    |> Map.put(:range_label, date_label)
    |> Map.put(:run_enabled?, true)
    |> Map.put(:run_disabled_reason, nil)
    |> Map.put(:run_label, "Run this window")
  end

  # A window's border is the boundary that says which state it is in, so it owes
  # 3:1 against the wash behind it, in both themes. The opacities are measured,
  # not chosen, and the light theme sets them: its tone colours sit much closer in
  # luminance to a pale wash than the dark theme's do. At the values these
  # replaced, all four measured under 2.6.
  defp timeline_window_class(%{status: :success}) do
    "border-success/90 bg-success/15 text-success"
  end

  defp timeline_window_class(%{status: :warning}) do
    "border-warning/90 bg-warning/15 text-warning"
  end

  defp timeline_window_class(%{status: :error}) do
    "border-error/80 bg-error/15 text-error"
  end

  defp timeline_window_class(%{status: :muted}) do
    "border-base-content/60 bg-base-content/10 favn-text-subtle"
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
