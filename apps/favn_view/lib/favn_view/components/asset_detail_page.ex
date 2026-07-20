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
  attr :timeline, :list, default: []
  attr :refresh_timeline, :list, default: nil
  attr :freshness_timeline, :list, default: nil
  attr :data_coverage_timeline, :list, default: nil
  attr :active_mode, :atom, default: :timeline
  attr :freshness, :map, default: nil
  attr :assurance, :map, default: nil
  attr :selected_window, :map, default: nil
  attr :run_config_open?, :boolean, default: false
  attr :run_config, :map, default: %{dependencies: "all", refresh: "auto"}
  attr :run_config_valid?, :boolean, default: true
  attr :submitting_window_run?, :boolean, default: false
  attr :selected_window_error, :string, default: nil
  attr :submitted_run_id, :string, default: nil
  attr :can_submit_runs?, :boolean, default: false

  def asset_detail_page(assigns) do
    assigns = assign(assigns, :refresh_timeline, assigns.refresh_timeline || assigns.timeline)

    ~H"""
    <AppShell.app_shell
      title={@title}
      status={@status}
      status_tone={@status_tone}
      nav_items={@nav_items}
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
        assurance={@assurance}
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
        <ModeRail.mode_rail active={@active_mode} modes={detail_modes()} on_select="set_mode" />
      </:mode_rail>
    </AppShell.app_shell>
    """
  end

  attr :active_mode, :atom, required: true
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
  attr :assurance, :map, default: nil
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
    <.window_timeline_panel
      :if={@active_mode == :timeline}
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
    />

    <.placeholder_panel :if={@active_mode == :runs} title="Runs coming soon" />
    <.placeholder_panel :if={@active_mode == :lineage} title="Lineage coming soon" />
    <.placeholder_panel :if={@active_mode == :docs} title="Docs coming soon" />
    <.placeholder_panel :if={@active_mode == :code} title="Code coming soon" />
    <div :if={@active_mode == :details} class="mx-auto w-full max-w-6xl space-y-6">
      <.assurance_panel :if={@assurance} assurance={@assurance} />
      <.freshness_detail_panel freshness={@freshness} />
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

  def window_timeline_panel(assigns) do
    assigns = assign(assigns, :timeline, active_timeline(assigns))
    assigns = assign(assigns, :timeline_range, active_timeline_range(assigns))
    assigns = assign(assigns, :timeline_label, active_timeline_label(assigns))
    assigns = assign(assigns, :timeline_kind_label, active_timeline_kind_label(assigns))

    ~H"""
    <GlassPanel.glass_panel
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
            <p class="mt-2 text-sm text-base-content/60">{@timeline_kind_label}</p>
          </div>

          <div class="join self-start text-sm text-base-content/70">
            <button
              :if={@has_data_windows? or @has_freshness_timeline?}
              type="button"
              class={[
                "btn btn-sm join-item",
                @active_timeline == :refresh && "btn-primary btn-soft",
                @active_timeline != :refresh && "btn-ghost"
              ]}
              phx-click="set_timeline"
              phx-value-timeline="refresh"
              data-testid="refresh-timeline-toggle"
            >
              Run
            </button>
            <button
              :if={@has_freshness_timeline?}
              type="button"
              class={[
                "btn btn-sm join-item",
                @active_timeline == :freshness && "btn-primary btn-soft",
                @active_timeline != :freshness && "btn-ghost"
              ]}
              phx-click="set_timeline"
              phx-value-timeline="freshness"
              data-testid="freshness-timeline-toggle"
            >
              Freshness
            </button>
            <button
              :if={@has_data_windows?}
              type="button"
              class={[
                "btn btn-sm join-item",
                @active_timeline == :data_coverage && "btn-primary btn-soft",
                @active_timeline != :data_coverage && "btn-ghost"
              ]}
              phx-click="set_timeline"
              phx-value-timeline="data_coverage"
              data-testid="data-coverage-timeline-toggle"
            >
              Data
            </button>
            <button
              type="button"
              class="btn btn-ghost btn-sm join-item"
              aria-label="Previous window range"
            >
              <.icon name="hero-chevron-left" class="size-4" />
            </button>
            <span class="btn btn-ghost btn-sm join-item pointer-events-none normal-case">
              {@timeline_range}
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
        />
      </div>
    </GlassPanel.glass_panel>
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
          <p class="text-xs uppercase tracking-[0.18em] text-base-content/45">Run context</p>
          <p class="mt-1 text-sm text-base-content/70">
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
            <span class="text-xs opacity-60">{run_context_policy_label(context)}</span>
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

  attr :assurance, :map, required: true

  def assurance_panel(assigns) do
    assigns =
      assigns
      |> assign(:contract, assigns.assurance[:contract])
      |> assign(:checks, assigns.assurance[:checks] || [])
      |> assign(:validation, assigns.assurance[:contract_validation])
      |> assign(:observed_by_name, observed_by_name(assigns.assurance[:contract_validation]))

    ~H"""
    <GlassPanel.glass_panel class="p-6 sm:p-8" data-testid="asset-assurance-panel">
      <div class="flex flex-col gap-4 sm:flex-row sm:items-start sm:justify-between">
        <div>
          <p class="text-xs uppercase tracking-[0.18em] text-base-content/45">Data assurance</p>
          <h2 class="mt-1 text-xl font-medium tracking-tight">Contract and quality checks</h2>
          <p class="mt-2 max-w-3xl text-sm text-base-content/60">
            Authored expectations, observed candidate evidence, and the latest generated and custom check outcomes.
          </p>
        </div>
        <div class="flex flex-wrap gap-2">
          <span
            :if={@assurance[:quality_status]}
            class={assurance_status_badge(@assurance[:quality_status])}
          >
            Quality {humanize(@assurance[:quality_status])}
          </span>
          <span :if={@assurance[:write_outcome]} class="badge badge-outline badge-sm">
            Write {humanize(@assurance[:write_outcome])}
          </span>
        </div>
      </div>

      <section :if={@contract} class="mt-8 space-y-5" data-testid="asset-output-contract">
        <div class="grid gap-3 md:grid-cols-3">
          <.assurance_fact label="Grain" value={grain_label(@contract[:grain])} />
          <.assurance_fact label="Unique keys" value={unique_keys_label(@contract[:unique_keys])} />
          <.assurance_fact
            label="Row count claims"
            value={row_counts_label(@contract[:row_counts])}
          />
        </div>

        <div
          :if={List.wrap(@contract[:row_counts]) != []}
          class="grid gap-3 lg:grid-cols-2"
          data-testid="contract-row-count-claims"
        >
          <article
            :for={{row_count, index} <- Enum.with_index(List.wrap(@contract[:row_counts]), 1)}
            class="rounded-box border border-base-content/10 bg-base-content/[0.03] p-4"
            data-testid="contract-row-count-claim"
            data-claim-id={row_count[:claim_id]}
          >
            <div class="flex flex-wrap items-start justify-between gap-2">
              <div>
                <p class="text-xs uppercase tracking-[0.14em] text-base-content/40">
                  Row count claim {index}
                </p>
                <p class="mt-1 text-sm font-medium">{row_count_constraint_label(row_count)}</p>
                <p class="mt-1 font-mono text-[0.7rem] text-base-content/45">
                  {row_count[:claim_id]}
                </p>
              </div>
              <span class={check_result_badge(row_count[:latest_result])}>
                {check_result_label(row_count[:latest_result])}
              </span>
            </div>
            <p class="mt-2 text-xs text-base-content/60">
              On violation {humanize(row_count[:on_violation])}
              <span :if={row_count[:when]}> · when {humanize(row_count[:when])}</span>
            </p>
          </article>
        </div>

        <div
          :if={List.wrap(@contract[:compositions]) != []}
          class="flex flex-wrap items-center gap-2 text-xs"
          data-testid="contract-compositions"
        >
          <span class="text-base-content/45">Composed fragments</span>
          <span
            :for={composition <- List.wrap(@contract[:compositions])}
            class="badge badge-outline badge-sm font-mono"
          >
            {inspect(composition[:module])}
          </span>
        </div>

        <div class="overflow-x-auto rounded-box border border-base-content/10">
          <table class="table table-sm min-w-[52rem]">
            <thead>
              <tr>
                <th>Column</th>
                <th>Expected</th>
                <th>Observed</th>
                <th>Lineage</th>
              </tr>
            </thead>
            <tbody>
              <tr :for={column <- @contract[:columns]} data-testid="contract-column">
                <td>
                  <p class="font-mono text-xs font-semibold">{column[:name]}</p>
                  <span
                    :if={column[:origin]}
                    class="mt-1 inline-flex max-w-xs break-all rounded border border-base-content/10 px-1.5 py-0.5 font-mono text-[0.65rem] text-base-content/50"
                    data-testid="contract-column-origin"
                    data-origin={column[:origin][:kind]}
                  >
                    {column_origin_label(column[:origin])}
                  </span>
                  <p :if={column[:description]} class="mt-1 max-w-xs text-xs text-base-content/55">
                    {column[:description]}
                  </p>
                  <div :if={column[:tags] != []} class="mt-1 flex flex-wrap gap-1">
                    <span :for={tag <- column[:tags]} class="badge badge-ghost badge-xs">{tag}</span>
                  </div>
                </td>
                <td class="text-xs">
                  <span class="font-mono">{column[:type]}</span>
                  <span class="text-base-content/45"> · {nullability_label(column[:nullable?])}</span>
                </td>
                <td class="text-xs">
                  <span :if={@observed_by_name[to_string(column[:name])]}>
                    <span class="font-mono">
                      {observed_type(@observed_by_name[to_string(column[:name])])}
                    </span>
                    <span class="text-base-content/45">
                      · {observed_nullability(@observed_by_name[to_string(column[:name])])}
                    </span>
                  </span>
                  <span
                    :if={!@observed_by_name[to_string(column[:name])]}
                    class="text-base-content/40"
                  >
                    Not observed
                  </span>
                </td>
                <td class="text-xs">
                  <div :if={column[:sources] != []} class="space-y-1">
                    <p :for={source <- column[:sources]} class="font-mono text-[0.7rem]">
                      {lineage_label(source)}
                    </p>
                    <span :if={column[:via]} class="badge badge-outline badge-xs">
                      {column[:via]}
                    </span>
                  </div>
                  <span :if={column[:sources] == []} class="text-base-content/40">Not declared</span>
                </td>
              </tr>
            </tbody>
          </table>
        </div>

        <div
          :if={@validation && @validation[:differences] != []}
          class="rounded-box border border-error/25 bg-error/10 p-4"
          data-testid="contract-schema-differences"
        >
          <h3 class="text-sm font-medium text-error">Schema differences</h3>
          <ul class="mt-2 space-y-1 text-xs text-base-content/70">
            <li :for={difference <- @validation[:differences]}>{difference_label(difference)}</li>
          </ul>
        </div>

        <p
          :if={@validation && @validation[:observed_truncated?]}
          class="text-xs text-warning"
        >
          Candidate schema evidence is bounded to the first {length(@validation[:observed_columns])} of {@validation[
            :observed_column_count
          ]} columns.
        </p>
      </section>

      <section :if={@checks != []} class="mt-8" data-testid="asset-quality-checks">
        <div class="flex items-center justify-between gap-3">
          <h3 class="text-sm font-medium">Checks</h3>
          <span :if={@assurance[:latest_run_id]} class="font-mono text-xs text-base-content/45">
            {@assurance[:latest_run_id]}
          </span>
        </div>

        <div class="mt-3 grid gap-3 lg:grid-cols-2">
          <article
            :for={check <- @checks}
            class="rounded-box border border-base-content/10 bg-base-content/[0.03] p-4"
            data-testid="asset-quality-check"
            data-check-origin={check[:origin]}
          >
            <div class="flex flex-wrap items-start justify-between gap-2">
              <div>
                <div class="flex flex-wrap items-center gap-2">
                  <span class={origin_badge(check[:origin])}>{origin_label(check[:origin])}</span>
                  <p class="font-mono text-xs font-semibold">{check[:name]}</p>
                </div>
                <p :if={check[:claim_id]} class="mt-1 font-mono text-[0.7rem] text-base-content/45">
                  {check[:claim_id]}
                </p>
              </div>
              <span class={check_result_badge(check[:latest_result])}>
                {check_result_label(check[:latest_result])}
              </span>
            </div>

            <p class="mt-2 text-xs text-base-content/60">
              {humanize(check[:phase])} · on violation {humanize(check[:on_violation])}
              <span :if={check[:when]}> · when {humanize(check[:when])}</span>
            </p>
            <p :if={check[:message]} class="mt-2 text-xs text-base-content/75">{check[:message]}</p>

            <dl :if={check_metrics(check) != []} class="mt-3 grid gap-1 text-xs">
              <div :for={{key, value} <- check_metrics(check)} class="flex justify-between gap-3">
                <dt class="text-base-content/50">{key}</dt>
                <dd class="font-mono">{value}</dd>
              </div>
            </dl>
          </article>
        </div>
      </section>
    </GlassPanel.glass_panel>
    """
  end

  attr :label, :string, required: true
  attr :value, :string, required: true

  defp assurance_fact(assigns) do
    ~H"""
    <div class="rounded-box border border-base-content/10 bg-base-content/[0.03] p-3">
      <p class="text-xs uppercase tracking-[0.14em] text-base-content/40">{@label}</p>
      <p class="mt-1 text-sm text-base-content/75">{@value}</p>
    </div>
    """
  end

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
        "text-center text-xs leading-tight text-base-content/60",
        @selected && "text-primary"
      ]}>
        <div class="max-w-16 text-balance">{@window.label}</div>
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

  defp row_counts_label([]), do: "Not declared"
  defp row_counts_label(nil), do: "Not declared"

  defp row_counts_label([_row_count]), do: "1 ordered claim"
  defp row_counts_label(row_counts), do: "#{length(row_counts)} ordered claims"

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

  defp nullability_label(true), do: "nullable"
  defp nullability_label(false), do: "required"

  defp column_origin_label(%{kind: :fragment, module: module}), do: inspect(module)
  defp column_origin_label(%{kind: :local}), do: "Local"
  defp column_origin_label(origin), do: inspect(origin)

  defp observed_type(column),
    do: value(column, :native_type) || value(column, :type) || "unknown"

  defp observed_nullability(column) do
    if value(column, :nullability_observed?) in [true, "true"] do
      case value(column, :nullable?) do
        true -> "nullable"
        false -> "required"
        _other -> "nullability unknown"
      end
    else
      "nullability unverified"
    end
  end

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

  defp assurance_status_badge(status) when status in [:passed, "passed"],
    do: "badge badge-success badge-soft badge-sm"

  defp assurance_status_badge(status) when status in [:warning, "warning"],
    do: "badge badge-warning badge-soft badge-sm"

  defp assurance_status_badge(_status), do: "badge badge-error badge-soft badge-sm"

  defp origin_badge(:contract), do: "badge badge-info badge-soft badge-xs"
  defp origin_badge(_origin), do: "badge badge-ghost badge-xs"
  defp origin_label(:contract), do: "Contract"
  defp origin_label(_origin), do: "Custom"

  defp check_result_label(nil), do: "Not run"
  defp check_result_label(result), do: result |> value(:outcome) |> humanize()

  defp check_result_badge(nil), do: "badge badge-ghost badge-sm"

  defp check_result_badge(result) do
    case value(result, :outcome) do
      outcome when outcome in [:passed, "passed", :condition_skipped, "condition_skipped"] ->
        "badge badge-success badge-soft badge-sm"

      outcome
      when outcome in [
             :warned,
             "warned",
             :materialization_skipped,
             "materialization_skipped"
           ] ->
        "badge badge-warning badge-soft badge-sm"

      outcome when outcome in [:not_run, "not_run"] ->
        "badge badge-ghost badge-sm"

      _outcome ->
        "badge badge-error badge-soft badge-sm"
    end
  end

  defp check_metrics(%{latest_result: result}) when is_map(result) do
    result
    |> value(:metrics, %{})
    |> Enum.map(fn {key, metric_value} -> {to_string(key), inspect(metric_value)} end)
    |> Enum.sort_by(&elem(&1, 0))
  end

  defp check_metrics(_check), do: []

  defp humanize(nil), do: "unknown"

  defp humanize(value) do
    value
    |> to_string()
    |> String.replace("_", " ")
  end

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
