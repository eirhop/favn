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
  alias FavnView.Components.RunConfigDialog
  alias FavnView.CoverageCalendar
  alias FavnView.LogsViewModel
  alias FavnView.UI.Typography

  attr :title, :string, required: true
  attr :status, :string, required: true
  attr :status_tone, :atom, default: :success

  attr :has_data_windows?, :boolean,
    default: false,
    doc: "whether the asset runs per window; decides if the rail offers Coverage"

  attr :can_run_asset?, :boolean, default: true
  attr :run_contexts, :list, default: []
  attr :selected_run_context, :map, default: nil
  attr :run_context_status, :atom, default: :unavailable
  attr :nav_items, :list, required: true
  attr :current_scope, :any, default: nil
  attr :operator_workspaces, :list, default: []
  attr :active_mode, :atom, default: :overview
  attr :freshness, :map, default: nil
  attr :coverage, :any, default: nil
  attr :coverage_policy, :map, default: nil

  attr :coverage_calendar, :map,
    default: %{
      layout: :empty,
      kind: nil,
      timezone: nil,
      unit_label: nil,
      blanks: 0,
      columns: 7,
      column_labels: [],
      cells: [],
      period_count: 0,
      missing_count: 0,
      selected_count: 0
    },
    doc: "a `FavnView.CoverageCalendar.build/1` result"

  attr :coverage_navigation, :map,
    default: %{previous: nil, next: nil, jumps: []},
    doc: "a `FavnView.CoverageCalendar.navigation/1` result"

  attr :compatibility, :map, default: nil
  attr :rebuild_target_id, :string, default: nil
  attr :manifest_version_id, :string, default: nil
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

  attr :documentation, :any,
    default: nil,
    doc: "`{:ok, docs}` or `{:error, reason}`; nil until the documentation page opens"

  attr :coverage_plan, :map, default: nil
  attr :coverage_action_error, :string, default: nil
  attr :planning_coverage?, :boolean, default: false
  attr :submitting_coverage?, :boolean, default: false
  attr :run_config_open?, :boolean, default: false
  attr :run_config_advanced_open?, :boolean, default: false
  attr :run_config, :map, default: %{dependencies: "all", refresh: "auto"}
  attr :run_config_valid?, :boolean, default: true
  attr :submitting_window_run?, :boolean, default: false
  attr :run_error, :string, default: nil, doc: "why the last run submission was refused"
  attr :can_submit_runs?, :boolean, default: false
  attr :flash, :map, default: %{}

  def asset_detail_page(assigns) do
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
        timezone={@current_scope}
        active_mode={@active_mode}
        can_run_asset?={@can_run_asset?}
        run_contexts={@run_contexts}
        selected_run_context={@selected_run_context}
        run_context_status={@run_context_status}
        freshness={@freshness}
        coverage={@coverage}
        coverage_policy={@coverage_policy}
        coverage_calendar={@coverage_calendar}
        coverage_navigation={@coverage_navigation}
        compatibility={@compatibility}
        rebuild_target_id={@rebuild_target_id}
        manifest_version_id={@manifest_version_id}
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
        documentation={@documentation}
        title={@title}
        coverage_plan={@coverage_plan}
        coverage_action_error={@coverage_action_error}
        planning_coverage?={@planning_coverage?}
        submitting_coverage?={@submitting_coverage?}
        submitting_window_run?={@submitting_window_run?}
        can_submit_runs?={@can_submit_runs?}
      />
      <:mode_rail>
        <ModeRail.mode_rail
          active={@active_mode}
          modes={detail_modes(@asset_id, @has_data_windows?)}
        />
      </:mode_rail>

      <:overlay>
        <RunConfigDialog.run_config_dialog
          :if={@run_config_open?}
          has_data_windows?={@has_data_windows?}
          advanced_open?={@run_config_advanced_open?}
          run_config={@run_config}
          run_config_valid?={@run_config_valid?}
          submitting_window_run?={@submitting_window_run?}
          error={@run_error}
          can_submit_runs?={@can_submit_runs?}
          command_resource={@rebuild_target_id}
        />
      </:overlay>
    </AppShell.app_shell>
    """
  end

  attr :active_mode, :atom, required: true
  attr :timezone, :any, required: true
  attr :title, :string, required: true
  attr :can_run_asset?, :boolean, default: true
  attr :run_contexts, :list, default: []
  attr :selected_run_context, :map, default: nil
  attr :run_context_status, :atom, default: :unavailable
  attr :freshness, :map, default: nil
  attr :coverage, :any, default: nil
  attr :coverage_policy, :map, default: nil

  attr :coverage_calendar, :map,
    default: %{
      layout: :empty,
      kind: nil,
      timezone: nil,
      unit_label: nil,
      blanks: 0,
      columns: 7,
      column_labels: [],
      cells: [],
      period_count: 0,
      missing_count: 0,
      selected_count: 0
    },
    doc: "a `FavnView.CoverageCalendar.build/1` result"

  attr :coverage_navigation, :map,
    default: %{previous: nil, next: nil, jumps: []},
    doc: "a `FavnView.CoverageCalendar.navigation/1` result"

  attr :compatibility, :map, default: nil
  attr :rebuild_target_id, :string, default: nil
  attr :manifest_version_id, :string, default: nil
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

  attr :documentation, :any,
    default: nil,
    doc: "`{:ok, docs}` or `{:error, reason}`; nil until the documentation page opens"

  attr :coverage_plan, :map, default: nil
  attr :coverage_action_error, :string, default: nil
  attr :planning_coverage?, :boolean, default: false
  attr :submitting_coverage?, :boolean, default: false
  attr :submitting_window_run?, :boolean, default: false
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
      run_contexts={@run_contexts}
      selected_run_context={@selected_run_context}
      run_context_status={@run_context_status}
      can_run_asset?={@can_run_asset?}
      can_submit_runs?={@can_submit_runs?}
      submitting_window_run?={@submitting_window_run?}
      problems={asset_problems(assigns)}
      timezone={@timezone}
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
          timezone={@timezone}
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
        class="max-h-72 shrink-0 lg:max-h-none lg:w-80"
        data-testid="asset-run-timeline"
      />
    </div>

    <.asset_documentation
      :if={@active_mode == :docs}
      title={@title}
      documentation={@documentation}
    />

    <.diagnostics_panel
      :if={@active_mode == :diagnostics}
      compatibility={@compatibility}
      coverage={@coverage}
      coverage_policy={@coverage_policy}
      manifest_version_id={@manifest_version_id}
      rebuild_target_id={@rebuild_target_id}
      timezone={@timezone}
    />

    <.coverage_panel
      :if={@active_mode == :coverage && @coverage}
      coverage={@coverage}
      calendar={@coverage_calendar}
      navigation={@coverage_navigation}
      plan={@coverage_plan}
      action_error={@coverage_action_error}
      planning?={@planning_coverage?}
      submitting?={@submitting_coverage?}
      can_plan?={@can_submit_runs? && @can_run_asset?}
      command_resource={@rebuild_target_id}
      timezone={@timezone}
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

  @doc """
  Whether anything is wrong with the table this asset writes, and how to fix it.

  The page leads with a verdict in one sentence, because the operator's question is
  "can this asset run" and the screen used to answer it with four hashes and the
  phrase "the desired descriptor and active physical target are compatible". The fix
  is a button beside the verdict rather than a page away.

  What changed is named in the asset's own terms — the period shape, the way it is
  built, the table itself — and the identifiers those verdicts were computed from are
  one disclosure down. They matter when someone is comparing two deployments by hand,
  and never before that.
  """
  attr :compatibility, :map, default: nil
  attr :coverage, :any, default: nil
  attr :coverage_policy, :map, default: nil
  attr :manifest_version_id, :string, default: nil
  attr :rebuild_target_id, :string, default: nil
  attr :timezone, :any, required: true

  def diagnostics_panel(assigns) do
    assigns = assign(assigns, :changes, compatibility_changes(assigns.compatibility))

    ~H"""
    <div class="mx-auto w-full max-w-3xl space-y-6" data-testid="asset-diagnostics">
      <.panel
        padding={:lg}
        class={["border", compatibility_panel_class(field(@compatibility, :status))]}
        data-testid="asset-compatibility-panel"
      >
        <div class="flex flex-wrap items-center justify-between gap-3">
          <h2 class={Typography.class(:eyebrow)}>
            The table it writes
          </h2>

          <span
            :if={field(@compatibility, :persisted?, false)}
            class={compatibility_badge_class(field(@compatibility, :status))}
          >
            {compatibility_status_label(field(@compatibility, :status))}
          </span>
        </div>

        <p class="mt-3 max-w-2xl">{compatibility_explanation(@compatibility)}</p>

        <p
          :if={inspection_retry_required?(@compatibility)}
          class="mt-2 max-w-2xl text-sm favn-text-muted"
          data-testid="retry-asset-inspection"
        >
          Correct the runner or data-system problem, then activate the manifest again.
        </p>

        <p
          :if={field(@compatibility, :blocks_writes?, false)}
          class="mt-2 max-w-2xl font-medium text-error"
          data-testid="asset-compatibility-blocked"
        >
          Nothing can run until this is resolved.
        </p>

        <section :if={@changes != []} class="mt-6">
          <h3 class="text-sm favn-text-subtle">What changed</h3>

          <dl class="mt-2 space-y-2 text-sm">
            <div :for={change <- @changes} class="sm:flex sm:gap-4">
              <dt class="shrink-0 font-medium sm:w-40">{change.label}</dt>

              <dd class="min-w-0 break-words favn-text-muted">{change.detail}</dd>
            </div>
          </dl>
        </section>

        <div
          :if={@rebuild_target_id && compatibility_actionable?(@compatibility)}
          class="mt-6 flex flex-wrap gap-2 border-t border-base-content/10 pt-5"
        >
          <.button
            :if={field(@compatibility, :reason_code) == "unmanaged_physical_relation"}
            navigate={~p"/recoveries?#{[target_id: @rebuild_target_id]}"}
            data-testid="recover-asset-ownership"
          >
            Take ownership of this table
          </.button>

          <.button
            :if={rebuild_available?(@compatibility)}
            variant={:secondary}
            navigate={~p"/rebuilds?#{[target_id: @rebuild_target_id]}"}
            data-testid="plan-asset-rebuild"
          >
            Rebuild the table
          </.button>
        </div>
      </.panel>

      <.panel padding={:lg} data-testid="asset-diagnostics-detail">
        <h2 class={Typography.class(:eyebrow)}>Under the hood</h2>

        <p class="mt-2 text-sm favn-text-muted">
          Worth opening when two deployments disagree, or when a rule above needs
          checking against what the asset declared.
        </p>

        <details :if={@coverage_policy} class="mt-5 border-t border-base-content/10 pt-4">
          <summary class="cursor-pointer font-medium">How periods are counted</summary>

          <.fact_list class="mt-3" columns={2} facts={coverage_rule_facts(assigns)} />
        </details>

        <!-- Rows rather than a fact grid, because a fingerprint truncated to fit a
        column is useless for the one thing this disclosure is for: telling two
        deployments apart. -->
        <details class="mt-4 border-t border-base-content/10 pt-4">
          <summary class="cursor-pointer font-medium">Identifiers Favn matched on</summary>

          <div class="mt-2 divide-y divide-base-content/5">
            <.field_row :for={fact <- identity_facts(assigns)} label={fact.label}>
              <.mono :if={fact[:mono]} value={fact.value} />
              <span :if={!fact[:mono]}>{fact.value}</span>
            </.field_row>
          </div>
        </details>
      </.panel>
    </div>
    """
  end

  # A verdict with no action is not actionable. `:ready` and `:uninitialized` both
  # describe a table nobody has to touch, and offering a rebuild there invites one.
  defp compatibility_actionable?(compatibility) do
    field(compatibility, :persisted?, false) and
      (field(compatibility, :reason_code) == "unmanaged_physical_relation" or
         rebuild_available?(compatibility))
  end

  defp rebuild_available?(compatibility) do
    is_binary(field(compatibility, :active_generation_id)) and
      field(compatibility, :status) not in [:ready, :uninitialized, nil]
  end

  defp inspection_retry_required?(compatibility) do
    field(compatibility, :persisted?, false) and
      field(compatibility, :reason_code) == "physical_inspection_unavailable"
  end

  defp coverage_rule_facts(assigns) do
    [
      %{
        label: "Periods are in",
        value: field(assigns.coverage_policy, :timezone) || "Not declared"
      },
      %{
        label: "Counted from",
        value: coverage_start_label(assigns.coverage_policy, assigns.timezone)
      },
      %{
        label: "A period counts once",
        value: availability_label(field(assigns.coverage_policy, :availability_delay_seconds, 0))
      },
      %{
        label: "Expected through",
        value:
          coverage_window_label(field(assigns.coverage, :last_expected_window), assigns.timezone)
      }
    ]
  end

  defp identity_facts(assigns) do
    [
      %{
        label: "Manifest version",
        value: assigns.manifest_version_id || "Unknown",
        mono: true
      },
      %{
        label: "Active generation",
        value: coverage_generation_label(field(assigns.compatibility, :active_generation_id)),
        mono: true
      },
      %{
        label: "Table Favn wants",
        value: field(assigns.compatibility, :desired_descriptor_hash) || "Not recorded",
        mono: true
      },
      %{
        label: "Table Favn found",
        value: field(assigns.compatibility, :physical_fingerprint) || "Not recorded",
        mono: true
      },
      %{
        label: "Coverage last checked",
        value: coverage_time(field(assigns.coverage, :evaluated_at), assigns.timezone)
      },
      %{label: "Verdict code", value: humanize(field(assigns.compatibility, :reason_code))}
    ]
  end

  # A diff is keyed by what Favn compares, which is not what an operator calls it, and
  # a `descriptor` key holds a list of per-field changes rather than one change. Both
  # flatten into rows named after the thing that moved.
  defp compatibility_changes(compatibility) do
    compatibility
    |> field(:diff, %{})
    |> compatibility_diff_entries()
    |> Enum.flat_map(&compatibility_change_rows/1)
    |> Enum.take(50)
  end

  defp compatibility_change_rows({_name, changes}) when is_list(changes) do
    Enum.map(changes, fn change ->
      %{label: change_label(field(change, :field)), detail: compatibility_change_label(change)}
    end)
  end

  defp compatibility_change_rows({name, change}) do
    [%{label: change_label(name), detail: compatibility_change_label(change)}]
  end

  defp change_label(:window_identity), do: "Period shape"
  defp change_label(:execution_package_hash), do: "How it is built"
  defp change_label(:contract_fingerprint), do: "Promised columns"
  defp change_label(:physical_fingerprint), do: "The table itself"
  defp change_label(:relation), do: "Where it lands"
  defp change_label(:write_mode), do: "How it writes"
  defp change_label(name), do: humanize(name)

  @doc """
  Which periods hold data, and a way to fill the ones that do not.

  One sentence answers the question and the calendar shows where, because the shape
  of a gap is the useful part: one bad week and every Sunday need different fixes and
  a count of six cannot tell them apart.

  What the periods are is a fact about this asset's configuration rather than about
  its data, so the timezone, the start date, and the availability delay are on
  Diagnostics and not here.
  """
  attr :coverage, :any, required: true

  attr :calendar, :map,
    required: true,
    doc: "a `FavnView.CoverageCalendar.build/1` result"

  attr :navigation, :map,
    default: %{previous: nil, next: nil, jumps: []},
    doc: "a `FavnView.CoverageCalendar.navigation/1` result"

  attr :plan, :map, default: nil
  attr :action_error, :string, default: nil
  attr :planning?, :boolean, default: false
  attr :submitting?, :boolean, default: false
  attr :can_plan?, :boolean, default: false
  attr :command_resource, :string, required: true
  attr :timezone, :any, required: true

  def coverage_panel(assigns) do
    ~H"""
    <.panel
      padding={:lg}
      class="mx-auto w-full max-w-5xl"
      data-testid="asset-coverage"
    >
      <div class="flex flex-wrap items-center justify-between gap-3">
        <h2 class={Typography.class(:eyebrow)}>Coverage</h2>

        <span class={coverage_badge_class(field(@coverage, :status))}>
          {coverage_status_label(field(@coverage, :status))}
        </span>
      </div>

      <p class="mt-3 max-w-2xl">{coverage_answer(@coverage, @calendar)}</p>

      <.calendar_navigator
        :if={@calendar.unit_label}
        class="mt-6"
        label={@calendar.unit_label}
        previous={@navigation.previous}
        next={@navigation.next}
        jumps={@navigation.jumps}
        on_step="show_coverage_period"
        on_jump="jump_coverage_period"
        data-testid="coverage-navigator"
      />

      <.coverage_calendar
        :if={@calendar.layout != :empty}
        class="mt-4"
        layout={@calendar.layout}
        cells={@calendar.cells}
        blanks={@calendar.blanks}
        columns={@calendar.columns}
        column_labels={@calendar.column_labels}
        on_select={(@can_plan? && "toggle_coverage_window") || nil}
        data-testid="asset-coverage-calendar"
      />

      <p :if={coverage_caption(@calendar)} class="mt-4 text-sm favn-text-subtle">
        {coverage_caption(@calendar)}
      </p>

      <div
        :if={coverage_backfillable?(@coverage) && is_nil(@plan)}
        class="mt-6 flex flex-wrap items-center gap-3 border-t border-base-content/10 pt-5"
      >
        <.button
          loading={@planning?}
          phx-click="plan_missing_coverage"
          disabled={!@can_plan? || @planning?}
          data-testid="plan-missing-coverage"
        >
          {coverage_backfill_label(@coverage, @calendar)}
        </.button>

        <.button
          :if={@calendar.selected_count > 0}
          variant={:ghost}
          phx-click="clear_coverage_selection"
          data-testid="clear-coverage-selection"
        >
          Clear selection
        </.button>

        <p :if={!@can_plan?} class="text-sm text-warning">
          Backfilling needs an operator account and a working target.
        </p>
      </div>

      <div
        :if={@plan}
        class="mt-6 border-t border-base-content/10 pt-5"
        data-testid="coverage-plan-review"
      >
        <p class="font-medium">
          Ready to backfill {field(@plan, :window_count, 0)} {CoverageCalendar.period_noun(
            @calendar.kind,
            field(@plan, :window_count, 0)
          )}
        </p>

        <ul class="mt-2 max-h-40 space-y-0.5 overflow-y-auto text-sm favn-text-muted">
          <li :for={window <- field(@plan, :windows, [])}>
            {coverage_plan_window_label(window, @timezone)}
          </li>
        </ul>

        <!-- `:primary`, not `:solid`: this is a page panel, and the filled violet is
        reserved for a dialog asking for a decision. Planning and submitting never show
        together, so there is still one action button per view state. -->
        <.button
          class="mt-4"
          loading={@submitting?}
          phx-click="submit_missing_coverage"
          data-command-operation="coverage_backfill_submit"
          data-command-resource={@command_resource}
          disabled={@submitting?}
          data-testid="submit-missing-coverage"
        >
          Start the backfill
        </.button>
      </div>

      <p :if={@action_error} class="mt-4 text-sm text-error" data-testid="coverage-action-error">
        {@action_error}
      </p>
    </.panel>
    """
  end

  # Coverage the operator can act on: incomplete, with at least one period named.
  defp coverage_backfillable?(coverage),
    do: field(coverage, :status) == :incomplete and field(coverage, :missing_count, 0) > 0

  defp coverage_backfill_label(_coverage, %{selected_count: selected} = calendar)
       when selected > 0 do
    "Backfill #{selected} selected #{CoverageCalendar.period_noun(calendar.kind, selected)}"
  end

  defp coverage_backfill_label(coverage, calendar) do
    missing = field(coverage, :missing_count, 0)
    "Backfill all #{missing} missing #{CoverageCalendar.period_noun(calendar.kind, missing)}"
  end

  defp coverage_answer(coverage, calendar) do
    expected = field(coverage, :expected_count, 0)
    missing = field(coverage, :missing_count, 0)
    periods = CoverageCalendar.period_noun(calendar.kind, expected)

    case field(coverage, :status) do
      :complete when expected == 0 ->
        "No period is due yet, so nothing is missing."

      :complete ->
        "All #{expected} #{periods} that should hold data do."

      :incomplete ->
        "#{missing} of #{expected} #{periods} have no data."

      _unknown ->
        coverage_unknown_answer(field(coverage, :unknown_reason))
    end
  end

  defp coverage_unknown_answer(:coverage_not_declared),
    do: "This asset does not say which periods it should cover, so there is nothing to check."

  defp coverage_unknown_answer(:non_windowed_asset),
    do: "This asset rewrites its whole table every run, so it has no periods to cover."

  defp coverage_unknown_answer(:target_generation_uninitialized),
    do: "This asset has never been built, so there is nothing to compare against yet."

  defp coverage_unknown_answer(_reason),
    do: "Favn could not read coverage just now. Reload the page to try again."

  defp coverage_caption(%{layout: :empty}), do: nil

  # Which grain the cells are and which clock they are read against. The screen shows
  # numbers in a grid, and neither fact is guessable from them.
  defp coverage_caption(calendar) do
    grain = CoverageCalendar.period_noun(calendar.kind, calendar.period_count)

    case calendar.timezone do
      nil -> "#{calendar.period_count} #{grain}."
      timezone -> "#{calendar.period_count} #{grain}, in #{timezone}."
    end
  end

  # Coverage starts at the later of what the author declared and when the target was
  # first built, so the two only need telling apart when they disagree.
  defp coverage_start_label(policy, timezone) do
    declared = field(policy, :declared_from)
    effective = field(policy, :effective_from)

    if declared && effective && declared != effective do
      "#{coverage_time(effective, timezone)}, declared #{coverage_time(declared, timezone)}"
    else
      coverage_time(effective, timezone)
    end
  end

  defp coverage_plan_window_label(window, timezone) do
    case field(window, :start_at) do
      %DateTime{} = start_at ->
        CoverageCalendar.period_label(field(window, :kind), start_at, timezone)

      _absent ->
        field(window, :window_key)
    end
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
          <.eyebrow>Run context</.eyebrow>

          <p class="mt-1 text-sm favn-text-muted">
            Which pipeline's schedule this asset follows. It decides the periods below
            and how freshness is judged.
          </p>
        </div>

        <div class="flex min-w-0 flex-wrap gap-2">
          <.link
            :for={context <- @contexts}
            patch={context.href}
            class={[
              "btn btn-sm h-auto min-h-8 max-w-full flex-col items-start whitespace-normal py-1.5 text-left",
              selected_run_context?(@selected, context) && "btn-primary btn-soft",
              !selected_run_context?(@selected, context) && "btn-ghost"
            ]}
            data-testid={"asset-run-context-#{context.id}"}
          >
            <span class="max-w-full break-words">{context.label}</span>
            <span class="max-w-full break-words text-sm opacity-60">
              {run_context_policy_label(context)}
            </span>
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
  What the asset is and how it is written.

  The top half reads the same whatever the asset is: the author's own documentation,
  its tags, its full address. The bottom half is the source, and that genuinely
  differs — a SQL asset has a query and the tables it reads, an Elixir asset has a
  module and a function. Showing one layout for both would mean showing empty fields
  half the time.
  """
  attr :documentation, :any,
    default: nil,
    doc: "`{:ok, docs}`, `{:error, reason}`, or nil while the page has not opened"

  attr :title, :string, required: true

  def asset_documentation(assigns) do
    docs = documentation_or_nil(assigns.documentation)

    assigns =
      assigns
      |> assign(:docs, docs)
      |> assign(:sql, docs && docs[:sql])

    ~H"""
    <div class="mx-auto w-full max-w-5xl space-y-6" data-testid="asset-documentation">
      <.error_state
        :if={match?({:error, _reason}, @documentation)}
        title="Documentation cannot be read"
        description="The control plane did not return this asset's definition."
        data-testid="asset-documentation-error"
      />

      <.panel :if={@docs} padding={:lg}>
        <h2 class={Typography.class(:eyebrow)}>What this is</h2>

        <p :if={@docs[:description]} class="mt-3 max-w-3xl whitespace-pre-line">
          {@docs.description}
        </p>

        <p :if={is_nil(@docs[:description])} class="mt-3 text-sm favn-text-muted">
          This asset has no documentation. Add a <code class="font-mono">@doc</code> to the
          module and it appears here.
        </p>

        <dl :if={@docs[:metadata] != []} class="mt-6 space-y-2" data-testid="asset-tags">
          <.field_row :for={entry <- @docs.metadata} label={humanize(entry.key)}>
            {entry.value}
          </.field_row>
        </dl>

        <div class="mt-6 flex flex-wrap items-baseline gap-x-3 border-t border-base-content/10 pt-4">
          <span class="text-sm favn-text-subtle">Full address</span>
          <.mono value={full_relation_address(@docs[:relation])} />
        </div>
      </.panel>

      <.panel :if={@sql} padding={:lg} data-testid="asset-sql">
        <div class="flex flex-wrap items-baseline justify-between gap-2">
          <h2 class={Typography.class(:eyebrow)}>The query</h2>
          <span class="text-sm favn-text-subtle">{sql_line_count(@sql.sql)}</span>
        </div>

        <.fact_list
          class="mt-4"
          columns={2}
          facts={
            [
              %{label: "Reads", value: reads_label(@sql.reads)},
              @sql[:resolver] &&
                %{label: "Input resolver", value: @sql.resolver, mono: true}
            ]
            |> Enum.filter(& &1)
          }
        />

        <div :if={@sql[:fragments] != []} class="mt-4 flex flex-wrap items-baseline gap-2">
          <span class="text-sm favn-text-subtle">Built from</span>
          <.badge :for={fragment <- @sql.fragments} variant={:outline}>{fragment.name}</.badge>
        </div>

        <pre class="favn-surface-control mt-4 overflow-x-auto rounded-box p-4 text-sm"><code class="font-mono">{@sql.sql}</code></pre>
      </.panel>

      <.panel
        :if={@docs && @docs[:entrypoint]}
        padding={:lg}
        data-testid="asset-entrypoint"
      >
        <h2 class={Typography.class(:eyebrow)}>The code that runs</h2>

        <.mono value={entrypoint_label(@docs.entrypoint)} class="mt-3 block text-base" />

        <p class="mt-2 text-sm favn-text-muted">
          Favn calls this function for every run. Its source lives in your project, not here.
        </p>
      </.panel>

      <.empty_state
        :if={@docs && is_nil(@sql) && is_nil(@docs[:entrypoint])}
        title="No source to show"
        description="This asset declares neither a query nor an entrypoint."
        icon="hero-document"
      />
    </div>
    """
  end

  defp documentation_or_nil({:ok, docs}), do: docs
  defp documentation_or_nil(_documentation), do: nil

  # Every level, unlike the overview's queryable address: this page is where someone
  # checks how the asset is addressed, including the connection Favn reaches it over.
  defp full_relation_address(nil), do: "Not declared"

  defp full_relation_address(relation) do
    [:connection, :catalog, :schema, :name]
    |> Enum.map(&field(relation, &1))
    |> Enum.reject(&(&1 in [nil, ""]))
    |> case do
      [] -> "Not declared"
      levels -> Enum.join(levels, ".")
    end
  end

  defp sql_line_count(sql) when is_binary(sql) do
    case length(String.split(sql, "\n")) do
      1 -> "1 line"
      count -> "#{count} lines"
    end
  end

  defp sql_line_count(_sql), do: nil

  defp reads_label([]), do: "Nothing. It reads its source directly."

  defp reads_label(reads),
    do: Enum.map_join(reads, ", ", &(&1[:name] || &1[:asset_ref] || "unknown"))

  defp entrypoint_label(%{module: module, function: function, arity: arity})
       when is_integer(arity),
       do: "#{module}.#{function}/#{arity}"

  defp entrypoint_label(%{module: module, function: function}), do: "#{module}.#{function}"
  defp entrypoint_label(_entrypoint), do: "Not declared"

  @doc """
  The asset in one screen: what state it is in, what is wrong, and what feeds it.

  Facts first, then problems, then lineage, then the one action. Nothing here is
  configuration — this page answers "is it working", and every panel that answered
  "how is it set up" moved to Documentation or Diagnostics. A healthy asset shows no
  problem panel at all rather than a green one saying nothing is wrong.
  """
  attr :freshness, :map, default: nil
  attr :timezone, :any, required: true
  attr :relation, :map, default: nil
  attr :runs, :list, default: []
  attr :upstream, :list, default: []
  attr :downstream, :list, default: []
  attr :title, :string, required: true
  attr :asset_id, :string, required: true
  attr :type, :string, default: nil
  attr :cadence_label, :string, default: nil
  attr :problems, :list, default: []
  attr :run_contexts, :list, default: []
  attr :selected_run_context, :map, default: nil
  attr :run_context_status, :atom, default: :unavailable
  attr :can_run_asset?, :boolean, default: true
  attr :can_submit_runs?, :boolean, default: false
  attr :submitting_window_run?, :boolean, default: false

  def asset_overview(assigns) do
    assigns = assign(assigns, :latest, List.first(assigns.runs))

    ~H"""
    <div class="mx-auto w-full max-w-[120rem] space-y-6" data-testid="asset-overview">
      <.panel padding={:lg}>
        <!-- The one thing an operator comes here to do sits where an action belongs, not
        at the bottom of a strip of run anchors. Which period it runs for is the dialog's
        business; filling gaps is Coverage's. -->
        <div class="mb-5 flex flex-wrap items-start justify-between gap-3">
          <div class="min-w-0">
            <.eyebrow>This asset</.eyebrow>
          </div>

          <.button
            icon="hero-play"
            phx-click="open_run_config"
            loading={@submitting_window_run?}
            disabled={!@can_submit_runs? || !@can_run_asset? || @submitting_window_run?}
            title={run_disabled_title(assigns)}
            data-testid="run-this-asset"
          >
            Run this asset
          </.button>
        </div>

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

      <!-- Only where the choice is real. One pipeline owns most assets, and a selector
      holding one option is a control that cannot change anything. -->
      <.panel
        :if={length(@run_contexts) > 1 || @run_context_status == :ambiguous}
        padding={:lg}
      >
        <.run_context_selector
          contexts={@run_contexts}
          selected={@selected_run_context}
          status={@run_context_status}
        />
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

      <.panel padding={:lg} data-testid="asset-lineage">
        <h2 class={Typography.class(:eyebrow)}>Lineage</h2>

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

  # A disabled control has to say why, and the two reasons are different problems: one
  # is the operator's role, the other is the asset's own state.
  defp run_disabled_title(%{can_submit_runs?: false}),
    do: "Running an asset needs an operator account"

  defp run_disabled_title(%{can_run_asset?: false}),
    do: "This asset cannot run until the problems above are resolved"

  defp run_disabled_title(_assigns), do: nil

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

  def freshness_detail_panel(assigns) do
    ~H"""
    <.panel
      padding={:lg}
      class="mx-auto w-full max-w-4xl"
      data-testid="asset-freshness-detail-panel"
    >
      <div :if={!@freshness} class="text-sm favn-text-muted">
        Freshness detail is not available from the backend.
      </div>

      <div :if={@freshness} class="space-y-6">
        <div>
          <.eyebrow>Freshness detail</.eyebrow>

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
            <dt class={Typography.class(:eyebrow)}>Latest success</dt>

            <dd class="mt-1 break-words font-mono text-sm favn-text-muted">
              {freshness_latest_success(@freshness)[:run_id]}
            </dd>
          </div>

          <div class="rounded-box border border-base-content/10 bg-base-content/[0.03] p-3">
            <dt class={Typography.class(:eyebrow)}>Freshness key</dt>

            <dd class="mt-1 break-words font-mono text-sm favn-text-muted">
              {freshness_latest_success(@freshness)[:freshness_key]}
            </dd>
          </div>

          <div class="rounded-box border border-base-content/10 bg-base-content/[0.03] p-3">
            <dt class={Typography.class(:eyebrow)}>At</dt>

            <dd class="mt-1 text-sm favn-text-muted">
              {freshness_time(freshness_latest_success(@freshness)[:at], @timezone)}
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
                  <dt class={Typography.class(:eyebrow)}>Previous</dt>

                  <dd class="mt-0.5 break-words font-mono">{reason[:previous_version]}</dd>
                </div>

                <div :if={reason[:current_version]}>
                  <dt class={Typography.class(:eyebrow)}>Current</dt>

                  <dd class="mt-0.5 break-words font-mono">{reason[:current_version]}</dd>
                </div>

                <div :if={reason[:run_id]}>
                  <dt class={Typography.class(:eyebrow)}>Run</dt>

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
    <.panel padding={:lg} data-testid="asset-assurance-panel">
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

                <td class="text-center">
                  <.status_icon
                    tone={row.tone}
                    icon={row.result_icon}
                    label={row.result}
                    tooltip={:left}
                  />
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
      result_icon: check_result_icon(result),
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
      result_icon: check_result_icon(result),
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
  attr :timezone, :any, required: true

  def run_detail_panel(assigns) do
    result = assigns.run[:asset_result]
    meta = (result && result[:meta]) || %{}

    assigns =
      assigns
      |> assign(:failed?, run_failed?(assigns.run))
      |> assign(:result, result)
      |> assign(:meta, meta)
      |> assign(:facts, run_facts(assigns.run, result, assigns.timezone))
      |> assign(:write, OutputMetadata.outcome(meta, result && result[:status]))
      |> assign(:inputs, List.wrap(assigns.run[:runtime_inputs]))

    ~H"""
    <div class="space-y-6" data-testid="asset-run-detail">
      <.panel padding={:lg}>
        <div class="flex flex-wrap items-start justify-between gap-3">
          <div class="min-w-0">
            <.eyebrow>Run</.eyebrow>
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

        <.fact_list class="mt-6" columns={3} facts={@facts} />

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

      <.panel :if={@inputs != []} padding={:lg}>
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

  defp run_facts(run, result, timezone) do
    [
      %{label: "Started", value: LogsViewModel.timestamp_label(run[:started_at], timezone)},
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

  def sample_nav_items, do: Navigation.items(:assets)

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
        id: :docs,
        label: "Documentation",
        icon: "hero-book-open",
        patch: ~p"/assets/#{asset_id}/docs"
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

  # One icon per outcome, not per tone: "not needed" and "not run" are both neutral and
  # mean different things, and "blocked the write" shares its tone with a warning. The
  # shape carries the state and the colour only reinforces it, which is also what keeps
  # the cell readable for anyone who cannot separate the hues.
  defp check_result_icon(nil), do: "hero-question-mark-circle"

  defp check_result_icon(result) do
    case value(result, :outcome) do
      outcome when outcome in [:passed, "passed"] ->
        "hero-check-circle"

      outcome when outcome in [:warned, "warned"] ->
        "hero-exclamation-triangle"

      outcome when outcome in [:failed, "failed"] ->
        "hero-x-circle"

      outcome when outcome in [:errored, "errored"] ->
        "hero-exclamation-circle"

      outcome when outcome in [:not_run, "not_run"] ->
        "hero-question-mark-circle"

      outcome when outcome in [:condition_skipped, "condition_skipped"] ->
        "hero-minus-circle"

      outcome when outcome in [:materialization_skipped, "materialization_skipped"] ->
        "hero-no-symbol"

      _outcome ->
        "hero-information-circle"
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

  defp coverage_time(%DateTime{} = value, timezone),
    do: FavnView.Time.format(value, "%b %-d, %Y %H:%M %Z", timezone)

  defp coverage_time(value, _timezone) when is_binary(value), do: value
  defp coverage_time(_value, _timezone), do: "-"

  defp coverage_window_label(nil, _timezone), do: "No windows expected yet"

  defp coverage_window_label(window, timezone) do
    case field(window, :start_at) do
      %DateTime{} = start_at -> coverage_time(start_at, timezone)
      value when is_binary(value) -> value
      _other -> "-"
    end
  end

  # An absent generation means the table was never built, which is a state with its own
  # meaning rather than a value that failed to load.
  defp coverage_generation_label(value) when is_binary(value), do: value
  defp coverage_generation_label(_value), do: "Never built"

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

  # Plain sentences, because the operator's question is whether the asset can run. The
  # words these replaced — "the desired descriptor and active physical target are
  # compatible" — named Favn's internals and answered a question nobody asked.
  defp compatibility_explanation(compatibility) do
    cond do
      !field(compatibility, :persisted?, false) ->
        "This asset does not manage a table of its own, so there is nothing to check here."

      true ->
        compatibility_verdict(compatibility)
    end
  end

  defp compatibility_verdict(%{reason_code: "physical_inspection_unavailable"}),
    do:
      "Favn could not inspect this table during activation, so it will not write until inspection succeeds."

  defp compatibility_verdict(%{"reason_code" => "physical_inspection_unavailable"}),
    do:
      "Favn could not inspect this table during activation, so it will not write until inspection succeeds."

  defp compatibility_verdict(compatibility),
    do: compatibility_status_verdict(field(compatibility, :status))

  defp compatibility_status_verdict(:ready),
    do: "The table Favn wants is the table it has. Nothing needs doing."

  defp compatibility_status_verdict(:uninitialized),
    do: "This table does not exist yet. The first successful run creates it."

  defp compatibility_status_verdict(:rebuild_available),
    do:
      "The way this asset builds its table changed. Runs still work, and rebuilding " <>
        "brings the existing rows in line with the new definition."

  defp compatibility_status_verdict(:rebuild_required),
    do:
      "The table Favn wants no longer fits the one it has, so writing to it would " <>
        "corrupt what is there. Rebuilding replaces it."

  defp compatibility_status_verdict(:unexpected_drift),
    do:
      "Something changed this table outside Favn. Favn will not write over a change " <>
        "it cannot account for."

  defp compatibility_status_verdict(:operator_decision),
    do:
      "Favn cannot tell whether it owns this table, so it will not write to it. " <>
        "Taking ownership says the table is Favn's to manage."

  defp compatibility_status_verdict(_status),
    do: "Favn could not read the state of this table just now."

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
      is_map(previous) and is_map(desired) ->
        changed_side_by_side(previous, desired)

      !is_nil(previous) or !is_nil(desired) ->
        "#{bounded_value(previous)} → #{bounded_value(desired)}"

      true ->
        bounded_value(change)
    end
  end

  defp compatibility_change_label(change), do: bounded_value(change)

  # Only the keys that moved. Both sides of a window identity carry the timezone even
  # when the timezone is not what changed, and printing it twice buried the one field
  # that did — the row read `%{kind: :day, timezone: "Europe/Oslo"} → %{kind: :month,
  # timezone: "Europe/Oslo"}` where it should read `day → month`.
  defp changed_side_by_side(previous, desired) do
    keys =
      (Map.keys(previous) ++ Map.keys(desired))
      |> Enum.uniq()
      |> Enum.filter(&(Map.get(previous, &1) != Map.get(desired, &1)))
      |> Enum.sort_by(&to_string/1)

    case keys do
      [] -> "unchanged"
      keys -> "#{side_values(previous, keys)} → #{side_values(desired, keys)}"
    end
  end

  defp side_values(side, keys),
    do: Enum.map_join(keys, ", ", &bounded_value(Map.get(side, &1)))

  defp bounded_value(nil), do: "-"

  # Long enough to be a hash, and the whole value is in the identifiers disclosure, so
  # a prefix is all a difference needs to be visible.
  defp bounded_value(value) when is_binary(value) and byte_size(value) > 24,
    do: String.slice(value, 0, 12) <> "…"

  defp bounded_value(value) when is_binary(value), do: value
  defp bounded_value(value) when is_atom(value) or is_number(value), do: to_string(value)

  defp bounded_value(value) when is_map(value),
    do:
      Enum.map_join(Enum.sort_by(Map.to_list(value), &to_string(elem(&1, 0))), ", ", fn
        {key, inner} -> "#{key}: #{bounded_value(inner)}"
      end)

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

  defp selected_run_context?(%{id: id}, %{id: id}), do: true
  defp selected_run_context?(_selected, _context), do: false

  defp run_context_policy_label(%{policy: %{kind: kind, anchor: anchor}, timezone: timezone}) do
    "#{humanize(kind)} / #{humanize(anchor)} / #{timezone}"
  end

  defp run_context_policy_label(%{timezone: timezone}), do: timezone

  defp freshness_state_label(:fresh), do: "Fresh"
  defp freshness_state_label(:stale), do: "Stale"
  defp freshness_state_label(:always_run), do: "Always run"
  defp freshness_state_label(_state), do: "Unknown"

  defp freshness_badge_class(:fresh), do: "badge badge-success badge-soft badge-sm"
  defp freshness_badge_class(:stale), do: "badge badge-warning badge-soft badge-sm"
  defp freshness_badge_class(:always_run), do: "badge badge-info badge-soft badge-sm"
  defp freshness_badge_class(_state), do: "badge badge-neutral badge-soft badge-sm"

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

  defp freshness_time(%DateTime{} = value, timezone),
    do: FavnView.Time.format(value, "%b %-d, %Y %H:%M:%S %Z", timezone)

  defp freshness_time(_value, _timezone), do: "-"
end
