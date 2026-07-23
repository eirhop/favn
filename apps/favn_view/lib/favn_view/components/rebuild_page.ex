defmodule FavnView.Components.RebuildPage do
  @moduledoc "Reusable operator surfaces for manual target rebuilds."

  use FavnView, :html

  alias FavnView.Components.AppShell
  alias FavnView.Components.AssetCataloguePage
  alias FavnView.Components.GlassPanel

  attr :operations, :list, required: true
  attr :plan, :map, default: nil
  attr :target_id, :string, default: ""
  attr :error, :string, default: nil
  attr :has_more?, :boolean, default: false
  attr :planning?, :boolean, default: false

  def rebuilds_page(assigns) do
    ~H"""
    <AppShell.app_shell
      title="Rebuilds"
      subtitle="Manual generation replacement"
      nav_items={nav_items()}
    >
      <div
        class="mx-auto grid w-full max-w-[120rem] gap-5 xl:grid-cols-[minmax(22rem,0.8fr)_minmax(0,1.7fr)]"
        data-testid="rebuilds-page"
      >
        <GlassPanel.glass_panel class="p-5 sm:p-6">
          <p class="text-xs font-semibold uppercase tracking-[0.18em] text-base-content/50">
            Plan a rebuild
          </p>
          <p class="mt-2 text-sm text-base-content/65">
            Planning is read-only. Review the immutable plan before starting it.
          </p>

          <.form for={%{}} as={:rebuild} phx-submit="plan_rebuild" class="mt-5 space-y-4">
            <label class="form-control block">
              <span class="label-text text-xs text-base-content/60">Target id</span>
              <input
                name="rebuild[target_id]"
                value={@target_id}
                required
                class="input input-bordered mt-1 w-full bg-base-100/30"
                data-testid="rebuild-target"
              />
            </label>
            <label class="form-control block">
              <span class="label-text text-xs text-base-content/60">Reason</span>
              <textarea
                name="rebuild[reason]"
                required
                maxlength="4096"
                class="textarea textarea-bordered mt-1 min-h-24 w-full bg-base-100/30"
                data-testid="rebuild-reason"
              ></textarea>
            </label>
            <button class="btn btn-primary w-full" disabled={@planning?} data-testid="plan-rebuild">
              {if @planning?, do: "Planning…", else: "Create immutable plan"}
            </button>
          </.form>

          <p :if={@error} class="mt-4 text-sm text-error" data-testid="rebuild-error">{@error}</p>

          <div
            :if={@plan}
            class="mt-5 rounded-box border border-info/25 bg-info/5 p-4"
            data-testid="rebuild-plan"
          >
            <p class="font-medium">Plan ready for review</p>
            <dl class="mt-3 space-y-2 text-xs">
              <.fact label="Plan id" value={field(@plan, :plan_id)} mono? />
              <.fact label="Plan hash" value={field(@plan, :plan_hash)} mono? />
              <.fact label="Expires" value={format_time(field(@plan, :expires_at))} />
              <.fact
                label="Root target"
                value={@plan |> field(:payload, %{}) |> field(:root_target_id)}
                mono?
              />
              <.fact
                label="Evaluated"
                value={@plan |> field(:payload, %{}) |> field(:evaluated_at) |> format_time()}
              />
              <.fact label="Declared coverage" value={plan_coverage(@plan, :declared_from)} />
              <.fact label="Effective coverage" value={plan_coverage(@plan, :effective_from)} />
              <.fact label="Coverage through" value={plan_coverage(@plan, :through)} />
              <.fact label="Evaluated range" value={plan_range(@plan)} />
              <.fact label="Availability delay" value={plan_availability_delay(@plan)} />
              <.fact
                label="Active generation"
                value={@plan |> field(:payload, %{}) |> field(:active_generation_id)}
                mono?
              />
              <.fact
                label="Candidate generation"
                value={@plan |> field(:payload, %{}) |> field(:candidate_generation_id)}
                mono?
              />
              <.fact
                label="Logical items"
                value={@plan |> field(:payload, %{}) |> field(:item_count, 0)}
              />
              <.fact
                label="Items digest"
                value={@plan |> field(:payload, %{}) |> field(:items_digest)}
                mono?
              />
              <.fact
                label="Compatibility"
                value={@plan |> root_binding_value(:compatibility_status) |> humanize()}
              />
              <.fact label="Compatibility reason" value={root_binding_value(@plan, :reason_code)} />
              <.fact
                label="Compatibility diff"
                value={json_text(root_binding_value(@plan, :compatibility_diff, %{}))}
                mono?
              />
            </dl>
            <div :if={plan_capabilities(@plan) != []} class="mt-4 space-y-2 text-xs">
              <p class="font-medium text-base-content/60">Adapter capabilities</p>
              <div
                :for={{target_id, capabilities} <- plan_capabilities(@plan)}
                class="rounded-box border border-base-content/10 p-3"
              >
                <p class="truncate font-mono">{target_id}</p>
                <p class="mt-1 break-all font-mono text-base-content/55">{json_text(capabilities)}</p>
              </div>
            </div>
            <div
              :if={plan_actions(@plan) != []}
              class="mt-4 max-h-64 overflow-y-auto rounded-box border border-base-content/10"
              data-testid="rebuild-plan-actions"
            >
              <div
                :for={action <- plan_actions(@plan)}
                class="border-b border-base-content/10 p-3 text-xs last:border-b-0"
              >
                <div class="flex items-center justify-between gap-2">
                  <span class="min-w-0 truncate font-mono">{field(action, :target_id)}</span>
                  <span class={state_badge(field(action, :action))}>
                    {humanize(field(action, :action))}
                  </span>
                </div>
                <p class="mt-1 text-base-content/55">{action_reason(action)}</p>
                <dl class="mt-2 grid gap-1 text-base-content/50">
                  <.action_detail label="Mapping proof" value={field(action, :mapping_proof)} />
                  <.action_detail
                    label="Pinned inputs"
                    value={field(action, :pinned_input_generation_ids, [])}
                  />
                  <.action_detail
                    label="Candidate generation"
                    value={
                      action
                      |> field(:candidate_generation, %{})
                      |> field(:target_generation_id)
                    }
                  />
                </dl>
              </div>
            </div>
            <button
              :if={@plan |> field(:permissions, %{}) |> field(:start, false)}
              type="button"
              phx-click="start_rebuild"
              class="btn btn-warning mt-4 w-full"
              data-confirm="Start this reviewed rebuild plan?"
              data-testid="start-rebuild"
            >
              Approve and start
            </button>
          </div>
        </GlassPanel.glass_panel>

        <GlassPanel.glass_panel class="min-w-0 p-5 sm:p-6">
          <div class="flex items-center justify-between gap-3">
            <div>
              <p class="text-xs font-semibold uppercase tracking-[0.18em] text-base-content/50">
                Operations
              </p>
              <p class="mt-1 text-sm text-base-content/60">Newest first</p>
            </div>
          </div>

          <div :if={@operations == []} class="py-16 text-center text-sm text-base-content/55">
            No rebuild operations yet.
          </div>

          <div :if={@operations != []} class="mt-4 divide-y divide-base-content/10">
            <.link
              :for={operation <- @operations}
              navigate={~p"/rebuilds/#{field(operation, :operation_id)}"}
              class="grid gap-2 py-4 transition hover:bg-base-content/[0.025] sm:grid-cols-[minmax(0,1fr)_auto] sm:items-center"
              data-testid="rebuild-operation"
            >
              <div class="min-w-0">
                <div class="flex flex-wrap items-center gap-2">
                  <p class="truncate font-medium">{field(operation, :root_target_id)}</p>
                  <span class={state_badge(field(operation, :state))}>{humanize(
                    field(operation, :state)
                  )}</span>
                </div>
                <p class="mt-1 truncate font-mono text-xs text-base-content/45">
                  {field(operation, :operation_id)}
                </p>
                <p class="mt-1 truncate text-xs text-base-content/55">{field(operation, :reason)}</p>
              </div>
              <div class="text-left text-xs text-base-content/55 sm:text-right">
                <p>{progress_label(field(operation, :progress, %{}))}</p>
                <p class="mt-1">{format_time(field(operation, :updated_at))}</p>
              </div>
            </.link>
          </div>

          <button
            :if={@has_more?}
            type="button"
            phx-click="load_more"
            class="btn btn-ghost btn-sm mt-4 w-full"
            data-testid="load-more-rebuilds"
          >
            Load more
          </button>
        </GlassPanel.glass_panel>
      </div>
    </AppShell.app_shell>
    """
  end

  attr :operation, :map, required: true
  attr :items, :list, required: true
  attr :items_has_more?, :boolean, default: false
  attr :error, :string, default: nil

  def rebuild_detail_page(assigns) do
    ~H"""
    <AppShell.app_shell
      title={field(@operation, :root_target_id, "Rebuild")}
      subtitle={field(@operation, :operation_id)}
      status={humanize(field(@operation, :state))}
      status_tone={state_tone(field(@operation, :state))}
      nav_items={nav_items()}
      back_href={~p"/rebuilds"}
      back_label="Rebuilds"
    >
      <div class="mx-auto w-full max-w-[120rem] space-y-5" data-testid="rebuild-detail-page">
        <p
          :if={@error}
          class="rounded-box border border-error/25 bg-error/5 p-4 text-sm text-error"
          data-testid="rebuild-error"
        >
          {@error}
        </p>

        <GlassPanel.glass_panel class="p-5 sm:p-6">
          <div class="grid gap-5 lg:grid-cols-[minmax(0,1fr)_auto]">
            <dl class="grid gap-4 text-xs sm:grid-cols-2 xl:grid-cols-4">
              <.fact label="Phase" value={humanize(field(@operation, :phase))} />
              <.fact label="Progress" value={progress_label(field(@operation, :progress, %{}))} />
              <.fact label="Actions" value={field(@operation, :action_count, 0)} />
              <.fact label="Windows" value={field(@operation, :window_count, 0)} />
              <.fact label="Active generation" value={field(@operation, :active_generation_id)} mono? />
              <.fact
                label="Candidate generation"
                value={field(@operation, :candidate_generation_id)}
                mono?
              />
              <.fact label="Plan hash" value={field(@operation, :plan_hash)} mono? />
              <.fact label="Started" value={format_time(field(@operation, :started_at))} />
              <.fact label="Completed" value={format_time(field(@operation, :completed_at))} />
              <.fact label="Cleanup" value={humanize(field(@operation, :cleanup_state))} />
            </dl>

            <div class="flex flex-wrap content-start justify-start gap-2 lg:max-w-64 lg:justify-end">
              <button
                :if={permitted?(@operation, :start)}
                phx-click="start_rebuild"
                class="btn btn-warning btn-sm"
                data-confirm="Start this reviewed rebuild plan?"
                data-testid="start-rebuild"
              >Start</button>
              <button
                :if={permitted?(@operation, :retry)}
                phx-click="retry_rebuild"
                class="btn btn-primary btn-sm"
                data-confirm="Retry safe failed rebuild work?"
                data-testid="retry-rebuild"
              >Retry</button>
              <button
                :if={permitted?(@operation, :reconcile)}
                phx-click="reconcile_rebuild"
                class="btn btn-info btn-sm"
                data-testid="reconcile-rebuild"
              >Reconcile</button>
            </div>
          </div>

          <.form
            :if={permitted?(@operation, :cancel)}
            for={%{}}
            as={:cancel}
            phx-submit="cancel_rebuild"
            class="mt-5 flex flex-col gap-2 border-t border-base-content/10 pt-5 sm:flex-row"
          >
            <input
              name="cancel[reason]"
              required
              maxlength="4096"
              placeholder="Cancellation reason"
              class="input input-bordered input-sm min-w-0 flex-1 bg-base-100/30"
              data-testid="cancel-rebuild-reason"
            />
            <button
              class="btn btn-error btn-outline btn-sm"
              data-confirm="Request rebuild cancellation?"
              data-testid="cancel-rebuild"
            >
              Cancel rebuild
            </button>
          </.form>
        </GlassPanel.glass_panel>

        <GlassPanel.glass_panel
          :if={present?(field(@operation, :terminal_error))}
          class="border border-error/25 p-5 text-sm"
        >
          <p class="font-medium text-error">Terminal error</p>
          <p class="mt-2 text-base-content/70">{error_label(field(@operation, :terminal_error))}</p>
        </GlassPanel.glass_panel>

        <GlassPanel.glass_panel
          :if={present?(field(@operation, :unknown_outcome))}
          class="border border-warning/30 p-5 text-sm"
        >
          <p class="font-medium text-warning">Outcome needs reconciliation</p>
          <p class="mt-2 break-all font-mono text-xs text-base-content/65">
            {json_text(field(@operation, :unknown_outcome))}
          </p>
        </GlassPanel.glass_panel>

        <GlassPanel.glass_panel
          :if={present?(field(@operation, :validation_result))}
          class="p-5 text-sm"
        >
          <p class="font-medium">Candidate validation</p>
          <p class="mt-2 break-all font-mono text-xs text-base-content/65">
            {json_text(field(@operation, :validation_result))}
          </p>
        </GlassPanel.glass_panel>

        <GlassPanel.glass_panel
          :if={field(@operation, :actions, []) != []}
          class="min-w-0 p-5 sm:p-6"
        >
          <p class="text-xs font-semibold uppercase tracking-[0.18em] text-base-content/50">
            Downstream actions
          </p>
          <div class="mt-4 divide-y divide-base-content/10">
            <div :for={action <- field(@operation, :actions, [])} class="py-4 text-xs">
              <div class="flex flex-wrap items-center gap-2">
                <p class="min-w-0 flex-1 truncate font-mono">{field(action, :target_id)}</p>
                <span class="badge badge-ghost badge-sm">{humanize(field(action, :action))}</span>
                <span class={state_badge(field(action, :status))}>
                  {humanize(field(action, :status))}
                </span>
              </div>
              <dl class="mt-2 grid gap-1 text-base-content/55 sm:grid-cols-2">
                <.action_detail label="Progress" value={field(action, :progress, %{})} />
                <.action_detail label="Validation" value={field(action, :validation_result)} />
                <.action_detail label="Failure" value={field(action, :terminal_error)} />
                <.action_detail label="Cleanup" value={field(action, :cleanup_state)} />
              </dl>
            </div>
          </div>
        </GlassPanel.glass_panel>

        <GlassPanel.glass_panel class="min-w-0 p-5 sm:p-6">
          <p class="text-xs font-semibold uppercase tracking-[0.18em] text-base-content/50">
            Logical work items
          </p>
          <div :if={@items == []} class="py-12 text-center text-sm text-base-content/55">
            No logical work items.
          </div>
          <div :if={@items != []} class="mt-4 overflow-x-auto">
            <table class="table table-sm">
              <thead>
                <tr>
                  <th>Target</th><th>Window</th><th>Status</th><th>Attempts</th><th>Rows</th>
                </tr>
              </thead>
              <tbody>
                <tr :for={item <- @items} data-testid="rebuild-item">
                  <td class="max-w-72 truncate">{field(item, :target_id)}</td>
                  <td class="font-mono text-xs">{field(item, :window_key) || "full generation"}</td>
                  <td>
                    <span class={state_badge(field(item, :status))}>{humanize(field(item, :status))}</span>
                  </td>
                  <td>{field(item, :attempt_count, 0)}</td>
                  <td>{field(item, :row_count, "-")}</td>
                </tr>
              </tbody>
            </table>
          </div>
          <button
            :if={@items_has_more?}
            phx-click="load_more_items"
            class="btn btn-ghost btn-sm mt-4 w-full"
            data-testid="load-more-rebuild-items"
          >Load more</button>
        </GlassPanel.glass_panel>
      </div>
    </AppShell.app_shell>
    """
  end

  attr :label, :string, required: true
  attr :value, :any, default: nil
  attr :mono?, :boolean, default: false

  defp fact(assigns) do
    ~H"""
    <div class="min-w-0">
      <dt class="text-base-content/45">{@label}</dt>
      <dd class={["mt-1 break-all text-base-content/80", @mono? && "font-mono"]}>{@value || "-"}</dd>
    </div>
    """
  end

  attr :label, :string, required: true
  attr :value, :any, default: nil

  defp action_detail(assigns) do
    ~H"""
    <div :if={present?(@value)} class="grid grid-cols-[8rem_minmax(0,1fr)] gap-2">
      <dt>{@label}</dt>
      <dd class="break-all font-mono">{json_text(@value)}</dd>
    </div>
    """
  end

  def nav_items, do: AssetCataloguePage.nav_items(:rebuilds)

  defp field(map, key, default \\ nil)

  defp field(map, key, default) when is_map(map) and is_atom(key),
    do: Map.get(map, key, Map.get(map, Atom.to_string(key), default))

  defp field(map, key, default) when is_map(map), do: Map.get(map, key, default)
  defp field(_map, _key, default), do: default

  defp permitted?(operation, action),
    do: operation |> field(:permissions, %{}) |> field(action, false)

  defp present?(nil), do: false
  defp present?(map) when is_map(map), do: map_size(map) > 0
  defp present?(value), do: value != ""

  defp progress_label(progress) when is_map(progress) do
    completed = field(progress, :completed, 0)
    total = field(progress, :total, 0)
    "#{completed} / #{total}"
  end

  defp progress_label(_progress), do: "0 / 0"

  defp plan_actions(plan), do: plan |> field(:payload, %{}) |> field(:actions, [])

  defp plan_capabilities(plan) do
    plan
    |> field(:payload, %{})
    |> field(:capabilities, %{})
    |> Enum.sort_by(&elem(&1, 0))
  end

  defp plan_coverage(plan, key) do
    coverage = plan |> field(:payload, %{}) |> field(:coverage, %{})

    case field(coverage, key) do
      period when is_map(period) -> period_range(period)
      value -> humanize(value)
    end
  end

  defp plan_range(plan) do
    plan
    |> field(:payload, %{})
    |> field(:evaluated_range, %{})
    |> period_range()
  end

  defp plan_availability_delay(plan) do
    seconds =
      plan |> field(:payload, %{}) |> field(:coverage, %{}) |> field(:availability_delay_seconds)

    if is_integer(seconds), do: "#{seconds} seconds", else: "-"
  end

  defp period_range(period) when is_map(period) do
    case {field(period, :start_at), field(period, :end_at)} do
      {nil, nil} -> "-"
      {start_at, end_at} -> "#{format_time(start_at)} – #{format_time(end_at)}"
    end
  end

  defp root_binding_value(plan, key, default \\ nil) do
    payload = field(plan, :payload, %{})
    root_target_id = field(payload, :root_target_id)

    payload
    |> field(:binding_snapshot, %{})
    |> field(root_target_id, %{})
    |> field(key, default)
  end

  defp action_reason(action) do
    case field(action, :reason) do
      reason when is_map(reason) ->
        field(reason, :reason_code) || field(reason, :kind) || "Planned downstream impact"

      reason when is_binary(reason) ->
        reason

      _reason ->
        "Planned downstream impact"
    end
  end

  defp format_time(%DateTime{} = value), do: Calendar.strftime(value, "%Y-%m-%d %H:%M UTC")
  defp format_time(value) when is_binary(value), do: value
  defp format_time(_value), do: "-"

  defp humanize(nil), do: "-"

  defp humanize(value),
    do: value |> to_string() |> String.replace("_", " ") |> String.capitalize()

  defp state_badge(state),
    do: ["badge badge-soft badge-sm", state_badge_tone(state)]

  defp state_badge_tone(state) when state in [:succeeded, :ready], do: "badge-success"

  defp state_badge_tone(state) when state in [:failed, :activation_unknown, :outcome_unknown],
    do: "badge-error"

  defp state_badge_tone(state)
       when state in [:building, :validating, :activating, :running, :claimed], do: "badge-info"

  defp state_badge_tone(state) when state in [:cancelling, :cancelled], do: "badge-warning"
  defp state_badge_tone(_state), do: "badge-neutral"

  defp state_tone(:succeeded), do: :success
  defp state_tone(state) when state in [:failed, :activation_unknown], do: :error
  defp state_tone(state) when state in [:cancelling, :cancelled], do: :warning
  defp state_tone(_state), do: :info

  defp error_label(error) when is_map(error),
    do: field(error, :message) || field(error, :code) || "Rebuild failed"

  defp error_label(error) when is_binary(error), do: error
  defp error_label(_error), do: "Rebuild failed"

  defp json_text(value) when value in [nil, %{}], do: "-"
  defp json_text(value) when is_binary(value), do: value
  defp json_text(value), do: JSON.encode!(value)
end
