defmodule FavnView.Components.SelectedWindowActions do
  @moduledoc """
  Compact action strip for the selected asset timeline window.
  """

  use FavnView, :html

  attr :selected_window, :map, default: nil
  attr :can_run_asset?, :boolean, default: true
  attr :has_data_windows?, :boolean, default: false
  attr :active_timeline, :atom, default: :refresh
  attr :run_config_open?, :boolean, default: false

  attr :run_config, :map,
    default: %{
      dependencies: "all",
      refresh: "auto",
      source: nil,
      kind: "",
      value: "",
      to: "",
      timezone: "Etc/UTC"
    }

  attr :submitting_window_run?, :boolean, default: false
  attr :selected_window_error, :string, default: nil
  attr :submitted_run_id, :string, default: nil
  attr :can_submit_runs?, :boolean, default: false

  def selected_window_actions(assigns) do
    ~H"""
    <div
      class="grid gap-3 rounded-box border border-base-content/10 bg-base-content/[0.04] p-4 sm:grid-cols-[1fr_auto] sm:items-center"
      data-testid="selected-window-actions"
    >
      <div class="min-w-0">
        <p class="text-xs uppercase tracking-[0.18em] text-base-content/45">Run asset</p>
        <p class="mt-1 text-sm font-medium text-base-content">{selection_label(@selected_window)}</p>
        <p class="mt-0.5 text-xs text-base-content/55">{status_label(@selected_window)}</p>
        <p
          :if={@selected_window && !@selected_window.run_enabled?}
          class="mt-1 text-xs text-base-content/45"
        >
          {run_disabled_reason_label(@selected_window.run_disabled_reason)}
        </p>
        <p
          :if={@selected_window_error}
          class="mt-1 text-xs text-error"
          data-testid="selected-window-error"
        >
          {@selected_window_error}
        </p>
        <p :if={@submitted_run_id} class="mt-1 text-xs text-success" data-testid="submitted-run-id">
          Submitted {@submitted_run_id}
        </p>
      </div>

      <div class="flex w-full shrink-0 justify-end gap-2 sm:w-auto">
        <button
          type="button"
          class="btn btn-primary btn-soft btn-sm"
          phx-click="open_run_config"
          disabled={
            !@can_submit_runs? || !@can_run_asset? ||
              (@selected_window && !@selected_window.run_enabled?) ||
              @submitting_window_run?
          }
          data-testid="run-selected-window"
        >
          <span :if={@submitting_window_run?} class="loading loading-spinner loading-xs"></span>
          Run asset
        </button>
      </div>

      <.run_config_panel
        :if={@run_config_open?}
        selected_window={@selected_window}
        has_data_windows?={@has_data_windows?}
        active_timeline={@active_timeline}
        run_config={@run_config}
        submitting_window_run?={@submitting_window_run?}
        can_submit_runs?={@can_submit_runs?}
      />
    </div>
    """
  end

  attr :selected_window, :map, default: nil
  attr :has_data_windows?, :boolean, default: false
  attr :active_timeline, :atom, default: :refresh
  attr :run_config, :map, required: true
  attr :submitting_window_run?, :boolean, default: false
  attr :can_submit_runs?, :boolean, default: false

  def run_config_panel(assigns) do
    ~H"""
    <div
      class="mt-2 w-full rounded-box border border-primary/20 bg-base-100/70 p-4 shadow-lg shadow-primary/5 sm:col-span-2"
      data-testid="run-config-panel"
    >
      <.form
        for={%{}}
        as={:run_config}
        phx-change="change_run_config"
        phx-submit="run_selected_window"
        class="space-y-4"
        data-testid="run-config-form"
      >
        <div class="flex items-start justify-between gap-3">
          <div>
            <h3 class="text-sm font-medium text-base-content">Run plan</h3>
            <p class="mt-1 text-xs text-base-content/55">
              {run_plan_description(@selected_window)}
            </p>
          </div>
          <button
            type="button"
            class="btn btn-ghost btn-xs"
            phx-click="close_run_config"
            disabled={@submitting_window_run?}
            data-testid="close-run-config"
          >
            Close
          </button>
        </div>

        <fieldset class="fieldset">
          <legend class="fieldset-legend">Plan scope / dependencies</legend>
          <.radio_card
            name="run_config[dependencies]"
            value="all"
            checked?={@run_config.dependencies == "all"}
            title="Include upstream dependencies"
            description="Default. Plan the selected asset/window with its supported upstream graph."
          />
          <.radio_card
            name="run_config[dependencies]"
            value="none"
            checked?={@run_config.dependencies == "none"}
            title="Only this asset/window"
            description="Plan only the selected target and window."
          />
        </fieldset>

        <fieldset :if={window_context_enabled?(@has_data_windows?, @selected_window)} class="fieldset">
          <legend class="fieldset-legend">{window_context_legend(@active_timeline)}</legend>
          <input
            type="hidden"
            name="run_config[source]"
            value={@run_config.source || default_source(@active_timeline)}
          />
          <div class="grid gap-3 sm:grid-cols-[8rem_1fr_1fr_10rem]">
            <label class="form-control">
              <span class="label-text text-xs">Kind</span>
              <select
                name="run_config[kind]"
                class="select select-bordered select-sm"
                disabled={@submitting_window_run? || !is_nil(@selected_window)}
                data-testid="run-config-window-kind"
              >
                <option value="hour" selected={@run_config.kind == "hour"}>Hour</option>
                <option value="day" selected={@run_config.kind == "day"}>Day</option>
                <option value="month" selected={@run_config.kind == "month"}>Month</option>
                <option value="year" selected={@run_config.kind == "year"}>Year</option>
              </select>
            </label>
            <label class="form-control">
              <span class="label-text text-xs">From</span>
              <input
                type="text"
                name="run_config[value]"
                value={@run_config.value}
                class="input input-bordered input-sm"
                placeholder="YYYY-MM-DD, YYYY-MM, or YYYY"
                disabled={@submitting_window_run? || !is_nil(@selected_window)}
                data-testid="run-config-window-value"
              />
            </label>
            <label class="form-control">
              <span class="label-text text-xs">To</span>
              <input
                type="text"
                name="run_config[to]"
                value={Map.get(@run_config, :to, "")}
                class="input input-bordered input-sm"
                placeholder="Optional end"
                disabled={@submitting_window_run? || !is_nil(@selected_window)}
                data-testid="run-config-window-to"
              />
            </label>
            <label class="form-control">
              <span class="label-text text-xs">Timezone</span>
              <input
                type="text"
                name="run_config[timezone]"
                value={@run_config.timezone || "Etc/UTC"}
                class="input input-bordered input-sm"
                disabled={@submitting_window_run? || !is_nil(@selected_window)}
                data-testid="run-config-window-timezone"
              />
            </label>
          </div>
          <p class="mt-2 text-xs text-base-content/55">
            {window_context_description(@selected_window, @active_timeline)}
          </p>
          <p :if={is_nil(@selected_window)} class="mt-1 text-xs text-base-content/55">
            Range backfills default to missing refresh; choose force explicitly to recompute existing successful windows.
          </p>
        </fieldset>

        <fieldset class="fieldset">
          <legend class="fieldset-legend">Refresh behavior</legend>
          <.radio_card
            name="run_config[refresh]"
            value="auto"
            checked?={@run_config.refresh == "auto"}
            title="Auto - obey freshness"
            description="Default. Let backend freshness policies decide which nodes run or skip."
          />
          <.radio_card
            name="run_config[refresh]"
            value="missing"
            checked?={@run_config.refresh == "missing"}
            title="Run missing only"
            description="Run nodes without prior successful freshness state."
          />
          <.radio_card
            name="run_config[refresh]"
            value="force_selected"
            checked?={@run_config.refresh == "force_selected"}
            title="Force selected asset"
            description="Run the selected asset even when backend freshness says it is current. Upstream assets are not forced."
          />
          <.radio_card
            name="run_config[refresh]"
            value="force_selected_upstream"
            checked?={@run_config.refresh == "force_selected_upstream"}
            title="Force selected + upstream dependencies"
            description="Force the selected asset and its planned upstream dependencies. Upstream changes can cause downstream nodes in the planned graph to rerun."
          />
          <.radio_card
            name="run_config[refresh]"
            value="force_all"
            checked?={@run_config.refresh == "force_all"}
            title="Force full planned graph"
            description="Run every node in the planned graph regardless of stored freshness."
          />
        </fieldset>

        <div class="rounded-box border border-warning/20 bg-warning/10 p-3 text-xs text-base-content/70">
          Forcing upstream assets can change inputs and cause downstream assets to rerun. Forcing only the selected asset does not automatically rerun upstream assets unless upstream dependencies are included.
        </div>

        <div class="flex justify-end gap-2">
          <button
            type="button"
            class="btn btn-ghost btn-sm"
            phx-click="close_run_config"
            disabled={@submitting_window_run?}
          >
            Cancel
          </button>
          <button
            type="submit"
            class="btn btn-primary btn-sm"
            disabled={!@can_submit_runs? || @submitting_window_run?}
            phx-disable-with="Submitting..."
            data-testid="submit-run-config"
          >
            <span :if={@submitting_window_run?} class="loading loading-spinner loading-xs"></span>
            Submit run
          </button>
        </div>
      </.form>
    </div>
    """
  end

  attr :name, :string, required: true
  attr :value, :string, required: true
  attr :checked?, :boolean, default: false
  attr :title, :string, required: true
  attr :description, :string, required: true

  def radio_card(assigns) do
    ~H"""
    <label class="mt-2 flex cursor-pointer gap-3 rounded-box border border-base-content/10 bg-base-content/[0.025] p-3 text-sm hover:border-primary/30">
      <input
        type="radio"
        name={@name}
        value={@value}
        checked={@checked?}
        class="radio radio-primary radio-sm mt-0.5"
      />
      <span>
        <span class="block font-medium text-base-content">{@title}</span>
        <span class="mt-0.5 block text-xs leading-5 text-base-content/55">{@description}</span>
      </span>
    </label>
    """
  end

  defp selection_label(nil), do: "No timeline context selected. The run will use default config."
  defp selection_label(window), do: window.range_label

  defp window_context_enabled?(true, _selected_window), do: true

  defp window_context_enabled?(_has_data_windows?, selected_window),
    do: not is_nil(selected_window)

  defp window_context_legend(:data_coverage), do: "Data window"
  defp window_context_legend(_active_timeline), do: "Run period"

  defp window_context_description(nil, :data_coverage),
    do:
      "Choose one exact data window or an inclusive range. Lookback is not added to target windows."

  defp window_context_description(nil, _active_timeline),
    do: "Choose one run period or an inclusive range. Asset lookback can expand from each period."

  defp window_context_description(window, _active_timeline),
    do: "Prefilled from #{window.range_label}. Clear the timeline selection to edit this context."

  defp default_source(:data_coverage), do: "data_coverage_timeline"
  defp default_source(_active_timeline), do: "refresh_timeline"

  defp run_plan_description(nil), do: "Submit a planned graph for this asset."

  defp run_plan_description(window),
    do: "Submit a planned graph using #{window.range_label} as editable context."

  defp status_label(nil), do: "Default run config"
  defp status_label(%{status_label: label}), do: label
  defp status_label(%{status: :success}), do: "Fresh"
  defp status_label(%{status: :warning}), do: "Running"
  defp status_label(%{status: :error}), do: "Failed"
  defp status_label(%{status: :muted}), do: "Unknown / never run"
  defp status_label(_status), do: "Unknown"

  defp run_disabled_reason_label(:asset_has_no_window_policy), do: "No window policy"
  defp run_disabled_reason_label(:invalid_window), do: "Invalid window"
  defp run_disabled_reason_label(_reason), do: "Not runnable"
end
