defmodule FavnView.Components.RunConfigDialog do
  @moduledoc """
  The dialog that submits one asset run.

  Render it at page level, through the shell's `:overlay` slot; see
  `FavnView.UI.Dialog` for why a dialog cannot live inside a panel.

  It opens with the period the asset is due for already filled in, so an operator who
  wants that period never opens the advanced section. Editing the period is offered
  only to an asset that runs per window; a full-refresh asset replaces its whole
  relation on every run and has no period to choose.
  """

  use FavnView, :html

  attr :has_data_windows?, :boolean,
    default: false,
    doc: "whether the asset runs per window; only then is a run period editable"

  attr :run_config, :map, required: true
  attr :advanced_open?, :boolean, default: false
  attr :run_config_valid?, :boolean, default: true
  attr :submitting_window_run?, :boolean, default: false
  attr :error, :string, default: nil, doc: "why the configuration cannot be submitted"
  attr :can_submit_runs?, :boolean, default: false
  attr :command_resource, :string, required: true

  def run_config_dialog(assigns) do
    ~H"""
    <.dialog
      id="run-config-panel"
      open?={true}
      title="Run asset"
      subtitle="Submit a planned graph for this asset."
      on_close="close_run_config"
    >
      <.form
        id="run-config-form"
        for={%{}}
        as={:run_config}
        phx-change="change_run_config"
        phx-submit="submit_run"
        data-command-operation="asset_run_submit"
        data-command-operation-present-field="run_config[to]"
        data-command-operation-present="asset_backfill_submit"
        data-command-resource={@command_resource}
        class="space-y-4"
        data-testid="run-config-form"
      >
        <.field_row label="Dependencies">{dependencies_summary(@run_config)}</.field_row>

        <.field_row label="Refresh">{refresh_label(@run_config.refresh)}</.field_row>

        <.disclosure
          label="Change how it runs"
          open?={@advanced_open?}
          data-testid="run-config-advanced"
        >
          <fieldset class="fieldset">
            <legend class="fieldset-legend">What else runs</legend>

            <.radio_card
              name="run_config[dependencies]"
              value="all"
              checked?={@run_config.dependencies == "all"}
              title="This asset and what it reads"
              description="Default. Plans the assets upstream of this one as well, so it reads current inputs."
            />
            <.radio_card
              name="run_config[dependencies]"
              value="none"
              checked?={@run_config.dependencies == "none"}
              title="Only this asset"
              description="Plans this asset alone. Its inputs are whatever they already are."
            />
          </fieldset>

          <fieldset :if={@has_data_windows?} class="fieldset">
            <legend class="fieldset-legend">Run period</legend>

            <input
              type="hidden"
              name="run_config[source]"
              value={@run_config.source || "refresh_timeline"}
            />
            <div class="grid gap-3 sm:grid-cols-[8rem_1fr_1fr_10rem]">
              <label class="form-control">
                <span class="label-text text-sm">Kind</span>
                <select
                  name="run_config[kind]"
                  class="select select-bordered select-sm"
                  disabled={@submitting_window_run?}
                  data-testid="run-config-window-kind"
                >
                  <option value="hour" selected={@run_config.kind == "hour"}>Hour</option>

                  <option value="day" selected={@run_config.kind == "day"}>Day</option>

                  <option value="month" selected={@run_config.kind == "month"}>Month</option>

                  <option value="year" selected={@run_config.kind == "year"}>Year</option>
                </select>
              </label>

              <label class="form-control">
                <span class="label-text text-sm">From</span>
                <input
                  type="text"
                  name="run_config[value]"
                  value={@run_config.value}
                  class="input input-bordered input-sm"
                  placeholder="YYYY-MM-DD, YYYY-MM, or YYYY"
                  disabled={@submitting_window_run?}
                  data-testid="run-config-window-value"
                />
              </label>

              <label class="form-control">
                <span class="label-text text-sm">To</span>
                <input
                  type="text"
                  name="run_config[to]"
                  value={Map.get(@run_config, :to, "")}
                  class="input input-bordered input-sm"
                  placeholder="Optional end"
                  disabled={@submitting_window_run?}
                  data-testid="run-config-window-to"
                />
              </label>

              <label class="form-control">
                <span class="label-text text-sm">Timezone</span>
                <input
                  type="text"
                  name="run_config[timezone]"
                  value={@run_config.timezone || "Etc/UTC"}
                  class="input input-bordered input-sm"
                  disabled={@submitting_window_run?}
                  data-testid="run-config-window-timezone"
                />
              </label>
            </div>

            <p class="mt-2 text-sm favn-text-muted">
              Prefilled with the period this asset is due for. Leave "To" empty to run that
              one period, or set it to run every period through to another one.
            </p>

            <p class="mt-1 text-sm favn-text-muted">
              A range runs only the periods that are missing. Choose a force option below to
              recompute periods that already succeeded.
            </p>
          </fieldset>

          <fieldset class="fieldset">
            <legend class="fieldset-legend">Whether to rerun what is already current</legend>

            <.radio_card
              name="run_config[refresh]"
              value="auto"
              checked?={@run_config.refresh == "auto"}
              title="Obey freshness"
              description="Default. Skips whatever the backend already considers current."
            />
            <.radio_card
              name="run_config[refresh]"
              value="missing"
              checked?={@run_config.refresh == "missing"}
              title="Run missing only"
              description="Runs the periods that have never succeeded, and nothing else."
            />
            <.radio_card
              name="run_config[refresh]"
              value="force_selected"
              checked?={@run_config.refresh == "force_selected"}
              title="Force this asset"
              description="Runs this asset even when freshness says it is current. Upstream assets are left alone."
            />
            <.radio_card
              name="run_config[refresh]"
              value="force_selected_upstream"
              checked?={@run_config.refresh == "force_selected_upstream"}
              title="Force this asset and what it reads"
              description="Also forces the upstream assets. Changing their output can make other assets downstream rerun."
            />
            <.radio_card
              name="run_config[refresh]"
              value="force_all"
              checked?={@run_config.refresh == "force_all"}
              title="Force everything planned"
              description="Runs every asset in the plan, whatever its freshness says."
            />
          </fieldset>
        </.disclosure>

        <.notice :if={forces_upstream?(@run_config)} tone={:warning}>
          Forcing upstream assets changes their inputs, so downstream assets in the planned graph can rerun too.
        </.notice>

        <!-- Said in the dialog rather than left to a disabled button, because a control
        that refuses without a reason reads as broken. A title attribute is not the
        answer: it is unreliable on hover and absent on touch. -->
        <p
          :if={!@can_submit_runs?}
          class="favn-text-muted mt-4 text-sm"
          data-testid="run-config-not-permitted"
        >
          Running an asset needs an operator account. You can read the plan here, but not queue it.
        </p>

        <p :if={@error} class="mt-4 text-sm text-error" data-testid="run-config-error">
          {@error}
        </p>
      </.form>

      <:actions>
        <.button
          variant={:ghost}
          phx-click="close_run_config"
          disabled={@submitting_window_run?}
          data-testid="close-run-config"
        >
          Cancel
        </.button>

        <.button
          variant={:solid}
          type="submit"
          form="run-config-form"
          loading={@submitting_window_run?}
          disabled={!@can_submit_runs? || !@run_config_valid? || @submitting_window_run?}
          phx-disable-with="Submitting..."
          data-testid="submit-run-config"
        >
          Run asset
        </.button>
      </:actions>
    </.dialog>
    """
  end

  # The dialog leads with what will happen, so the two choices that matter are stated
  # as sentences before any control is shown. An operator who agrees with both never
  # opens the advanced section, which is the point of having one.
  defp dependencies_summary(%{dependencies: "none"}), do: "This asset only"
  defp dependencies_summary(_run_config), do: "With upstream dependencies"

  defp refresh_label("auto"), do: "Obey freshness"
  defp refresh_label("missing"), do: "Missing periods only"
  defp refresh_label("force_selected"), do: "Force this asset"
  defp refresh_label("force_selected_upstream"), do: "Force this asset and what it reads"
  defp refresh_label("force_all"), do: "Force everything planned"
  defp refresh_label(_refresh), do: "Obey freshness"

  # The warning is shown only when it applies. A caution that is always on screen is
  # read once and then never again.
  #
  # "Force everything planned" forces whatever the plan holds, so it reaches upstream
  # assets only when upstream assets were planned. With dependencies excluded the plan
  # is this asset alone, and warning about upstream would describe a graph that is not
  # being run. "Force this asset and what it reads" always plans upstream —
  # `FavnView.AssetRunConfig` refuses it otherwise.
  defp forces_upstream?(%{refresh: "force_selected_upstream"}), do: true

  defp forces_upstream?(%{refresh: "force_all"} = run_config),
    do: Map.get(run_config, :dependencies) == "all"

  defp forces_upstream?(_run_config), do: false
end
