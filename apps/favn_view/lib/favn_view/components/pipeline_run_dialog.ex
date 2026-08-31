defmodule FavnView.Components.PipelineRunDialog do
  @moduledoc """
  The dialog that submits one pipeline run.

  Render it at page level, through the shell's `:overlay` slot; see
  `FavnView.UI.Dialog` for why a dialog cannot live inside a panel.

  It leads with what the pipeline declares — the period it runs, its dependency
  mode, its concurrency — because the manifest owns those values and an operator
  can read them nowhere else in the product. Nothing here edits the pipeline: the
  disclosure overrides the declared values for one submission, and every override
  marks its summary row, so a deviation cannot hide inside a closed disclosure.

  ## Backfilling is a period, not a mode

  An empty period asks the control plane for the latest complete one, a single
  period runs that window, and a range runs every period in it — which is a
  different command, chosen by the page from
  `FavnView.PipelineRunConfig.range_requested?/1`. A pipeline that declares no
  window has no period to override, so it shows no period controls and can reach
  no backfill.

  The period *size* is not offered. A pipeline declares one window kind and the
  control plane rejects a submission naming another, so a size control could only
  mislead: discarded when no period was named, refused when one was.
  """

  use FavnView, :html

  alias FavnView.PipelineRunConfig

  attr :pipeline, :map, required: true
  attr :run_config, :map, required: true, doc: "see `FavnView.PipelineRunConfig`"
  attr :defaults, :map, required: true, doc: "what the pipeline declares, for the changed marks"
  attr :advanced_open?, :boolean, default: false
  attr :valid?, :boolean, default: true, doc: "whether the configuration may be submitted"
  attr :error, :string, default: nil, doc: "why the configuration cannot be submitted"
  attr :can_submit_runs?, :boolean, default: false

  def pipeline_run_dialog(assigns) do
    assigns =
      assigns
      |> assign(:changed, PipelineRunConfig.changed_fields(assigns.run_config, assigns.defaults))
      |> assign(:windowed?, windowed?(assigns.pipeline))
      |> assign(:range?, PipelineRunConfig.range_requested?(assigns.run_config))

    ~H"""
    <.dialog
      id="pipeline-run-dialog"
      open?={true}
      title={"Run #{@pipeline.name}"}
      subtitle={@pipeline.label}
      size={:lg}
      on_close="close_run_dialog"
    >
      <.form
        id="pipeline-run-form"
        for={%{}}
        as={:run_config}
        phx-change="change_run_config"
        phx-submit="submit_pipeline_run"
        data-command-operation="pipeline_run_submit"
        data-command-operation-present-field="run_config[to]"
        data-command-operation-present="pipeline_backfill_submit"
        data-command-resource={@pipeline.id}
        data-testid="pipeline-run-form"
      >
        <div data-testid="pipeline-run-summary">
          <.field_row label={(@windowed? && "Period") || "Runs"}>
            {period_summary(assigns)}
            <.changed_badge :if={:period in @changed} />
          </.field_row>

          <.field_row label="Assets">{assets_summary(@pipeline)}</.field_row>

          <.field_row label="Refresh">
            {refresh_summary(@run_config.refresh)}
            <.changed_badge :if={:refresh in @changed} />
          </.field_row>

          <.field_row :if={@range?} label="Periods">
            {combine_summary(@run_config.combine_windows)}
            <.changed_badge :if={:combine_windows in @changed} />
          </.field_row>

          <.field_row :if={concurrency_summary(@pipeline)} label="Concurrency">
            {concurrency_summary(@pipeline)}
          </.field_row>
        </div>

        <.disclosure
          label="Change how it runs"
          open?={@advanced_open?}
          class="mt-4"
          data-testid="pipeline-run-advanced"
        >
          <input type="hidden" name="run_config[timezone]" value={@run_config.timezone} />

          <fieldset :if={@windowed?} class="fieldset" data-testid="pipeline-run-period">
            <legend class="fieldset-legend">Which {kind_word(@run_config.kind)}s to run</legend>

            <div class="grid gap-3 sm:grid-cols-2">
              <.input
                id="pipeline-run-from"
                label="From"
                name="run_config[from]"
                value={@run_config.from}
                class="w-full input input-sm favn-surface-control"
                placeholder={period_placeholder(@run_config.kind)}
              />
              <.input
                id="pipeline-run-to"
                label="To"
                name="run_config[to]"
                value={@run_config.to}
                class="w-full input input-sm favn-surface-control"
                placeholder="Optional end"
              />
            </div>

            <p class="mt-1 text-sm favn-text-muted">
              Leave both empty to run the last complete {kind_word(@run_config.kind)}. Fill
              <span class="font-mono">From</span>
              alone to run one, or both to run every {kind_word(@run_config.kind)} between them.
            </p>
          </fieldset>

          <fieldset class="fieldset">
            <legend class="fieldset-legend">Whether to rerun what is already current</legend>

            <.radio_card
              name="run_config[refresh]"
              value="auto"
              checked?={@run_config.refresh == "auto"}
              title="Obey freshness"
              description="What the pipeline declares. Skips whatever the backend already considers current."
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
              value="force_all"
              checked?={@run_config.refresh == "force_all"}
              title="Force everything planned"
              description="Runs every asset in the plan, whatever its freshness says."
            />
          </fieldset>

          <.input
            :if={@range?}
            id="pipeline-run-combine-windows"
            type="checkbox"
            name="run_config[combine_windows]"
            label="Combine windows"
            tooltip="Run all selected windows in one child run instead of creating one child run per window."
            checked={@run_config.combine_windows}
            data-testid="pipeline-run-combine-windows"
          />
        </.disclosure>

        <.notice
          :if={PipelineRunConfig.forces?(@run_config)}
          tone={:warning}
          class="mt-4"
          data-testid="pipeline-run-force-notice"
        >
          {force_warning(assigns)}
        </.notice>

        <!-- Said in the dialog rather than left to a disabled button, because a control
        that refuses without a reason reads as broken. -->
        <p
          :if={!@can_submit_runs?}
          class="mt-4 text-sm favn-text-muted"
          data-testid="pipeline-run-not-permitted"
        >
          Running a pipeline needs an operator account. You can read what it would run here, but not queue it.
        </p>

        <.notice :if={@error} tone={:error} class="mt-4" data-testid="pipeline-run-error">
          {@error}
        </.notice>
      </.form>

      <:actions>
        <.button
          :if={@changed != []}
          variant={:ghost}
          phx-click="reset_run_config"
          class="mr-auto"
          data-testid="reset-run-config"
        >
          Reset to defaults
        </.button>
        <.button variant={:ghost} phx-click="close_run_dialog" data-testid="close-run-dialog">
          Cancel
        </.button>
        <!-- `phx-disable-with` repeats the label rather than replacing it, so the
        button keeps its width while the submission is in flight. -->
        <.button
          variant={:solid}
          type="submit"
          form="pipeline-run-form"
          disabled={!@can_submit_runs? || !@valid?}
          phx-disable-with={submit_label(assigns)}
          data-testid="submit-pipeline-run"
        >
          {submit_label(assigns)}
        </.button>
      </:actions>
    </.dialog>
    """
  end

  defp changed_badge(assigns) do
    ~H"""
    <.badge
      tone={:warning}
      variant={:outline}
      size={:xs}
      class="ml-2"
      data-testid="run-config-changed"
    >
      Changed
    </.badge>
    """
  end

  @doc """
  What the submit button will do, said as the action itself.

  A button named for its screen — "Run pipeline" — tells an operator nothing
  about which periods are about to run, which is the one thing worth confirming.

  ## Examples

      iex> alias FavnView.Components.PipelineRunDialog
      iex> PipelineRunDialog.submit_label(%{run_config: %{from: "2026-03", to: ""}, windowed?: true})
      "Run 2026-03"

      iex> alias FavnView.Components.PipelineRunDialog
      iex> PipelineRunDialog.submit_label(%{run_config: %{from: "2026-01", to: "2026-08"}, windowed?: true})
      "Backfill 2026-01 to 2026-08"
  """
  @spec submit_label(map()) :: String.t()
  def submit_label(%{run_config: %{to: to, from: from}}) when to != "" and to != nil,
    do: "Backfill #{from} to #{to}"

  def submit_label(%{run_config: %{from: from}}) when from != "" and from != nil,
    do: "Run #{from}"

  def submit_label(%{windowed?: true, run_config: config}),
    do: "Run the last complete #{kind_word(config.kind)}"

  def submit_label(_assigns), do: "Run pipeline"

  defp period_summary(%{windowed?: false}), do: "The whole relation, every run"

  defp period_summary(%{run_config: %{from: from, to: to}}) when to != "" and from != "",
    do: "#{from} to #{to}"

  defp period_summary(%{run_config: %{from: from}}) when from != "", do: from

  defp period_summary(%{run_config: config}),
    do: "The last complete #{kind_word(config.kind)}, #{config.timezone}"

  defp assets_summary(%{dependencies: :none} = pipeline),
    do: "#{asset_count_label(pipeline)}, and nothing upstream"

  defp assets_summary(pipeline), do: "#{asset_count_label(pipeline)}, with what they read"

  defp asset_count_label(%{asset_count: 1}), do: "1 asset"
  defp asset_count_label(%{asset_count: count}), do: "#{count} assets"

  # The same words the radio cards use, so the summary and the control an
  # operator just changed cannot describe one choice two ways.
  defp refresh_summary("missing"), do: "Run missing only"
  defp refresh_summary("force_all"), do: "Force everything planned"
  defp refresh_summary(_refresh), do: "Obey freshness"

  defp combine_summary(true), do: "Combined into one child run"
  defp combine_summary(_combine), do: "One child run per period"

  defp concurrency_summary(%{max_concurrency: max, execution_pool: pool})
       when is_integer(max) and is_binary(pool),
       do: "#{max} at a time, on the #{pool} pool"

  defp concurrency_summary(%{max_concurrency: max}) when is_integer(max), do: "#{max} at a time"

  defp concurrency_summary(%{execution_pool: pool}) when is_binary(pool),
    do: "On the #{pool} pool"

  defp concurrency_summary(_pipeline), do: nil

  # The warning states the blast radius rather than the word "force", because the
  # cost of forcing is the asset count and the period count, and neither is
  # visible from the radio label that caused it.
  defp force_warning(%{range?: true, pipeline: pipeline, run_config: config}) do
    "Forcing recomputes #{asset_count_label(pipeline)} for every #{kind_word(config.kind)} " <>
      "from #{config.from} to #{config.to}, including periods that already succeeded."
  end

  defp force_warning(%{pipeline: pipeline}) do
    "Forcing recomputes #{asset_count_label(pipeline)} even where freshness says they are current."
  end

  defp windowed?(%{window: window}) when is_map(window), do: true
  defp windowed?(_pipeline), do: false

  defp kind_word("hour"), do: "hour"
  defp kind_word("day"), do: "day"
  defp kind_word("year"), do: "year"
  defp kind_word(_kind), do: "month"

  defp period_placeholder("hour"), do: "2026-01-31T13"
  defp period_placeholder("day"), do: "2026-01-31"
  defp period_placeholder("year"), do: "2026"
  defp period_placeholder(_kind), do: "2026-01"
end
