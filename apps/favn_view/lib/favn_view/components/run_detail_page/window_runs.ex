defmodule FavnView.Components.RunDetailPage.WindowRuns do
  @moduledoc """
  The child runs a backfill created, one per requested anchor.

  A window run is a different object from an asset attempt — it has its own id,
  its own status, and its own page — which is why it survived the collapse of the
  other modes into the flow.
  """

  use FavnView, :html

  attr :run, :map, required: true
  attr :selected_child_run_id, :string, default: nil

  def window_runs_panel(assigns) do
    ~H"""
    <section data-testid="window-runs-view">
      <.empty_state
        :if={@run.child_runs == []}
        icon="hero-rectangle-stack"
        title="No window runs"
        description="This run executes its assets directly rather than through per-window child runs."
      />

      <.panel :if={@run.child_runs != []} padding={:sm}>
        <:header title="Window runs" subtitle="One child run per requested backfill anchor." />
        <.data_table
          id="window-runs"
          rows={@run.child_runs}
          row_id={&"window-run-#{&1.id}"}
          row_navigate={&~p"/runs/#{&1.id}"}
          row_testid="window-run-row"
          row_class={&selected_class(&1.id == @selected_child_run_id)}
        >
          <:col :let={child} label="Window">
            <p class="font-medium">{child.window_label}</p>
            <.mono value={child.id} truncate />
          </:col>
          <:col :let={child} label="Status">
            <.status_badge tone={child.status_tone} label={child.status} size={:sm} />
          </:col>
          <:col :let={child} label="Assets">{child.assets}</:col>
          <:col :let={child} label="Duration">{child.duration}</:col>
          <:col :let={child} label="Outcome">
            <p class="mb-1 text-sm favn-text-subtle">{child.outcome}</p>
            <.outcome_meter
              segments={[
                %{tone: :success, count: child.succeeded_count, label: "ran"},
                %{tone: :neutral, count: child.skipped_count, label: "already fresh"},
                %{tone: :error, count: child.failed_count, label: "failed"},
                %{tone: :info, count: child.running_count, label: "running"},
                %{tone: :warning, count: child.queued_count, label: "queued"},
                %{tone: :neutral, count: child.planned_count, label: "planned"}
              ]}
              size={:sm}
              legend?={false}
              class="w-32"
            />
          </:col>
        </.data_table>
      </.panel>
    </section>
    """
  end

  defp selected_class(true), do: "bg-primary/10 outline outline-1 outline-primary/40"
  defp selected_class(false), do: nil
end
