defmodule FavnView.Components.RunDetailPage.Submission do
  @moduledoc false

  use FavnView, :html

  attr :run, :map, required: true

  def submission_panel(assigns) do
    ~H"""
    <div
      class="mx-auto flex w-full max-w-[80rem] flex-col gap-4"
      data-testid="run-submission-state"
      data-submission-status={@run.raw_status}
    >
      <.error_state
        :if={@run.raw_status == :failed}
        title={failure(@run).title}
        description={failure(@run).message}
        data-testid="run-submission-failed"
      >
        <:action>
          <.button navigate={~p"/runners"} icon="hero-server-stack">
            View runner diagnostics
          </.button>
        </:action>
      </.error_state>

      <.panel
        :if={@run.raw_status in [:queued, :preparing, :admitting, :submitted]}
        data-testid="run-submission-active"
      >
        <:header
          title={active_title(@run.raw_status)}
          subtitle="The request is durable. This page will update when execution starts."
          icon="hero-arrow-path"
        />
        <.notice tone={:info} icon="hero-circle-stack">
          {active_description(@run.raw_status)}
        </.notice>
      </.panel>

      <.panel
        :if={@run.raw_status in [:cancelled, :superseded]}
        data-testid="run-submission-terminal"
      >
        <:header
          title={active_title(@run.raw_status)}
          subtitle="Execution did not start for this request."
          icon="hero-information-circle"
        />
        <.notice tone={:warning} icon="hero-information-circle">
          {active_description(@run.raw_status)}
        </.notice>
      </.panel>

      <.panel :if={@run.raw_status == :failed && @run.failure}>
        <:header title="How to resolve it" icon="hero-wrench-screwdriver" />
        <p class="text-sm favn-text-muted">{@run.failure.remediation}</p>
        <.fact_list
          class="mt-5"
          columns={2}
          facts={[
            %{label: "Error code", value: @run.failure.code, mono: true},
            %{label: "Last updated", value: @run.updated_at || "-"}
          ]}
        />
      </.panel>
    </div>
    """
  end

  defp failure(%{failure: failure}) when is_map(failure), do: failure

  defp failure(_run) do
    %{
      title: "Run request failed",
      message: "The request failed before run execution started."
    }
  end

  defp active_title(:queued), do: "Run request queued"
  defp active_title(:preparing), do: "Preparing run"
  defp active_title(:admitting), do: "Starting run"
  defp active_title(:submitted), do: "Starting run"
  defp active_title(:cancelled), do: "Run request cancelled"
  defp active_title(:superseded), do: "Run request superseded"

  defp active_description(:queued),
    do: "The orchestrator is waiting to prepare this request."

  defp active_description(:preparing),
    do: "The orchestrator is validating targets and preparing execution."

  defp active_description(status) when status in [:admitting, :submitted],
    do: "The execution record is being admitted and projected."

  defp active_description(:cancelled), do: "This request was cancelled before execution started."
  defp active_description(:superseded), do: "A newer request replaced this submission."
end
