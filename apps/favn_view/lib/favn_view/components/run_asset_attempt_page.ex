defmodule FavnView.Components.RunAssetAttemptPage do
  @moduledoc """
  Standalone detail page for one observed asset attempt.

  The run Flow stays lean. Complete errors, output metadata, and execution
  identifiers are rendered here only after the operator opens an asset row.
  """

  use FavnView, :html

  alias FavnView.Components.AppShell
  alias FavnView.Components.OutputMetadata

  attr :nav_items, :list, default: []
  attr :current_scope, :any, default: nil
  attr :operator_workspaces, :list, default: []
  attr :run_id, :string, required: true
  attr :attempt, :map, default: nil
  attr :loading?, :boolean, default: false
  attr :error, :string, default: nil

  def run_asset_attempt_page(assigns) do
    ~H"""
    <AppShell.app_shell
      title={title(@attempt)}
      subtitle="Asset run details"
      status={@attempt && @attempt.status_label}
      status_tone={@attempt && @attempt.status_tone}
      nav_items={@nav_items}
      current_scope={@current_scope}
      operator_workspaces={@operator_workspaces}
      back_href={~p"/runs/#{@run_id}"}
      back_label="Back to run"
      facts={(@attempt && @attempt.facts) || []}
    >
      <.panel :if={@loading?} padding={:lg} data-testid="asset-attempt-loading">
        <div class="space-y-3 animate-pulse" aria-label="Loading asset run details">
          <div class="h-5 w-48 rounded bg-base-content/10"></div>
          <div class="h-20 rounded bg-base-content/10"></div>
        </div>
      </.panel>

      <.panel :if={@error} padding={:lg} data-testid="asset-attempt-error">
        <.notice tone={:error} icon="hero-exclamation-triangle">{@error}</.notice>
      </.panel>

      <div :if={@attempt} class="space-y-4" data-testid="asset-attempt-detail">
        <.notice
          :if={@attempt.error_summary}
          tone={:error}
          icon="hero-exclamation-triangle"
          data-testid="asset-attempt-failure"
        >
          {@attempt.error_summary}
        </.notice>

        <OutputMetadata.output_metadata
          id={"asset-attempt-output-#{@attempt.id}"}
          metadata={@attempt.output_metadata}
          status={@attempt.raw_status}
        />

        <.panel padding={:lg}>
          <h2 class="mb-4 text-base font-semibold tracking-tight">Execution details</h2>
          <.fact_list facts={@attempt.execution_facts} columns={2} />
        </.panel>

        <div :if={@attempt.logs_href} class="flex justify-end">
          <.button
            navigate={@attempt.logs_href}
            variant={:secondary}
            trailing_icon="hero-arrow-top-right-on-square"
            data-testid="asset-attempt-logs-link"
          >
            Open logs
          </.button>
        </div>
      </div>
    </AppShell.app_shell>
    """
  end

  defp title(nil), do: "Asset run"
  defp title(attempt), do: attempt.name
end
