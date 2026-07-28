defmodule FavnView.Components.LogPages do
  @moduledoc """
  Page shells for the global, run, and asset-step log views.
  """

  use FavnView, :html

  alias FavnView.Components.AppShell
  alias FavnView.Components.LogViewer
  alias FavnView.Components.OutputMetadata

  attr :nav_items, :list, default: []
  attr :title, :string, required: true
  attr :subtitle, :string, default: nil
  attr :status, :string, default: nil
  attr :status_tone, :atom, default: :neutral
  attr :facts, :list, default: []
  attr :back_href, :string, default: nil
  attr :back_label, :string, default: nil
  attr :logs, :list, default: []
  attr :visible_logs, :list, default: []
  attr :filter, :any, default: nil
  attr :scope, :atom, default: :global
  attr :logs_status, :atom, default: :ready
  attr :live?, :boolean, default: false
  attr :live_tail?, :boolean, default: true
  attr :wrap?, :boolean, default: true
  attr :search_query, :string, default: ""
  attr :selected_level, :string, default: "all"
  attr :selected_source, :string, default: "all"
  attr :next_cursor, :any, default: nil
  attr :empty_state, :string, default: "No logs yet."
  attr :stream_warning, :string, default: nil
  attr :context_note, :string, default: nil
  attr :output_metadata, :any, default: nil
  attr :output_status, :any, default: nil

  def global_logs_page(assigns) do
    ~H"""
    <AppShell.app_shell
      title={@title}
      subtitle={@subtitle}
      nav_items={@nav_items}
    >
      <.viewer assigns={assigns} viewer_title="Logs" viewer_subtitle={@subtitle} />
    </AppShell.app_shell>
    """
  end

  def run_logs_page(assigns) do
    ~H"""
    <AppShell.app_shell
      title={@title}
      subtitle={@subtitle}
      status={@status}
      status_tone={@status_tone}
      nav_items={@nav_items}
      back_href={@back_href}
      back_label={@back_label}
      facts={@facts}
    >
      <.viewer assigns={assigns} viewer_title="Logs" viewer_subtitle="Run-scoped backend logs" />
    </AppShell.app_shell>
    """
  end

  def asset_run_logs_page(assigns) do
    ~H"""
    <AppShell.app_shell
      title={@title}
      subtitle={@subtitle}
      status={@status}
      status_tone={@status_tone}
      nav_items={@nav_items}
      back_href={@back_href}
      back_label={@back_label}
      facts={@facts}
    >
      <OutputMetadata.output_metadata
        :if={@output_status}
        id="asset-log-output-metadata"
        class="mx-auto mb-3 w-full max-w-[120rem]"
        metadata={@output_metadata}
        status={@output_status}
      />
      <.viewer
        assigns={assigns}
        viewer_title="Logs"
        viewer_subtitle="Asset-step scoped backend logs"
      />
    </AppShell.app_shell>
    """
  end

  attr :assigns, :map, required: true
  attr :viewer_title, :string, required: true
  attr :viewer_subtitle, :string, default: nil
  attr :facts, :list, default: []

  def viewer(assigns) do
    ~H"""
    <LogViewer.log_viewer
      logs={@assigns.logs}
      visible_logs={@assigns.visible_logs}
      filter={@assigns.filter}
      scope={@assigns.scope}
      title={@viewer_title}
      subtitle={@viewer_subtitle}
      status={@assigns.logs_status}
      live?={@assigns.live?}
      live_tail?={@assigns.live_tail?}
      wrap?={@assigns.wrap?}
      search_query={@assigns.search_query}
      selected_level={@assigns.selected_level}
      selected_source={@assigns.selected_source}
      next_cursor={@assigns.next_cursor}
      empty_state={@assigns.empty_state}
      warning={@assigns.stream_warning}
      context_note={@assigns.context_note}
      facts={@facts}
    />
    """
  end
end
