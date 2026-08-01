defmodule FavnView.Components.LogPages do
  @moduledoc """
  Page shells for the global, run, and asset-step log views.

  Each page declares its own attrs even where they overlap, because attrs
  attach to the function they precede: a shared block would register only the
  first page with `Phoenix.Component`, leaving the others undiscoverable to
  the design system and undocumented to callers.
  """

  use FavnView, :html

  alias FavnView.Components.AppShell
  alias FavnView.Components.LogViewer
  alias FavnView.Components.OutputMetadata

  attr :nav_items, :list, default: []
  attr :current_scope, :any, default: nil
  attr :operator_workspaces, :list, default: []
  attr :title, :string, required: true
  attr :subtitle, :string, default: nil
  attr :visible_logs, :list, default: []
  attr :scope, :atom, default: :global
  attr :logs_status, :atom, default: :ready
  attr :live?, :boolean, default: false
  attr :live_tail?, :boolean, default: true
  attr :wrap?, :boolean, default: true
  attr :search_query, :string, default: ""
  attr :selected_level, :string, default: "all"
  attr :selected_source, :string, default: "all"
  attr :empty_state, :string, default: "No logs yet."
  attr :stream_warning, :string, default: nil
  attr :context_note, :string, default: nil

  def global_logs_page(assigns) do
    ~H"""
    <AppShell.app_shell
      title={@title}
      subtitle={@subtitle}
      nav_items={@nav_items}
      current_scope={@current_scope}
      operator_workspaces={@operator_workspaces}
    >
      <.viewer assigns={assigns} />
    </AppShell.app_shell>
    """
  end

  attr :nav_items, :list, default: []
  attr :current_scope, :any, default: nil
  attr :operator_workspaces, :list, default: []
  attr :title, :string, required: true
  attr :subtitle, :string, default: nil
  attr :status, :string, default: nil
  attr :status_tone, :atom, default: :neutral
  attr :facts, :list, default: []
  attr :back_href, :string, default: nil
  attr :back_label, :string, default: nil
  attr :run_id, :string, default: nil
  attr :run_steps, :list, default: []
  attr :visible_logs, :list, default: []
  attr :scope, :atom, default: :run
  attr :logs_status, :atom, default: :ready
  attr :live?, :boolean, default: false
  attr :live_tail?, :boolean, default: true
  attr :wrap?, :boolean, default: true
  attr :search_query, :string, default: ""
  attr :selected_level, :string, default: "all"
  attr :selected_source, :string, default: "all"
  attr :empty_state, :string, default: "No logs yet."
  attr :stream_warning, :string, default: nil
  attr :context_note, :string, default: nil

  def run_logs_page(assigns) do
    ~H"""
    <AppShell.app_shell
      title={@title}
      subtitle={@subtitle}
      status={@status}
      status_tone={@status_tone}
      nav_items={@nav_items}
      current_scope={@current_scope}
      operator_workspaces={@operator_workspaces}
      back_href={@back_href}
      back_label={@back_label}
      facts={@facts}
    >
      <.step_strip :if={@run_steps != []} run_id={@run_id} steps={@run_steps} />
      <.viewer assigns={assigns} />
    </AppShell.app_shell>
    """
  end

  attr :nav_items, :list, default: []
  attr :current_scope, :any, default: nil
  attr :operator_workspaces, :list, default: []
  attr :title, :string, required: true
  attr :subtitle, :string, default: nil
  attr :status, :string, default: nil
  attr :status_tone, :atom, default: :neutral
  attr :facts, :list, default: []
  attr :back_href, :string, default: nil
  attr :back_label, :string, default: nil
  attr :output_metadata, :any, default: nil
  attr :output_status, :any, default: nil
  attr :visible_logs, :list, default: []
  attr :scope, :atom, default: :asset
  attr :logs_status, :atom, default: :ready
  attr :live?, :boolean, default: false
  attr :live_tail?, :boolean, default: true
  attr :wrap?, :boolean, default: true
  attr :search_query, :string, default: ""
  attr :selected_level, :string, default: "all"
  attr :selected_source, :string, default: "all"
  attr :empty_state, :string, default: "No logs yet."
  attr :stream_warning, :string, default: nil
  attr :context_note, :string, default: nil

  def asset_run_logs_page(assigns) do
    ~H"""
    <AppShell.app_shell
      title={@title}
      subtitle={@subtitle}
      status={@status}
      status_tone={@status_tone}
      nav_items={@nav_items}
      current_scope={@current_scope}
      operator_workspaces={@operator_workspaces}
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
      <.viewer assigns={assigns} />
    </AppShell.app_shell>
    """
  end

  @doc """
  The run's steps, each linking to its own log view.

  A run-scoped log page must say how the run went before an operator reads a
  single line: which steps ran, which one failed, and where its logs are.
  """
  attr :run_id, :string, required: true
  attr :steps, :list, required: true

  def step_strip(assigns) do
    ~H"""
    <nav
      class="mx-auto mb-3 flex w-full max-w-[120rem] flex-wrap gap-2"
      aria-label="Run steps"
      data-testid="log-run-steps"
    >
      <.link
        :for={step <- @steps}
        navigate={"/runs/#{@run_id}/assets/#{step.id}/logs"}
        class={[
          "favn-surface-control inline-flex items-center gap-2 rounded-box px-3 py-1.5 text-sm",
          step.status_tone == :error && "border-error/60"
        ]}
        data-testid="log-run-step"
      >
        <.status_dot tone={step.status_tone} label={step.status} glow={step.status_tone == :error} />
        <span class="max-w-[16rem] truncate font-medium">{step.display_name}</span>
        <span class="favn-text-muted text-xs">{step.status} · {step.duration}</span>
      </.link>
    </nav>
    """
  end

  attr :assigns, :map, required: true

  defp viewer(assigns) do
    ~H"""
    <LogViewer.log_viewer
      visible_logs={@assigns.visible_logs}
      scope={@assigns.scope}
      status={@assigns.logs_status}
      live?={@assigns.live?}
      live_tail?={@assigns.live_tail?}
      wrap?={@assigns.wrap?}
      search_query={@assigns.search_query}
      selected_level={@assigns.selected_level}
      selected_source={@assigns.selected_source}
      empty_state={@assigns.empty_state}
      warning={@assigns.stream_warning}
      context_note={@assigns.context_note}
    />
    """
  end
end
