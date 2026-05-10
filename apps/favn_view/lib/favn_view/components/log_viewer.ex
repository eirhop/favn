defmodule FavnView.Components.LogViewer do
  @moduledoc """
  Reusable terminal-style backend log viewer.
  """

  use FavnView, :html

  alias FavnView.LogsViewModel

  attr :logs, :list, default: []
  attr :visible_logs, :list, default: []
  attr :filter, :any, default: nil
  attr :scope, :atom, default: :global
  attr :title, :string, default: "Logs"
  attr :subtitle, :string, default: nil
  attr :status, :atom, default: :ready
  attr :live?, :boolean, default: false
  attr :live_tail?, :boolean, default: true
  attr :wrap?, :boolean, default: true
  attr :search_query, :string, default: ""
  attr :selected_level, :string, default: "all"
  attr :selected_source, :string, default: "all"
  attr :next_cursor, :any, default: nil
  attr :empty_state, :string, default: "No logs yet."
  attr :facts, :list, default: []
  attr :warning, :string, default: nil
  attr :context_note, :string, default: nil

  def log_viewer(assigns) do
    assigns =
      assigns
      |> assign(:levels, LogsViewModel.levels())
      |> assign(:sources, LogsViewModel.sources())
      |> assign(:copy_text, LogsViewModel.plain_text(assigns.visible_logs))

    ~H"""
    <section class="mx-auto w-full max-w-6xl" data-testid="log-viewer" data-log-scope={@scope}>
      <div class="card glass favn-glass-panel overflow-hidden rounded-box border border-primary/20 bg-base-200/35 shadow-2xl">
        <div class="flex flex-col gap-5 border-b border-base-content/10 p-5 sm:p-6 lg:flex-row lg:items-start lg:justify-between">
          <div class="min-w-0">
            <div class="flex flex-wrap items-center gap-3">
              <h2 class="text-2xl font-medium tracking-tight">{@title}</h2>
              <span class={live_badge_class(@live?)} data-testid="log-live-status">
                <span class={["status", @live? && "status-success", !@live? && "status-neutral"]}>
                </span>
                {if @live?, do: "Live streaming", else: "Loaded"}
              </span>
            </div>
            <p :if={@subtitle} class="mt-2 text-sm text-base-content/55">{@subtitle}</p>
            <p :if={@context_note} class="mt-3 text-sm text-warning/80" data-testid="log-context-note">
              {@context_note}
            </p>
            <p :if={@warning} class="mt-3 text-sm text-warning/80" data-testid="log-stream-warning">
              {@warning}
            </p>
          </div>

          <dl :if={@facts != []} class="grid gap-4 text-sm sm:grid-cols-3 lg:min-w-[24rem]">
            <div
              :for={fact <- @facts}
              class="border-base-content/20 sm:border-l sm:pl-5 first:border-l-0 first:pl-0"
            >
              <dt class="text-base-content/55">{fact.label}</dt>
              <dd class="mt-1 font-medium text-base-content">{fact.value}</dd>
            </div>
          </dl>
        </div>

        <.toolbar
          search_query={@search_query}
          selected_level={@selected_level}
          selected_source={@selected_source}
          levels={@levels}
          sources={@sources}
          wrap?={@wrap?}
          live_tail?={@live_tail?}
        />

        <div
          id="log-terminal"
          phx-hook="FavnLogViewer"
          data-live-tail={to_string(@live_tail?)}
          data-copy-source="log-copy-text"
          class="relative border-t border-base-content/10 bg-base-100/55 p-4 sm:p-5"
        >
          <textarea id="log-copy-text" class="hidden" readonly>{@copy_text}</textarea>

          <div
            :if={@status == :loading}
            class="rounded-box border border-dashed border-base-content/15 p-8 text-center text-sm text-base-content/55"
            data-testid="log-loading-state"
          >
            Loading logs...
          </div>

          <div
            :if={@status == :error}
            class="rounded-box border border-error/25 bg-error/5 p-8 text-center text-sm text-error"
            data-testid="log-error-state"
          >
            Unable to load logs.
          </div>

          <div
            :if={@status != :loading and @status != :error and @visible_logs == []}
            class="rounded-box border border-dashed border-base-content/15 p-8 text-center text-sm text-base-content/55"
            data-testid="log-empty-state"
          >
            {@empty_state}
            <span :if={@live?} class="mt-2 block text-xs text-base-content/45">
              Listening for logs...
            </span>
          </div>

          <div
            :if={@status not in [:loading, :error] and @visible_logs != []}
            class={[
              "max-h-[34rem] overflow-auto rounded-box border border-base-content/10 bg-[#020817]/75 p-4 font-mono text-sm leading-6 shadow-inner",
              !@wrap? && "whitespace-nowrap"
            ]}
            data-testid="log-terminal-window"
          >
            <div class="min-w-max space-y-5">
              <.log_row :for={log <- @visible_logs} log={log} wrap?={@wrap?} />
            </div>
          </div>
        </div>
      </div>
    </section>
    """
  end

  attr :search_query, :string, required: true
  attr :selected_level, :string, required: true
  attr :selected_source, :string, required: true
  attr :levels, :list, required: true
  attr :sources, :list, required: true
  attr :wrap?, :boolean, required: true
  attr :live_tail?, :boolean, required: true

  def toolbar(assigns) do
    ~H"""
    <form
      phx-change="filter_logs"
      class="grid gap-3 border-t border-base-content/10 p-4 sm:grid-cols-[minmax(14rem,1fr)_10rem_11rem_auto_auto_auto] sm:items-center sm:p-5"
    >
      <label class="input favn-control-glass flex items-center gap-2 rounded-box">
        <.icon name="hero-magnifying-glass" class="size-5 text-base-content/55" />
        <input
          type="search"
          name="filters[search]"
          value={@search_query}
          placeholder="Search logs..."
          class="grow"
          data-testid="log-search-input"
        />
      </label>

      <label class="select favn-control-glass rounded-box">
        <span class="label">Level</span>
        <select name="filters[level]" data-testid="log-level-filter">
          <option value="all" selected={@selected_level == "all"}>All</option>
          <option
            :for={level <- @levels}
            value={level}
            selected={@selected_level == Atom.to_string(level)}
          >
            {level_label(level)}
          </option>
        </select>
      </label>

      <label class="select favn-control-glass rounded-box">
        <span class="label">Source</span>
        <select name="filters[source]" data-testid="log-source-filter">
          <option value="all" selected={@selected_source == "all"}>All</option>
          <option
            :for={source <- @sources}
            value={source}
            selected={@selected_source == Atom.to_string(source)}
          >
            {source_label(source)}
          </option>
        </select>
      </label>

      <.toggle_button event="toggle_wrap" label="Wrap" enabled?={@wrap?} testid="log-wrap-toggle" />
      <.toggle_button
        event="toggle_live_tail"
        label="Live tail"
        enabled?={@live_tail?}
        testid="log-live-tail-toggle"
      />

      <button
        type="button"
        class="btn favn-control-glass rounded-box"
        data-copy-logs
        data-testid="log-copy-button"
      >
        <.icon name="hero-clipboard-document" class="size-5" /> Copy
      </button>
    </form>
    """
  end

  attr :event, :string, required: true
  attr :label, :string, required: true
  attr :enabled?, :boolean, required: true
  attr :testid, :string, required: true

  def toggle_button(assigns) do
    ~H"""
    <button
      type="button"
      phx-click={@event}
      class="btn favn-control-glass rounded-box justify-between gap-3"
      aria-pressed={to_string(@enabled?)}
      data-testid={@testid}
    >
      <span>{@label}</span>
      <span class={["toggle toggle-success toggle-sm", @enabled? && "toggle-checked"]}></span>
    </button>
    """
  end

  attr :log, :map, required: true
  attr :wrap?, :boolean, required: true

  def log_row(assigns) do
    ~H"""
    <article
      class="grid gap-2 text-base-content/90 sm:grid-cols-[6.5rem_4.5rem_13rem_minmax(0,1fr)]"
      data-testid="log-row"
      title={sequence_title(@log)}
    >
      <time class="text-base-content/55">{@log.timestamp}</time>
      <span class={["font-semibold", level_class(@log.level)]}>{@log.level_label}</span>
      <span class="truncate text-base-content/55">{@log.source_label}</span>
      <div class="min-w-0">
        <pre class={[
          "m-0 font-mono text-base-content/90",
          @wrap? && "whitespace-pre-wrap break-words",
          !@wrap? && "whitespace-pre"
        ]}><code>{@log.message}</code></pre>
        <span
          :if={@log.truncated?}
          class="mt-1 inline-flex rounded-full border border-warning/30 px-2 py-0.5 text-[0.65rem] uppercase tracking-[0.16em] text-warning/80"
        >
          truncated
        </span>
      </div>
    </article>
    """
  end

  defp level_label(level), do: level |> Atom.to_string() |> String.upcase()
  defp source_label(source), do: source |> Atom.to_string() |> String.replace("_", ":")

  defp level_class("debug"), do: "text-base-content/45"
  defp level_class("warning"), do: "text-warning"
  defp level_class("error"), do: "text-error"
  defp level_class(_level), do: "text-info"

  defp live_badge_class(true), do: "badge badge-success badge-soft gap-2 px-3 py-3"
  defp live_badge_class(false), do: "badge badge-ghost gap-2 px-3 py-3"

  defp sequence_title(%{global_sequence: nil}), do: nil
  defp sequence_title(%{global_sequence: sequence}), do: "global sequence #{sequence}"
end
