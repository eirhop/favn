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

    ~H"""
    <section
      class="mx-auto flex min-h-0 w-full max-w-[120rem] flex-1"
      data-testid="log-viewer"
      data-log-scope={@scope}
    >
      <div class="card glass favn-glass-panel min-h-0 flex-1 overflow-hidden rounded-box border border-primary/20 bg-base-200/35 shadow-2xl">
        <div class="flex flex-col gap-3 border-b border-base-content/10 p-4 sm:gap-5 sm:p-6 lg:flex-row lg:items-start lg:justify-between">
          <div class="min-w-0">
            <div class="flex flex-wrap items-center gap-2 sm:gap-3">
              <h2 class="text-xl font-medium tracking-tight sm:text-2xl">{@title}</h2>
              <span class={live_badge_class(@live?)} data-testid="log-live-status">
                <span class={["status", @live? && "status-success", !@live? && "status-neutral"]}></span>
                {if @live?, do: "Live streaming", else: "Loaded"}
              </span>
            </div>
            <p :if={@subtitle} class="mt-1 text-xs text-base-content/55 sm:mt-2 sm:text-sm">
              {@subtitle}
            </p>
            <p
              :if={@context_note}
              class="mt-2 text-xs text-warning/80 sm:mt-3 sm:text-sm"
              data-testid="log-context-note"
            >
              {@context_note}
            </p>
            <p
              :if={@warning}
              class="mt-2 text-xs text-warning/80 sm:mt-3 sm:text-sm"
              data-testid="log-stream-warning"
            >
              {@warning}
            </p>
          </div>

          <dl
            :if={@facts != []}
            class="grid grid-cols-3 gap-2 rounded-box border border-base-content/10 bg-base-content/[0.03] p-2 text-xs sm:gap-4 sm:border-0 sm:bg-transparent sm:p-0 sm:text-sm lg:min-w-[24rem]"
          >
            <div
              :for={fact <- @facts}
              class="min-w-0 border-base-content/10 px-2 first:pl-0 sm:border-l sm:border-base-content/20 sm:pl-5 sm:first:border-l-0 sm:first:pl-0"
            >
              <dt class="truncate text-base-content/55">{fact.label}</dt>
              <dd class="mt-0.5 truncate font-medium text-base-content sm:mt-1">{fact.value}</dd>
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
          class="relative flex min-h-0 flex-1 flex-col border-t border-base-content/10 bg-base-100/55 p-4 sm:p-5"
        >
          <div
            :if={@status == :loading}
            class="flex min-h-[16rem] flex-1 items-center justify-center rounded-box border border-dashed border-base-content/15 p-8 text-center text-sm text-base-content/55"
            data-testid="log-loading-state"
          >
            Loading logs...
          </div>

          <div
            :if={@status == :error}
            class="flex min-h-[16rem] flex-1 items-center justify-center rounded-box border border-error/25 bg-error/5 p-8 text-center text-sm text-error"
            data-testid="log-error-state"
          >
            Unable to load logs.
          </div>

          <div
            :if={@status != :loading and @status != :error and @visible_logs == []}
            class="flex min-h-[16rem] flex-1 flex-col items-center justify-center rounded-box border border-dashed border-base-content/15 p-8 text-center text-sm text-base-content/55"
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
              "min-h-[16rem] flex-1 overflow-auto rounded-box border border-base-content/10 bg-[#020817]/75 p-4 font-mono text-sm leading-6 shadow-inner",
              !@wrap? && "whitespace-nowrap"
            ]}
            data-testid="log-terminal-window"
          >
            <div class="min-w-max space-y-5" data-log-copy-rows>
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
      class="border-t border-base-content/10 p-3 sm:grid sm:grid-cols-[minmax(14rem,1fr)_10rem_11rem_auto_auto_auto] sm:items-center sm:gap-3 sm:p-5"
    >
      <div class="flex items-center gap-2 sm:contents">
        <label class="input favn-control-glass min-w-0 flex-1 items-center gap-2 rounded-box sm:flex">
          <.icon name="hero-magnifying-glass" class="size-5 text-base-content/55" />
          <input
            type="search"
            name="filters[search]"
            value={@search_query}
            placeholder="Search logs..."
            class="grow"
            phx-debounce="250"
            data-testid="log-search-input"
          />
        </label>

        <details class="dropdown dropdown-end sm:hidden">
          <summary
            class="btn btn-square favn-control-glass rounded-box"
            aria-label="Log filters"
            data-testid="log-filter-menu-toggle"
          >
            <.icon name="hero-funnel" class="size-5" />
          </summary>
          <div class="dropdown-content z-20 mt-2 w-[min(20rem,calc(100vw-2rem))] rounded-box border border-base-content/10 bg-base-200/95 p-3 shadow-2xl backdrop-blur">
            <div class="grid gap-2">
              <.level_select
                selected_level={@selected_level}
                levels={@levels}
                testid="log-level-filter-mobile"
              />
              <.source_select
                selected_source={@selected_source}
                sources={@sources}
                testid="log-source-filter-mobile"
              />
              <.toggle_button
                event="toggle_wrap"
                label="Wrap"
                enabled?={@wrap?}
                testid="log-wrap-toggle-mobile"
              />
              <.toggle_button
                event="toggle_live_tail"
                label="Live tail"
                enabled?={@live_tail?}
                testid="log-live-tail-toggle-mobile"
              />
              <.copy_button class="w-full justify-center" />
            </div>
          </div>
        </details>
      </div>

      <div class="hidden sm:contents">
        <.level_select selected_level={@selected_level} levels={@levels} testid="log-level-filter" />
        <.source_select
          selected_source={@selected_source}
          sources={@sources}
          testid="log-source-filter"
        />
        <.toggle_button event="toggle_wrap" label="Wrap" enabled?={@wrap?} testid="log-wrap-toggle" />
        <.toggle_button
          event="toggle_live_tail"
          label="Live tail"
          enabled?={@live_tail?}
          testid="log-live-tail-toggle"
        />
        <.copy_button />
      </div>
    </form>
    """
  end

  attr :selected_level, :string, required: true
  attr :levels, :list, required: true
  attr :testid, :string, required: true

  def level_select(assigns) do
    ~H"""
    <label class="select favn-control-glass rounded-box">
      <span class="label">Level</span>
      <select name="filters[level]" data-testid={@testid}>
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
    """
  end

  attr :selected_source, :string, required: true
  attr :sources, :list, required: true
  attr :testid, :string, required: true

  def source_select(assigns) do
    ~H"""
    <label class="select favn-control-glass rounded-box">
      <span class="label">Source</span>
      <select name="filters[source]" data-testid={@testid}>
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
    """
  end

  attr :class, :any, default: nil

  def copy_button(assigns) do
    ~H"""
    <button
      type="button"
      class={["btn favn-control-glass rounded-box", @class]}
      data-copy-logs
      data-testid="log-copy-button"
    >
      <.icon name="hero-clipboard-document" class="size-5" /> Copy
    </button>
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
      class="grid gap-2 text-slate-100/90 sm:grid-cols-[9.5rem_4.5rem_13rem_minmax(0,1fr)]"
      data-testid="log-row"
      data-log-copy-row
      data-log-copy-text={log_copy_text(@log)}
      title={sequence_title(@log)}
    >
      <time class="text-slate-400">{@log.timestamp}</time>
      <span class={["font-semibold", level_class(@log.level)]}>{@log.level_label}</span>
      <span class="truncate text-slate-400">{@log.source_label}</span>
      <div class="min-w-0">
        <div class="flex gap-2">
          <pre class={[
            "m-0 min-w-0 flex-1 font-mono text-slate-100/90",
            @wrap? && "whitespace-pre-wrap break-words",
            !@wrap? && "whitespace-pre"
          ]}><code>{@log.message}</code></pre>
          <button
            :if={@log.level == "error"}
            type="button"
            class="btn btn-error btn-soft btn-xs shrink-0 rounded-box"
            data-copy-text={log_copy_text(@log)}
            data-testid="log-error-copy-button"
            aria-label="Copy error log"
          >
            <.icon name="hero-clipboard-document" class="size-4" /> Copy
          </button>
        </div>
        <div :if={@log.details != []} class="mt-2 flex flex-wrap gap-1.5 text-[0.68rem] leading-4">
          <span
            :for={detail <- @log.details}
            class="rounded-full border border-slate-700/80 bg-slate-900/80 px-2 py-0.5 text-slate-300"
            title={detail.title}
            data-testid="log-detail-chip"
          >
            <span class="text-slate-500">{detail.label}</span> {detail.display}
          </span>
        </div>
        <details
          :if={@log.details != [] or @log.metadata_text != ""}
          class="mt-2 rounded-box border border-slate-700/70 bg-slate-950/70 px-3 py-2 text-xs text-slate-300"
          data-testid="log-details-panel"
        >
          <summary class="cursor-pointer text-slate-400">All details</summary>
          <dl :if={@log.details != []} class="mt-2 grid gap-1.5 sm:grid-cols-[7rem_minmax(0,1fr)]">
            <div :for={detail <- @log.details} class="contents">
              <dt class="text-slate-500">{detail.label}</dt>
              <dd class="break-all text-slate-200">{detail.title}</dd>
            </div>
          </dl>
          <div :if={@log.metadata_text != ""} class="mt-3">
            <p class="text-slate-500">metadata</p>
            <pre class="mt-1 whitespace-pre-wrap break-words text-slate-200"><code>{@log.metadata_text}</code></pre>
          </div>
        </details>
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

  defp level_class("debug"), do: "text-slate-500"
  defp level_class("warning"), do: "text-amber-300"
  defp level_class("error"), do: "text-rose-300"
  defp level_class(_level), do: "text-sky-300"

  defp live_badge_class(true),
    do:
      "badge badge-success badge-soft gap-1.5 px-2 py-2 text-xs sm:gap-2 sm:px-3 sm:py-3 sm:text-sm"

  defp live_badge_class(false),
    do: "badge badge-ghost gap-1.5 px-2 py-2 text-xs sm:gap-2 sm:px-3 sm:py-3 sm:text-sm"

  defp sequence_title(%{global_sequence: nil}), do: nil
  defp sequence_title(%{global_sequence: sequence}), do: "global sequence #{sequence}"

  defp log_copy_text(log) do
    [log.message, detail_copy_text(log), metadata_copy_text(log)]
    |> Enum.reject(&(&1 in [nil, ""]))
    |> Enum.join("\n")
  end

  defp detail_copy_text(%{details: details}) when is_list(details) do
    details
    |> Enum.map(fn detail -> "#{detail.label}=#{detail.title}" end)
    |> Enum.join(" ")
  end

  defp detail_copy_text(_log), do: ""

  defp metadata_copy_text(%{metadata_text: ""}), do: ""
  defp metadata_copy_text(%{metadata_text: metadata_text}), do: "metadata=" <> metadata_text
  defp metadata_copy_text(_log), do: ""
end
