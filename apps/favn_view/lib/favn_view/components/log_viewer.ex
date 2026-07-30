defmodule FavnView.Components.LogViewer do
  @moduledoc """
  The log terminal.

  One entry is one monospaced line — time, level tag, source, message — on a
  dark canvas that stays dark in both themes, because a terminal is an idiom
  rather than a surface. Identity and metadata live behind each line's
  disclosure, so the reading surface holds nothing but the log itself.

  Failures are the reason an operator is here, so error lines take the error
  colour, a lit left edge, and a wash, and the toolbar grows a jump control
  that walks between them without a server round trip.

  The page owns the title, status, and facts; this component owns only the
  terminal and its controls.
  """

  use FavnView, :html

  alias FavnView.LogsViewModel

  attr :visible_logs, :list, default: []
  attr :scope, :atom, default: :global
  attr :status, :atom, default: :ready
  attr :live?, :boolean, default: false
  attr :live_tail?, :boolean, default: true
  attr :wrap?, :boolean, default: true
  attr :search_query, :string, default: ""
  attr :selected_level, :string, default: "all"
  attr :selected_source, :string, default: "all"
  attr :empty_state, :string, default: "No logs yet."
  attr :warning, :string, default: nil
  attr :context_note, :string, default: nil

  def log_viewer(assigns) do
    assigns =
      assigns
      |> assign(:levels, LogsViewModel.levels())
      |> assign(:sources, LogsViewModel.sources())
      |> assign(:error_count, Enum.count(assigns.visible_logs, &(&1.level == "error")))

    ~H"""
    <section
      class="mx-auto flex min-h-0 w-full max-w-[120rem] flex-1 flex-col"
      data-testid="log-viewer"
      data-log-scope={@scope}
    >
      <.notice :if={@context_note} class="mb-3" data-testid="log-context-note">
        {@context_note}
      </.notice>
      <.notice :if={@warning} class="mb-3" data-testid="log-stream-warning">
        {@warning}
      </.notice>

      <div
        id="log-terminal"
        phx-hook="FavnLogViewer"
        data-live-tail={to_string(@live_tail?)}
        class="flex min-h-0 flex-1 flex-col"
      >
        <.toolbar
          search_query={@search_query}
          selected_level={@selected_level}
          selected_source={@selected_source}
          levels={@levels}
          sources={@sources}
          wrap?={@wrap?}
          live_tail?={@live_tail?}
          live?={@live?}
          error_count={@error_count}
        />

        <div
          class="favn-terminal min-h-[16rem] flex-1 overflow-auto rounded-box py-2 font-mono text-[0.8125rem] leading-6"
          data-testid="log-terminal-window"
        >
          <div
            :if={@status == :loading}
            class="favn-log-dim flex h-full min-h-[14rem] items-center justify-center text-sm"
            data-testid="log-loading-state"
          >
            Loading logs...
          </div>

          <div
            :if={@status == :error}
            class="flex h-full min-h-[14rem] items-center justify-center text-sm text-[var(--favn-terminal-error)]"
            data-testid="log-error-state"
          >
            Unable to load logs.
          </div>

          <div
            :if={@status not in [:loading, :error] and @visible_logs == []}
            class="favn-log-dim flex h-full min-h-[14rem] flex-col items-center justify-center gap-2 text-sm"
            data-testid="log-empty-state"
          >
            {@empty_state}
            <span :if={@live?} class="text-xs">Listening for logs...</span>
          </div>

          <div
            :if={@status not in [:loading, :error] and @visible_logs != []}
            class={[!@wrap? && "w-max min-w-full"]}
            data-log-copy-rows
          >
            <.log_row :for={log <- @visible_logs} log={log} wrap?={@wrap?} />
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
  attr :live?, :boolean, required: true
  attr :error_count, :integer, required: true

  def toolbar(assigns) do
    ~H"""
    <form phx-change="filter_logs" class="flex flex-wrap items-center gap-2 pb-3">
      <label class="input input-sm favn-surface-control min-w-[12rem] flex-1 items-center gap-2 rounded-box">
        <.icon name="hero-magnifying-glass" size={:sm} class="favn-text-muted" />
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

      <.level_select selected_level={@selected_level} levels={@levels} testid="log-level-filter" />
      <.source_select
        selected_source={@selected_source}
        sources={@sources}
        testid="log-source-filter"
      />

      <.error_jump :if={@error_count > 0} error_count={@error_count} />

      <.toggle_button event="toggle_wrap" label="Wrap" enabled?={@wrap?} testid="log-wrap-toggle" />
      <.toggle_button
        event="toggle_live_tail"
        label="Follow"
        enabled?={@live_tail?}
        testid="log-live-tail-toggle"
      />
      <.copy_logs_button />

      <span
        class="favn-text-muted inline-flex items-center gap-1.5 text-xs"
        data-testid="log-live-status"
      >
        <.status_dot tone={if @live?, do: :success, else: :neutral} label="" glow={@live?} />
        {if @live?, do: "Live", else: "Loaded"}
      </span>
    </form>
    """
  end

  attr :error_count, :integer, required: true

  defp error_jump(assigns) do
    ~H"""
    <div
      class="inline-flex items-center gap-1 rounded-box border border-error/40 bg-error/10 py-0.5 pr-0.5 pl-2.5"
      data-testid="log-error-jump"
    >
      <span class="text-xs font-medium text-error">
        {@error_count} {if @error_count == 1, do: "error", else: "errors"}
      </span>
      <.icon_button
        icon="hero-chevron-up"
        label="Previous error"
        size={:xs}
        data-log-error-nav="prev"
        data-testid="log-error-prev"
      />
      <.icon_button
        icon="hero-chevron-down"
        label="Next error"
        size={:xs}
        data-log-error-nav="next"
        data-testid="log-error-next"
      />
    </div>
    """
  end

  attr :selected_level, :string, required: true
  attr :levels, :list, required: true
  attr :testid, :string, required: true

  def level_select(assigns) do
    ~H"""
    <label class="select select-sm favn-surface-control w-[9.5rem] rounded-box">
      <span class="label favn-text-muted">Level</span>
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
    <label class="select select-sm favn-surface-control w-[11rem] rounded-box">
      <span class="label favn-text-muted">Source</span>
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

  @doc """
  Copies every line currently in the terminal.

  Not `FavnView.UI.Button.copy_button/1`: that copies one known value, and this
  copies whatever the filters have left on screen, which only the client knows.
  Hence `data-copy-logs` and no value.
  """
  attr :class, :any, default: nil

  def copy_logs_button(assigns) do
    ~H"""
    <button
      type="button"
      class={["btn btn-sm favn-surface-control rounded-box", @class]}
      data-copy-logs
      data-testid="log-copy-button"
    >
      <.icon name="hero-clipboard-document" size={:sm} /> Copy
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
      class="btn btn-sm favn-surface-control favn-control-boundary rounded-box gap-2"
      aria-pressed={to_string(@enabled?)}
      data-testid={@testid}
    >
      <span>{@label}</span>
      <span class={["toggle toggle-success toggle-xs", @enabled? && "toggle-checked"]}></span>
    </button>
    """
  end

  attr :log, :map, required: true
  attr :wrap?, :boolean, required: true

  def log_row(assigns) do
    ~H"""
    <details
      class="favn-log-line"
      data-testid="log-row"
      data-log-level={@log.level}
      data-log-copy-row
      data-log-copy-text={log_copy_text(@log)}
    >
      <summary class="gap-3" title={sequence_title(@log)}>
        <.icon
          name="hero-chevron-right"
          class="favn-log-marker favn-log-dim size-3 shrink-0"
        />
        <time class="favn-log-dim shrink-0 tabular-nums">{@log.time}</time>
        <span class="favn-log-level w-9 shrink-0 font-semibold">{level_tag(@log.level)}</span>
        <span class="favn-log-dim w-28 shrink-0 truncate">{@log.source_label}</span>
        <code class={[
          "min-w-0 flex-1",
          @wrap? && "whitespace-pre-wrap break-words",
          !@wrap? && "whitespace-pre"
        ]}>{@log.message}</code>
        <span
          :if={@log.truncated?}
          class="shrink-0 self-start rounded-full border border-[var(--favn-terminal-warning)] px-2 text-[0.65rem] uppercase tracking-[0.16em] text-[var(--favn-terminal-warning)]"
        >
          truncated
        </span>
      </summary>

      <div class="favn-log-expanded text-xs" data-testid="log-details-panel">
        <dl class="grid gap-x-4 gap-y-1 sm:grid-cols-[6rem_minmax(0,1fr)]">
          <dt class="favn-log-dim">logged</dt>
          <dd class="break-all">{@log.timestamp} UTC</dd>
          <%= for detail <- @log.details do %>
            <dt class="favn-log-dim">{detail.label}</dt>
            <dd class="break-all">{detail.title}</dd>
          <% end %>
        </dl>
        <div :if={@log.metadata_text != ""} class="mt-2">
          <p class="favn-log-dim">metadata</p>
          <pre class="mt-1 whitespace-pre-wrap break-words"><code>{@log.metadata_text}</code></pre>
        </div>
        <.copy_button
          value={log_copy_text(@log)}
          label="Copy entry"
          variant={:ghost}
          size={:xs}
          class="mt-2 border border-[var(--favn-terminal-boundary)]"
          data-testid="log-row-copy-button"
        />
      </div>
    </details>
    """
  end

  defp level_label(level), do: level |> Atom.to_string() |> String.upcase()
  defp source_label(source), do: source |> Atom.to_string() |> String.replace("_", ":")

  defp level_tag("debug"), do: "DBG"
  defp level_tag("info"), do: "INF"
  defp level_tag("warning"), do: "WRN"
  defp level_tag("error"), do: "ERR"
  defp level_tag(level), do: level |> String.upcase() |> String.slice(0, 3)

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
