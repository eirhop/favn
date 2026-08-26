defmodule FavnView.Components.RunDetailPage.WindowFailures do
  @moduledoc """
  Why a backfill's coverage windows failed, one row per distinct reason.

  A window that fails before its child run exists has no run page to carry its
  diagnosis, so without this panel a backfill can report thirty-one failures and
  offer no account of any of them. The reason is on the backfill ledger, and this
  is the only operator surface that reads it.

  Rows are reasons, not windows. A planning or submission failure fails every
  window the backfill planned for the same cause, and thirty-one rows saying one
  thing is how the cause gets lost. Each row states how much coverage the reason
  cost and, when some of its windows did produce runs, where to read those.

  The reason is monospaced and shown as recorded. It is the string an operator
  pastes into a search of the logs or the source, so it must survive the trip
  intact rather than be prettified into something that no longer matches.
  """

  use FavnView, :html

  attr :groups, :list, default: nil, doc: "grouped failures, or nil when unread"
  attr :truncated?, :boolean, default: false
  attr :error, :string, default: nil
  attr :failed_windows, :integer, default: 0

  def window_failures(assigns) do
    ~H"""
    <.panel :if={show?(@groups, @error)} padding={:none} data-testid="window-failures">
      <:header title="Why the windows failed" subtitle={subtitle(@groups)} />
      <:actions>
        <.badge :if={@groups} tone={:error} variant={:outline}>
          {length(@groups)} {plural(length(@groups), "reason")}
        </.badge>
      </:actions>

      <.notice
        :if={@error}
        tone={:warning}
        icon="hero-exclamation-triangle"
        class="m-3"
        data-testid="window-failures-error"
      >
        {@error}
      </.notice>

      <.stack :if={@groups not in [nil, []]} gap={:none} data-testid="window-failure-rows">
        <div
          :for={group <- @groups}
          class="border-b border-base-content/10 p-4 last:border-b-0"
          data-testid="window-failure-row"
        >
          <div class="flex flex-wrap items-start justify-between gap-3">
            <.mono value={group.reason} class="min-w-0 font-medium text-error" />
            <.badge tone={:error} size={:sm}>
              {group.window_count} {plural(group.window_count, "window")}
            </.badge>
          </div>

          <p
            :if={group.detail}
            class="mt-2 text-sm favn-text-muted"
            data-testid="window-failure-detail"
          >
            {group.detail}
          </p>

          <.inline gap={:sm} class="mt-2 text-sm favn-text-subtle">
            <span :if={group.span} data-testid="window-failure-span">Covering {group.span}</span>
            <span :if={group.first_window} data-testid="window-failure-window">
              {group.first_window}
            </span>
            <span :if={group.attempts > 1}>
              {group.attempts} attempts
            </span>
            <%!-- A window that failed with a run keeps its diagnosis on that
            run's page, so the row says the deeper reading exists rather than
            duplicating it here. --%>
            <span :if={group.run_count > 0} data-testid="window-failure-runs">
              {group.run_count} of these started a run
            </span>
            <span :if={group.run_count == 0} data-testid="window-failure-no-runs">
              No run was started
            </span>
          </.inline>

          <%!-- The link list is bounded, so a group with more runs than links
          says how many it is not showing. "4 of these started a run" over three
          links otherwise reads as a missing link rather than a bound. --%>
          <.inline :if={group.run_ids != []} gap={:sm} class="mt-2">
            <.link
              :for={run_id <- group.run_ids}
              navigate={~p"/runs/#{run_id}"}
              class="link link-hover text-sm"
              data-testid="window-failure-run-link"
            >
              {short_id(run_id)}
            </.link>
            <span
              :if={group.run_count > length(group.run_ids)}
              class="text-sm favn-text-subtle"
              data-testid="window-failure-more-runs"
            >
              and {group.run_count - length(group.run_ids)} more
            </span>
          </.inline>
        </div>
      </.stack>

      <%!-- The ledger read is bounded, so a backfill with more failed windows
      than the bound must say the list is partial. Reasons repeat, so the bound
      almost never bites; when it does, silence would make a partial list look
      complete. --%>
      <.notice
        :if={@truncated?}
        tone={:info}
        icon="hero-scissors"
        class="m-3"
        data-testid="window-failures-truncated"
      >
        More windows failed than this page reads at once. The reasons above are from the
        earliest {@failed_windows} in window order.
      </.notice>
    </.panel>
    """
  end

  defp show?(nil, nil), do: false
  defp show?([], nil), do: false
  defp show?(_groups, _error), do: true

  defp subtitle(nil), do: "The backfill ledger could not be read."

  defp subtitle(groups) do
    windows = Enum.reduce(groups, 0, &(&1.window_count + &2))

    "#{windows} #{plural(windows, "window")} failed. Grouped by the reason each one recorded."
  end

  defp plural(1, word), do: word
  defp plural(_count, word), do: word <> "s"

  defp short_id(id) when is_binary(id) and byte_size(id) > 14, do: String.slice(id, 0, 14)
  defp short_id(id), do: to_string(id)
end
