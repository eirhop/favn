defmodule FavnView.Components.RunDetailPage.AttemptDrawer do
  @moduledoc """
  One asset attempt, ordered by what the operator asks about it.

  The verdict first — what this attempt wrote and whether the checks were
  satisfied — then the failure if there was one, then timing. Identifiers come
  last and collapsed: three opaque ids at the top of a panel push the one useful
  sentence below the fold, and an operator who wants an id is deliberately
  looking for it.
  """

  use FavnView, :html

  alias FavnView.Components.OutputMetadata
  alias FavnView.UI.Tokens

  attr :attempt, :map, required: true

  def attempt_drawer(assigns) do
    assigns = assign(assigns, :outcome, outcome(assigns.attempt))

    ~H"""
    <div class="fixed inset-0 z-50 bg-base-300/20 backdrop-blur-[1px]" phx-click="close_attempt" />
    <aside
      class="fixed inset-y-0 right-0 z-50 flex w-full max-w-[30rem] flex-col border-l border-base-content/10 bg-base-100/95 shadow-2xl shadow-primary/20 backdrop-blur-xl lg:max-w-[34rem]"
      data-testid="asset-attempt-drawer"
      role="dialog"
      aria-label={"Asset attempt: #{@attempt.short_asset_name}"}
    >
      <header class="flex items-start justify-between gap-3 border-b border-base-content/10 px-5 py-4 lg:pr-24">
        <div class="min-w-0">
          <div class="flex min-w-0 flex-wrap items-center gap-2">
            <h2 class="truncate text-lg font-semibold tracking-tight">
              {@attempt.short_asset_name}
            </h2>
            <.status_badge tone={@attempt.status_tone} label={@attempt.status} size={:sm} />
          </div>

          <p class="mt-1 truncate text-sm favn-text-subtle">
            {attempt_subtitle(@attempt)}
          </p>
        </div>

        <.icon_button
          phx-click="close_attempt"
          icon="hero-x-mark"
          label="Close attempt"
          shape={:circle}
        />
      </header>

      <div class="min-h-0 flex-1 space-y-3 overflow-y-auto px-5 py-5 lg:pr-24">
        <div
          :if={@outcome}
          class={[
            "rounded-box border p-3",
            Tokens.border_class(@outcome.tone),
            Tokens.surface_class(@outcome.tone)
          ]}
          data-testid="attempt-data-outcome"
        >
          <p class={["text-sm font-medium", Tokens.text_class(@outcome.tone)]}>
            {@outcome.headline}
          </p>

          <p :if={@outcome.target} class="mt-0.5 truncate font-mono text-sm favn-text-muted">
            {@outcome.target}
          </p>

          <p
            :if={@outcome.checks}
            class={["mt-1 text-sm", Tokens.text_class(@outcome.checks.tone)]}
            data-testid="attempt-checks-verdict"
          >
            {@outcome.checks.label}
          </p>
        </div>

        <.notice
          :if={@attempt.error_summary && !skipped_attempt?(@attempt)}
          tone={:error}
          icon="hero-exclamation-triangle"
        >
          {@attempt.error_summary}
        </.notice>

        <.notice :if={skipped_attempt?(@attempt)} tone={:neutral} icon="hero-minus-circle">
          Nothing to do: this window already had successful work{skipped_at(@attempt)}.
          <.link
            :if={attempt_run_href(@attempt)}
            navigate={attempt_run_href(@attempt)}
            class="ml-1 font-medium underline-offset-2 hover:underline"
          >
            Open that run
          </.link>
        </.notice>
        <.fact_list facts={timing_facts(@attempt)} columns={3} />
        <OutputMetadata.output_metadata
          id={"output-metadata-#{@attempt.id}"}
          metadata={Map.get(@attempt, :output_metadata)}
          status={@attempt.raw_status}
        />
        <details class="rounded-box border border-base-content/10 p-3">
          <summary class="cursor-pointer text-sm font-medium favn-text-muted">Identifiers</summary>

          <div class="mt-3 space-y-1">
            <.field_row label="Asset key"><.mono value={@attempt.asset_key} /></.field_row>

            <.field_row label="Window run">
              <.mono value={@attempt.child_run_id || @attempt.run_id || "-"} />
            </.field_row>

            <.field_row label="Run group">
              <.mono value={@attempt.root_execution_group_id || "-"} />
            </.field_row>
          </div>
        </details>
      </div>

      <footer
        :if={@attempt.logs_href}
        class="border-t border-base-content/10 bg-base-200/40 p-5 lg:pr-24"
      >
        <.button
          navigate={@attempt.logs_href}
          variant={:secondary}
          trailing_icon="hero-arrow-top-right-on-square"
          block
          data-testid="attempt-logs-link"
        >
          Open logs
        </.button>
      </footer>
    </aside>
    """
  end

  defp outcome(attempt),
    do: OutputMetadata.outcome(Map.get(attempt, :output_metadata), attempt.raw_status)

  defp timing_facts(attempt) do
    [
      %{label: "Window", value: attempt.window_label},
      %{label: "Started", value: attempt.started_at},
      %{label: "Duration", value: attempt.duration}
    ]
  end

  defp attempt_subtitle(attempt) do
    [attempt.stage_label, attempt_label(Map.get(attempt, :attempt_number))]
    |> Enum.reject(&is_nil/1)
    |> Enum.join(" · ")
  end

  defp attempt_label(nil), do: nil
  defp attempt_label(1), do: "First attempt"
  defp attempt_label(number), do: "Attempt #{number}"

  defp skipped_attempt?(%{raw_status: status})
       when status in [:skipped, :skipped_fresh, "skipped", "skipped_fresh"],
       do: true

  defp skipped_attempt?(_attempt), do: false

  defp skipped_at(%{finished_at: finished_at}) when is_binary(finished_at) and finished_at != "-",
    do: " on #{finished_at}"

  defp skipped_at(%{started_at: started_at}) when is_binary(started_at) and started_at != "-",
    do: " on #{started_at}"

  defp skipped_at(_attempt), do: ""

  defp attempt_run_href(%{child_run_id: run_id}) when is_binary(run_id), do: "/runs/#{run_id}"
  defp attempt_run_href(%{run_id: run_id}) when is_binary(run_id), do: "/runs/#{run_id}"
  defp attempt_run_href(_attempt), do: nil
end
