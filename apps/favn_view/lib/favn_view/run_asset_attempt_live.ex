defmodule FavnView.RunAssetAttemptLive do
  @moduledoc false

  use FavnView, :live_view

  alias FavnView.Components.AssetCataloguePage
  alias FavnView.Components.RunAssetAttemptPage
  alias FavnView.LogsViewModel
  alias FavnView.OperatorErrorLabels
  alias FavnView.Orchestrator

  @impl true
  def mount(%{"run_id" => run_id, "asset_step_id" => asset_step_id}, _session, socket) do
    socket =
      assign(socket,
        run_id: run_id,
        asset_step_id: asset_step_id,
        attempt: nil,
        loading?: true,
        error: nil,
        nav_items: AssetCataloguePage.nav_items(:runs)
      )

    socket = if connected?(socket), do: load_attempt(socket), else: socket
    {:ok, socket}
  end

  @impl true
  def render(assigns) do
    ~H"""
    <RunAssetAttemptPage.run_asset_attempt_page {assigns} />
    """
  end

  defp load_attempt(socket) do
    context = socket.assigns.current_scope.operator_context

    case get_asset_attempt(context, socket.assigns.run_id, socket.assigns.asset_step_id) do
      {:ok, attempt} ->
        assign(socket,
          attempt: attempt_view(attempt, socket.assigns.current_scope),
          loading?: false
        )

      {:error, reason} ->
        assign(socket, loading?: false, error: load_error(reason))
    end
  end

  defp attempt_view(attempt, timezone) do
    status = Map.get(attempt, :status)

    %{
      id: attempt.asset_step_id,
      name: LogsViewModel.display_name(attempt.asset_ref),
      raw_status: status,
      status_label: LogsViewModel.status_label(status),
      status_tone: LogsViewModel.status_tone(status),
      output_metadata: attempt.output_metadata,
      error_summary: error_summary(attempt.error),
      facts: [
        %{label: "Started", value: LogsViewModel.timestamp_label(attempt.started_at, timezone)},
        %{label: "Finished", value: LogsViewModel.timestamp_label(attempt.finished_at, timezone)},
        %{label: "Duration", value: LogsViewModel.duration_ms_label(attempt.duration_ms)}
      ],
      execution_facts: [
        %{label: "Asset", value: LogsViewModel.ref_label(attempt.asset_ref)},
        %{label: "Attempt", value: attempt.attempt_number || "-"},
        %{label: "Stage", value: attempt.stage || "-"},
        %{label: "Execution pool", value: attempt.execution_pool || "-"},
        %{label: "Queue reason", value: attempt.queue_reason || "-"},
        %{label: "Window", value: window_label(attempt.window, timezone)},
        %{label: "Run id", value: attempt.run_id},
        %{label: "Asset step id", value: attempt.asset_step_id}
      ],
      logs_href: ~p"/runs/#{attempt.run_id}/assets/#{attempt.asset_step_id}/logs"
    }
  end

  defp get_asset_attempt(context, run_id, asset_step_id) do
    Application.get_env(
      :favn_view,
      :operator_run_asset_attempt_fun,
      &Orchestrator.get_operator_run_asset_attempt/3
    )
    |> then(fn fun -> fun.(context, run_id, asset_step_id) end)
  end

  defp load_error(reason) do
    case OperatorErrorLabels.load(reason) do
      :not_found -> "Asset run not found."
      label -> label
    end
  end

  defp error_summary(nil), do: nil
  defp error_summary(reason), do: OperatorErrorLabels.run_failure_detail(reason)

  defp window_label(%{label: label}, _timezone) when is_binary(label), do: label
  defp window_label(%{value: value}, _timezone) when is_binary(value), do: value

  defp window_label(%{start_at: %DateTime{} = start_at, end_at: %DateTime{} = end_at}, timezone) do
    "#{LogsViewModel.timestamp_label(start_at, timezone)} – #{LogsViewModel.timestamp_label(end_at, timezone)}"
  end

  defp window_label(nil, _timezone), do: "Full refresh"
  defp window_label(_window, _timezone), do: "Windowed"
end
