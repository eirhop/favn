defmodule FavnOrchestrator.Operator.Catalogue.Status do
  @moduledoc """
  Applies target status and selects the latest run evidence for catalogue DTOs.
  """

  alias FavnOrchestrator.AssetFreshnessState
  alias FavnOrchestrator.Operator.Catalogue.RunHistory
  alias FavnOrchestrator.TargetStatus
  alias FavnOrchestrator.Persistence.Results.TargetStatus, as: PersistenceTargetStatus

  @doc "Applies a projected target status to a target DTO."
  @spec put(map(), TargetStatus.t()) :: map()
  def put(target, %TargetStatus{} = status) when is_map(target) do
    target
    |> Map.put(:status, status.status)
    |> Map.put(:latest_run_id, status.latest_run_id)
    |> Map.put(:latest_run_status, status.latest_run_status)
    |> Map.put(:latest_run_at, status.latest_run_at)
    |> Map.put(:latest_run_duration_ms, status.latest_run_duration_ms)
  end

  def put(target, %PersistenceTargetStatus{} = status) when is_map(target) do
    target
    |> Map.put(:status, status.status)
    |> Map.put(:latest_run_id, status.run_id)
    |> Map.put(:latest_run_status, run_status(status.status))
    |> Map.put(:latest_run_at, status.updated_at)
    |> Map.put(:latest_run_duration_ms, nil)
  end

  @doc "Maps freshness or run evidence to a catalogue health status."
  @spec catalogue(AssetFreshnessState.t() | nil, map() | nil) ::
          :healthy | :running | :failed | :unknown
  def catalogue(%AssetFreshnessState{} = freshness, _run) do
    case freshness.latest_attempt_status || freshness.status do
      status when status in [:ok, :skipped_fresh] -> :healthy
      :running -> :running
      status when status in [:error, :cancelled, :timed_out, :blocked] -> :failed
      _other -> :unknown
    end
  end

  def catalogue(nil, run), do: run_status(run)

  @doc "Returns the best latest run id from freshness and run evidence."
  @spec latest_run_id(AssetFreshnessState.t() | nil, map() | nil) :: String.t() | nil
  def latest_run_id(%AssetFreshnessState{latest_attempt_run_id: id}, _run) when is_binary(id),
    do: id

  def latest_run_id(%AssetFreshnessState{latest_success_run_id: id}, _run) when is_binary(id),
    do: id

  def latest_run_id(_freshness, %{id: id}) when is_binary(id), do: id
  def latest_run_id(_freshness, _run), do: nil

  @doc "Returns the best latest run status from freshness and run evidence."
  @spec latest_run_status(AssetFreshnessState.t() | nil, map() | nil) :: atom() | nil
  def latest_run_status(%AssetFreshnessState{latest_attempt_status: status}, _run)
      when not is_nil(status),
      do: status

  def latest_run_status(%AssetFreshnessState{status: status}, _run) when not is_nil(status),
    do: status

  def latest_run_status(_freshness, %{status: status}), do: status
  def latest_run_status(_freshness, _run), do: nil

  @doc "Returns the best latest run timestamp from freshness and run evidence."
  @spec latest_run_at(AssetFreshnessState.t() | nil, map() | nil) :: DateTime.t() | nil
  def latest_run_at(%AssetFreshnessState{latest_attempt_at: %DateTime{} = at}, _run), do: at
  def latest_run_at(%AssetFreshnessState{latest_success_at: %DateTime{} = at}, _run), do: at
  def latest_run_at(_freshness, run) when is_map(run), do: RunHistory.time_key(run)
  def latest_run_at(_freshness, _run), do: nil

  defp run_status(nil), do: :unknown
  defp run_status(%{status: status}) when status in [:pending, :running], do: :running
  defp run_status(%{status: :ok}), do: :healthy

  defp run_status(%{status: status})
       when status in [:partial, :error, :cancelled, :timed_out],
       do: :failed

  defp run_status(:healthy), do: :ok
  defp run_status(:running), do: :running
  defp run_status(:failed), do: :error
  defp run_status(:unknown), do: nil
  defp run_status(_run), do: :unknown
end
