defmodule FavnOrchestrator.Scheduler.Readiness do
  @moduledoc """
  Validates that an enabled scheduler is ticking without errors or excess lag.
  """

  @minimum_lag_budget_ms 5_000
  @lag_multiplier 3

  @doc "Checks scheduler diagnostics against its configured tick interval."
  @spec check(map(), DateTime.t()) :: :ok | {:error, atom()}
  def check(diagnostics, now \\ DateTime.utc_now())
      when is_map(diagnostics) and is_struct(now, DateTime) do
    cond do
      not is_nil(Map.get(diagnostics, :last_error)) ->
        {:error, :scheduler_tick_failed}

      Map.get(diagnostics, :auto_tick?) != true ->
        {:error, :scheduler_automatic_ticks_disabled}

      true ->
        check_freshness(diagnostics, now)
    end
  end

  defp check_freshness(diagnostics, now) do
    with {:ok, tick_ms} <- positive_tick_ms(diagnostics),
         {:ok, reference} <- freshness_reference(diagnostics) do
      if DateTime.diff(now, reference, :millisecond) <= lag_budget_ms(tick_ms),
        do: :ok,
        else: {:error, :scheduler_tick_stale}
    end
  end

  defp positive_tick_ms(%{tick_ms: tick_ms}) when is_integer(tick_ms) and tick_ms > 0,
    do: {:ok, tick_ms}

  defp positive_tick_ms(_diagnostics), do: {:error, :scheduler_tick_interval_unavailable}

  defp freshness_reference(%{last_tick_at: %DateTime{} = last_tick_at}),
    do: {:ok, last_tick_at}

  defp freshness_reference(%{last_tick_at: nil, started_at: %DateTime{} = started_at}),
    do: {:ok, started_at}

  defp freshness_reference(_diagnostics), do: {:error, :scheduler_tick_time_unavailable}

  defp lag_budget_ms(tick_ms), do: max(tick_ms * @lag_multiplier, @minimum_lag_budget_ms)
end
