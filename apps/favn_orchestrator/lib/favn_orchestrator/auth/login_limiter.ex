defmodule FavnOrchestrator.Auth.LoginLimiter do
  @moduledoc false

  @max_keys 10_000
  @minimum_retention_seconds 300

  @type state :: %{optional(term()) => map()}

  @spec begin_attempt(state(), [term()], DateTime.t(), pos_integer(), pos_integer()) ::
          {:allowed | :blocked, state()}
  def begin_attempt(attempts, keys, now, limit, backoff_seconds) do
    attempts = prune(attempts, now, max(backoff_seconds, @minimum_retention_seconds))

    cond do
      Enum.any?(keys, &blocked?(attempts, &1, now, limit)) ->
        {:blocked, attempts}

      new_key_count(attempts, keys) + map_size(attempts) > @max_keys ->
        {:blocked, attempts}

      true ->
        {:allowed, Enum.reduce(keys, attempts, &reserve(&2, &1, now))}
    end
  end

  @spec finish_attempt(
          state(),
          [term()],
          :ok | :error,
          DateTime.t(),
          pos_integer(),
          pos_integer()
        ) ::
          state()
  def finish_attempt(attempts, keys, result, now, limit, backoff_seconds) do
    Enum.reduce(keys, attempts, fn key, acc ->
      finish_key(acc, key, result, now, limit, backoff_seconds)
    end)
  end

  defp blocked?(attempts, key, now, limit) do
    case Map.get(attempts, key) do
      %{blocked_until: %DateTime{} = blocked_until} -> DateTime.compare(blocked_until, now) == :gt
      %{failures: failures, in_flight: in_flight} -> failures + in_flight >= limit
      nil -> false
    end
  end

  defp reserve(attempts, key, now) do
    current = Map.get(attempts, key, empty_attempt(now))
    Map.put(attempts, key, %{current | in_flight: current.in_flight + 1, updated_at: now})
  end

  defp finish_key(attempts, key, :ok, _now, _limit, _backoff_seconds),
    do: Map.delete(attempts, key)

  defp finish_key(attempts, key, :error, now, limit, backoff_seconds) do
    current = Map.get(attempts, key, empty_attempt(now))
    failures = current.failures + 1

    updated = %{
      failures: failures,
      in_flight: max(current.in_flight - 1, 0),
      blocked_until:
        if(failures >= limit, do: DateTime.add(now, backoff_seconds, :second), else: nil),
      updated_at: now
    }

    Map.put(attempts, key, updated)
  end

  defp prune(attempts, now, retention_seconds) do
    Map.reject(attempts, fn {_key, attempt} ->
      attempt.in_flight == 0 and
        DateTime.diff(now, attempt.updated_at, :second) >= retention_seconds
    end)
  end

  defp new_key_count(attempts, keys), do: Enum.count(keys, &(not Map.has_key?(attempts, &1)))

  defp empty_attempt(now),
    do: %{failures: 0, in_flight: 0, blocked_until: nil, updated_at: now}
end
