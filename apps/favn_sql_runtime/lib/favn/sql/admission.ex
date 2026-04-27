defmodule Favn.SQL.Admission do
  @moduledoc false

  alias Favn.SQL.Admission.Limiter
  alias Favn.SQL.{ConcurrencyPolicy, Session}

  @permit_key {__MODULE__, :permits}
  @write_prefixes ~w(
    alter analyze attach checkpoint copy create delete detach drop export import insert install
    load merge pragma set truncate update vacuum
  )

  @spec with_permit(Session.t(), atom(), iodata() | term(), (() -> term())) :: term()
  def with_permit(%Session{concurrency_policy: %ConcurrencyPolicy{} = policy}, operation, payload, fun)
      when is_function(fun, 0) do
    if permit_required?(policy, operation, payload) do
      acquire_and_run(policy, fun)
    else
      fun.()
    end
  end

  def with_permit(%Session{}, _operation, _payload, fun) when is_function(fun, 0), do: fun.()

  @spec acquire_session(ConcurrencyPolicy.t() | nil) :: term()
  def acquire_session(%ConcurrencyPolicy{scope: scope} = policy) do
    if permit_required?(policy, :connect, nil) do
      if already_holding?(scope) do
        increment_held(scope)
        {:borrowed, scope, self()}
      else
        acquire_lease(policy)
      end
    end
  end

  def acquire_session(_policy), do: nil

  @spec release_session(term()) :: :ok
  def release_session({_kind, scope, owner}) when owner == self() do
    if decrement_held(scope) == 0 do
      Limiter.release(scope)
    end

    :ok
  end

  def release_session(_lease), do: :ok

  defp permit_required?(%ConcurrencyPolicy{limit: :unlimited}, _operation, _payload), do: false
  defp permit_required?(%ConcurrencyPolicy{applies_to: :all}, _operation, _payload), do: true

  defp permit_required?(%ConcurrencyPolicy{applies_to: :writes}, operation, payload) do
    operation in [:execute, :materialize, :transaction] or write_query?(operation, payload)
  end

  defp write_query?(:query, statement) do
    statement
    |> IO.iodata_to_binary()
    |> String.trim_leading()
    |> String.downcase()
    |> first_token()
    |> then(&(&1 in @write_prefixes or is_nil(&1)))
  rescue
    _ -> true
  end

  defp write_query?(_operation, _payload), do: false

  defp first_token(""), do: nil

  defp first_token(sql) do
    sql
    |> String.split(~r/\s+/, parts: 2)
    |> List.first()
  end

  defp acquire_and_run(%ConcurrencyPolicy{scope: scope, limit: limit}, fun) do
    if already_holding?(scope) do
      fun.()
    else
      _lease = acquire_lease(scope, limit)

      try do
        fun.()
      after
        if decrement_held(scope) == 0 do
          Limiter.release(scope)
        end
      end
    end
  end

  defp acquire_lease(%ConcurrencyPolicy{scope: scope, limit: limit}), do: acquire_lease(scope, limit)

  defp acquire_lease(scope, limit) do
    :ok = Limiter.acquire(scope, limit)
    increment_held(scope)
    {:held, scope, self()}
  end

  defp already_holding?(scope), do: Map.get(held(), scope, 0) > 0

  defp increment_held(scope), do: Process.put(@permit_key, Map.update(held(), scope, 1, &(&1 + 1)))

  defp decrement_held(scope) do
    next =
      held()
      |> Map.update(scope, 0, &max(&1 - 1, 0))
      |> Enum.reject(fn {_scope, count} -> count == 0 end)
      |> Map.new()

    Process.put(@permit_key, next)
    Map.get(next, scope, 0)
  end

  defp held do
    case Process.get(@permit_key, %{}) do
      value when is_map(value) -> value
      _ -> %{}
    end
  end
end
