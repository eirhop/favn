defmodule Favn.Freshness do
  @moduledoc """
  Thin freshness policy helpers over persisted run/node window state.
  """

  alias Favn.Run
  alias Favn.Window.Key

  @type result :: %{
          status: :fresh | :stale | :missing,
          ref: Favn.asset_ref(),
          window_key: map() | nil,
          max_age_seconds: non_neg_integer() | nil,
          checked_at: DateTime.t(),
          last_materialized_at: DateTime.t() | nil,
          age_seconds: non_neg_integer() | nil
        }

  @spec check(Favn.asset_ref(), keyword()) :: {:ok, result()} | {:error, term()}
  def check(ref, opts \\ [])

  def check({module, name} = ref, opts)
      when is_atom(module) and is_atom(name) and is_list(opts) do
    max_age_seconds = Keyword.get(opts, :max_age_seconds)
    window_key = Keyword.get(opts, :window_key)
    checked_at = Keyword.get(opts, :now, DateTime.utc_now())
    limit = Keyword.get(opts, :limit)

    with :ok <- validate_max_age(max_age_seconds),
         :ok <- validate_window_key(window_key),
         :ok <- validate_checked_at(checked_at),
         {:ok, runs} <- Favn.list_runs(status: :ok, limit: limit) do
      materialized_at = latest_materialized_at(runs, {ref, window_key})
      {:ok, build_result(ref, window_key, max_age_seconds, checked_at, materialized_at)}
    end
  end

  def check(_ref, _opts), do: {:error, :invalid_target_ref}

  @spec missing_windows(Favn.asset_ref(), Favn.backfill_anchor_range(), keyword()) ::
          {:ok, [Favn.Plan.node_key()]} | {:error, term()}
  def missing_windows(ref, range, opts \\ [])

  def missing_windows({module, name} = ref, range, opts)
      when is_atom(module) and is_atom(name) and is_map(range) and is_list(opts) do
    limit = Keyword.get(opts, :limit)

    with {:ok, plan} <- Favn.plan_asset_run(ref, dependencies: :none, anchor_ranges: [range]),
         {:ok, runs} <- Favn.list_runs(status: :ok, limit: limit) do
      successful = successful_node_key_set(runs)
      missing = Enum.reject(plan.target_node_keys, &MapSet.member?(successful, &1))
      {:ok, missing}
    end
  end

  def missing_windows(_ref, _range, _opts), do: {:error, :invalid_backfill_range}

  defp latest_materialized_at(runs, node_key) do
    runs
    |> Enum.reduce([], fn
      %Run{node_results: node_results}, acc when is_map(node_results) ->
        case Map.get(node_results, node_key) do
          %{status: :ok, finished_at: %DateTime{} = finished_at} -> [finished_at | acc]
          _ -> acc
        end

      _, acc ->
        acc
    end)
    |> case do
      [] -> nil
      datetimes -> Enum.max_by(datetimes, &DateTime.to_unix(&1, :microsecond))
    end
  end

  defp successful_node_key_set(runs) do
    Enum.reduce(runs, MapSet.new(), fn
      %Run{node_results: node_results}, acc when is_map(node_results) ->
        Enum.reduce(node_results, acc, fn
          {node_key, %{status: :ok}}, set -> MapSet.put(set, node_key)
          _, set -> set
        end)

      _, acc ->
        acc
    end)
  end

  defp build_result(ref, window_key, max_age_seconds, checked_at, nil) do
    %{
      status: :missing,
      ref: ref,
      window_key: window_key,
      max_age_seconds: max_age_seconds,
      checked_at: checked_at,
      last_materialized_at: nil,
      age_seconds: nil
    }
  end

  defp build_result(ref, window_key, nil, checked_at, materialized_at) do
    %{
      status: :fresh,
      ref: ref,
      window_key: window_key,
      max_age_seconds: nil,
      checked_at: checked_at,
      last_materialized_at: materialized_at,
      age_seconds: max(DateTime.diff(checked_at, materialized_at, :second), 0)
    }
  end

  defp build_result(ref, window_key, max_age_seconds, checked_at, materialized_at) do
    age_seconds = max(DateTime.diff(checked_at, materialized_at, :second), 0)

    %{
      status: if(age_seconds <= max_age_seconds, do: :fresh, else: :stale),
      ref: ref,
      window_key: window_key,
      max_age_seconds: max_age_seconds,
      checked_at: checked_at,
      last_materialized_at: materialized_at,
      age_seconds: age_seconds
    }
  end

  defp validate_max_age(nil), do: :ok
  defp validate_max_age(value) when is_integer(value) and value >= 0, do: :ok
  defp validate_max_age(_), do: {:error, :invalid_max_age_seconds}

  defp validate_window_key(nil), do: :ok

  defp validate_window_key(value) when is_map(value) do
    case Key.validate(value) do
      :ok -> :ok
      {:error, _} -> {:error, :invalid_window_key}
    end
  end

  defp validate_window_key(_), do: {:error, :invalid_window_key}

  defp validate_checked_at(%DateTime{}), do: :ok
  defp validate_checked_at(_), do: {:error, :invalid_now}
end
