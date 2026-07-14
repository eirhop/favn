defmodule FavnOrchestrator.Storage.ExecutionLeaseCodec do
  @moduledoc false

  alias FavnOrchestrator.Storage.PayloadCodec

  @spec normalize(map()) :: {:ok, map()} | {:error, term()}
  def normalize(lease) when is_map(lease) do
    with {:ok, lease_id} <- fetch_string_field(lease, :lease_id),
         {:ok, run_id} <- fetch_string_field(lease, :run_id),
         {:ok, asset_step_id} <- fetch_string_field(lease, :asset_step_id),
         {:ok, scopes} <- normalize_scopes(field_value(lease, :scopes)),
         {:ok, acquired_at} <- fetch_datetime_field(lease, :acquired_at),
         {:ok, expires_at} <- fetch_datetime_field(lease, :expires_at),
         :ok <- validate_expiry(acquired_at, expires_at) do
      {:ok,
       %{
         lease_id: lease_id,
         run_id: run_id,
         asset_step_id: asset_step_id,
         scopes: scopes,
         acquired_at: acquired_at,
         expires_at: expires_at
       }}
    end
  end

  def normalize(_lease), do: {:error, :invalid_execution_lease}

  @spec encode(map()) :: {:ok, binary()} | {:error, term()}
  def encode(lease) when is_map(lease) do
    with {:ok, normalized} <- normalize(lease) do
      PayloadCodec.encode(normalized)
    end
  rescue
    exception -> {:error, {:invalid_execution_lease_payload, exception}}
  end

  @spec decode(binary()) :: {:ok, map()} | {:error, term()}
  def decode(payload) when is_binary(payload) do
    with {:ok, lease} <- PayloadCodec.decode(payload) do
      normalize(lease)
    end
  rescue
    exception -> {:error, {:invalid_execution_lease_payload, exception}}
  end

  def decode(_payload), do: {:error, :invalid_execution_lease_payload}

  @spec scope_identity(map()) :: {String.t(), String.t()}
  def scope_identity(%{kind: kind, key: key}), do: {to_string(kind), key}

  def normalize_scope(scope) when is_map(scope) do
    with {:ok, kind} <- normalize_scope_kind(field_value(scope, :kind)),
         {:ok, key} <- fetch_string_field(scope, :key),
         {:ok, limit} <- fetch_positive_integer_field(scope, :limit) do
      {:ok, %{kind: kind, key: key, limit: limit}}
    end
  end

  def normalize_scope(_scope), do: {:error, :invalid_execution_lease_scope}

  defp normalize_scopes(scopes) when is_list(scopes) and scopes != [] do
    scopes
    |> Enum.reduce_while({:ok, []}, fn scope, {:ok, acc} ->
      case normalize_scope(scope) do
        {:ok, normalized} -> {:cont, {:ok, [normalized | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, normalized} ->
        normalized =
          normalized
          |> Enum.reverse()
          |> Enum.uniq_by(&scope_identity/1)

        {:ok, normalized}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp normalize_scopes(_scopes), do: {:error, :invalid_execution_lease_scopes}

  defp fetch_string_field(map, field) do
    case field_value(map, field) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _other -> {:error, {:invalid_execution_lease_field, field}}
    end
  end

  defp normalize_scope_kind(kind) when kind in [:run, :pool, :global], do: {:ok, kind}
  defp normalize_scope_kind("run"), do: {:ok, :run}
  defp normalize_scope_kind("pool"), do: {:ok, :pool}
  defp normalize_scope_kind("global"), do: {:ok, :global}

  defp normalize_scope_kind(_kind),
    do: {:error, {:invalid_execution_lease_field, :kind}}

  defp fetch_positive_integer_field(map, field) do
    case field_value(map, field) do
      value when is_integer(value) and value > 0 -> {:ok, value}
      _other -> {:error, {:invalid_execution_lease_field, field}}
    end
  end

  defp fetch_datetime_field(map, field) do
    case field_value(map, field) do
      %DateTime{} = value -> {:ok, value}
      _other -> {:error, {:invalid_execution_lease_field, field}}
    end
  end

  defp validate_expiry(acquired_at, expires_at) do
    if DateTime.compare(expires_at, acquired_at) == :gt,
      do: :ok,
      else: {:error, {:invalid_execution_lease_field, :expires_at}}
  end

  defp field_value(map, field) do
    case Map.fetch(map, field) do
      {:ok, value} -> value
      :error -> Map.get(map, Atom.to_string(field))
    end
  end
end
