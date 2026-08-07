defmodule FavnOrchestrator.Storage.ExecutionLeaseCodec do
  @moduledoc false

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

  defp field_value(map, field) do
    case Map.fetch(map, field) do
      {:ok, value} -> value
      :error -> Map.get(map, Atom.to_string(field))
    end
  end
end
