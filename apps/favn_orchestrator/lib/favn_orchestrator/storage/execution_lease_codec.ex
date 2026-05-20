defmodule FavnOrchestrator.Storage.ExecutionLeaseCodec do
  @moduledoc false

  @spec normalize(map()) :: {:ok, map()} | {:error, term()}
  def normalize(lease) when is_map(lease) do
    with {:ok, lease_id} <- fetch_string_field(lease, :lease_id),
         {:ok, run_id} <- fetch_string_field(lease, :run_id),
         {:ok, asset_step_id} <- fetch_string_field(lease, :asset_step_id),
         {:ok, scopes} <- normalize_scopes(field_value(lease, :scopes)),
         {:ok, acquired_at} <- fetch_datetime_field(lease, :acquired_at),
         {:ok, expires_at} <- fetch_datetime_field(lease, :expires_at) do
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
    {:ok, Base.encode64(:erlang.term_to_binary(lease))}
  rescue
    exception -> {:error, {:invalid_execution_lease_payload, exception}}
  end

  @spec decode(binary()) :: {:ok, map()} | {:error, term()}
  def decode(payload) when is_binary(payload) do
    payload
    |> Base.decode64!()
    |> :erlang.binary_to_term([:safe])
    |> normalize()
  rescue
    exception -> {:error, {:invalid_execution_lease_payload, exception}}
  end

  def scope_identity(scope), do: {to_string(scope.kind), scope.key}

  def normalize_scope(scope) when is_map(scope) do
    with {:ok, kind} <- fetch_atom_or_string_field(scope, :kind),
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
      {:ok, normalized} -> {:ok, Enum.reverse(normalized)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp normalize_scopes(_scopes), do: {:error, :invalid_execution_lease_scopes}

  defp fetch_string_field(map, field) do
    case field_value(map, field) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _other -> {:error, {:invalid_execution_lease_field, field}}
    end
  end

  defp fetch_atom_or_string_field(map, field) do
    case field_value(map, field) do
      value when is_atom(value) and not is_nil(value) -> {:ok, value}
      value when is_binary(value) and value != "" -> {:ok, value}
      _other -> {:error, {:invalid_execution_lease_field, field}}
    end
  end

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

  defp field_value(map, field), do: Map.get(map, field) || Map.get(map, Atom.to_string(field))
end
