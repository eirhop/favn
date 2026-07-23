defmodule Favn.GenerationDataPlaneMarker do
  @moduledoc """
  JSON-safe active-generation marker persisted by the control plane.

  SQL adapters exchange typed generation-marker structs with the runner. The
  orchestrator persists the same identity as this canonical map so later
  activation requests can perform an exact compare-and-swap.
  """

  @type t :: %{
          required(:target_id) => String.t(),
          required(:active_relation) => map(),
          required(:active_generation_id) => String.t(),
          required(:activation_operation_id) => String.t(),
          required(:activation_token) => String.t(),
          required(:activated_at) => String.t()
        }

  @marker_fields [
    :target_id,
    :active_relation,
    :active_generation_id,
    :activation_operation_id,
    :activation_token,
    :activated_at
  ]
  @relation_fields [:connection, :catalog, :schema, :name]
  @uuid_pattern ~r/\A[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\z/

  @doc "Validates the canonical marker shape and its target-generation identity."
  @spec validate(term(), String.t(), String.t()) :: :ok | {:error, term()}
  def validate(marker, target_id, target_generation_id) when is_map(marker) do
    with {:ok, fields} <- canonical_fields(marker, @marker_fields, :marker),
         :ok <- identifier(:target_id, fields.target_id),
         :ok <- identity(:target_id, fields.target_id, target_id),
         :ok <- generation_id(fields.active_generation_id),
         :ok <-
           identity(
             :active_generation_id,
             fields.active_generation_id,
             target_generation_id
           ),
         :ok <- identifier(:activation_operation_id, fields.activation_operation_id),
         :ok <- identifier(:activation_token, fields.activation_token),
         :ok <- relation(fields.active_relation),
         :ok <- timestamp(fields.activated_at) do
      :ok
    end
  end

  def validate(marker, _target_id, _target_generation_id),
    do: {:error, {:invalid_generation_data_plane_marker, marker}}

  defp canonical_fields(value, allowed_fields, kind) when is_map(value) do
    Enum.reduce_while(value, {:ok, %{}}, fn {key, child}, {:ok, fields} ->
      case canonical_key(key, allowed_fields) do
        {:ok, field} ->
          if Map.has_key?(fields, field) do
            {:halt, {:error, {:duplicate_generation_data_plane_marker_field, kind, field}}}
          else
            {:cont, {:ok, Map.put(fields, field, child)}}
          end

        :error ->
          {:halt, {:error, {:invalid_generation_data_plane_marker_field, kind, key}}}
      end
    end)
    |> case do
      {:ok, fields} when map_size(fields) == length(allowed_fields) -> {:ok, fields}
      {:ok, fields} -> {:error, {:incomplete_generation_data_plane_marker, kind, fields}}
      {:error, _reason} = error -> error
    end
  end

  defp canonical_fields(value, _allowed_fields, kind),
    do: {:error, {:invalid_generation_data_plane_marker_field, kind, value}}

  defp canonical_key(key, allowed_fields) when is_atom(key) do
    if key in allowed_fields, do: {:ok, key}, else: :error
  end

  defp canonical_key(key, allowed_fields) when is_binary(key) do
    case Enum.find(allowed_fields, &(Atom.to_string(&1) == key)) do
      nil -> :error
      field -> {:ok, field}
    end
  end

  defp canonical_key(_key, _allowed_fields), do: :error

  defp identity(_field, value, value), do: :ok

  defp identity(field, actual, expected),
    do: {:error, {:generation_data_plane_marker_identity_mismatch, field, actual, expected}}

  defp identifier(_field, value) when is_binary(value) and byte_size(value) in 1..255,
    do: :ok

  defp identifier(field, value),
    do: {:error, {:invalid_generation_data_plane_marker_identifier, field, value}}

  defp generation_id(value) when is_binary(value) do
    if Regex.match?(@uuid_pattern, value),
      do: :ok,
      else: {:error, {:invalid_generation_data_plane_marker_generation_id, value}}
  end

  defp generation_id(value),
    do: {:error, {:invalid_generation_data_plane_marker_generation_id, value}}

  defp relation(value) do
    with {:ok, fields} <- canonical_fields(value, @relation_fields, :active_relation),
         :ok <- optional_identifier(:connection, fields.connection),
         :ok <- optional_identifier(:catalog, fields.catalog),
         :ok <- optional_identifier(:schema, fields.schema) do
      identifier(:name, fields.name)
    end
  end

  defp optional_identifier(_field, nil), do: :ok
  defp optional_identifier(_field, value) when is_atom(value), do: :ok
  defp optional_identifier(field, value), do: identifier(field, value)

  defp timestamp(%DateTime{}), do: :ok

  defp timestamp(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, _datetime, 0} -> :ok
      _invalid -> {:error, {:invalid_generation_data_plane_marker_timestamp, value}}
    end
  end

  defp timestamp(value),
    do: {:error, {:invalid_generation_data_plane_marker_timestamp, value}}
end
