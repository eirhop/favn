defmodule Favn.Connection.Sanitizer do
  @moduledoc false

  alias Favn.Connection.Info
  alias Favn.Connection.Resolved

  @spec redact(Resolved.t()) :: Info.t()
  def redact(%Resolved{} = resolved) do
    redacted_config =
      Enum.reduce(resolved.secret_fields, resolved.config, fn key, acc ->
        if Map.has_key?(acc, key), do: Map.put(acc, key, :redacted), else: acc
      end)

    redacted_config =
      Enum.reduce(resolved.secret_paths, redacted_config, &redact_path(&2, &1))

    %Info{
      name: resolved.name,
      adapter: resolved.adapter,
      module: resolved.module,
      config: redacted_config,
      required_keys: resolved.required_keys,
      secret_fields: resolved.secret_fields,
      schema_keys: resolved.schema_keys,
      metadata: resolved.metadata
    }
  end

  defp redact_path(value, []), do: value

  defp redact_path(value, [key]) when is_map(value) do
    if Map.has_key?(value, key), do: Map.put(value, key, :redacted), else: value
  end

  defp redact_path(value, [key | rest]) when is_map(value) do
    case Map.fetch(value, key) do
      {:ok, child} -> Map.put(value, key, redact_path(child, rest))
      :error -> value
    end
  end

  defp redact_path(value, [key]) when is_list(value) and is_atom(key) do
    if Keyword.keyword?(value) and Keyword.has_key?(value, key) do
      Keyword.put(value, key, :redacted)
    else
      value
    end
  end

  defp redact_path(value, [key | rest]) when is_list(value) and is_atom(key) do
    if Keyword.keyword?(value) and Keyword.has_key?(value, key) do
      Keyword.update!(value, key, &redact_path(&1, rest))
    else
      value
    end
  end

  defp redact_path(value, [index | rest]) when is_list(value) and is_integer(index) do
    List.update_at(value, index, &redact_path(&1, rest))
  end

  defp redact_path(value, _path), do: value
end
