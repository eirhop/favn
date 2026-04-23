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
end
