defmodule Favn.Dev.ChildEnvironment do
  @moduledoc false

  @proxy_keys ["HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"]

  @spec sanitize_proxy_variables(%{optional(String.t()) => String.t() | nil}) ::
          %{optional(String.t()) => String.t() | nil}
  def sanitize_proxy_variables(env) when is_map(env) do
    Enum.reduce(@proxy_keys, env, fn key, sanitized ->
      case Map.fetch(sanitized, key) do
        {:ok, ""} -> Map.put(sanitized, key, nil)
        {:ok, _value} -> sanitized
        :error -> maybe_unset_inherited_empty(sanitized, key)
      end
    end)
  end

  @spec empty_proxy_overrides() :: %{optional(String.t()) => nil}
  def empty_proxy_overrides do
    sanitize_proxy_variables(%{})
  end

  defp maybe_unset_inherited_empty(env, key) do
    if System.get_env(key) == "", do: Map.put(env, key, nil), else: env
  end
end
