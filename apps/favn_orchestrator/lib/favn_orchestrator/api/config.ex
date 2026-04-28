defmodule FavnOrchestrator.API.Config do
  @moduledoc false

  @spec validate() :: :ok | {:error, term()}
  def validate do
    api_opts = Application.get_env(:favn_orchestrator, :api_server, [])

    if Keyword.get(api_opts, :enabled, false) do
      with :ok <- validate_bind_ip(api_opts) do
        tokens = Application.get_env(:favn_orchestrator, :api_service_tokens, [])

        case Enum.filter(tokens, &(is_binary(&1) and &1 != "")) do
          [] -> {:error, {:invalid_api_config, :missing_service_tokens}}
          _ -> :ok
        end
      end
    else
      :ok
    end
  end

  @doc false
  @spec bind_ip(keyword()) :: {:ok, :inet.ip4_address() | nil} | {:error, term()}
  def bind_ip(api_opts) when is_list(api_opts) do
    api_opts
    |> Keyword.get(:bind_ip, Keyword.get(api_opts, :host))
    |> normalize_bind_ip()
  end

  defp validate_bind_ip(api_opts) do
    case bind_ip(api_opts) do
      {:ok, _bind_ip} -> :ok
      {:error, reason} -> {:error, {:invalid_api_config, reason}}
    end
  end

  defp normalize_bind_ip(nil), do: {:ok, nil}

  defp normalize_bind_ip({a, b, c, d} = ip)
       when a in 0..255 and b in 0..255 and c in 0..255 and d in 0..255,
       do: {:ok, ip}

  defp normalize_bind_ip(host) when is_binary(host) do
    parsed =
      host
      |> String.split(".", parts: 4)
      |> Enum.map(&Integer.parse/1)

    case parsed do
      [{a, ""}, {b, ""}, {c, ""}, {d, ""}]
      when a in 0..255 and b in 0..255 and c in 0..255 and d in 0..255 ->
        {:ok, {a, b, c, d}}

      _other ->
        {:error, {:invalid_bind_ip, host}}
    end
  end

  defp normalize_bind_ip(other), do: {:error, {:invalid_bind_ip, other}}
end
