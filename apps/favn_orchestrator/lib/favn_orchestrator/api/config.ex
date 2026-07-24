defmodule FavnOrchestrator.API.Config do
  @moduledoc false

  alias FavnOrchestrator.Auth.ServiceTokens
  alias FavnOrchestrator.API.ManifestPublication.Config, as: ManifestPublicationConfig
  alias FavnOrchestrator.RuntimeConfig

  @default_bind_ip {127, 0, 0, 1}
  @default_port 4101
  @post_drain_shutdown_timeout_ms 5_000

  @spec validate() :: :ok | {:error, term()}
  def validate do
    api_opts = Application.get_env(:favn_orchestrator, :api_server, [])

    if Keyword.get(api_opts, :enabled, false) do
      with {:ok, _manifest_publication} <- ManifestPublicationConfig.from_app_env(),
           {:ok, _options} <- server_options(api_opts),
           :ok <- validate_access_config() do
        :ok
      else
        {:error, {:invalid_api_config, _reason}} = error -> error
        {:error, reason} -> {:error, {:invalid_api_config, reason}}
      end
    else
      :ok
    end
  end

  @doc false
  @spec server_options(keyword()) :: {:ok, keyword()} | {:error, term()}
  def server_options(api_opts) when is_list(api_opts) do
    with {:ok, bind_ip} <- bind_ip(api_opts),
         {:ok, port} <- port(api_opts) do
      http = RuntimeConfig.http_server()

      {:ok,
       [
         port: port,
         ip: bind_ip,
         thousand_island_options: [
           num_acceptors: 1,
           num_connections: http.max_connections,
           read_timeout: http.idle_timeout_ms,
           shutdown_timeout: @post_drain_shutdown_timeout_ms
         ]
       ]}
    end
  end

  @doc false
  @spec bind_ip(keyword()) :: {:ok, :inet.ip4_address()} | {:error, term()}
  def bind_ip(api_opts) when is_list(api_opts) do
    bind_ip =
      Keyword.get(api_opts, :bind_ip) ||
        Keyword.get(api_opts, :host) ||
        @default_bind_ip

    normalize_bind_ip(bind_ip)
  end

  defp validate_access_config do
    case ServiceTokens.validate_runtime_config() do
      :ok ->
        :ok

      {:error, {:missing_env, "FAVN_ORCHESTRATOR_API_SERVICE_TOKENS"}} ->
        {:error, {:invalid_api_config, :missing_service_tokens}}

      {:error, reason} ->
        {:error, reason}
    end
  end

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

  defp port(api_opts) do
    case Keyword.get(api_opts, :port, @default_port) do
      port when is_integer(port) and port in 1..65_535 -> {:ok, port}
      port -> {:error, {:invalid_port, port}}
    end
  end
end
