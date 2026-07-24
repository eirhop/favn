defmodule FavnView.ProductionRuntimeConfig do
  @moduledoc """
  Production runtime configuration for the Phoenix web boundary.

  `favn_view` runs in the same BEAM as the orchestrator for the current
  production target, so readiness does not require orchestrator URLs or service
  tokens. This module validates web-owned operator settings only.
  """

  @default_timeout_ms 1_000
  @post_drain_shutdown_timeout_ms 5_000
  @max_timeout_ms 30_000
  @max_proxy_cidrs 32
  @max_proxy_cidrs_bytes 4_096
  @persistent_key {__MODULE__, :config}

  alias Favn.DeploymentMode

  @type config :: %{
          deployment_mode: DeploymentMode.t(),
          public_origin: String.t(),
          orchestrator_readiness_timeout_ms: pos_integer(),
          secret_key_base: String.t(),
          bind_host: String.t(),
          bind_ip: :inet.ip4_address(),
          port: :inet.port_number(),
          trusted_proxy_cidrs: [map()],
          http_server: map(),
          shutdown_drain_timeout_ms: pos_integer(),
          session_cookie_options: keyword(),
          force_ssl?: boolean()
        }

  @type runtime_config :: %{
          deployment_mode: DeploymentMode.t(),
          trusted_proxy_cidrs: [map()],
          http_server: map(),
          orchestrator_readiness_timeout_ms: pos_integer(),
          session_cookie_options: keyword(),
          force_ssl?: boolean()
        }

  @doc """
  Applies production web config when explicitly enabled or when web env is set.
  """
  @spec apply_from_env_if_configured(map()) :: :ok | {:error, map()}
  def apply_from_env_if_configured(env) when is_map(env) do
    if configured?(env) do
      apply_from_env(env)
    else
      :ok
    end
  end

  @doc """
  Validates and applies production web config.
  """
  @spec apply_from_env(map()) :: :ok | {:error, map()}
  def apply_from_env(env) when is_map(env) do
    case validate(env) do
      {:ok, config} ->
        apply(config)

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc "Applies one already validated web configuration before the endpoint starts."
  @spec apply(config()) :: :ok
  def apply(config) when is_map(config) do
    Application.put_env(:favn_view, :public_origin, config.public_origin)
    configure_endpoint(config)

    Application.put_env(
      :favn_view,
      :orchestrator_readiness_timeout_ms,
      config.orchestrator_readiness_timeout_ms
    )

    put_endpoint_secret_key_base(config.secret_key_base)
    Application.put_env(:favn_view, :production_runtime_diagnostics, diagnostics(config))
    :persistent_term.put(@persistent_key, runtime_config(config))
    :ok
  end

  @doc """
  Validates production web config without mutating application env.
  """
  @spec validate(map()) :: {:ok, config()} | {:error, map()}
  def validate(env) when is_map(env) do
    with {:ok, deployment_mode} <- DeploymentMode.from_env(env),
         {:ok, public_origin} <- public_origin(env, deployment_mode),
         {:ok, timeout_ms} <- timeout_ms(env),
         {:ok, secret_key_base} <- secret_key_base(env),
         {:ok, {bind_host, bind_ip}} <- bind_host(env),
         {:ok, port} <- port(env),
         {:ok, trusted_proxy_cidrs} <- trusted_proxy_cidrs(env),
         {:ok, http_server} <- http_server(env),
         {:ok, shutdown_drain_timeout_ms} <- shutdown_drain_timeout_ms(env),
         {:ok, session_cookie_options} <- session_cookie_options(deployment_mode) do
      {:ok,
       %{
         deployment_mode: deployment_mode,
         public_origin: public_origin,
         orchestrator_readiness_timeout_ms: timeout_ms,
         secret_key_base: secret_key_base,
         bind_host: bind_host,
         bind_ip: bind_ip,
         port: port,
         trusted_proxy_cidrs: trusted_proxy_cidrs,
         http_server: http_server,
         shutdown_drain_timeout_ms: shutdown_drain_timeout_ms,
         session_cookie_options: session_cookie_options,
         force_ssl?: deployment_mode == :production
       }}
    else
      {:error, reason} -> {:error, %{status: :invalid, error: redact(reason)}}
    end
  end

  @doc """
  Returns redacted production web config diagnostics.
  """
  @spec diagnostics(config()) :: map()
  def diagnostics(config) when is_map(config) do
    %{
      status: :ok,
      deployment_mode: config.deployment_mode,
      public_origin: %{
        configured?: true,
        redacted: true
      },
      listener: %{bind_host: config.bind_host, port: config.port},
      trusted_proxies: %{configured_count: length(config.trusted_proxy_cidrs)},
      http_server: config.http_server,
      shutdown: %{drain_timeout_ms: config.shutdown_drain_timeout_ms},
      orchestrator: %{
        boundary: :same_beam_facade,
        readiness_timeout_ms: config.orchestrator_readiness_timeout_ms
      }
    }
  end

  @spec configured?(map()) :: boolean()
  def configured?(_env) do
    Application.get_env(:favn_view, :production_runtime_config, false) == true
  end

  @doc "Returns the frozen HTTP limits, or development defaults before production config loads."
  @spec http_server() :: map()
  def http_server do
    case :persistent_term.get(@persistent_key, :missing) do
      %{http_server: http_server} -> http_server
      :missing -> default_http_server()
    end
  end

  @doc "Returns the frozen session-cookie contract for HTTP and LiveView sockets."
  @spec session_cookie_options() :: keyword()
  def session_cookie_options do
    case :persistent_term.get(@persistent_key, :missing) do
      %{session_cookie_options: options} ->
        options

      :missing ->
        Application.get_env(:favn_view, :session_cookie_options, [])
    end
  end

  @doc "Returns whether the current runtime requires proxy-aware HTTPS redirects."
  @spec force_ssl?() :: boolean()
  def force_ssl? do
    case :persistent_term.get(@persistent_key, :missing) do
      %{force_ssl?: force_ssl?} -> force_ssl?
      :missing -> Application.get_env(:favn_view, :trusted_proxy_force_ssl, false)
    end
  end

  @doc "Returns whether a socket peer belongs to the configured private proxy allowlist."
  @spec trusted_proxy?(:inet.ip_address()) :: boolean()
  def trusted_proxy?(address) when is_tuple(address) do
    Enum.any?(runtime_config!().trusted_proxy_cidrs, &cidr_match?(address, &1))
  end

  @spec configured_timeout_ms() :: pos_integer()
  def configured_timeout_ms do
    case :persistent_term.get(@persistent_key, :missing) do
      %{orchestrator_readiness_timeout_ms: timeout_ms} ->
        timeout_ms

      :missing ->
        Application.get_env(:favn_view, :orchestrator_readiness_timeout_ms, @default_timeout_ms)
    end
  end

  defp public_origin(env, deployment_mode) do
    case fetch(env, "FAVN_VIEW_PUBLIC_ORIGIN") do
      {:ok, origin} ->
        validate_public_origin(origin, deployment_mode)

      :error ->
        {:error, {:missing_env, "FAVN_VIEW_PUBLIC_ORIGIN"}}
    end
  end

  defp validate_public_origin(origin, :production) do
    case URI.parse(origin) do
      %URI{
        scheme: "https",
        host: host,
        path: path,
        query: nil,
        fragment: nil,
        userinfo: nil,
        port: port
      }
      when is_binary(host) and host != "" and path in [nil, ""] and
             (is_nil(port) or port in 1..65_535) ->
        {:ok, origin}

      _other ->
        {:error, {:invalid_env, "FAVN_VIEW_PUBLIC_ORIGIN", "absolute https origin"}}
    end
  end

  @doc """
  Validates browser session cookie options required for production.
  """
  @spec validate_session_cookie_options(keyword()) :: :ok | {:error, term()}
  def validate_session_cookie_options(options) when is_list(options) do
    cond do
      Keyword.get(options, :secure) != true ->
        {:error, {:invalid_session_cookie, :secure_required}}

      Keyword.get(options, :http_only) != true ->
        {:error, {:invalid_session_cookie, :http_only_required}}

      Keyword.get(options, :same_site) != "Lax" ->
        {:error, {:invalid_session_cookie, :same_site_lax_required}}

      not is_binary(Keyword.get(options, :encryption_salt)) ->
        {:error, {:invalid_session_cookie, :encryption_salt_required}}

      true ->
        :ok
    end
  end

  defp session_cookie_options(:production) do
    options = Application.get_env(:favn_view, :session_cookie_options, [])

    if Application.get_env(:favn_view, :require_secure_cookies, false) do
      with :ok <- validate_session_cookie_options(options), do: {:ok, options}
    else
      {:ok, options}
    end
  end

  defp timeout_ms(env) do
    env
    |> fetch("FAVN_VIEW_ORCHESTRATOR_READINESS_TIMEOUT_MS")
    |> case do
      {:ok, value} ->
        int(
          "FAVN_VIEW_ORCHESTRATOR_READINESS_TIMEOUT_MS",
          value,
          100,
          @max_timeout_ms
        )

      :error ->
        {:ok, @default_timeout_ms}
    end
  end

  defp secret_key_base(env) do
    case fetch(env, "FAVN_VIEW_SECRET_KEY_BASE") do
      {:ok, value} -> validate_secret_key_base(value)
      :error -> {:error, {:missing_env, "FAVN_VIEW_SECRET_KEY_BASE"}}
    end
  end

  defp validate_secret_key_base(value) do
    if byte_size(value) >= 64 do
      {:ok, value}
    else
      {:error, {:invalid_secret_env, "FAVN_VIEW_SECRET_KEY_BASE", "at least 64 characters"}}
    end
  end

  defp bind_host(env) do
    with {:ok, host} <- required_or_default(env, "FAVN_VIEW_BIND_HOST", "0.0.0.0") do
      case :inet.parse_ipv4_address(String.to_charlist(host)) do
        {:ok, ip} -> {:ok, {host, ip}}
        {:error, _reason} -> {:error, {:invalid_env, "FAVN_VIEW_BIND_HOST", "IPv4 address"}}
      end
    end
  end

  defp port(env) do
    with {:ok, value} <- required_or_default(env, "FAVN_VIEW_PORT", "4000") do
      int("FAVN_VIEW_PORT", value, 1, 65_535)
    end
  end

  defp trusted_proxy_cidrs(env) do
    with {:ok, raw} <- required(env, "FAVN_VIEW_TRUSTED_PROXY_CIDRS"),
         true <- byte_size(raw) <= @max_proxy_cidrs_bytes,
         entries <- raw |> String.split(",", trim: true) |> Enum.map(&String.trim/1),
         true <- entries != [] and length(entries) <= @max_proxy_cidrs,
         {:ok, cidrs} <- parse_proxy_cidrs(entries) do
      {:ok, cidrs}
    else
      {:error, _reason} = error -> error
      false -> invalid_proxy_cidrs()
    end
  end

  defp parse_proxy_cidrs(entries) do
    Enum.reduce_while(entries, {:ok, []}, fn entry, {:ok, acc} ->
      case parse_proxy_cidr(entry) do
        {:ok, cidr} -> {:cont, {:ok, [cidr | acc]}}
        {:error, _reason} -> {:halt, invalid_proxy_cidrs()}
      end
    end)
    |> case do
      {:ok, cidrs} -> {:ok, Enum.reverse(cidrs)}
      {:error, _reason} = error -> error
    end
  end

  defp parse_proxy_cidr(entry) do
    with [address_text, prefix_text] <- String.split(entry, "/", parts: 2),
         {:ok, address} <- :inet.parse_address(String.to_charlist(address_text)),
         {prefix, ""} <- Integer.parse(prefix_text),
         true <- valid_prefix?(address, prefix),
         cidr = %{address: address, prefix: prefix},
         true <- private_cidr?(cidr) do
      {:ok, cidr}
    else
      _invalid -> invalid_proxy_cidrs()
    end
  end

  defp private_cidr?(%{address: address, prefix: prefix}) do
    private_ranges(address)
    |> Enum.any?(fn %{prefix: private_prefix} = private ->
      prefix >= private_prefix and cidr_match?(address, private)
    end)
  end

  defp private_ranges(address) when tuple_size(address) == 4 do
    [
      %{address: {10, 0, 0, 0}, prefix: 8},
      %{address: {172, 16, 0, 0}, prefix: 12},
      %{address: {192, 168, 0, 0}, prefix: 16},
      %{address: {127, 0, 0, 0}, prefix: 8}
    ]
  end

  defp private_ranges(address) when tuple_size(address) == 8 do
    [
      %{address: {0xFC00, 0, 0, 0, 0, 0, 0, 0}, prefix: 7},
      %{address: {0xFE80, 0, 0, 0, 0, 0, 0, 0}, prefix: 10},
      %{address: {0, 0, 0, 0, 0, 0, 0, 1}, prefix: 128}
    ]
  end

  defp valid_prefix?(address, prefix) when tuple_size(address) == 4,
    do: prefix in 0..32

  defp valid_prefix?(address, prefix) when tuple_size(address) == 8,
    do: prefix in 0..128

  defp cidr_match?(address, %{address: network, prefix: prefix}) do
    address_binary = ip_binary(address)
    network_binary = ip_binary(network)

    if byte_size(address_binary) == byte_size(network_binary) do
      full_bytes = div(prefix, 8)
      remaining_bits = rem(prefix, 8)

      binary_part(address_binary, 0, full_bytes) ==
        binary_part(network_binary, 0, full_bytes) and
        partial_byte_matches?(address_binary, network_binary, full_bytes, remaining_bits)
    else
      false
    end
  end

  defp partial_byte_matches?(_address, _network, _offset, 0), do: true

  defp partial_byte_matches?(address, network, offset, bits) do
    mask = Bitwise.band(Bitwise.bsl(0xFF, 8 - bits), 0xFF)

    binary_part(address, offset, 1) |> :binary.decode_unsigned() |> Bitwise.band(mask) ==
      binary_part(network, offset, 1) |> :binary.decode_unsigned() |> Bitwise.band(mask)
  end

  defp ip_binary({a, b, c, d}), do: <<a, b, c, d>>

  defp ip_binary({a, b, c, d, e, f, g, h}),
    do: <<a::16, b::16, c::16, d::16, e::16, f::16, g::16, h::16>>

  defp invalid_proxy_cidrs do
    {:error,
     {:invalid_env, "FAVN_VIEW_TRUSTED_PROXY_CIDRS", "comma-separated private IPv4 or IPv6 CIDRs"}}
  end

  defp http_server(env) do
    with {:ok, max_connections} <-
           env_int(env, "FAVN_HTTP_MAX_CONNECTIONS", "1024", 1, 100_000),
         {:ok, request_timeout_ms} <-
           env_int(env, "FAVN_HTTP_REQUEST_TIMEOUT_MS", "30000", 1_000, 120_000),
         {:ok, idle_timeout_ms} <-
           env_int(env, "FAVN_HTTP_IDLE_TIMEOUT_MS", "60000", 1_000, 300_000),
         {:ok, body_limit_bytes} <-
           env_int(
             env,
             "FAVN_HTTP_BODY_LIMIT_BYTES",
             Integer.to_string(1 * 1_024 * 1_024),
             64 * 1_024,
             8 * 1_024 * 1_024
           ) do
      {:ok,
       default_http_server()
       |> Map.put(:max_connections, max_connections)
       |> Map.put(:request_timeout_ms, request_timeout_ms)
       |> Map.put(:idle_timeout_ms, idle_timeout_ms)
       |> Map.put(:body_limit_bytes, body_limit_bytes)}
    end
  end

  defp default_http_server do
    %{
      max_connections: 1_024,
      request_timeout_ms: 30_000,
      idle_timeout_ms: 60_000,
      body_limit_bytes: 1_048_576
    }
  end

  defp runtime_config(config) do
    Map.take(config, [
      :deployment_mode,
      :trusted_proxy_cidrs,
      :http_server,
      :orchestrator_readiness_timeout_ms,
      :session_cookie_options,
      :force_ssl?
    ])
  end

  defp runtime_config! do
    case :persistent_term.get(@persistent_key, :missing) do
      :missing -> raise "Favn View production runtime configuration is not loaded"
      config -> config
    end
  end

  defp env_int(env, name, default, min, max) do
    with {:ok, value} <- required_or_default(env, name, default) do
      int(name, value, min, max)
    end
  end

  defp shutdown_drain_timeout_ms(env),
    do: env_int(env, "FAVN_SHUTDOWN_DRAIN_TIMEOUT_MS", "120000", 1_000, 3_600_000)

  defp int(name, value, min, max) do
    case Integer.parse(value) do
      {int, ""} when int >= min and int <= max -> {:ok, int}
      _other -> {:error, {:invalid_env, name, "#{min}..#{max}"}}
    end
  end

  defp required(env, name) do
    case fetch(env, name) do
      {:ok, value} -> {:ok, value}
      :error -> {:error, {:missing_env, name}}
    end
  end

  defp required_or_default(env, name, default) do
    case fetch(env, name) do
      {:ok, value} -> {:ok, value}
      :error -> {:ok, default}
    end
  end

  defp fetch(env, name) do
    case Map.get(env, name) do
      value when is_binary(value) ->
        value = String.trim(value)
        if value == "", do: :error, else: {:ok, value}

      _other ->
        :error
    end
  end

  defp redact({:missing_env, name}), do: {:missing_env, name}
  defp redact({:invalid_env, name, expected}), do: {:invalid_env, name, expected}
  defp redact({:invalid_secret_env, name, expected}), do: {:invalid_secret_env, name, expected}
  defp redact({:invalid_session_cookie, reason}), do: {:invalid_session_cookie, reason}

  defp configure_endpoint(%{public_origin: origin} = config) when is_binary(origin) do
    endpoint_config = Application.get_env(:favn_view, FavnView.Endpoint, [])

    Application.put_env(
      :favn_view,
      FavnView.Endpoint,
      endpoint_config
      |> Keyword.put(:url, endpoint_url(origin))
      |> Keyword.put(:check_origin, [origin])
      |> Keyword.put(:server, true)
      |> Keyword.put(:http,
        ip: config.bind_ip,
        port: config.port,
        thousand_island_options: [
          num_acceptors: 1,
          num_connections: config.http_server.max_connections,
          read_timeout: config.http_server.idle_timeout_ms,
          shutdown_timeout: @post_drain_shutdown_timeout_ms
        ]
      )
    )

    :ok
  end

  defp put_endpoint_secret_key_base(secret_key_base) do
    endpoint_config = Application.get_env(:favn_view, FavnView.Endpoint, [])

    Application.put_env(
      :favn_view,
      FavnView.Endpoint,
      Keyword.put(endpoint_config, :secret_key_base, secret_key_base)
    )

    :ok
  end

  defp endpoint_url(origin) do
    uri = URI.parse(origin)

    [
      scheme: uri.scheme,
      host: uri.host,
      port: uri.port || default_port(uri.scheme)
    ]
  end

  defp default_port("https"), do: 443
  defp default_port("http"), do: 80
end
