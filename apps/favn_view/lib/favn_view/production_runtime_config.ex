defmodule FavnView.ProductionRuntimeConfig do
  @moduledoc """
  Production runtime configuration for the Phoenix web boundary.

  `favn_view` runs in the same BEAM as the orchestrator for the current
  production target, so readiness does not require orchestrator URLs or service
  tokens. This module validates web-owned operator settings only.
  """

  @default_timeout_ms 1_000

  @type config :: %{
          public_origin: String.t() | nil,
          orchestrator_readiness_timeout_ms: pos_integer(),
          secret_key_base: String.t()
        }

  @doc """
  Applies production web config when explicitly enabled or when web env is set.
  """
  @spec apply_from_env_if_configured(map()) :: :ok | {:error, map()}
  def apply_from_env_if_configured(env \\ System.get_env()) when is_map(env) do
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
  def apply_from_env(env \\ System.get_env()) when is_map(env) do
    case validate(env) do
      {:ok, config} ->
        Application.put_env(:favn_view, :public_origin, config.public_origin)
        configure_endpoint(config)

        Application.put_env(
          :favn_view,
          :orchestrator_readiness_timeout_ms,
          config.orchestrator_readiness_timeout_ms
        )

        put_endpoint_secret_key_base(config.secret_key_base)

        Application.put_env(:favn_view, :production_runtime_diagnostics, diagnostics(config))
        :ok

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Validates production web config without mutating application env.
  """
  @spec validate(map()) :: {:ok, config()} | {:error, map()}
  def validate(env \\ System.get_env()) when is_map(env) do
    with {:ok, public_origin} <- public_origin(env),
         {:ok, timeout_ms} <- timeout_ms(env),
         {:ok, secret_key_base} <- secret_key_base(env),
         :ok <- secure_cookie_config() do
      {:ok,
       %{
         public_origin: public_origin,
         orchestrator_readiness_timeout_ms: timeout_ms,
         secret_key_base: secret_key_base
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
      public_origin: %{
        configured?: is_binary(config.public_origin),
        redacted: true
      },
      orchestrator: %{
        boundary: :same_beam_facade,
        readiness_timeout_ms: config.orchestrator_readiness_timeout_ms
      }
    }
  end

  @spec configured?(map()) :: boolean()
  def configured?(env) when is_map(env) do
    Application.get_env(:favn_view, :production_runtime_config, false) == true or
      Map.has_key?(env, "FAVN_VIEW_PUBLIC_ORIGIN") or
      Map.has_key?(env, "FAVN_VIEW_SECRET_KEY_BASE") or
      Map.has_key?(env, "FAVN_VIEW_ORCHESTRATOR_READINESS_TIMEOUT_MS")
  end

  @spec configured_timeout_ms() :: pos_integer()
  def configured_timeout_ms do
    Application.get_env(:favn_view, :orchestrator_readiness_timeout_ms, @default_timeout_ms)
  end

  defp public_origin(env) do
    case fetch(env, "FAVN_VIEW_PUBLIC_ORIGIN") do
      {:ok, origin} ->
        validate_public_origin(origin)

      :error ->
        if require_public_origin?(),
          do: {:error, {:missing_env, "FAVN_VIEW_PUBLIC_ORIGIN"}},
          else: {:ok, nil}
    end
  end

  defp require_public_origin? do
    Application.get_env(:favn_view, :production_runtime_config, false) == true
  end

  defp validate_public_origin(origin) do
    case URI.parse(origin) do
      %URI{scheme: scheme, host: host, path: path, query: nil, fragment: nil}
      when scheme in ["http", "https"] and is_binary(host) and path in [nil, ""] ->
        validate_public_origin_scheme(scheme, host, origin)

      _other ->
        {:error,
         {:invalid_env, "FAVN_VIEW_PUBLIC_ORIGIN",
          "absolute https origin or localhost http origin"}}
    end
  end

  defp validate_public_origin_scheme("https", _host, origin), do: {:ok, origin}

  defp validate_public_origin_scheme("http", host, origin) do
    if localhost?(host) do
      {:ok, origin}
    else
      {:error,
       {:invalid_env, "FAVN_VIEW_PUBLIC_ORIGIN", "absolute https origin or localhost http origin"}}
    end
  end

  defp localhost?(host), do: host in ["localhost", "127.0.0.1", "::1"]

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

  defp secure_cookie_config do
    if Application.get_env(:favn_view, :require_secure_cookies, false) do
      case Application.fetch_env(:favn_view, :session_cookie_options) do
        {:ok, options} -> validate_session_cookie_options(options)
        :error -> {:error, {:invalid_session_cookie, :missing_options}}
      end
    else
      :ok
    end
  end

  defp timeout_ms(env) do
    env
    |> fetch("FAVN_VIEW_ORCHESTRATOR_READINESS_TIMEOUT_MS")
    |> case do
      {:ok, value} -> positive_int("FAVN_VIEW_ORCHESTRATOR_READINESS_TIMEOUT_MS", value)
      :error -> {:ok, @default_timeout_ms}
    end
  end

  defp secret_key_base(env) do
    case fetch(env, "FAVN_VIEW_SECRET_KEY_BASE") do
      {:ok, value} -> validate_secret_key_base(value)
      :error -> {:error, {:missing_env, "FAVN_VIEW_SECRET_KEY_BASE"}}
    end
  end

  defp validate_secret_key_base(value) do
    if String.length(value) >= 64 do
      {:ok, value}
    else
      {:error, {:invalid_secret_env, "FAVN_VIEW_SECRET_KEY_BASE", "at least 64 characters"}}
    end
  end

  defp positive_int(name, value) do
    case Integer.parse(value) do
      {int, ""} when int > 0 -> {:ok, int}
      _other -> {:error, {:invalid_env, name, "> 0"}}
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

  defp configure_endpoint(%{public_origin: nil}), do: :ok

  defp configure_endpoint(%{public_origin: origin}) when is_binary(origin) do
    endpoint_config = Application.get_env(:favn_view, FavnView.Endpoint, [])

    Application.put_env(
      :favn_view,
      FavnView.Endpoint,
      endpoint_config
      |> Keyword.put(:url, endpoint_url(origin))
      |> Keyword.put(:check_origin, [origin])
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
