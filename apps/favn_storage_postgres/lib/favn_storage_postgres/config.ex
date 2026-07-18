defmodule FavnStoragePostgres.Config do
  @moduledoc """
  Validates and normalizes the Favn-owned PostgreSQL connection configuration.

  Returned diagnostics contain no URL, password, certificate contents, or other
  credential material.
  """

  @default_statement_timeout 15_000
  @default_lock_timeout 3_000
  @default_idle_transaction_timeout 15_000

  @doc "Returns validated repo options from application configuration and overrides."
  @spec repo_options(keyword()) :: {:ok, keyword()} | {:error, term()}
  def repo_options(overrides \\ []) when is_list(overrides) do
    configured = Application.get_env(:favn_storage_postgres, FavnStoragePostgres.Repo, [])
    options = Keyword.merge(configured, overrides)

    with {:ok, url} <- fetch_url(options),
         :ok <- validate_url(url),
         {:ok, pool_size} <- positive_integer(options, :pool_size, 15),
         {:ok, queue_target} <- positive_integer(options, :queue_target, 50),
         {:ok, queue_interval} <- positive_integer(options, :queue_interval, 1_000),
         {:ok, timeout} <- positive_integer(options, :timeout, 15_000),
         {:ok, ssl} <- ssl_options(options, url) do
      {:ok,
       options
       |> Keyword.drop([:ssl_mode, :ssl_ca_file, :allow_insecure_database?])
       |> Keyword.put(:url, url)
       |> Keyword.put(:pool_size, pool_size)
       |> Keyword.put(:queue_target, queue_target)
       |> Keyword.put(:queue_interval, queue_interval)
       |> Keyword.put(:timeout, timeout)
       |> Keyword.put(:ssl, ssl)
       |> Keyword.put(:migration_default_prefix, "favn_control")
       |> Keyword.put(:after_connect, {__MODULE__, :after_connect, []})}
    end
  end

  @doc "Applies bounded, UTC PostgreSQL session defaults to a new connection."
  @spec after_connect(pid()) :: :ok
  def after_connect(connection) do
    settings = [
      {"application_name", application_name()},
      {"statement_timeout", Integer.to_string(@default_statement_timeout)},
      {"lock_timeout", Integer.to_string(@default_lock_timeout)},
      {"idle_in_transaction_session_timeout",
       Integer.to_string(@default_idle_transaction_timeout)},
      {"timezone", "UTC"}
    ]

    Postgrex.query!(connection, "SET search_path TO pg_catalog, favn_control", [])

    Enum.each(settings, fn {name, value} ->
      Postgrex.query!(connection, "SELECT pg_catalog.set_config($1, $2, false)", [name, value])
    end)

    :ok
  end

  @doc "Returns redacted pool and TLS configuration for diagnostics."
  @spec redacted(keyword()) :: map()
  def redacted(options) do
    %{
      configured?: Keyword.has_key?(options, :url),
      pool_size: Keyword.get(options, :pool_size),
      queue_target_ms: Keyword.get(options, :queue_target),
      queue_interval_ms: Keyword.get(options, :queue_interval),
      timeout_ms: Keyword.get(options, :timeout),
      tls?: Keyword.get(options, :ssl, false) != false
    }
  end

  defp fetch_url(options) do
    case Keyword.get(options, :url) || System.get_env("FAVN_DATABASE_URL") do
      url when is_binary(url) and url != "" -> {:ok, url}
      _value -> {:error, :database_url_required}
    end
  end

  defp validate_url(url) do
    case URI.parse(url) do
      %URI{scheme: scheme, host: host}
      when scheme in ["ecto", "postgres", "postgresql"] and is_binary(host) ->
        :ok

      _uri ->
        {:error, :invalid_database_url}
    end
  end

  defp positive_integer(options, key, default) do
    case Keyword.get(options, key, default) do
      value when is_integer(value) and value > 0 -> {:ok, value}
      _value -> {:error, {:invalid_database_option, key}}
    end
  end

  defp ssl_options(options, url) do
    mode = Keyword.get(options, :ssl_mode, default_ssl_mode())

    case mode do
      :disable ->
        if production?() and not Keyword.get(options, :allow_insecure_database?, false),
          do: {:error, :production_tls_required},
          else: {:ok, false}

      :verify_full ->
        verified_tls_options(options, url)

      _mode ->
        {:error, :invalid_ssl_mode}
    end
  end

  defp verified_tls_options(options, url) do
    ca_file = Keyword.get(options, :ssl_ca_file) || System.get_env("FAVN_DATABASE_SSL_CA_FILE")
    hostname = URI.parse(url).host

    if is_binary(ca_file) and ca_file != "" and File.regular?(ca_file) and is_binary(hostname) do
      {:ok,
       [
         verify: :verify_peer,
         cacertfile: ca_file,
         server_name_indication: String.to_charlist(hostname),
         customize_hostname_check: [match_fun: :public_key.pkix_verify_hostname_match_fun(:https)]
       ]}
    else
      {:error, :database_tls_trust_required}
    end
  end

  defp application_name do
    instance = System.get_env("FAVN_INSTANCE_ID", "local")
    "favn_orchestrator:#{String.slice(instance, 0, 64)}"
  end

  defp default_ssl_mode, do: if(production?(), do: :verify_full, else: :disable)
  defp production?, do: Application.get_env(:favn_storage_postgres, :environment) == :prod
end
