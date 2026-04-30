defmodule Favn.Dev.Config do
  @moduledoc """
  Resolves minimal local developer tooling configuration.
  """

  @enforce_keys [
    :storage,
    :sqlite_path,
    :postgres,
    :orchestrator_api_enabled,
    :orchestrator_port,
    :web_port,
    :orchestrator_base_url,
    :web_base_url,
    :scheduler_enabled,
    :service_token,
    :web_session_secret
  ]
  defstruct [
    :storage,
    :sqlite_path,
    :postgres,
    :orchestrator_api_enabled,
    :orchestrator_port,
    :web_port,
    :orchestrator_base_url,
    :web_base_url,
    :scheduler_enabled,
    :service_token,
    :web_session_secret
  ]

  @type postgres_opts :: %{
          hostname: String.t(),
          port: pos_integer(),
          username: String.t(),
          password: String.t(),
          database: String.t(),
          ssl: boolean(),
          pool_size: pos_integer()
        }

  @type storage_mode :: :memory | :sqlite | :postgres

  @type t :: %__MODULE__{
          storage: storage_mode(),
          sqlite_path: Path.t(),
          postgres: postgres_opts(),
          orchestrator_api_enabled: boolean(),
          orchestrator_port: pos_integer(),
          web_port: pos_integer(),
          orchestrator_base_url: String.t(),
          web_base_url: String.t(),
          scheduler_enabled: boolean(),
          service_token: String.t() | nil,
          web_session_secret: String.t() | nil
        }

  @typedoc "Keyword overrides used by local tooling tasks."
  @type opts :: keyword()

  @default_storage :memory
  @default_sqlite_path ".favn/data/orchestrator.sqlite3"
  @default_postgres %{
    hostname: "127.0.0.1",
    port: 5432,
    username: "postgres",
    password: "postgres",
    database: "favn",
    ssl: false,
    pool_size: 10
  }
  @default_orchestrator_port 4101
  @default_web_port 4173

  @doc """
  Resolves local tooling configuration from app config plus runtime overrides.
  """
  @spec resolve(opts()) :: t()
  def resolve(opts \\ []) when is_list(opts) do
    dev_config = Application.get_env(:favn, :dev, [])
    local_config = Application.get_env(:favn, :local, [])
    merged = dev_config |> Keyword.merge(local_config) |> Keyword.merge(opts)

    orchestrator_port =
      merged
      |> Keyword.get(:orchestrator_port, @default_orchestrator_port)
      |> normalize_int(@default_orchestrator_port)

    web_port =
      merged
      |> Keyword.get(:web_port, @default_web_port)
      |> normalize_int(@default_web_port)

    %__MODULE__{
      storage: normalize_storage(Keyword.get(merged, :storage, @default_storage)),
      sqlite_path: Keyword.get(merged, :sqlite_path, @default_sqlite_path),
      postgres: normalize_postgres(Keyword.get(merged, :postgres, @default_postgres)),
      orchestrator_api_enabled: Keyword.get(merged, :orchestrator_api_enabled, true),
      orchestrator_port: orchestrator_port,
      web_port: web_port,
      orchestrator_base_url:
        Keyword.get(merged, :orchestrator_base_url, "http://127.0.0.1:#{orchestrator_port}"),
      web_base_url: Keyword.get(merged, :web_base_url, "http://127.0.0.1:#{web_port}"),
      scheduler_enabled: normalize_bool(Keyword.get(merged, :scheduler, false), false),
      service_token: Keyword.get(merged, :service_token),
      web_session_secret: Keyword.get(merged, :web_session_secret)
    }
  end

  defp normalize_storage(:sqlite), do: :sqlite
  defp normalize_storage("sqlite"), do: :sqlite
  defp normalize_storage(:postgres), do: :postgres
  defp normalize_storage("postgres"), do: :postgres
  defp normalize_storage(_other), do: :memory

  defp normalize_postgres(value) when is_list(value),
    do: value |> Enum.into(%{}) |> normalize_postgres()

  defp normalize_postgres(value) when is_map(value) do
    map = for {k, v} <- value, into: %{}, do: {normalize_postgres_key(k), v}

    %{
      hostname: to_string(Map.get(map, :hostname, @default_postgres.hostname)),
      port: normalize_int(Map.get(map, :port, @default_postgres.port), @default_postgres.port),
      username: to_string(Map.get(map, :username, @default_postgres.username)),
      password: to_string(Map.get(map, :password, @default_postgres.password)),
      database: to_string(Map.get(map, :database, @default_postgres.database)),
      ssl: normalize_bool(Map.get(map, :ssl, @default_postgres.ssl), @default_postgres.ssl),
      pool_size:
        normalize_int(
          Map.get(map, :pool_size, @default_postgres.pool_size),
          @default_postgres.pool_size
        )
    }
  end

  defp normalize_postgres(_other), do: @default_postgres

  defp normalize_postgres_key(key) when is_atom(key), do: key
  defp normalize_postgres_key("hostname"), do: :hostname
  defp normalize_postgres_key("port"), do: :port
  defp normalize_postgres_key("username"), do: :username
  defp normalize_postgres_key("password"), do: :password
  defp normalize_postgres_key("database"), do: :database
  defp normalize_postgres_key("ssl"), do: :ssl
  defp normalize_postgres_key("pool_size"), do: :pool_size
  defp normalize_postgres_key(_key), do: :unknown

  defp normalize_int(value, _default) when is_integer(value) and value > 0, do: value

  defp normalize_int(value, default) when is_binary(value) do
    case value |> String.trim() |> Integer.parse() do
      {int, ""} when int > 0 -> int
      _ -> default
    end
  end

  defp normalize_int(_value, default), do: default

  defp normalize_bool(value, _default) when is_boolean(value), do: value
  defp normalize_bool("true", _default), do: true
  defp normalize_bool("false", _default), do: false
  defp normalize_bool(_value, default), do: default
end
