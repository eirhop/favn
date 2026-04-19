defmodule Favn.Dev.Config do
  @moduledoc """
  Resolves minimal local developer tooling configuration.
  """

  @enforce_keys [
    :storage,
    :sqlite_path,
    :orchestrator_api_enabled,
    :orchestrator_port,
    :web_port,
    :orchestrator_base_url,
    :web_base_url,
    :service_token,
    :web_session_secret
  ]
  defstruct [
    :storage,
    :sqlite_path,
    :orchestrator_api_enabled,
    :orchestrator_port,
    :web_port,
    :orchestrator_base_url,
    :web_base_url,
    :service_token,
    :web_session_secret
  ]

  @type storage_mode :: :memory | :sqlite

  @type t :: %__MODULE__{
          storage: storage_mode(),
          sqlite_path: Path.t(),
          orchestrator_api_enabled: boolean(),
          orchestrator_port: pos_integer(),
          web_port: pos_integer(),
          orchestrator_base_url: String.t(),
          web_base_url: String.t(),
          service_token: String.t() | nil,
          web_session_secret: String.t() | nil
        }

  @typedoc "Keyword overrides used by local tooling tasks."
  @type opts :: keyword()

  @default_storage :memory
  @default_sqlite_path ".favn/data/orchestrator.sqlite3"
  @default_orchestrator_port 4101
  @default_web_port 4173

  @doc """
  Resolves local tooling configuration from app config plus runtime overrides.
  """
  @spec resolve(opts()) :: t()
  def resolve(opts \\ []) when is_list(opts) do
    app_config = Application.get_env(:favn, :dev, [])
    merged = Keyword.merge(app_config, opts)

    orchestrator_port = Keyword.get(merged, :orchestrator_port, @default_orchestrator_port)
    web_port = Keyword.get(merged, :web_port, @default_web_port)

    %__MODULE__{
      storage: normalize_storage(Keyword.get(merged, :storage, @default_storage)),
      sqlite_path: Keyword.get(merged, :sqlite_path, @default_sqlite_path),
      orchestrator_api_enabled: Keyword.get(merged, :orchestrator_api_enabled, true),
      orchestrator_port: orchestrator_port,
      web_port: web_port,
      orchestrator_base_url:
        Keyword.get(merged, :orchestrator_base_url, "http://127.0.0.1:#{orchestrator_port}"),
      web_base_url: Keyword.get(merged, :web_base_url, "http://127.0.0.1:#{web_port}"),
      service_token: Keyword.get(merged, :service_token),
      web_session_secret: Keyword.get(merged, :web_session_secret)
    }
  end

  defp normalize_storage(:sqlite), do: :sqlite
  defp normalize_storage("sqlite"), do: :sqlite
  defp normalize_storage(_other), do: :memory
end
