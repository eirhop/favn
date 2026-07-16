defmodule Favn.Dev.Secrets do
  @moduledoc false

  alias Favn.Dev.Config
  alias Favn.Dev.DistributedErlang
  alias Favn.Dev.State

  @schema_version 1

  @type root_opt :: [root_dir: Path.t()]

  @spec resolve(Config.t(), root_opt()) :: {:ok, map()} | {:error, term()}
  def resolve(%Config{} = config, opts \\ []) when is_list(opts) do
    with :ok <- State.ensure_layout(opts),
         {:ok, stored} <- read_or_initialize(opts),
         :ok <- validate(stored),
         :ok <- persist_if_missing(stored, opts),
         secrets <- apply_configured_overrides(stored, config),
         :ok <- validate(secrets) do
      {:ok, Map.drop(secrets, ["schema_version"])}
    end
  end

  defp read_or_initialize(opts) do
    case State.read_secrets(opts) do
      {:ok, %{"schema_version" => @schema_version} = secrets} ->
        {:ok, secrets}

      {:ok, _invalid} ->
        {:error, :invalid_local_secrets}

      {:error, :not_found} ->
        {:ok, new_secrets()}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp new_secrets do
    %{
      "schema_version" => @schema_version,
      "service_token" => random_secret(24),
      "web_session_secret" => random_secret(48),
      "rpc_cookie" => random_cookie(32)
    }
  end

  defp apply_configured_overrides(secrets, config) do
    secrets
    |> maybe_put("service_token", config.service_token)
    |> maybe_put("web_session_secret", config.web_session_secret)
  end

  defp maybe_put(secrets, _key, nil), do: secrets
  defp maybe_put(secrets, key, value), do: Map.put(secrets, key, value)

  defp validate(secrets) do
    with token when is_binary(token) and token != "" <- secrets["service_token"],
         session when is_binary(session) and byte_size(session) >= 32 <-
           secrets["web_session_secret"],
         :ok <- DistributedErlang.validate_cookie(secrets["rpc_cookie"]) do
      :ok
    else
      _invalid -> {:error, :invalid_local_secrets}
    end
  end

  defp persist_if_missing(secrets, opts) do
    case State.read_secrets(opts) do
      {:ok, ^secrets} -> :ok
      _other -> State.write_secrets(secrets, opts)
    end
  end

  defp random_cookie(size) do
    size
    |> :crypto.strong_rand_bytes()
    |> Base.encode32(padding: false)
  end

  defp random_secret(size) when is_integer(size) and size > 0 do
    size
    |> :crypto.strong_rand_bytes()
    |> Base.url_encode64(padding: false)
  end
end
