defmodule FavnOrchestrator.Auth.ServiceTokens do
  @moduledoc false

  @min_token_bytes 32
  @weak_tokens MapSet.new(
                 ~w(change-me changeme replace-me secret password test test-service-token)
               )

  @type token_config :: %{
          required(:service_identity) => String.t(),
          required(:token_hash) => String.t(),
          required(:enabled) => boolean()
        }

  @spec min_token_bytes() :: pos_integer()
  def min_token_bytes, do: @min_token_bytes

  @spec from_env_string(String.t()) :: {:ok, [token_config()]} | {:error, term()}
  def from_env_string(raw) when is_binary(raw) do
    tokens =
      raw
      |> String.split(",", trim: true)
      |> Enum.map(&String.trim/1)

    with :ok <- ensure_present(tokens),
         {:ok, configs} <- parse_env_tokens(tokens),
         :ok <- reject_duplicate_identities(configs) do
      {:ok, configs}
    end
  end

  @spec authenticate(String.t() | nil, [term()]) ::
          {:ok, String.t()} | {:error, :service_unauthorized}
  def authenticate(provided, configured_tokens) when is_binary(provided) and provided != "" do
    provided_hash = hash_token(provided)

    configured_tokens
    |> normalize_many()
    |> Enum.find_value(fn
      {:ok, %{enabled: true, token_hash: token_hash, service_identity: identity}} ->
        if byte_size(token_hash) == byte_size(provided_hash) and
             Plug.Crypto.secure_compare(token_hash, provided_hash) do
          {:ok, identity}
        else
          false
        end

      _other ->
        false
    end)
    |> case do
      {:ok, identity} -> {:ok, identity}
      _other -> {:error, :service_unauthorized}
    end
  end

  def authenticate(_provided, _configured_tokens), do: {:error, :service_unauthorized}

  @spec validate_config([term()]) :: :ok | {:error, term()}
  def validate_config(configured_tokens) when is_list(configured_tokens) do
    configured_tokens
    |> normalize_many()
    |> Enum.filter(&match?({:ok, %{enabled: true}}, &1))
    |> case do
      [] -> {:error, {:invalid_api_config, :missing_service_tokens}}
      _tokens -> :ok
    end
  end

  def validate_config(_configured_tokens),
    do: {:error, {:invalid_api_config, :missing_service_tokens}}

  @spec configured_count([term()]) :: non_neg_integer()
  def configured_count(configured_tokens) when is_list(configured_tokens) do
    configured_tokens
    |> normalize_many()
    |> Enum.count(&match?({:ok, %{enabled: true}}, &1))
  end

  def configured_count(_configured_tokens), do: 0

  @spec hash_token(String.t()) :: String.t()
  def hash_token(token) when is_binary(token) do
    :sha256
    |> :crypto.hash(token)
    |> Base.url_encode64(padding: false)
  end

  defp ensure_present([]), do: {:error, {:missing_env, "FAVN_ORCHESTRATOR_API_SERVICE_TOKENS"}}
  defp ensure_present(_tokens), do: :ok

  defp parse_env_tokens(tokens) do
    tokens
    |> Enum.reduce_while({:ok, []}, fn token, {:ok, acc} ->
      case parse_env_token(token) do
        {:ok, config} -> {:cont, {:ok, [config | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, configs} -> {:ok, Enum.reverse(configs)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp parse_env_token(entry) do
    with [identity, token] <- String.split(entry, ":", parts: 2),
         {:ok, identity} <- normalize_identity(identity),
         :ok <- validate_secret(token) do
      {:ok,
       %{service_identity: identity, token_hash: hash_token(String.trim(token)), enabled: true}}
    else
      [_token_without_identity] ->
        {:error, {:invalid_env, "FAVN_ORCHESTRATOR_API_SERVICE_TOKENS", "identity:token"}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp normalize_many(configured_tokens), do: Enum.map(configured_tokens, &normalize_config/1)

  defp normalize_config(config) when is_list(config) do
    if Keyword.keyword?(config) do
      normalize_config(Map.new(config))
    else
      {:error, :invalid_service_token_config}
    end
  end

  defp normalize_config({identity, token}) when is_binary(identity) and is_binary(token) do
    normalize_config(%{service_identity: identity, token: token, enabled: true})
  end

  defp normalize_config(config) when is_map(config) do
    with {:ok, identity} <- normalize_identity(value(config, :service_identity)),
         enabled? <- Map.get(config, :enabled, Map.get(config, "enabled", true)),
         true <- is_boolean(enabled?),
         {:ok, token_hash} <- normalize_token_hash(config) do
      {:ok, %{service_identity: identity, token_hash: token_hash, enabled: enabled?}}
    else
      _other -> {:error, :invalid_service_token_config}
    end
  end

  defp normalize_config(_config), do: {:error, :invalid_service_token_config}

  defp normalize_token_hash(config) do
    case value(config, :token_hash) do
      hash when is_binary(hash) and hash != "" -> {:ok, hash}
      _other -> normalize_raw_token(value(config, :token))
    end
  end

  defp normalize_raw_token(token) when is_binary(token) and token != "",
    do: {:ok, hash_token(token)}

  defp normalize_raw_token(_token), do: {:error, :missing_token}

  defp normalize_identity(identity) when is_binary(identity) do
    case String.trim(identity) do
      "" -> {:error, {:invalid_env, "FAVN_ORCHESTRATOR_API_SERVICE_TOKENS", :blank_identity}}
      trimmed -> {:ok, trimmed}
    end
  end

  defp normalize_identity(_identity),
    do: {:error, {:invalid_env, "FAVN_ORCHESTRATOR_API_SERVICE_TOKENS", :blank_identity}}

  defp validate_secret(token) when is_binary(token) do
    trimmed = String.trim(token)

    cond do
      byte_size(trimmed) < @min_token_bytes ->
        {:error, {:invalid_secret_env, "FAVN_ORCHESTRATOR_API_SERVICE_TOKENS", :too_short}}

      weak_token?(trimmed) ->
        {:error, {:invalid_secret_env, "FAVN_ORCHESTRATOR_API_SERVICE_TOKENS", :weak}}

      true ->
        :ok
    end
  end

  defp weak_token?(token) do
    downcased = String.downcase(token)

    MapSet.member?(@weak_tokens, downcased) or
      token |> String.graphemes() |> Enum.uniq() |> length() == 1
  end

  defp reject_duplicate_identities(configs) do
    identities = Enum.map(configs, & &1.service_identity)

    if length(identities) == length(Enum.uniq(identities)) do
      :ok
    else
      {:error, {:invalid_env, "FAVN_ORCHESTRATOR_API_SERVICE_TOKENS", :duplicate_identity}}
    end
  end

  defp value(config, key) do
    Map.get(config, key, Map.get(config, Atom.to_string(key)))
  end
end
