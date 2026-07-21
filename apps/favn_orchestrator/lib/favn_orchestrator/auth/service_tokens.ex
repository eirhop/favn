defmodule FavnOrchestrator.Auth.ServiceTokens do
  @moduledoc false

  @min_token_bytes 32
  @max_token_bytes 4_096
  @max_token_count 100
  @max_env_bytes 512 * 1_024
  @max_identity_bytes 128
  @weak_fragments ~w(replace change placeholder example secret password test token todo)
  @identity_pattern ~r/\A[A-Za-z0-9][A-Za-z0-9_.-]*\z/
  @platform_roles [:platform_reader, :platform_operator, :platform_admin]
  @config_keys [:enabled, :platform_roles, :service_identity, :token, :token_hash]

  @type token_config :: %{
          required(:service_identity) => String.t(),
          required(:token_hash) => String.t(),
          required(:enabled) => boolean(),
          required(:platform_roles) => [FavnOrchestrator.Persistence.PlatformContext.role()]
        }

  @type authenticated_principal :: %{
          required(:service_identity) => String.t(),
          required(:platform_roles) => [FavnOrchestrator.Persistence.PlatformContext.role()]
        }

  @spec min_token_bytes() :: pos_integer()
  def min_token_bytes, do: @min_token_bytes

  @spec from_env_string(String.t()) :: {:ok, [token_config()]} | {:error, term()}
  def from_env_string(raw) when is_binary(raw) do
    if byte_size(raw) <= @max_env_bytes do
      tokens = raw |> String.split(",", trim: true) |> Enum.map(&String.trim/1)

      with :ok <- validate_env_bounds(tokens),
           :ok <- ensure_present(tokens) do
        parse_env_tokens(tokens)
      end
    else
      too_many_tokens()
    end
  end

  @spec runtime_config() :: {:ok, [token_config()]} | {:error, term()}
  def runtime_config do
    configured_tokens = Application.get_env(:favn_orchestrator, :api_service_tokens, [])

    if configured_tokens == [] do
      :favn_orchestrator
      |> Application.get_env(:api_service_tokens_env, "")
      |> from_env_string()
    else
      normalize_config_list(configured_tokens)
    end
  end

  @spec configured_tokens() :: [token_config()]
  def configured_tokens do
    case runtime_config() do
      {:ok, tokens} -> tokens
      {:error, _reason} -> []
    end
  end

  @spec authenticate(String.t() | nil, [term()]) ::
          {:ok, authenticated_principal()} | {:error, :service_unauthorized}
  def authenticate(provided, configured_tokens)
      when is_binary(provided) and provided != "" and byte_size(provided) <= @max_token_bytes do
    provided_hash = hash_token(provided)

    configured_tokens
    |> normalize_config_list()
    |> case do
      {:ok, tokens} -> tokens
      {:error, _reason} -> []
    end
    |> Enum.find_value(fn
      %{enabled: true, token_hash: token_hash} = config ->
        if byte_size(token_hash) == byte_size(provided_hash) and
             Plug.Crypto.secure_compare(token_hash, provided_hash) do
          {:ok, Map.take(config, [:service_identity, :platform_roles])}
        else
          false
        end

      _token ->
        false
    end)
    |> case do
      {:ok, principal} -> {:ok, principal}
      _other -> {:error, :service_unauthorized}
    end
  end

  def authenticate(_provided, _configured_tokens), do: {:error, :service_unauthorized}

  @spec validate_runtime_config() :: :ok | {:error, term()}
  def validate_runtime_config do
    case runtime_config() do
      {:ok, tokens} -> validate_config(tokens)
      {:error, reason} -> {:error, reason}
    end
  end

  @spec validate_config([term()]) :: :ok | {:error, term()}
  def validate_config(configured_tokens) when is_list(configured_tokens) do
    with {:ok, tokens} <- normalize_config_list(configured_tokens) do
      active_tokens = Enum.filter(tokens, & &1.enabled)

      with :ok <- ensure_active_tokens(active_tokens) do
        with :ok <- reject_duplicate_identities(active_tokens) do
          reject_duplicate_token_hashes(active_tokens)
        end
      end
    end
  end

  def validate_config(_configured_tokens),
    do: {:error, {:invalid_api_config, :missing_service_tokens}}

  @spec configured_count([term()]) :: non_neg_integer()
  def configured_count(configured_tokens) when is_list(configured_tokens) do
    configured_tokens
    |> normalize_config_list()
    |> case do
      {:ok, tokens} -> Enum.count(tokens, & &1.enabled)
      {:error, _reason} -> 0
    end
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

  defp validate_env_bounds(tokens) do
    if length(tokens) <= @max_token_count do
      :ok
    else
      too_many_tokens()
    end
  end

  defp too_many_tokens,
    do: {:error, {:invalid_env, "FAVN_ORCHESTRATOR_API_SERVICE_TOKENS", :too_many_tokens}}

  defp parse_env_tokens(tokens) do
    tokens
    |> Enum.reduce_while({:ok, []}, fn token, {:ok, acc} ->
      case parse_env_token(token) do
        {:ok, config} -> {:cont, {:ok, [config | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, configs} -> validate_normalized_configs(Enum.reverse(configs))
      {:error, reason} -> {:error, reason}
    end
  end

  defp parse_env_token(entry) do
    with [principal, token] <- String.split(entry, ":", parts: 2),
         {:ok, {identity, platform_roles}} <- normalize_env_principal(principal),
         :ok <- validate_secret(token) do
      {:ok,
       %{
         service_identity: identity,
         token_hash: hash_token(String.trim(token)),
         enabled: true,
         platform_roles: platform_roles
       }}
    else
      [_token_without_identity] ->
        {:error,
         {:invalid_env, "FAVN_ORCHESTRATOR_API_SERVICE_TOKENS",
          "identity[|platform_role+...]:token"}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp normalize_config_list(configured_tokens) do
    configured_tokens
    |> Enum.reduce_while({:ok, []}, fn config, {:ok, acc} ->
      case normalize_config(config) do
        {:ok, normalized} -> {:cont, {:ok, [normalized | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, configs} -> validate_normalized_configs(Enum.reverse(configs))
      {:error, reason} -> {:error, reason}
    end
  end

  defp normalize_config(config) when is_list(config) do
    if Keyword.keyword?(config) and
         length(Keyword.keys(config)) == length(Enum.uniq(Keyword.keys(config))) do
      normalize_config(Map.new(config))
    else
      {:error, :invalid_service_token_config}
    end
  end

  defp normalize_config({identity, token}) when is_binary(identity) and is_binary(token) do
    normalize_config(%{
      service_identity: identity,
      token: token,
      enabled: true,
      platform_roles: []
    })
  end

  defp normalize_config(config) when is_map(config) do
    with :ok <- validate_config_keys(config),
         :ok <- reject_raw_and_hashed_token(config),
         {:ok, identity} <- normalize_identity(value(config, :service_identity)),
         enabled? <- Map.get(config, :enabled, Map.get(config, "enabled", true)),
         true <- is_boolean(enabled?),
         {:ok, platform_roles} <- normalize_platform_roles(value(config, :platform_roles) || []),
         {:ok, token_hash} <- normalize_token_hash(config) do
      {:ok,
       %{
         service_identity: identity,
         token_hash: token_hash,
         enabled: enabled?,
         platform_roles: platform_roles
       }}
    else
      {:error, reason} -> {:error, reason}
      _other -> {:error, :invalid_service_token_config}
    end
  end

  defp normalize_config(_config), do: {:error, :invalid_service_token_config}

  defp normalize_token_hash(config) do
    case value(config, :token_hash) do
      hash when is_binary(hash) and hash != "" -> validate_token_hash(hash)
      _other -> normalize_raw_token(value(config, :token))
    end
  end

  defp normalize_raw_token(token) when is_binary(token) and token != "" do
    with :ok <- validate_secret(token) do
      {:ok, hash_token(String.trim(token))}
    end
  end

  defp normalize_raw_token(_token), do: {:error, :missing_token}

  defp validate_token_hash(hash) do
    if String.match?(hash, ~r/\A[A-Za-z0-9_-]{43}\z/) do
      {:ok, hash}
    else
      {:error, :invalid_service_token_hash}
    end
  end

  defp normalize_identity(identity) when is_binary(identity) do
    case String.trim(identity) do
      "" ->
        {:error, {:invalid_env, "FAVN_ORCHESTRATOR_API_SERVICE_TOKENS", :blank_identity}}

      trimmed ->
        if byte_size(trimmed) <= @max_identity_bytes and Regex.match?(@identity_pattern, trimmed) do
          {:ok, trimmed}
        else
          {:error, {:invalid_env, "FAVN_ORCHESTRATOR_API_SERVICE_TOKENS", :invalid_identity}}
        end
    end
  end

  defp normalize_identity(_identity),
    do: {:error, {:invalid_env, "FAVN_ORCHESTRATOR_API_SERVICE_TOKENS", :blank_identity}}

  defp normalize_env_principal(principal) do
    case String.split(principal, "|", parts: 2) do
      [identity] ->
        with {:ok, identity} <- normalize_identity(identity), do: {:ok, {identity, []}}

      [identity, roles] ->
        with {:ok, identity} <- normalize_identity(identity),
             {:ok, roles} <- roles |> String.split("+", trim: true) |> normalize_platform_roles(),
             true <- roles != [] do
          {:ok, {identity, roles}}
        else
          false -> invalid_platform_roles()
          {:error, _reason} = error -> error
        end
    end
  end

  defp normalize_platform_roles(roles) when is_list(roles) do
    normalized =
      Enum.map(roles, fn
        role when role in @platform_roles -> role
        role when is_binary(role) -> Enum.find(@platform_roles, &(Atom.to_string(&1) == role))
        _role -> nil
      end)

    if length(normalized) == length(Enum.uniq(normalized)) and
         Enum.all?(normalized, &(&1 in @platform_roles)),
       do: {:ok, normalized},
       else: invalid_platform_roles()
  end

  defp normalize_platform_roles(_roles), do: invalid_platform_roles()

  defp invalid_platform_roles,
    do: {:error, {:invalid_env, "FAVN_ORCHESTRATOR_API_SERVICE_TOKENS", :invalid_platform_roles}}

  defp validate_secret(token) when is_binary(token) do
    trimmed = String.trim(token)

    cond do
      byte_size(trimmed) > @max_token_bytes ->
        {:error, {:invalid_secret_env, "FAVN_ORCHESTRATOR_API_SERVICE_TOKENS", :too_long}}

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

    Enum.any?(@weak_fragments, &String.contains?(downcased, &1)) or
      token |> String.graphemes() |> Enum.uniq() |> length() == 1
  end

  defp ensure_active_tokens([]), do: {:error, {:invalid_api_config, :missing_service_tokens}}
  defp ensure_active_tokens(_tokens), do: :ok

  defp reject_duplicate_identities(configs) do
    configs
    |> Enum.filter(& &1.enabled)
    |> reject_duplicate_enabled_identities()
  end

  defp reject_duplicate_enabled_identities(configs) do
    identities = Enum.map(configs, & &1.service_identity)

    if length(identities) == length(Enum.uniq(identities)) do
      :ok
    else
      {:error, {:invalid_env, "FAVN_ORCHESTRATOR_API_SERVICE_TOKENS", :duplicate_identity}}
    end
  end

  defp reject_duplicate_token_hashes(configs) do
    hashes = configs |> Enum.filter(& &1.enabled) |> Enum.map(& &1.token_hash)

    if length(hashes) == length(Enum.uniq(hashes)) do
      :ok
    else
      {:error, {:invalid_env, "FAVN_ORCHESTRATOR_API_SERVICE_TOKENS", :duplicate_token}}
    end
  end

  defp validate_normalized_configs(configs) do
    with :ok <- reject_duplicate_identities(configs),
         :ok <- reject_duplicate_token_hashes(configs) do
      {:ok, configs}
    end
  end

  defp validate_config_keys(config) do
    keys = Enum.map(Map.keys(config), &normalize_config_key/1)

    if Enum.all?(keys, &(&1 in @config_keys)) and length(keys) == length(Enum.uniq(keys)) do
      :ok
    else
      {:error, :invalid_service_token_config}
    end
  end

  defp reject_raw_and_hashed_token(config) do
    if not is_nil(value(config, :token)) and not is_nil(value(config, :token_hash)) do
      {:error, :invalid_service_token_config}
    else
      :ok
    end
  end

  defp normalize_config_key(key) when key in @config_keys, do: key
  defp normalize_config_key("enabled"), do: :enabled
  defp normalize_config_key("platform_roles"), do: :platform_roles
  defp normalize_config_key("service_identity"), do: :service_identity
  defp normalize_config_key("token"), do: :token
  defp normalize_config_key("token_hash"), do: :token_hash
  defp normalize_config_key(_key), do: nil

  defp value(config, key) do
    Map.get(config, key, Map.get(config, Atom.to_string(key)))
  end
end
