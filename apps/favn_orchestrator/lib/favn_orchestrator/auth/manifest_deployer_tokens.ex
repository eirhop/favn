defmodule FavnOrchestrator.Auth.ManifestDeployerTokens do
  @moduledoc false

  alias FavnOrchestrator.Auth.ServiceTokens

  @env "FAVN_ORCHESTRATOR_MANIFEST_DEPLOYER_TOKENS"
  @max_env_bytes 512 * 1_024
  @max_credentials 100
  @max_workspaces 100
  @keys MapSet.new(["service_identity", "workspace_ids", "token"])
  @versioned_identity ~r/\A[A-Za-z0-9][A-Za-z0-9_.-]*-v[1-9][0-9]*\z/

  @type token_config :: %{
          required(:service_identity) => String.t(),
          required(:workspace_ids) => MapSet.t(String.t()),
          required(:token_hash) => String.t()
        }

  @spec from_env_string(String.t() | nil) :: {:ok, [token_config()]} | {:error, term()}
  def from_env_string(nil), do: {:ok, []}
  def from_env_string(""), do: {:ok, []}

  def from_env_string(raw) when is_binary(raw) and byte_size(raw) <= @max_env_bytes do
    with {:ok, values} when is_list(values) <- Jason.decode(raw),
         true <- length(values) <= @max_credentials,
         {:ok, configs} <- parse_configs(values),
         :ok <- reject_duplicates(configs) do
      {:ok, configs}
    else
      false -> invalid(:too_many_credentials)
      {:ok, _other} -> invalid(:expected_json_array)
      {:error, {:invalid_manifest_deployer_tokens, _reason}} = error -> error
      {:error, _reason} -> invalid(:malformed_json)
    end
  end

  def from_env_string(raw) when is_binary(raw), do: invalid(:configuration_too_large)
  def from_env_string(_raw), do: invalid(:malformed_json)

  @spec configured_tokens() :: [token_config()]
  def configured_tokens do
    Application.get_env(:favn_orchestrator, :manifest_deployer_tokens, [])
  end

  @spec authenticate(String.t() | nil, String.t(), [token_config()]) ::
          {:ok, String.t()} | {:error, :service_unauthorized | :workspace_forbidden}
  def authenticate(token, workspace_id, configs)
      when is_binary(token) and token != "" and byte_size(token) <= 4_096 and
             is_binary(workspace_id) and is_list(configs) do
    provided_hash = ServiceTokens.hash_token(token)

    Enum.find_value(configs, fn config ->
      if secure_hash_match?(config.token_hash, provided_hash) do
        if MapSet.member?(config.workspace_ids, workspace_id),
          do: {:ok, config.service_identity},
          else: {:error, :workspace_forbidden}
      end
    end) || {:error, :service_unauthorized}
  end

  def authenticate(_token, _workspace_id, _configs), do: {:error, :service_unauthorized}

  defp parse_configs(values) do
    values
    |> Enum.reduce_while({:ok, []}, fn value, {:ok, acc} ->
      case parse_config(value) do
        {:ok, config} -> {:cont, {:ok, [config | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> then(fn
      {:ok, configs} -> {:ok, Enum.reverse(configs)}
      error -> error
    end)
  end

  defp parse_config(%{} = value) do
    with true <- MapSet.new(Map.keys(value)) == @keys,
         identity when is_binary(identity) <- value["service_identity"],
         true <- Regex.match?(@versioned_identity, identity) and byte_size(identity) <= 128,
         workspaces when is_list(workspaces) <- value["workspace_ids"],
         {:ok, workspace_ids} <- normalize_workspaces(workspaces),
         token when is_binary(token) <- value["token"],
         {:ok, hashed} <- ServiceTokens.from_raw_token(identity, [], token, @env) do
      {:ok,
       %{
         service_identity: identity,
         workspace_ids: workspace_ids,
         token_hash: hashed.token_hash
       }}
    else
      false -> invalid(:invalid_credential)
      nil -> invalid(:invalid_credential)
      {:error, _reason} -> invalid(:invalid_credential)
      _other -> invalid(:invalid_credential)
    end
  end

  defp parse_config(_value), do: invalid(:invalid_credential)

  defp normalize_workspaces(values) do
    if values != [] and length(values) <= @max_workspaces and
         Enum.all?(values, &(is_binary(&1) and byte_size(&1) in 1..255)) and
         length(values) == length(Enum.uniq(values)) do
      {:ok, MapSet.new(values)}
    else
      invalid(:invalid_workspace_allowlist)
    end
  end

  defp reject_duplicates(configs) do
    identities = Enum.map(configs, & &1.service_identity)
    hashes = Enum.map(configs, & &1.token_hash)

    if length(identities) == length(Enum.uniq(identities)) and
         length(hashes) == length(Enum.uniq(hashes)) do
      :ok
    else
      invalid(:duplicate_credential)
    end
  end

  defp secure_hash_match?(configured, provided) do
    byte_size(configured) == byte_size(provided) and
      Plug.Crypto.secure_compare(configured, provided)
  end

  defp invalid(reason), do: {:error, {:invalid_manifest_deployer_tokens, reason}}
end
