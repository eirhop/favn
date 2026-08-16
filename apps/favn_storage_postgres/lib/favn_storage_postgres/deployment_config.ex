defmodule FavnStoragePostgres.DeploymentConfig do
  @moduledoc false

  @max_bytes 262_144
  @max_resources 1_000
  @top_level_keys ~w(schema_version secret_store_url resources execution_pool_policy workspace_environment connection_circuit_policy)
  @resource_keys ~w(type endpoint secret_ref catalog database container account region schema read_only ssl_mode)
  @string_resource_keys ~w(type secret_ref catalog database container account region schema)
  @safe_ssl_modes ~w(verify-full require)
  @resource_name ~r/\A[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\z/
  @safe_string ~r/\A[^\x00-\x1F\x7F]{1,1024}\z/u
  @policy_keys ~w(schema_version manifest_fingerprint effective_fingerprint operator_overrides orphaned_overrides effective sources)
  @fingerprint ~r/\A[0-9a-f]{64}\z/

  @spec validate(map()) :: {:ok, map()} | {:error, term()}
  def validate(configuration) when is_map(configuration) do
    with :ok <- validate_keys(configuration, @top_level_keys, []),
         :ok <- validate_schema_version(configuration),
         :ok <- validate_optional_url(configuration, "secret_store_url", []),
         :ok <- validate_resources(value(configuration, "resources", %{})),
         :ok <- validate_execution_pool_policy(value(configuration, "execution_pool_policy", nil)),
         :ok <- validate_workspace_environment(configuration),
         :ok <- validate_connection_circuit_policy(configuration),
         {:ok, encoded} <- Jason.encode(configuration),
         :ok <- validate_size(encoded) do
      {:ok, configuration}
    end
  rescue
    error -> {:error, {:invalid_deployment_configuration, error}}
  end

  def validate(_configuration), do: {:error, :invalid_deployment_configuration}

  defp validate_schema_version(configuration) do
    case value(configuration, "schema_version", 1) do
      version when version in [1, 2, 3] -> :ok
      _version -> {:error, {:unsupported_deployment_configuration_version, 3}}
    end
  end

  defp validate_resources(resources)
       when is_map(resources) and map_size(resources) <= @max_resources do
    Enum.reduce_while(resources, :ok, fn {name, descriptor}, :ok ->
      normalized_name = to_string(name)

      with true <- Regex.match?(@resource_name, normalized_name),
           true <- is_map(descriptor),
           :ok <- validate_resource_descriptor(descriptor, normalized_name) do
        {:cont, :ok}
      else
        false -> {:halt, {:error, {:invalid_resource_binding, normalized_name}}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp validate_resources(_resources), do: {:error, :invalid_resource_bindings}

  defp validate_execution_pool_policy(nil), do: :ok

  defp validate_execution_pool_policy(policy) when is_map(policy) do
    with :ok <- validate_keys(policy, @policy_keys, ["execution_pool_policy"]),
         1 <- value(policy, "schema_version", nil),
         :ok <- validate_fingerprint(value(policy, "manifest_fingerprint", nil)),
         :ok <- validate_fingerprint(value(policy, "effective_fingerprint", nil)),
         {:ok, overrides} <-
           Favn.ExecutionPool.PolicySet.new(value(policy, "operator_overrides", %{})),
         {:ok, orphaned} <-
           Favn.ExecutionPool.PolicySet.new(value(policy, "orphaned_overrides", %{})),
         {:ok, effective} <- Favn.ExecutionPool.PolicySet.new(value(policy, "effective", %{})),
         :ok <- validate_effective_fingerprint(policy, effective),
         :ok <- validate_policy_sets(overrides, orphaned, effective),
         :ok <- validate_sources(value(policy, "sources", %{}), effective, overrides) do
      :ok
    else
      version when is_integer(version) ->
        {:error, {:unsupported_execution_pool_policy_version, 1}}

      {:error, _reason} = error ->
        error

      _invalid ->
        {:error, :invalid_execution_pool_policy}
    end
  end

  defp validate_execution_pool_policy(_policy), do: {:error, :invalid_execution_pool_policy}

  defp validate_workspace_environment(configuration) do
    case value(configuration, "schema_version", 1) do
      1 ->
        missing = make_ref()

        case value(configuration, "workspace_environment", missing) do
          ^missing -> :ok
          _value -> {:error, {:workspace_environment_requires_schema_version, 2}}
        end

      version when version in [2, 3] ->
        case FavnOrchestrator.WorkspaceConfiguration.from_configuration(configuration) do
          {:ok, _environment} -> :ok
          {:error, reason} -> {:error, reason}
        end
    end
  end

  defp validate_connection_circuit_policy(configuration) do
    case value(configuration, "schema_version", 1) do
      version when version in [1, 2] ->
        missing = make_ref()

        case value(configuration, "connection_circuit_policy", missing) do
          ^missing -> :ok
          _value -> {:error, {:connection_circuit_policy_requires_schema_version, 3}}
        end

      3 ->
        case FavnOrchestrator.ConnectionCircuitPolicy.effective(configuration) do
          {:ok, _policies} -> :ok
          {:error, reason} -> {:error, reason}
        end
    end
  end

  defp validate_fingerprint(value) when is_binary(value) do
    if Regex.match?(@fingerprint, value), do: :ok, else: {:error, :invalid_policy_fingerprint}
  end

  defp validate_fingerprint(_value), do: {:error, :invalid_policy_fingerprint}

  defp validate_effective_fingerprint(policy, effective) do
    if value(policy, "effective_fingerprint", nil) ==
         Favn.ExecutionPool.PolicySet.fingerprint(effective),
       do: :ok,
       else: {:error, :execution_pool_policy_fingerprint_mismatch}
  end

  defp validate_policy_sets(overrides, orphaned, effective) do
    active_overrides_valid? =
      Enum.all?(overrides, fn {name, policy} -> Map.get(effective, name) == policy end)

    orphaned_disjoint? =
      orphaned
      |> Map.keys()
      |> Enum.all?(&(not Map.has_key?(effective, &1)))

    if active_overrides_valid? and orphaned_disjoint?,
      do: :ok,
      else: {:error, :inconsistent_execution_pool_policy}
  end

  defp validate_sources(sources, effective, overrides) when is_map(sources) do
    with {:ok, normalized} <- normalize_named_map(sources) do
      valid? =
        Enum.sort(Map.keys(normalized)) == Enum.sort(Map.keys(effective)) and
          Enum.all?(normalized, fn {name, source} ->
            expected =
              if Map.has_key?(overrides, name), do: "operator_override", else: "manifest"

            source == expected
          end)

      if valid?, do: :ok, else: {:error, :invalid_execution_pool_policy_sources}
    end
  end

  defp validate_sources(_sources, _effective, _overrides),
    do: {:error, :invalid_execution_pool_policy_sources}

  defp validate_resource_descriptor(descriptor, name) do
    with :ok <- validate_keys(descriptor, @resource_keys, ["resources", name]),
         :ok <- validate_optional_url(descriptor, "endpoint", ["resources", name]),
         :ok <- validate_resource_strings(descriptor, name),
         :ok <- validate_optional_boolean(descriptor, "read_only", name),
         :ok <- validate_optional_ssl_mode(descriptor, name) do
      :ok
    end
  end

  defp validate_resource_strings(descriptor, name) do
    Enum.reduce_while(@string_resource_keys, :ok, fn key, :ok ->
      case value(descriptor, key, nil) do
        nil ->
          {:cont, :ok}

        string when is_binary(string) ->
          if Regex.match?(@safe_string, string),
            do: {:cont, :ok},
            else: {:halt, {:error, {:invalid_resource_value, name, key}}}

        _value ->
          {:halt, {:error, {:invalid_resource_value, name, key}}}
      end
    end)
  end

  defp validate_optional_boolean(descriptor, key, name) do
    case value(descriptor, key, nil) do
      nil -> :ok
      value when is_boolean(value) -> :ok
      _value -> {:error, {:invalid_resource_value, name, key}}
    end
  end

  defp validate_optional_ssl_mode(descriptor, name) do
    case value(descriptor, "ssl_mode", nil) do
      nil -> :ok
      mode when mode in @safe_ssl_modes -> :ok
      _mode -> {:error, {:invalid_resource_value, name, "ssl_mode"}}
    end
  end

  defp validate_optional_url(map, key, path) do
    case value(map, key, nil) do
      nil -> :ok
      url when is_binary(url) -> validate_https_url(url, path ++ [key])
      _value -> {:error, {:invalid_endpoint, path ++ [key]}}
    end
  end

  defp validate_https_url(url, path) do
    case URI.parse(url) do
      %URI{
        scheme: "https",
        host: host,
        userinfo: nil,
        query: nil,
        fragment: nil
      }
      when is_binary(host) and host != "" ->
        :ok

      _uri ->
        {:error, {:invalid_endpoint, path}}
    end
  end

  defp validate_keys(map, allowed, path) do
    with {:ok, normalized} <- normalize_names(Map.keys(map)) do
      invalid = Enum.reject(normalized, &(&1 in allowed))

      if invalid == [], do: :ok, else: {:error, {:unknown_configuration_keys, path, invalid}}
    end
  end

  defp normalize_named_map(map) do
    Enum.reduce_while(map, {:ok, %{}}, fn
      {name, item}, {:ok, acc} when is_atom(name) or is_binary(name) ->
        normalized = to_string(name)

        if Map.has_key?(acc, normalized),
          do: {:halt, {:error, :duplicate_configuration_keys}},
          else: {:cont, {:ok, Map.put(acc, normalized, item)}}

      _entry, _acc ->
        {:halt, {:error, :invalid_configuration_key}}
    end)
  end

  defp normalize_names(names) do
    names
    |> Enum.reduce_while({:ok, MapSet.new()}, fn
      name, {:ok, acc} when is_atom(name) or is_binary(name) ->
        normalized = to_string(name)

        if MapSet.member?(acc, normalized),
          do: {:halt, {:error, :duplicate_configuration_keys}},
          else: {:cont, {:ok, MapSet.put(acc, normalized)}}

      _name, _acc ->
        {:halt, {:error, :invalid_configuration_key}}
    end)
    |> case do
      {:ok, normalized} -> {:ok, normalized |> MapSet.to_list() |> Enum.sort()}
      {:error, _reason} = error -> error
    end
  end

  defp value(map, key, default) do
    case Map.fetch(map, key) do
      {:ok, value} ->
        value

      :error ->
        Enum.reduce_while(map, default, fn
          {candidate, value}, _acc when is_atom(candidate) ->
            if Atom.to_string(candidate) == key,
              do: {:halt, value},
              else: {:cont, default}

          _entry, _acc ->
            {:cont, default}
        end)
    end
  end

  defp validate_size(encoded) when byte_size(encoded) <= @max_bytes, do: :ok
  defp validate_size(_encoded), do: {:error, :deployment_configuration_too_large}
end
