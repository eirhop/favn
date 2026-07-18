defmodule FavnStoragePostgres.DeploymentConfig do
  @moduledoc false

  @max_bytes 262_144
  @max_resources 1_000
  @top_level_keys ~w(schema_version secret_store_url resources)
  @resource_keys ~w(type endpoint secret_ref catalog database container account region schema read_only ssl_mode)
  @string_resource_keys ~w(type secret_ref catalog database container account region schema)
  @safe_ssl_modes ~w(verify-full require)
  @resource_name ~r/\A[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\z/
  @safe_string ~r/\A[^\x00-\x1F\x7F]{1,1024}\z/u

  @spec validate(map()) :: {:ok, map()} | {:error, term()}
  def validate(configuration) when is_map(configuration) do
    with :ok <- validate_keys(configuration, @top_level_keys, []),
         :ok <- validate_schema_version(configuration),
         :ok <- validate_optional_url(configuration, "secret_store_url", []),
         :ok <- validate_resources(value(configuration, "resources", %{})),
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
      1 -> :ok
      _version -> {:error, {:unsupported_deployment_configuration_version, 1}}
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
    invalid =
      map
      |> Map.keys()
      |> Enum.map(&to_string/1)
      |> Enum.reject(&(&1 in allowed))

    if invalid == [], do: :ok, else: {:error, {:unknown_configuration_keys, path, invalid}}
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
