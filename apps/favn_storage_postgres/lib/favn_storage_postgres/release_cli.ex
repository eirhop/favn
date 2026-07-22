defmodule FavnStoragePostgres.ReleaseCLI do
  @moduledoc """
  Bounded command dispatcher used by one-off control-plane release containers.

  Operation names are fixed by the release overlay. Values needed by an
  operation are read from environment variables, so database credentials and
  other secrets never appear in process arguments.
  """

  alias FavnStoragePostgres.Release

  @operations [
    :migrate,
    :verify_schema,
    :verify_restore,
    :grant_runtime,
    :provision_workspace,
    :runtime_input_key_inventory,
    :compact_runtime_input_keys,
    :preflight_upgrade
  ]
  @max_key_versions 100

  @type operation ::
          :migrate
          | :verify_schema
          | :verify_restore
          | :grant_runtime
          | :provision_workspace
          | :runtime_input_key_inventory
          | :compact_runtime_input_keys
          | :preflight_upgrade

  @doc "Runs one fixed release operation and raises only its stable error code on failure."
  @spec run!(operation()) :: :ok
  def run!(operation) when operation in @operations do
    run!(operation, System.get_env(), Release)
  end

  @doc false
  @spec run!(operation(), map(), module()) :: :ok
  def run!(operation, env, release) when operation in @operations and is_map(env) do
    result = dispatch(operation, env, release)

    case result do
      {:ok, %{operation: ^operation, status: :ok} = details} ->
        IO.puts("favn.release operation=#{operation} status=ok")
        IO.puts("result: " <> inspect(details, pretty: false, limit: 100))
        :ok

      {:error, %{operation: ^operation, status: :error, code: code}} when is_atom(code) ->
        raise "release operation #{operation} failed: #{code}"

      _invalid ->
        raise "release operation #{operation} failed: invalid_result"
    end
  end

  defp dispatch(:migrate, _env, release), do: release.migrate()
  defp dispatch(:verify_schema, _env, release), do: release.verify_schema()
  defp dispatch(:verify_restore, _env, release), do: release.verify_restore()
  defp dispatch(:grant_runtime, _env, release), do: release.grant_runtime()

  defp dispatch(:provision_workspace, env, release) do
    with {:ok, workspace} <- workspace(env) do
      release.provision_workspace(workspace)
    end
  end

  defp dispatch(:runtime_input_key_inventory, _env, release),
    do: release.runtime_input_key_inventory()

  defp dispatch(:compact_runtime_input_keys, env, release) do
    with {:ok, versions} <- key_versions(env) do
      release.compact_runtime_input_keys(versions)
    end
  end

  defp dispatch(:preflight_upgrade, _env, release), do: release.preflight_upgrade()

  defp workspace(env) do
    with {:ok, workspace_id} <- required(env, "FAVN_WORKSPACE_ID"),
         {:ok, slug} <- optional(env, "FAVN_WORKSPACE_SLUG", workspace_id),
         {:ok, display_name} <- optional(env, "FAVN_WORKSPACE_NAME", slug) do
      {:ok, %{workspace_id: workspace_id, slug: slug, display_name: display_name}}
    else
      {:error, code} -> operation_error(:provision_workspace, code)
    end
  end

  defp key_versions(env) do
    with {:ok, encoded} <- required(env, "FAVN_RUNTIME_INPUT_KEY_VERSIONS"),
         versions when versions != [] <- String.split(encoded, ",", trim: true),
         true <- length(versions) <= @max_key_versions,
         {:ok, versions} <- parse_versions(versions) do
      {:ok, Enum.uniq(versions)}
    else
      _invalid -> operation_error(:compact_runtime_input_keys, :invalid_key_versions)
    end
  end

  defp parse_versions(values) do
    values
    |> Enum.reduce_while({:ok, []}, fn value, {:ok, acc} ->
      case Integer.parse(value) do
        {version, ""} when version > 0 -> {:cont, {:ok, [version | acc]}}
        _invalid -> {:halt, :error}
      end
    end)
    |> case do
      {:ok, versions} -> {:ok, Enum.reverse(versions)}
      :error -> :error
    end
  end

  defp required(env, name) do
    case Map.get(env, name) do
      value when is_binary(value) and value != "" and byte_size(value) <= 255 -> {:ok, value}
      _invalid -> {:error, :missing_or_invalid_environment}
    end
  end

  defp optional(env, name, default) do
    case Map.get(env, name, default) do
      value when is_binary(value) and value != "" and byte_size(value) <= 255 -> {:ok, value}
      _invalid -> {:error, :missing_or_invalid_environment}
    end
  end

  defp operation_error(operation, code),
    do: {:error, %{operation: operation, status: :error, code: code}}
end
