defmodule FavnRunner.SQLRuntimePreflight do
  @moduledoc false

  alias Favn.Connection.Error, as: ConnectionError
  alias Favn.Connection.Loader
  alias Favn.Connection.Registry, as: ConnectionRegistry
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Version
  alias Favn.RelationRef
  alias FavnRunner.ManifestHandle
  alias FavnRunner.ManifestStore

  @runner_registry FavnRunner.ConnectionRegistry

  @spec run(Version.t() | ManifestHandle.t(), [Favn.Ref.t()], keyword()) ::
          :ok | {:error, map()}
  def run(manifest, planned_asset_refs, opts \\ [])

  def run(%Version{} = version, planned_asset_refs, _opts) when is_list(planned_asset_refs) do
    refs = normalize_refs(planned_asset_refs)
    emit(version.manifest_version_id, refs)

    with {:ok, assets} <- planned_assets(version, refs) do
      assets
      |> sql_connection_requirements()
      |> preflight_connections()
    end
  end

  def run(%ManifestHandle{} = handle, planned_asset_refs, opts)
      when is_list(planned_asset_refs) do
    refs = normalize_refs(planned_asset_refs)
    emit(handle.manifest_version_id, refs)

    with {:ok, assets} <- planned_assets(handle, refs, opts) do
      assets
      |> sql_connection_requirements()
      |> preflight_connections()
    else
      {:error, reason} -> {:error, manifest_cache_diagnostic(reason)}
    end
  end

  @doc false
  @spec run_asset(Asset.t()) :: :ok | {:error, map()}
  def run_asset(%Asset{} = asset) do
    [asset]
    |> sql_connection_requirements()
    |> preflight_connections()
  end

  defp planned_assets(%Version{manifest: %{assets: assets}}, planned_asset_refs)
       when is_list(assets) do
    by_ref = Map.new(assets, &{&1.ref, &1})

    assets =
      Enum.flat_map(planned_asset_refs, fn ref ->
        case Map.fetch(by_ref, ref) do
          {:ok, %Asset{} = asset} -> [asset]
          :error -> []
        end
      end)

    {:ok, assets}
  end

  defp planned_assets(_version, _planned_asset_refs), do: {:ok, []}

  defp planned_assets(%ManifestHandle{} = handle, planned_asset_refs, opts) do
    ManifestStore.fetch_assets(handle, planned_asset_refs, opts)
  end

  defp normalize_refs(refs) do
    refs
    |> Enum.filter(&valid_ref?/1)
    |> Enum.uniq()
    |> Enum.sort(&compare_refs/2)
  end

  defp sql_connection_requirements(assets) do
    entries =
      Enum.flat_map(assets, fn
        %Asset{type: :sql, ref: asset_ref, relation: %RelationRef{connection: connection}}
        when is_atom(connection) ->
          [{connection, asset_ref}]

        _asset ->
          []
      end)

    %{
      connections: entries |> Enum.map(&elem(&1, 0)) |> Enum.uniq() |> Enum.sort(),
      sql_asset_refs: entries |> Enum.map(&elem(&1, 1)) |> normalize_refs(),
      connection_asset_refs:
        entries
        |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))
        |> Map.new(fn {connection, refs} -> {connection, normalize_refs(refs)} end)
    }
  end

  defp preflight_connections(%{connections: []}), do: :ok

  defp preflight_connections(%{connections: connection_names} = requirements) do
    case Loader.resolve_required(connection_names) do
      {:ok, _resolved} ->
        :ok

      {:error, errors} when is_list(errors) ->
        errors
        |> ignore_registry_resolved_missing_connections()
        |> case do
          [] -> :ok
          remaining -> {:error, diagnostic(requirements, remaining)}
        end
    end
  end

  defp ignore_registry_resolved_missing_connections(errors) do
    Enum.reject(errors, fn
      %ConnectionError{type: :missing_connection, connection: connection}
      when is_atom(connection) ->
        match?(
          {:ok, _resolved},
          ConnectionRegistry.fetch(connection, registry_name: @runner_registry)
        )

      _error ->
        false
    end)
  catch
    :exit, _reason -> errors
  end

  defp diagnostic(requirements, errors) do
    %{
      type: :missing_runtime_config,
      phase: :sql_preflight,
      message: "missing required SQL runtime config",
      details: %{
        connections: requirements.connections,
        sql_asset_refs: requirements.sql_asset_refs,
        connection_asset_refs: requirements.connection_asset_refs,
        errors: Enum.map(errors, &safe_error/1)
      }
    }
  end

  defp manifest_cache_diagnostic(reason) do
    %{
      type: :manifest_cache_unavailable,
      phase: :sql_preflight,
      message: "pinned manifest index is unavailable",
      details: %{reason: reason}
    }
  end

  defp safe_error(%ConnectionError{} = error) do
    details = error.details || %{}

    %{
      type: error.type,
      connection: error.connection,
      module: error.module,
      key: Map.get(details, :key),
      provider: Map.get(details, :provider),
      env: Map.get(details, :env),
      secret?: Map.get(details, :secret?, false),
      message: safe_message(error, details)
    }
  end

  defp safe_error(_error) do
    %{type: :unknown, message: "SQL connection preflight failed"}
  end

  defp valid_ref?({module, name}) when is_atom(module) and is_atom(name), do: true
  defp valid_ref?(_ref), do: false

  defp safe_message(%ConnectionError{type: :missing_env}, %{env: env}) when is_binary(env),
    do: "missing_env #{env}"

  defp safe_message(
         %ConnectionError{type: :missing_connection, connection: connection},
         _details
       ),
       do: "connection definition not found for #{inspect(connection)}"

  defp safe_message(%ConnectionError{type: :missing_required}, %{key: key}),
    do: "missing required connection key #{inspect(key)}"

  defp safe_message(%ConnectionError{type: :invalid_type}, %{key: key}),
    do: "invalid connection config type for #{inspect(key)}"

  defp safe_message(%ConnectionError{type: type}, _details),
    do: "SQL connection preflight failed with #{type}"

  defp compare_refs({left_module, left_name}, {right_module, right_name}) do
    {Atom.to_string(left_module), Atom.to_string(left_name)} <=
      {Atom.to_string(right_module), Atom.to_string(right_name)}
  end

  defp emit(manifest_version_id, planned_asset_refs) do
    :telemetry.execute(
      [:favn, :runner, :sql_runtime_preflight],
      %{count: 1, planned_asset_count: length(planned_asset_refs)},
      %{manifest_version_id: manifest_version_id}
    )
  end
end
