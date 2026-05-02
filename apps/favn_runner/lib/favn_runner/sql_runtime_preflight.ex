defmodule FavnRunner.SQLRuntimePreflight do
  @moduledoc false

  alias Favn.Connection.Error, as: ConnectionError
  alias Favn.Connection.Loader
  alias Favn.Connection.Registry, as: ConnectionRegistry
  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Version
  alias Favn.RelationRef

  @runner_registry FavnRunner.ConnectionRegistry

  @spec run(RunnerWork.t(), Version.t()) :: :ok | {:error, map()}
  def run(%RunnerWork{} = work, %Version{} = version) do
    requirements =
      version
      |> planned_assets(work)
      |> sql_connection_requirements()

    preflight_connections(requirements)
  end

  defp planned_assets(%Version{manifest: %{assets: assets}}, %RunnerWork{} = work)
       when is_list(assets) do
    by_ref = Map.new(assets, &{&1.ref, &1})

    work
    |> planned_refs()
    |> Enum.flat_map(fn ref ->
      case Map.fetch(by_ref, ref) do
        {:ok, %Asset{} = asset} -> [asset]
        :error -> []
      end
    end)
  end

  defp planned_assets(_version, _work), do: []

  defp planned_refs(%RunnerWork{planned_asset_refs: refs}) when is_list(refs) and refs != [],
    do: normalize_refs(refs)

  defp planned_refs(%RunnerWork{} = work), do: planned_refs_without_plan(work)

  defp planned_refs_without_plan(%RunnerWork{asset_refs: refs})
       when is_list(refs) and refs != [] do
    normalize_refs(refs)
  end

  defp planned_refs_without_plan(%RunnerWork{asset_ref: ref}) when is_tuple(ref), do: [ref]
  defp planned_refs_without_plan(_work), do: []

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
end
