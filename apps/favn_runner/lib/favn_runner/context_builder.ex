defmodule FavnRunner.ContextBuilder do
  @moduledoc """
  Builds `%Favn.Run.Context{}` values for manifest-backed runner execution.
  """

  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest.Asset
  alias Favn.Run.Context
  alias Favn.RuntimeConfig.Resolver, as: RuntimeConfigResolver

  @rest_keys [:path, :data_path, :params, :primary_key, :paginator, :incremental, :method, :extra]
  @rest_methods [:get, :post, :put, :patch, :delete]
  @paginator_kinds [:cursor, :offset, :page]
  @incremental_kinds [:cursor]
  @extra_refresh_types [:full_refresh, :incremental]

  @spec build(RunnerWork.t(), Asset.t(), String.t()) :: {:ok, Context.t()} | {:error, term()}
  def build(%RunnerWork{} = work, %Asset{} = asset, execution_id) when is_binary(execution_id) do
    run_id = work.run_id || execution_id
    stage = normalized_stage(work.metadata)
    attempt = normalized_attempt(work.metadata)
    max_attempts = normalized_max_attempts(work.metadata)

    with {:ok, runtime_config} <- RuntimeConfigResolver.resolve_asset(asset.runtime_config || %{}) do
      asset_config = ergonomic_asset_config(asset)

      {:ok,
       %Context{
         run_id: run_id,
         target_refs: [asset.ref],
         current_ref: asset.ref,
         asset: %{ref: asset.ref, relation: asset.relation, config: asset_config},
         config: runtime_config,
         params: normalized_map(work.params),
         window: Map.get(work.trigger, :window),
         pipeline: Map.get(work.trigger, :pipeline),
         run_started_at: DateTime.utc_now(),
         stage: stage,
         attempt: attempt,
         max_attempts: max_attempts
       }}
    end
  end

  defp normalized_map(map) when is_map(map), do: map
  defp normalized_map(_other), do: %{}

  defp ergonomic_asset_config(%Asset{config: config}) do
    config
    |> normalized_map()
    |> rehydrate_multi_asset_config()
  end

  defp rehydrate_multi_asset_config(config) when is_map(config) do
    config
    |> rehydrate_known_key(:rest)
    |> rehydrate_rest_config()
  end

  defp rehydrate_rest_config(%{rest: rest} = config) when is_map(rest),
    do: %{config | rest: rehydrate_rest_fields(rest)}

  defp rehydrate_rest_config(config), do: config

  defp rehydrate_rest_fields(rest) do
    rest
    |> rehydrate_known_keys(@rest_keys)
    |> rehydrate_enum(:method, @rest_methods)
    |> rehydrate_nested_known_keys(:paginator, [:kind])
    |> rehydrate_nested_enum([:paginator, :kind], @paginator_kinds)
    |> rehydrate_nested_known_keys(:incremental, [:kind])
    |> rehydrate_nested_enum([:incremental, :kind], @incremental_kinds)
    |> rehydrate_nested_known_keys(:extra, [:refresh_type])
    |> rehydrate_nested_enum([:extra, :refresh_type], @extra_refresh_types)
  end

  defp rehydrate_known_keys(map, keys) when is_map(map) and is_list(keys) do
    Enum.reduce(keys, map, &rehydrate_known_key(&2, &1))
  end

  defp rehydrate_known_key(map, atom_key) when is_map(map) do
    string_key = Atom.to_string(atom_key)

    cond do
      Map.has_key?(map, atom_key) ->
        map

      Map.has_key?(map, string_key) ->
        map |> Map.put(atom_key, Map.get(map, string_key)) |> Map.delete(string_key)

      true ->
        map
    end
  end

  defp rehydrate_nested_known_keys(map, key, nested_keys) when is_map(map) do
    case Map.fetch(map, key) do
      {:ok, value} when is_map(value) ->
        Map.put(map, key, rehydrate_known_keys(value, nested_keys))

      _other ->
        map
    end
  end

  defp rehydrate_enum(map, key, allowed) when is_map(map) and is_atom(key) do
    case Map.fetch(map, key) do
      {:ok, value} -> Map.put(map, key, decode_known_enum(value, allowed))
      :error -> map
    end
  end

  defp rehydrate_nested_enum(map, [root, key], allowed) when is_map(map) do
    case Map.fetch(map, root) do
      {:ok, nested} when is_map(nested) ->
        Map.put(map, root, rehydrate_enum(nested, key, allowed))

      _other ->
        map
    end
  end

  defp decode_known_enum(value, _allowed) when is_atom(value), do: value

  defp decode_known_enum(value, allowed) when is_binary(value) do
    Enum.find(allowed, value, &(Atom.to_string(&1) == value))
  end

  defp decode_known_enum(value, _allowed), do: value

  defp normalized_stage(metadata) when is_map(metadata) do
    case Map.get(metadata, :stage, 0) do
      stage when is_integer(stage) and stage >= 0 -> stage
      _other -> 0
    end
  end

  defp normalized_stage(_metadata), do: 0

  defp normalized_attempt(metadata) when is_map(metadata) do
    case Map.get(metadata, :attempt, 1) do
      attempt when is_integer(attempt) and attempt > 0 -> attempt
      _other -> 1
    end
  end

  defp normalized_attempt(_metadata), do: 1

  defp normalized_max_attempts(metadata) when is_map(metadata) do
    case Map.get(metadata, :max_attempts, 1) do
      max_attempts when is_integer(max_attempts) and max_attempts > 0 -> max_attempts
      _other -> 1
    end
  end

  defp normalized_max_attempts(_metadata), do: 1
end
