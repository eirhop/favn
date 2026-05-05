defmodule FavnRunner.ContextBuilder do
  @moduledoc """
  Builds `%Favn.Run.Context{}` values for manifest-backed runner execution.
  """

  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest.Asset
  alias Favn.Run.Context
  alias Favn.RuntimeConfig.Resolver, as: RuntimeConfigResolver

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

  defp ergonomic_asset_config(%Asset{module: module, config: config}) do
    _ = if is_atom(module), do: Code.ensure_loaded(module), else: :ok

    config
    |> normalized_map()
    |> atomize_multi_asset_config()
  end

  defp atomize_multi_asset_config(config) when is_map(config) do
    config
    |> atomize_known_key(:rest)
    |> atomize_rest_config()
  end

  defp atomize_rest_config(%{rest: rest} = config) when is_map(rest),
    do: %{config | rest: atomize_rest_fields(rest)}

  defp atomize_rest_config(config), do: config

  defp atomize_rest_fields(rest) do
    rest
    |> atomize_known_key(:path)
    |> atomize_known_key(:data_path)
    |> atomize_known_key(:params)
    |> atomize_known_key(:primary_key)
    |> atomize_known_key(:paginator)
    |> atomize_known_key(:incremental)
    |> atomize_known_key(:method)
    |> atomize_known_key(:extra)
    |> atomize_nested_existing_keys(:params)
    |> atomize_nested_existing_keys(:paginator)
    |> atomize_nested_existing_keys(:incremental)
    |> atomize_nested_existing_terms(:extra)
    |> atomize_existing_value(:method)
    |> atomize_existing_value([:paginator, :kind])
    |> atomize_existing_value([:incremental, :kind])
  end

  defp atomize_known_key(map, atom_key) when is_map(map) do
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

  defp atomize_nested_existing_keys(map, key) when is_map(map) do
    case Map.fetch(map, key) do
      {:ok, value} when is_map(value) -> Map.put(map, key, atomize_existing_keys(value))
      _other -> map
    end
  end

  defp atomize_nested_existing_terms(map, key) when is_map(map) do
    case Map.fetch(map, key) do
      {:ok, value} when is_map(value) -> Map.put(map, key, atomize_existing_terms(value))
      _other -> map
    end
  end

  defp atomize_existing_keys(value) when is_map(value) do
    Map.new(value, fn {key, child} -> {existing_atom_key(key), atomize_existing_keys(child)} end)
  end

  defp atomize_existing_keys(value) when is_list(value),
    do: Enum.map(value, &atomize_existing_keys/1)

  defp atomize_existing_keys(value), do: value

  defp atomize_existing_terms(value) when is_map(value) do
    Map.new(value, fn {key, child} -> {existing_atom_key(key), atomize_existing_terms(child)} end)
  end

  defp atomize_existing_terms(value) when is_list(value),
    do: Enum.map(value, &atomize_existing_terms/1)

  defp atomize_existing_terms(value) when is_binary(value), do: existing_atom_value(value)
  defp atomize_existing_terms(value), do: value

  defp atomize_existing_value(map, key) when is_map(map) and is_atom(key) do
    case Map.fetch(map, key) do
      {:ok, value} -> Map.put(map, key, existing_atom_value(value))
      :error -> map
    end
  end

  defp atomize_existing_value(map, [root, key]) when is_map(map) do
    case Map.fetch(map, root) do
      {:ok, nested} when is_map(nested) ->
        Map.put(map, root, atomize_existing_value(nested, key))

      _other ->
        map
    end
  end

  defp existing_atom_key(key) when is_atom(key), do: key

  defp existing_atom_key(key) when is_binary(key) do
    String.to_existing_atom(key)
  rescue
    ArgumentError -> key
  end

  defp existing_atom_key(key), do: key

  defp existing_atom_value(value) when is_binary(value) do
    String.to_existing_atom(value)
  rescue
    ArgumentError -> value
  end

  defp existing_atom_value(value), do: value

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
