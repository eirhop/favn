defmodule FavnOrchestrator.API.Filters do
  @moduledoc """
  Validates private API query filters and converts allowlisted manifest names.

  External module and asset names are matched against stored manifests before
  existing atoms are returned. Composite asset-window filters load that
  allowlist once per request rather than scanning all manifests for each field.
  """

  alias FavnOrchestrator

  @status_filters %{
    "pending" => :pending,
    "running" => :running,
    "ok" => :ok,
    "partial" => :partial,
    "error" => :error,
    "cancelled" => :cancelled,
    "timed_out" => :timed_out
  }

  @default_limit 100
  @max_limit 500
  @max_sample_limit 20

  @doc "Builds filters for the run read model."
  @spec runs(map()) :: {:ok, keyword()} | {:error, :invalid_filter}
  def runs(params) when is_map(params) do
    with {:ok, limit} <- integer(Map.get(params, "limit", @default_limit), 1, @max_limit),
         {:ok, opts} <- put_status([limit: limit], Map.get(params, "status")) do
      {:ok, opts}
    else
      {:error, :invalid_pagination} -> {:error, :invalid_filter}
      {:error, _reason} = error -> error
    end
  end

  @doc "Builds filters for one backfill's window page."
  @spec backfill_windows(map(), String.t()) :: {:ok, keyword()} | {:error, term()}
  def backfill_windows(params, backfill_run_id)
      when is_map(params) and is_binary(backfill_run_id) do
    with {:ok, filters} <- page(params),
         {:ok, filters} <- put_pipeline(filters, Map.get(params, "pipeline_module")),
         {:ok, filters} <- put_status(filters, Map.get(params, "status")) do
      {:ok,
       filters
       |> Keyword.put(:backfill_run_id, backfill_run_id)
       |> put_string(:window_key, Map.get(params, "window_key"))}
    end
  end

  @doc "Builds filters for coverage-baseline pages."
  @spec coverage_baselines(map()) :: {:ok, keyword()} | {:error, term()}
  def coverage_baselines(params) when is_map(params) do
    with {:ok, filters} <- page(params),
         {:ok, filters} <- put_pipeline(filters, Map.get(params, "pipeline_module")),
         {:ok, filters} <- put_status(filters, Map.get(params, "status")) do
      {:ok,
       filters
       |> put_string(:source_key, Map.get(params, "source_key"))
       |> put_string(:segment_key_hash, Map.get(params, "segment_key_hash"))}
    end
  end

  @doc "Builds filters for asset-window state pages."
  @spec asset_window_states(map()) :: {:ok, keyword()} | {:error, term()}
  def asset_window_states(params) when is_map(params) do
    with {:ok, opts} <- page(params),
         {:ok, manifest_values} <- manifest_values_if_needed(params),
         {:ok, opts} <- put_asset_ref(opts, params, manifest_values.asset_refs),
         {:ok, opts} <-
           put_pipeline(opts, Map.get(params, "pipeline_module"), manifest_values.pipelines),
         {:ok, opts} <- put_status(opts, Map.get(params, "status")) do
      {:ok, put_string(opts, :window_key, Map.get(params, "window_key"))}
    end
  end

  @doc "Builds and validates the projection-repair command scope."
  @spec backfill_repair(map()) :: {:ok, keyword()} | {:error, term()}
  def backfill_repair(params) when is_map(params) do
    opts =
      []
      |> Keyword.put(:apply, Map.get(params, "apply") == true)
      |> put_string(:backfill_run_id, Map.get(params, "backfill_run_id"))

    with {:ok, opts} <- put_pipeline(opts, Map.get(params, "pipeline_module")) do
      if Keyword.has_key?(opts, :backfill_run_id) and Keyword.has_key?(opts, :pipeline_module),
        do: {:error, :invalid_repair_scope},
        else: {:ok, opts}
    end
  end

  @doc "Returns a stable audit resource id for repair options."
  @spec repair_resource_id(keyword()) :: String.t()
  def repair_resource_id(opts) when is_list(opts) do
    cond do
      id = Keyword.get(opts, :backfill_run_id) -> id
      module = Keyword.get(opts, :pipeline_module) -> Atom.to_string(module)
      true -> "all"
    end
  end

  @doc "Validates and caps relation inspection sample size."
  @spec inspection_sample_limit(map()) :: {:ok, non_neg_integer()} | {:error, atom()}
  def inspection_sample_limit(params) when is_map(params) do
    case Map.get(params, "sample_limit") || Map.get(params, "limit") || @max_sample_limit do
      value when is_integer(value) and value >= 0 -> {:ok, min(value, @max_sample_limit)}
      value when is_binary(value) -> parse_sample_limit(value)
      _invalid -> {:error, :invalid_sample_limit}
    end
  end

  @doc "Builds a bounded limit/offset page request."
  @spec page(map()) :: {:ok, keyword()} | {:error, :invalid_pagination}
  def page(params) when is_map(params) do
    with {:ok, limit} <- integer(Map.get(params, "limit", @default_limit), 1, @max_limit),
         {:ok, offset} <- integer(Map.get(params, "offset", 0), 0, nil) do
      {:ok, [limit: limit, offset: offset]}
    end
  end

  defp parse_sample_limit(value) do
    case Integer.parse(value) do
      {integer, ""} when integer >= 0 -> {:ok, min(integer, @max_sample_limit)}
      _invalid -> {:error, :invalid_sample_limit}
    end
  end

  defp integer(value, min, max) when is_integer(value), do: validate_integer(value, min, max)

  defp integer(value, min, max) when is_binary(value) do
    case Integer.parse(value) do
      {integer, ""} -> validate_integer(integer, min, max)
      _invalid -> {:error, :invalid_pagination}
    end
  end

  defp integer(_value, _min, _max), do: {:error, :invalid_pagination}

  defp validate_integer(value, min, nil) when value >= min, do: {:ok, value}
  defp validate_integer(value, min, max) when value >= min and value <= max, do: {:ok, value}
  defp validate_integer(_value, _min, _max), do: {:error, :invalid_pagination}

  defp put_status(opts, value) when value in [nil, ""], do: {:ok, opts}

  defp put_status(opts, value) when is_binary(value) do
    case Map.fetch(@status_filters, value) do
      {:ok, status} -> {:ok, Keyword.put(opts, :status, status)}
      :error -> {:error, :invalid_filter}
    end
  end

  defp put_status(_opts, _value), do: {:error, :invalid_filter}

  defp put_pipeline(opts, value) when value in [nil, ""], do: {:ok, opts}

  defp put_pipeline(opts, value) when is_binary(value) do
    with {:ok, values} <- manifest_values() do
      put_pipeline(opts, value, values.pipelines)
    end
  end

  defp put_pipeline(_opts, _value), do: {:error, :invalid_filter}

  defp put_pipeline(opts, value, _pipelines) when value in [nil, ""], do: {:ok, opts}

  defp put_pipeline(opts, value, pipelines) when is_binary(value) do
    case find_module(value, pipelines) do
      {:ok, module} -> {:ok, Keyword.put(opts, :pipeline_module, module)}
      {:error, :not_allowed} -> {:error, :invalid_filter}
    end
  end

  defp put_pipeline(_opts, _value, _pipelines), do: {:error, :invalid_filter}

  defp put_asset_ref(opts, params, asset_refs) do
    case {Map.get(params, "asset_ref_module"), Map.get(params, "asset_ref_name")} do
      {nil, nil} ->
        {:ok, opts}

      {module_name, asset_name} when is_binary(module_name) and is_binary(asset_name) ->
        case find_asset_ref(module_name, asset_name, asset_refs) do
          {:ok, {module, name}} ->
            {:ok,
             opts
             |> Keyword.put(:asset_ref_module, module)
             |> Keyword.put(:asset_ref_name, name)}

          {:error, :not_allowed} ->
            {:error, :invalid_asset_ref}
        end

      _invalid ->
        {:error, :invalid_asset_ref}
    end
  end

  defp manifest_values_if_needed(params) do
    if asset_or_pipeline_filter?(params), do: manifest_values(), else: {:ok, empty_values()}
  end

  defp asset_or_pipeline_filter?(params) do
    Enum.any?(["asset_ref_module", "asset_ref_name", "pipeline_module"], fn key ->
      Map.get(params, key) not in [nil, ""]
    end)
  end

  defp manifest_values do
    case FavnOrchestrator.list_manifests() do
      {:ok, versions} ->
        {:ok,
         %{
           pipelines:
             versions
             |> Enum.flat_map(& &1.manifest.pipelines)
             |> Enum.map(& &1.module)
             |> Enum.uniq(),
           asset_refs:
             versions |> Enum.flat_map(& &1.manifest.assets) |> Enum.map(& &1.ref) |> Enum.uniq()
         }}

      {:error, reason} ->
        {:error, {:manifest_filter_lookup_failed, reason}}
    end
  end

  defp empty_values, do: %{pipelines: [], asset_refs: []}

  defp find_module(value, modules) do
    Enum.find_value(modules, {:error, :not_allowed}, fn module ->
      if value in module_names(module), do: {:ok, module}
    end)
  end

  defp find_asset_ref(module_name, asset_name, refs) do
    Enum.find_value(refs, {:error, :not_allowed}, fn {module, name} = ref ->
      if module_name in module_names(module) and asset_name == Atom.to_string(name),
        do: {:ok, ref}
    end)
  end

  defp module_names(module) do
    module
    |> Atom.to_string()
    |> then(fn
      "Elixir." <> short_name = full_name -> [full_name, short_name]
      full_name -> [full_name]
    end)
  end

  defp put_string(opts, key, value) when is_binary(value) and value != "",
    do: Keyword.put(opts, key, value)

  defp put_string(opts, _key, _value), do: opts
end
