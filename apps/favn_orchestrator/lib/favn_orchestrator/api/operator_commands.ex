defmodule FavnOrchestrator.API.OperatorCommands do
  @moduledoc """
  Translates private HTTP command payloads into the public operator facade.

  Target module names are resolved only against the selected manifest. This
  avoids scanning every stored manifest and prevents a module from an unrelated
  version being accepted during request validation.
  """

  alias FavnOrchestrator
  alias FavnOrchestrator.ManifestTarget
  alias Favn.Retry.Policy

  @type actor_context :: %{required(:actor) => map(), required(:session) => map()}

  @doc "Submits an asset or pipeline run described by HTTP request parameters."
  @spec submit_run(map(), actor_context()) :: {:ok, String.t()} | {:error, term()}
  def submit_run(params, actor_context) when is_map(params) and is_map(actor_context) do
    with :ok <- reject_legacy_retry_fields(params),
         {:ok, manifest_version_id} <- manifest_version(params),
         {:ok, target} <- target(params, manifest_version_id),
         {:ok, command_input} <- run_input(params, target) do
      FavnOrchestrator.submit_operator_run(
        actor_context,
        manifest_version_id,
        target,
        command_input
      )
    end
  end

  @doc "Submits a pipeline backfill described by HTTP request parameters."
  @spec submit_backfill(map()) :: {:ok, String.t()} | {:error, term()}
  def submit_backfill(params) when is_map(params) do
    with :ok <- reject_legacy_retry_fields(params),
         :ok <- reject_removed_lookback(params),
         {:ok, manifest_version_id} <- manifest_version(params),
         {:ok, %{type: "pipeline", id: target_id}} <- target(params, manifest_version_id),
         {:ok, range_request} <- range_request(params),
         {:ok, opts} <- backfill_options(params, range_request) do
      FavnOrchestrator.submit_pipeline_backfill_for_manifest(
        manifest_version_id,
        target_id,
        opts
      )
    else
      {:ok, _target} -> {:error, :invalid_target}
      {:error, _reason} = error -> error
    end
  end

  @doc "Plans a pipeline backfill without submitting it."
  @spec plan_backfill(map()) :: {:ok, map()} | {:error, term()}
  def plan_backfill(params) when is_map(params) do
    with :ok <- reject_legacy_retry_fields(params),
         :ok <- reject_removed_lookback(params),
         {:ok, manifest_version_id} <- manifest_version(params),
         {:ok, %{type: "pipeline", id: target_id}} <- target(params, manifest_version_id),
         {:ok, range_request} <- range_request(params),
         {:ok, opts} <- backfill_options(params, range_request) do
      FavnOrchestrator.plan_pipeline_backfill_for_manifest(manifest_version_id, target_id, opts)
    else
      {:ok, _target} -> {:error, :invalid_target}
      {:error, _reason} = error -> error
    end
  end

  defp run_input(params, %{type: "asset"}) do
    with {:ok, selection} <- asset_selection(params) do
      {:ok,
       []
       |> put_optional(:selection, selection)
       |> put_optional(:dependency_mode, Map.get(params, "dependencies"))
       |> put_optional(:refresh_mode, Map.get(params, "refresh"))
       |> put_optional(:metadata, Map.get(params, "metadata"))
       |> put_optional(:retry_policy, Map.get(params, "retry_policy"))
       |> put_optional(:timeout_ms, Map.get(params, "timeout_ms"))}
    end
  end

  defp run_input(params, %{type: "pipeline"}) do
    if Map.has_key?(params, "dependencies") do
      {:error, :invalid_dependencies}
    else
      {:ok,
       []
       |> put_optional(:window, Map.get(params, "window"))
       |> put_optional(:refresh_mode, Map.get(params, "refresh"))
       |> put_optional(:metadata, Map.get(params, "metadata"))
       |> put_optional(:retry_policy, Map.get(params, "retry_policy"))
       |> put_optional(:timeout_ms, Map.get(params, "timeout_ms"))}
    end
  end

  defp asset_selection(%{"window" => %{} = window}) do
    kind = field(window, "kind", :kind)
    value = field(window, "value", :value)
    timezone = field(window, "timezone", :timezone) || "Etc/UTC"

    if non_empty_string?(kind) and non_empty_string?(value) do
      {:ok, %{source: :data_coverage_timeline, kind: kind, value: value, timezone: timezone}}
    else
      {:error, :invalid_window_request}
    end
  end

  defp asset_selection(%{"window" => _invalid}), do: {:error, :invalid_window_request}
  defp asset_selection(params), do: {:ok, Map.get(params, "selection")}

  defp range_request(params) do
    case Map.get(params, "range") || Map.get(params, "range_request") do
      %{} = range -> {:ok, range}
      _missing_or_invalid -> {:error, :invalid_backfill_range_request}
    end
  end

  defp reject_removed_lookback(params) do
    cond do
      Map.has_key?(params, "lookback") ->
        {:error, {:unsupported_backfill_option, :lookback}}

      Map.has_key?(params, "lookback_policy") ->
        {:error, {:unsupported_backfill_option, :lookback_policy}}

      true ->
        :ok
    end
  end

  defp reject_legacy_retry_fields(params) do
    case Enum.find(["max_attempts", "retry_backoff_ms"], &Map.has_key?(params, &1)) do
      nil ->
        :ok

      field ->
        {:error, {:unsupported_retry_option, String.to_existing_atom(field), :use_retry_policy}}
    end
  end

  defp backfill_options(params, range_request) do
    with {:ok, metadata} <- optional_metadata(Map.get(params, "metadata")),
         {:ok, coverage_baseline_id} <-
           optional_non_empty_string(
             Map.get(params, "coverage_baseline_id"),
             :coverage_baseline_id
           ),
         {:ok, retry_policy} <- optional_retry_policy(Map.get(params, "retry_policy")),
         {:ok, timeout_ms} <- optional_positive_integer(params, "timeout_ms") do
      {:ok,
       []
       |> Keyword.put(:range_request, range_request)
       |> put_optional(:coverage_baseline_id, coverage_baseline_id)
       |> put_optional(:metadata, metadata)
       |> put_optional(:refresh, Map.get(params, "refresh"))
       |> put_optional(:refresh_policy, Map.get(params, "refresh_policy"))
       |> put_optional(:retry_policy, retry_policy)
       |> put_optional(:timeout_ms, timeout_ms)}
    end
  end

  defp optional_positive_integer(params, field) do
    optional_integer(Map.get(params, field), field, &(&1 > 0))
  end

  defp optional_retry_policy(nil), do: {:ok, nil}

  defp optional_retry_policy(value) do
    case Policy.new(value) do
      {:ok, policy} -> {:ok, policy}
      {:error, reason} -> {:error, {:invalid_operator_retry_policy, reason}}
    end
  end

  defp optional_integer(nil, _field, _valid?), do: {:ok, nil}

  defp optional_integer(value, field, valid?) when is_integer(value) and is_function(valid?, 1) do
    if valid?.(value), do: {:ok, value}, else: invalid_integer(value, field)
  end

  defp optional_integer(value, field, _valid?), do: invalid_integer(value, field)

  defp invalid_integer(value, field) do
    reason =
      case field do
        "timeout_ms" -> :invalid_operator_timeout_ms
      end

    {:error, {reason, value}}
  end

  defp optional_non_empty_string(nil, _field), do: {:ok, nil}

  defp optional_non_empty_string(value, _field) when is_binary(value) and value != "",
    do: {:ok, value}

  defp optional_non_empty_string(value, :coverage_baseline_id),
    do: {:error, {:invalid_operator_coverage_baseline_id, value}}

  defp optional_metadata(nil), do: {:ok, nil}
  defp optional_metadata(value) when is_map(value), do: {:ok, value}
  defp optional_metadata(value), do: {:error, {:invalid_operator_metadata, value}}

  defp target(params, manifest_version_id) do
    with %{} = target <- Map.get(params, "target"),
         {:ok, type} <- required_string(target, "type") do
      target(type, target, manifest_version_id)
    else
      _invalid -> {:error, :invalid_target}
    end
  end

  defp target(type, target, manifest_version_id)
       when type in ["asset", "pipeline"] do
    case Map.get(target, "id") do
      id when is_binary(id) and id != "" -> {:ok, %{type: type, id: id}}
      _missing when type == "pipeline" -> pipeline_module_target(target, manifest_version_id)
      _missing -> {:error, :invalid_target}
    end
  end

  defp target(_type, _target, _manifest_version_id), do: {:error, :invalid_target}

  defp pipeline_module_target(target, manifest_version_id) do
    with module_name when is_binary(module_name) and module_name != "" <-
           Map.get(target, "module"),
         {:ok, version} <- FavnOrchestrator.get_manifest(manifest_version_id),
         {:ok, module} <- find_pipeline_module(version.manifest.pipelines, module_name) do
      {:ok, %{type: "pipeline", id: ManifestTarget.pipeline_id(module)}}
    else
      nil -> {:error, :invalid_target}
      "" -> {:error, :invalid_target}
      {:error, :not_allowed} -> {:error, :invalid_target}
      {:error, reason} -> {:error, {:manifest_filter_lookup_failed, reason}}
      _invalid -> {:error, :invalid_target}
    end
  end

  defp find_pipeline_module(pipelines, module_name) do
    Enum.find_value(pipelines, {:error, :not_allowed}, fn pipeline ->
      if module_name in module_names(pipeline.module), do: {:ok, pipeline.module}
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

  defp manifest_version(params) do
    case Map.get(params, "manifest_selection", %{"mode" => "active"}) do
      %{"mode" => "active"} ->
        FavnOrchestrator.active_manifest()

      %{"mode" => "version", "manifest_version_id" => id}
      when is_binary(id) and id != "" ->
        {:ok, id}

      _invalid ->
        {:error, :invalid_manifest_selection}
    end
  end

  defp required_string(params, key) do
    case Map.get(params, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _missing_or_invalid -> {:error, {:missing_field, key}}
    end
  end

  defp field(map, string_key, atom_key), do: Map.get(map, string_key) || Map.get(map, atom_key)

  defp non_empty_string?(value), do: is_binary(value) and value != ""

  defp put_optional(opts, _key, nil), do: opts
  defp put_optional(opts, _key, ""), do: opts
  defp put_optional(opts, key, value), do: Keyword.put(opts, key, value)
end
