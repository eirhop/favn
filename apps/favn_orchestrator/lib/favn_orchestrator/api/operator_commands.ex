defmodule FavnOrchestrator.API.OperatorCommands do
  @moduledoc """
  Translates private HTTP command payloads into the public operator facade.

  Target module names are resolved only against the selected manifest. This
  avoids scanning every stored manifest and prevents a module from an unrelated
  version being accepted during request validation.
  """

  alias FavnOrchestrator
  alias FavnOrchestrator.Backfills
  alias FavnOrchestrator.ManifestTarget
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias Favn.Retry.Policy

  @type actor_context :: %{required(:actor) => map(), required(:session) => map()}

  @doc false
  @spec submit_run(map(), actor_context() | WorkspaceContext.t(), keyword()) ::
          {:ok, String.t()} | {:error, term()}
  def submit_run(params, actor_context, opts)
      when is_map(params) and is_map(actor_context) and is_list(opts) do
    with :ok <- reject_legacy_retry_fields(params),
         {:ok, manifest_version_id} <- manifest_version(params, actor_context),
         {:ok, target} <- target(params, manifest_version_id, actor_context),
         {:ok, command_input} <- normalize_run_input(params, target) do
      submit_operator_run(actor_context, manifest_version_id, target, command_input, opts)
    end
  end

  defp submit_operator_run(
         %WorkspaceContext{} = context,
         manifest_version_id,
         target,
         command_input,
         opts
       ) do
    FavnOrchestrator.submit_operator_run(
      context,
      manifest_version_id,
      target,
      command_input,
      opts
    )
  end

  @doc false
  @spec submit_backfill(map(), WorkspaceContext.t(), keyword()) ::
          {:ok, FavnOrchestrator.Persistence.Results.Backfill.t()} | {:error, term()}
  def submit_backfill(params, %WorkspaceContext{} = context, opts)
      when is_map(params) and is_list(opts) do
    with :ok <- reject_legacy_retry_fields(params),
         :ok <- reject_removed_lookback(params),
         :ok <- reject_removed_coverage_baseline(params),
         {:ok, manifest_version_id} <- manifest_version(params, context),
         {:ok, %{type: "pipeline", id: target_id}} <-
           target(params, manifest_version_id, context),
         {:ok, range_request} <- range_request(params),
         {:ok, command_opts} <- backfill_options(params, range_request) do
      Backfills.submit_pipeline(
        context,
        manifest_version_id,
        target_id,
        range_request,
        Keyword.merge(command_opts, opts)
        |> Keyword.delete(:range_request)
        |> Keyword.delete(:coverage_baseline_id)
      )
    else
      {:ok, _target} -> {:error, :invalid_target}
      {:error, _reason} = error -> error
    end
  end

  @doc false
  @spec plan_backfill(map(), WorkspaceContext.t()) :: {:ok, map()} | {:error, term()}
  def plan_backfill(params, %WorkspaceContext{} = context) when is_map(params) do
    with :ok <- reject_legacy_retry_fields(params),
         :ok <- reject_removed_lookback(params),
         :ok <- reject_removed_coverage_baseline(params),
         {:ok, manifest_version_id} <- manifest_version(params, context),
         {:ok, %{type: "pipeline", id: target_id}} <-
           target(params, manifest_version_id, context),
         {:ok, range_request} <- range_request(params),
         {:ok, opts} <- backfill_options(params, range_request) do
      Backfills.plan_pipeline(
        context,
        manifest_version_id,
        target_id,
        range_request,
        opts
        |> Keyword.delete(:range_request)
        |> Keyword.delete(:coverage_baseline_id)
      )
    else
      {:ok, _target} -> {:error, :invalid_target}
      {:error, _reason} = error -> error
    end
  end

  @doc false
  @spec normalize_run_input(map(), %{required(:type) => String.t()}) ::
          {:ok, keyword()} | {:error, term()}
  def normalize_run_input(params, %{type: "asset"}) do
    with {:ok, selection} <- asset_selection(params) do
      {:ok,
       []
       |> put_optional(:selection, selection)
       |> put_optional(:run_context_id, Map.get(params, "run_context_id"))
       |> put_present(:dependency_mode, params, "dependencies")
       |> put_present(:refresh_mode, params, "refresh")
       |> put_optional(:metadata, Map.get(params, "metadata"))
       |> put_optional(:retry_policy, Map.get(params, "retry_policy"))
       |> put_optional(:timeout_ms, Map.get(params, "timeout_ms"))}
    end
  end

  def normalize_run_input(params, %{type: "pipeline"}) do
    if Map.has_key?(params, "dependencies") do
      {:error, :invalid_dependencies}
    else
      {:ok,
       []
       |> put_optional(:window, Map.get(params, "window"))
       |> put_present(:refresh_mode, params, "refresh")
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

  defp reject_removed_coverage_baseline(params) do
    if Map.has_key?(params, "coverage_baseline_id"),
      do: {:error, {:unsupported_backfill_option, :coverage_baseline_id}},
      else: :ok
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

  defp target(params, manifest_version_id, context) do
    with %{} = target <- Map.get(params, "target"),
         {:ok, type} <- required_string(target, "type") do
      target(type, target, manifest_version_id, context)
    else
      _invalid -> {:error, :invalid_target}
    end
  end

  defp target(type, target, manifest_version_id, context)
       when type in ["asset", "pipeline"] do
    case Map.get(target, "id") do
      id when is_binary(id) and id != "" ->
        {:ok, %{type: type, id: id}}

      _missing when type == "pipeline" ->
        pipeline_module_target(target, manifest_version_id, context)

      _missing ->
        {:error, :invalid_target}
    end
  end

  defp target(_type, _target, _manifest_version_id, _context), do: {:error, :invalid_target}

  defp pipeline_module_target(target, manifest_version_id, context) do
    with module_name when is_binary(module_name) and module_name != "" <-
           Map.get(target, "module"),
         {:ok, version} <- get_manifest(context, manifest_version_id),
         {:ok, pipeline} <- find_pipeline(version.manifest.pipelines, module_name) do
      {:ok, %{type: "pipeline", id: ManifestTarget.pipeline_id(pipeline.module, pipeline.name)}}
    else
      nil -> {:error, :invalid_target}
      "" -> {:error, :invalid_target}
      {:error, :not_allowed} -> {:error, :invalid_target}
      {:error, reason} -> {:error, {:manifest_filter_lookup_failed, reason}}
      _invalid -> {:error, :invalid_target}
    end
  end

  defp find_pipeline(pipelines, module_name) do
    Enum.find_value(pipelines, {:error, :not_allowed}, fn pipeline ->
      if module_name in module_names(pipeline.module), do: {:ok, pipeline}
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

  defp manifest_version(params, context) do
    case Map.get(params, "manifest_selection", %{"mode" => "active"}) do
      %{"mode" => "active"} ->
        active_manifest(context)

      %{"mode" => "version", "manifest_version_id" => id}
      when is_binary(id) and id != "" ->
        {:ok, id}

      _invalid ->
        {:error, :invalid_manifest_selection}
    end
  end

  defp get_manifest(%WorkspaceContext{} = context, manifest_version_id),
    do: FavnOrchestrator.get_manifest(context, manifest_version_id)

  defp active_manifest(%WorkspaceContext{} = context),
    do: FavnOrchestrator.active_manifest(context)

  defp required_string(params, key) do
    case Map.get(params, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _missing_or_invalid -> {:error, {:missing_field, key}}
    end
  end

  defp field(map, string_key, atom_key), do: Map.get(map, string_key) || Map.get(map, atom_key)

  defp non_empty_string?(value), do: is_binary(value) and value != ""

  defp put_present(opts, key, params, field) do
    if Map.has_key?(params, field),
      do: Keyword.put(opts, key, Map.get(params, field)),
      else: opts
  end

  defp put_optional(opts, _key, nil), do: opts
  defp put_optional(opts, _key, ""), do: opts
  defp put_optional(opts, key, value), do: Keyword.put(opts, key, value)
end
