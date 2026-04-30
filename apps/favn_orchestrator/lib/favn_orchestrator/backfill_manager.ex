defmodule FavnOrchestrator.BackfillManager do
  @moduledoc """
  Orchestrator-owned parent/child pipeline backfill submission.

  This module is deliberately below the public `Favn` facade. It owns the
  control-plane mechanics for issue 168 style operational backfills:

  - resolve an operator range request into concrete windows
  - persist a parent `:backfill_pipeline` run
  - persist one `FavnOrchestrator.Backfill.BackfillWindow` row per requested
    window
  - submit one normal child pipeline run per window with lineage metadata

  HTTP, CLI, and future web surfaces should call the orchestrator facade rather
  than duplicating these mechanics.
  """

  alias Favn.Backfill.RangeResolver
  alias Favn.Manifest.Index
  alias Favn.Manifest.Pipeline
  alias Favn.Manifest.PipelineResolver
  alias Favn.Window.Key, as: WindowKey
  alias FavnOrchestrator.Backfill.BackfillWindow
  alias FavnOrchestrator.ManifestStore
  alias FavnOrchestrator.RunManager
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage
  alias FavnOrchestrator.TransitionWriter

  @default_max_windows 500

  @doc """
  Submits a parent pipeline backfill run and one child pipeline run per resolved anchor.

  See `FavnOrchestrator.submit_pipeline_backfill/2` for option semantics.
  """
  @spec submit_pipeline_backfill(module(), keyword()) :: {:ok, String.t()} | {:error, term()}
  def submit_pipeline_backfill(pipeline_module, opts \\ [])

  def submit_pipeline_backfill(pipeline_module, opts)
      when is_atom(pipeline_module) and is_list(opts) do
    run_id = Keyword.get(opts, :run_id, new_run_id())
    range_request = Keyword.get(opts, :range_request)

    with {:ok, manifest_version_id} <- resolve_manifest_version_id(opts),
         {:ok, version} <- ManifestStore.get_manifest(manifest_version_id),
         {:ok, index} <- Index.build_from_version(version),
         {:ok, pipeline} <- fetch_pipeline_by_module(index, pipeline_module),
         {:ok, range_request} <- maybe_resolve_coverage_baseline(range_request, opts),
         {:ok, range} <- RangeResolver.resolve(range_request),
         :ok <- validate_window_count(range, opts),
         {:ok, resolution} <- resolve_parent_pipeline(index, pipeline, range),
         {:ok, parent} <- build_parent_run(run_id, version, pipeline, resolution, range, opts),
         :ok <- persist_parent(parent, range, opts),
         :ok <-
           create_pending_windows(
             parent.id,
             pipeline_module,
             version.manifest_version_id,
             range,
             opts
           ),
         :ok <- submit_child_runs(parent, pipeline_module, range, opts) do
      {:ok, parent.id}
    else
      {:error, reason} = error ->
        maybe_compensate_submit_failure(run_id, pipeline_module, reason)
        error
    end
  end

  def submit_pipeline_backfill(_pipeline_module, _opts), do: {:error, :invalid_pipeline_module}

  defp resolve_parent_pipeline(index, %Pipeline{} = pipeline, range) do
    first_anchor = List.first(range.anchors)

    PipelineResolver.resolve(index, pipeline,
      trigger: %{kind: :backfill, phase: :parent},
      params: %{},
      anchor_window: first_anchor
    )
  end

  defp build_parent_run(run_id, version, %Pipeline{} = pipeline, resolution, range, opts) do
    user_metadata = Keyword.get(opts, :metadata, %{})

    metadata =
      if is_map(user_metadata) do
        Map.merge(user_metadata, %{
          submit_kind: :backfill_pipeline,
          pipeline_target_refs: resolution.target_refs,
          pipeline_context:
            Map.put(resolution.pipeline_ctx, :backfill_range, backfill_range_summary(range)),
          pipeline_dependencies: resolution.dependencies,
          pipeline_submit_ref: pipeline.module,
          backfill: backfill_summary(range, opts)
        })
      else
        user_metadata
      end

    with :ok <- validate_metadata(metadata),
         {:ok, max_attempts} <- positive_integer_option(opts, :max_attempts, 1),
         {:ok, retry_backoff_ms} <- non_neg_integer_option(opts, :retry_backoff_ms, 0),
         {:ok, timeout_ms} <- positive_integer_option(opts, :timeout_ms, 5_000) do
      parent =
        RunState.new(
          id: run_id,
          manifest_version_id: version.manifest_version_id,
          manifest_content_hash: version.content_hash,
          asset_ref: List.first(resolution.target_refs),
          target_refs: resolution.target_refs,
          plan: nil,
          params: %{},
          trigger: %{kind: :backfill, pipeline_module: pipeline.module},
          metadata: metadata,
          submit_kind: :backfill_pipeline,
          max_attempts: max_attempts,
          retry_backoff_ms: retry_backoff_ms,
          timeout_ms: timeout_ms
        )
        |> Map.put(:status, :running)
        |> Map.put(:updated_at, DateTime.utc_now())
        |> RunState.with_snapshot_hash()

      {:ok, parent}
    end
  end

  defp persist_parent(%RunState{} = parent, range, opts) do
    with :ok <-
           TransitionWriter.persist_transition(parent, :run_created, %{
             status: parent.status,
             submit_kind: :backfill_pipeline,
             backfill: backfill_summary(range, opts)
           }) do
      started = RunState.transition(parent, metadata: parent.metadata)

      TransitionWriter.persist_transition(started, :backfill_started, %{
        status: parent.status,
        requested_count: range.requested_count,
        window_keys: encoded_window_keys(range.anchors)
      })
    end
  end

  defp create_pending_windows(backfill_run_id, pipeline_module, manifest_version_id, range, opts) do
    now = DateTime.utc_now()
    coverage_baseline_id = Keyword.get(opts, :coverage_baseline_id)

    Enum.reduce_while(range.anchors, :ok, fn anchor, :ok ->
      with {:ok, window} <-
             BackfillWindow.new(%{
               backfill_run_id: backfill_run_id,
               pipeline_module: pipeline_module,
               manifest_version_id: manifest_version_id,
               coverage_baseline_id: coverage_baseline_id,
               window_kind: anchor.kind,
               window_start_at: anchor.start_at,
               window_end_at: anchor.end_at,
               timezone: anchor.timezone,
               window_key: WindowKey.encode(anchor.key),
               status: :pending,
               attempt_count: 0,
               created_at: now,
               updated_at: now
             }),
           :ok <- Storage.put_backfill_window(window) do
        {:cont, :ok}
      else
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp submit_child_runs(%RunState{} = parent, pipeline_module, range, opts) do
    Enum.reduce_while(range.anchors, :ok, fn anchor, :ok ->
      window_key = WindowKey.encode(anchor.key)

      child_opts =
        opts
        |> Keyword.drop([:range_request, :lookback, :run_id, :coverage_baseline_id])
        |> Keyword.put(:manifest_version_id, parent.manifest_version_id)
        |> Keyword.put(:anchor_window, anchor)
        |> Keyword.put(:parent_run_id, parent.id)
        |> Keyword.put(:root_run_id, parent.id)
        |> Keyword.put(:lineage_depth, 1)
        |> Keyword.put(:trigger, %{
          kind: :backfill,
          backfill_run_id: parent.id,
          window_key: window_key
        })

      case RunManager.submit_pipeline_module_run(pipeline_module, child_opts) do
        {:ok, _child_run_id} -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp validate_window_count(range, opts) do
    max_windows = Keyword.get(opts, :max_windows, @default_max_windows)

    cond do
      not is_integer(max_windows) or max_windows <= 0 ->
        {:error, :invalid_max_windows}

      range.requested_count > max_windows ->
        {:error, {:too_many_backfill_windows, range.requested_count, max_windows}}

      true ->
        :ok
    end
  end

  defp maybe_resolve_coverage_baseline(range_request, opts) do
    coverage_baseline_id = Keyword.get(opts, :coverage_baseline_id)

    cond do
      is_nil(coverage_baseline_id) or coverage_baseline_id == "" ->
        {:ok, range_request}

      has_relative_reference?(range_request) ->
        {:ok, range_request}

      relative_last_request?(range_request) ->
        with {:ok, baseline} <- Storage.get_coverage_baseline(coverage_baseline_id) do
          {:ok, put_baseline(range_request, baseline)}
        end

      true ->
        {:ok, range_request}
    end
  end

  defp relative_last_request?(value) when is_map(value), do: has_key?(value, :last)
  defp relative_last_request?(value) when is_list(value), do: Keyword.has_key?(value, :last)
  defp relative_last_request?(_value), do: false

  defp has_relative_reference?(value) when is_map(value) do
    has_key?(value, :relative_to) or has_key?(value, :baseline)
  end

  defp has_relative_reference?(value) when is_list(value) do
    Keyword.has_key?(value, :relative_to) or Keyword.has_key?(value, :baseline)
  end

  defp has_relative_reference?(_value), do: false

  defp has_key?(map, key), do: Map.has_key?(map, key) or Map.has_key?(map, Atom.to_string(key))

  defp put_baseline(value, baseline) when is_map(value), do: Map.put(value, :baseline, baseline)

  defp put_baseline(value, baseline) when is_list(value),
    do: Keyword.put(value, :baseline, baseline)

  defp maybe_compensate_submit_failure(backfill_run_id, pipeline_module, reason) do
    with {:ok, parent} <- Storage.get_run(backfill_run_id),
         true <- parent.submit_kind == :backfill_pipeline,
         {:ok, windows} <- Storage.list_backfill_windows(backfill_run_id: backfill_run_id) do
      now = DateTime.utc_now()
      error = {:backfill_child_submission_failed, reason}

      windows
      |> Enum.filter(
        &(&1.pipeline_module == pipeline_module and &1.status in [:pending, :running])
      )
      |> Enum.each(fn window ->
        _ =
          Storage.put_backfill_window(%{
            window
            | status: :error,
              last_error: error,
              errors: window.errors ++ [error],
              finished_at: window.finished_at || now,
              updated_at: now
          })
      end)

      parent
      |> RunState.transition(status: :error, error: error)
      |> TransitionWriter.persist_transition(:backfill_failed, %{status: :error, error: error})
    else
      _other -> :ok
    end
  end

  defp backfill_summary(range, opts) do
    %{
      kind: range.kind,
      timezone: range.timezone,
      requested_count: range.requested_count,
      range_start_at: range.range_start_at,
      range_end_at: range.range_end_at,
      window_keys: encoded_window_keys(range.anchors),
      lookback: Keyword.get(opts, :lookback),
      coverage_baseline_id: Keyword.get(opts, :coverage_baseline_id)
    }
  end

  defp backfill_range_summary(range) do
    Map.take(range, [:kind, :timezone, :range_start_at, :range_end_at, :requested_count])
  end

  defp encoded_window_keys(anchors), do: Enum.map(anchors, &WindowKey.encode(&1.key))

  defp resolve_manifest_version_id(opts) when is_list(opts) do
    case Keyword.get(opts, :manifest_version_id) do
      nil -> ManifestStore.get_active_manifest()
      value when is_binary(value) and value != "" -> {:ok, value}
      invalid -> {:error, {:invalid_manifest_version_id, invalid}}
    end
  end

  defp fetch_pipeline_by_module(%Index{} = index, pipeline_module)
       when is_atom(pipeline_module) do
    case Enum.filter(Index.list_pipelines(index), &(&1.module == pipeline_module)) do
      [%Pipeline{} = pipeline] -> {:ok, pipeline}
      [] -> {:error, :pipeline_not_found}
      _many -> {:error, :ambiguous_pipeline_module}
    end
  end

  defp validate_metadata(value) when is_map(value), do: :ok
  defp validate_metadata(_value), do: {:error, :invalid_run_metadata}

  defp positive_integer_option(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_integer(value) and value > 0 -> {:ok, value}
      _value -> {:error, String.to_atom("invalid_#{key}")}
    end
  end

  defp non_neg_integer_option(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_integer(value) and value >= 0 -> {:ok, value}
      _value -> {:error, String.to_atom("invalid_#{key}")}
    end
  end

  defp new_run_id do
    "run_" <> Base.encode16(:crypto.strong_rand_bytes(16), case: :lower)
  end
end
