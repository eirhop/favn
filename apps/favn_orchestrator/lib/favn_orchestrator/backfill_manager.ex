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
  alias FavnOrchestrator.Backfill.CoverageBaseline
  alias FavnOrchestrator.Backfill.Projector, as: BackfillProjector
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

    with :ok <- reject_unsupported_lookback(opts),
         {:ok, manifest_version_id} <- resolve_manifest_version_id(opts),
         {:ok, version} <- ManifestStore.get_manifest(manifest_version_id),
         {:ok, index} <- Index.build_from_version(version),
         {:ok, pipeline} <- fetch_pipeline_by_module(index, pipeline_module),
         {:ok, range_request} <-
           maybe_validate_coverage_baseline(range_request, pipeline_module, opts),
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
        case maybe_compensate_submit_failure(run_id, pipeline_module, reason, opts) do
          :ok -> error
          {:error, _reason} = compensation_error -> compensation_error
        end
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
         {:ok, max_attempts} <-
           positive_integer_option(opts, :max_attempts, 1, :invalid_max_attempts),
         {:ok, retry_backoff_ms} <-
           non_neg_integer_option(opts, :retry_backoff_ms, 0, :invalid_retry_backoff_ms),
         {:ok, timeout_ms} <-
           positive_integer_option(
             opts,
             :timeout_ms,
             RunState.default_timeout_ms(),
             :invalid_timeout_ms
           ) do
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
        |> Keyword.drop([:range_request, :run_id, :coverage_baseline_id])
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

      case submit_child_run(pipeline_module, child_opts, opts) do
        {:ok, _child_run_id} -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp submit_child_run(pipeline_module, child_opts, opts) do
    case Keyword.get(opts, :_child_submitter) do
      submitter when is_function(submitter, 2) -> submitter.(pipeline_module, child_opts)
      _other -> RunManager.submit_pipeline_module_run(pipeline_module, child_opts)
    end
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

  defp maybe_validate_coverage_baseline(range_request, pipeline_module, opts) do
    coverage_baseline_id = Keyword.get(opts, :coverage_baseline_id)

    if is_nil(coverage_baseline_id) or coverage_baseline_id == "" do
      {:ok, range_request}
    else
      with {:ok, baseline} <- fetch_coverage_baseline(coverage_baseline_id),
           :ok <- validate_coverage_baseline(baseline, pipeline_module, range_request) do
        {:ok,
         range_request
         |> maybe_apply_coverage_baseline(baseline)
         |> put_default_timezone(baseline.timezone)}
      end
    end
  end

  defp fetch_coverage_baseline(coverage_baseline_id) do
    case Storage.get_coverage_baseline(coverage_baseline_id) do
      {:ok, %CoverageBaseline{} = baseline} -> {:ok, baseline}
      {:error, :not_found} -> {:error, {:coverage_baseline_not_found, coverage_baseline_id}}
      {:error, _reason} = error -> error
    end
  end

  defp relative_last_request?(value) when is_map(value), do: has_key?(value, :last)
  defp relative_last_request?(value) when is_list(value), do: Keyword.has_key?(value, :last)
  defp relative_last_request?(_value), do: false

  defp validate_coverage_baseline(%CoverageBaseline{} = baseline, pipeline_module, range_request) do
    with :ok <- validate_baseline_pipeline(baseline, pipeline_module),
         :ok <- validate_baseline_status(baseline),
         :ok <- validate_baseline_kind(baseline, range_request),
         do: validate_baseline_timezone(baseline, range_request)
  end

  defp validate_baseline_pipeline(
         %CoverageBaseline{pipeline_module: pipeline_module},
         pipeline_module
       ),
       do: :ok

  defp validate_baseline_pipeline(
         %CoverageBaseline{pipeline_module: baseline_pipeline},
         pipeline_module
       ),
       do: {:error, {:coverage_baseline_pipeline_mismatch, baseline_pipeline, pipeline_module}}

  defp validate_baseline_status(%CoverageBaseline{status: :ok}), do: :ok

  defp validate_baseline_status(%CoverageBaseline{status: status}),
    do: {:error, {:coverage_baseline_not_ok, status}}

  defp validate_baseline_kind(%CoverageBaseline{window_kind: kind}, range_request) do
    case request_kind(range_request) do
      {:ok, ^kind} ->
        :ok

      {:ok, request_kind} ->
        {:error, {:coverage_baseline_window_kind_mismatch, kind, request_kind}}

      {:error, _reason} = error ->
        error
    end
  end

  defp request_kind(%{__struct__: _struct, kind: kind}), do: normalize_kind(kind)

  defp request_kind(value) when is_map(value) do
    cond do
      has_key?(value, :last) -> value |> get_value(:last) |> last_kind()
      has_key?(value, :kind) -> value |> get_value(:kind) |> normalize_kind()
      true -> {:error, {:invalid_backfill_range_request, value}}
    end
  end

  defp request_kind(value) when is_list(value) do
    cond do
      Keyword.has_key?(value, :last) -> value |> Keyword.get(:last) |> last_kind()
      Keyword.has_key?(value, :kind) -> value |> Keyword.get(:kind) |> normalize_kind()
      true -> {:error, {:invalid_backfill_range_request, value}}
    end
  end

  defp validate_baseline_timezone(%CoverageBaseline{timezone: timezone}, range_request) do
    case request_timezone(range_request) do
      nil ->
        :ok

      ^timezone ->
        :ok

      request_timezone ->
        {:error, {:coverage_baseline_timezone_mismatch, timezone, request_timezone}}
    end
  end

  defp last_kind({_count, kind}), do: normalize_kind(kind)
  defp last_kind([_count, kind]), do: normalize_kind(kind)
  defp last_kind(%{kind: kind}), do: normalize_kind(kind)
  defp last_kind(%{"kind" => kind}), do: normalize_kind(kind)
  defp last_kind(value), do: {:error, {:invalid_last_request, value}}

  defp normalize_kind(kind) when kind in [:hour, :day, :month, :year], do: {:ok, kind}
  defp normalize_kind("hour"), do: {:ok, :hour}
  defp normalize_kind("hourly"), do: {:ok, :hour}
  defp normalize_kind("day"), do: {:ok, :day}
  defp normalize_kind("daily"), do: {:ok, :day}
  defp normalize_kind("month"), do: {:ok, :month}
  defp normalize_kind("monthly"), do: {:ok, :month}
  defp normalize_kind("year"), do: {:ok, :year}
  defp normalize_kind("yearly"), do: {:ok, :year}
  defp normalize_kind(value), do: {:error, {:invalid_window_policy_kind, value}}

  defp has_meaningful_relative_reference?(value) when is_map(value) do
    meaningful?(get_value(value, :relative_to)) or meaningful?(get_value(value, :baseline))
  end

  defp has_meaningful_relative_reference?(value) when is_list(value) do
    meaningful?(Keyword.get(value, :relative_to)) or meaningful?(Keyword.get(value, :baseline))
  end

  defp has_meaningful_relative_reference?(_value), do: false

  defp meaningful?(nil), do: false
  defp meaningful?(""), do: false
  defp meaningful?(value) when is_map(value), do: map_size(value) > 0
  defp meaningful?(_value), do: true

  defp has_key?(map, key), do: Map.has_key?(map, key) or Map.has_key?(map, Atom.to_string(key))

  defp get_value(map, key), do: Map.get(map, key, Map.get(map, Atom.to_string(key)))

  defp put_baseline(value, baseline) when is_map(value), do: Map.put(value, :baseline, baseline)

  defp put_baseline(value, baseline) when is_list(value),
    do: Keyword.put(value, :baseline, baseline)

  defp maybe_apply_coverage_baseline(range_request, baseline) do
    if relative_last_request?(range_request) and
         not has_meaningful_relative_reference?(range_request) do
      put_baseline(range_request, baseline)
    else
      range_request
    end
  end

  defp request_timezone(value) when is_map(value) do
    case get_value(value, :timezone) do
      value when is_binary(value) and value != "" -> value
      _other -> nil
    end
  end

  defp request_timezone(value) when is_list(value) do
    case Keyword.get(value, :timezone) do
      value when is_binary(value) and value != "" -> value
      _other -> nil
    end
  end

  defp put_default_timezone(value, timezone) when is_map(value) do
    case request_timezone(value) do
      nil -> Map.put(value, :timezone, timezone)
      _timezone -> value
    end
  end

  defp put_default_timezone(value, timezone) when is_list(value) do
    case request_timezone(value) do
      nil -> Keyword.put(value, :timezone, timezone)
      _timezone -> value
    end
  end

  defp maybe_compensate_submit_failure(backfill_run_id, pipeline_module, reason, opts) do
    case Storage.get_run(backfill_run_id) do
      {:ok, %RunState{submit_kind: :backfill_pipeline} = parent} ->
        compensate_existing_backfill(parent, pipeline_module, reason, opts)

      {:ok, _other_run} ->
        :ok

      {:error, :not_found} ->
        :ok

      {:error, _reason} = error ->
        error
    end
  end

  defp compensate_existing_backfill(%RunState{} = parent, pipeline_module, reason, opts) do
    with {:ok, windows} <- BackfillProjector.list_all_backfill_windows(backfill_run_id: parent.id),
         now <- DateTime.utc_now(),
         error <- {:backfill_child_submission_failed, reason},
         {:ok, updated_windows} <-
           windows_with_compensation(windows, pipeline_module, error, now, opts) do
      status = BackfillProjector.parent_status(updated_windows)

      parent
      |> RunState.transition(
        status: status,
        error: error,
        result: %{status: status, backfill_windows: length(updated_windows)}
      )
      |> TransitionWriter.persist_transition(parent_event_type(status), %{
        status: status,
        error: error,
        window_counts: window_counts(updated_windows)
      })
    else
      {:error, _reason} = error -> error
    end
  end

  defp windows_with_compensation(windows, pipeline_module, error, now, opts) do
    result =
      Enum.reduce_while(windows, {:ok, []}, fn window, {:ok, acc} ->
        if window.pipeline_module == pipeline_module and window.status in [:pending, :running] do
          updated = %{
            window
            | status: :error,
              last_error: error,
              errors: window.errors ++ [error],
              finished_at: window.finished_at || now,
              updated_at: now
          }

          case put_compensated_window(updated, opts) do
            :ok -> {:cont, {:ok, [updated | acc]}}
            {:error, reason} -> {:halt, {:error, {:backfill_compensation_failed, reason}}}
          end
        else
          {:cont, {:ok, [window | acc]}}
        end
      end)

    case result do
      {:ok, windows} -> {:ok, Enum.reverse(windows)}
      {:error, _reason} = error -> error
    end
  end

  defp put_compensated_window(window, opts) do
    case Keyword.get(opts, :_compensation_window_writer) do
      writer when is_function(writer, 1) -> writer.(window)
      _other -> Storage.put_backfill_window(window)
    end
  end

  defp parent_event_type(:ok), do: :backfill_finished
  defp parent_event_type(:partial), do: :backfill_partial
  defp parent_event_type(:cancelled), do: :backfill_cancelled
  defp parent_event_type(:timed_out), do: :backfill_timed_out
  defp parent_event_type(:error), do: :backfill_failed
  defp parent_event_type(:running), do: :backfill_progressed

  defp window_counts(windows) do
    Enum.reduce(windows, %{}, fn %BackfillWindow{status: status}, acc ->
      Map.update(acc, status, 1, &(&1 + 1))
    end)
  end

  defp backfill_summary(range, opts) do
    %{
      kind: range.kind,
      timezone: range.timezone,
      requested_count: range.requested_count,
      range_start_at: range.range_start_at,
      range_end_at: range.range_end_at,
      window_keys: encoded_window_keys(range.anchors),
      coverage_baseline_id: Keyword.get(opts, :coverage_baseline_id)
    }
  end

  defp reject_unsupported_lookback(opts) do
    cond do
      Keyword.has_key?(opts, :lookback) ->
        {:error, {:unsupported_backfill_option, :lookback}}

      Keyword.has_key?(opts, :lookback_policy) ->
        {:error, {:unsupported_backfill_option, :lookback_policy}}

      true ->
        :ok
    end
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

  defp positive_integer_option(opts, key, default, error_reason) do
    case Keyword.get(opts, key, default) do
      value when is_integer(value) and value > 0 -> {:ok, value}
      _value -> {:error, error_reason}
    end
  end

  defp non_neg_integer_option(opts, key, default, error_reason) do
    case Keyword.get(opts, key, default) do
      value when is_integer(value) and value >= 0 -> {:ok, value}
      _value -> {:error, error_reason}
    end
  end

  defp new_run_id do
    "run_" <> Base.encode16(:crypto.strong_rand_bytes(16), case: :lower)
  end
end
