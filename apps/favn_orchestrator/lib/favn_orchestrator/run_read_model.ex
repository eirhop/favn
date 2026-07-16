defmodule FavnOrchestrator.RunReadModel do
  @moduledoc """
  Public orchestrator-owned read models for persisted runs.

  This module turns internal run snapshots and operational backfill ledgers into
  boring maps for thin callers. Callers should use the `FavnOrchestrator`
  facade functions rather than reaching into storage or run snapshot structs.
  """

  alias Favn.Log.Filter
  alias Favn.RuntimeInput.Pin
  alias FavnOrchestrator.Backfill.BackfillWindow
  alias FavnOrchestrator.ExecutionStatus
  alias FavnOrchestrator.Page
  alias FavnOrchestrator.RunEvent
  alias FavnOrchestrator.RunReadModel.StepProjection
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage
  alias FavnOrchestrator.Storage.RunQuery
  alias FavnOrchestrator.WindowSummary

  @backfill_failure_detail_limit 10
  @operator_step_event_limit 200
  @operator_step_state_event_types [
    :step_queued,
    :step_started,
    :step_retry_started,
    :step_finished,
    :step_failed,
    :step_timed_out,
    :step_cancelled,
    :step_retry_scheduled,
    :step_skipped_fresh,
    :step_blocked
  ]
  @operator_event_default_limit 50
  @operator_event_max_limit 500

  @type run_role :: :asset | :pipeline | :backfill_parent | :backfill_child | :rerun

  @type window_summary :: WindowSummary.t()

  @type progress_summary :: %{
          required(:unit) => :assets | :steps | :windows,
          required(:label) => String.t(),
          required(:counts) => map()
        }

  @type step_summary :: StepProjection.t()

  @type run_summary :: %{
          required(:id) => String.t(),
          required(:kind) => run_role(),
          required(:role) => run_role(),
          required(:status) => RunState.status(),
          required(:submit_kind) => atom(),
          required(:manifest_version_id) => String.t(),
          required(:asset_ref) => Favn.Ref.t(),
          required(:target_refs) => [Favn.Ref.t()],
          required(:parent_run_id) => String.t() | nil,
          required(:root_run_id) => String.t() | nil,
          required(:rerun_of_run_id) => String.t() | nil,
          required(:window) => window_summary() | nil,
          required(:progress_unit) => :assets | :steps | :windows | nil,
          required(:progress) => progress_summary() | nil,
          required(:started_at) => DateTime.t() | nil,
          required(:finished_at) => DateTime.t() | nil,
          required(:updated_at) => DateTime.t() | nil,
          required(:duration_ms) => non_neg_integer() | nil
        }

  @type backfill_failure :: %{
          required(:child_run_id) => String.t() | nil,
          required(:status) => BackfillWindow.status(),
          required(:window) => window_summary(),
          required(:asset_ref) => String.t() | nil,
          required(:error) => term(),
          required(:attempt_count) => non_neg_integer(),
          required(:started_at) => DateTime.t() | nil,
          required(:finished_at) => DateTime.t() | nil,
          required(:duration_ms) => non_neg_integer() | nil
        }

  @type run_detail :: %{
          required(:summary) => run_summary(),
          required(:params) => map(),
          required(:trigger) => map(),
          required(:metadata) => map(),
          required(:result) => map() | nil,
          required(:error) => term(),
          required(:runner_execution_id) => String.t() | nil,
          required(:event_seq) => pos_integer(),
          required(:steps) => [step_summary()],
          required(:backfill_failures) => [backfill_failure()],
          required(:backfill_failure_count) => non_neg_integer(),
          required(:events) => [RunEvent.t()],
          required(:retry) => map(),
          required(:runtime_input_pins) => [map()]
        }

  @type asset_attempt_summary :: %{
          required(:id) => String.t(),
          required(:root_execution_group_id) => String.t(),
          required(:child_run_id) => String.t() | nil,
          required(:run_id) => String.t(),
          required(:status) => atom() | nil,
          required(:asset_key) => String.t(),
          required(:asset_ref) => String.t(),
          required(:stage) => non_neg_integer() | nil,
          required(:execution_pool) => atom() | String.t() | nil,
          required(:queue_reason) => atom() | String.t() | nil,
          required(:attempt_number) => non_neg_integer() | nil,
          required(:started_at) => DateTime.t() | nil,
          required(:finished_at) => DateTime.t() | nil,
          required(:duration_ms) => non_neg_integer() | nil,
          required(:error_summary) => String.t() | nil,
          required(:output_metadata) => map() | nil,
          required(:window) => window_summary() | nil,
          required(:window_start_at) => DateTime.t() | nil,
          required(:window_end_at) => DateTime.t() | nil
        }

  @type execution_group_summary :: %{
          required(:id) => String.t(),
          required(:root_execution_group_id) => String.t(),
          required(:status) => RunState.status(),
          required(:health) => :ok | :warning | :error | :active,
          required(:active?) => boolean(),
          required(:trigger_type) => atom() | nil,
          required(:target_assets) => [String.t()],
          required(:root_status) => RunState.status(),
          required(:started_at) => DateTime.t() | nil,
          required(:finished_at) => DateTime.t() | nil,
          required(:duration_ms) => non_neg_integer() | nil,
          required(:total_windows) => non_neg_integer(),
          required(:completed_windows) => non_neg_integer(),
          required(:failed_windows) => non_neg_integer(),
          required(:total_asset_attempts) => non_neg_integer(),
          required(:completed_asset_attempts) => non_neg_integer(),
          required(:failed_asset_attempts) => non_neg_integer(),
          required(:running_asset_attempts) => non_neg_integer(),
          required(:queued_asset_attempts) => non_neg_integer(),
          required(:failure_count) => non_neg_integer(),
          required(:progress) => progress_summary() | nil,
          required(:summary_totals) => map(),
          required(:last_activity_at) => DateTime.t() | nil,
          required(:currently_running_asset_attempts) => [asset_attempt_summary()],
          required(:child_run_ids) => [String.t()]
        }

  @type timeline_entry :: %{
          required(:started_at) => DateTime.t() | nil,
          required(:finished_at) => DateTime.t() | nil,
          required(:asset_key) => String.t(),
          required(:window) => window_summary() | nil,
          required(:status) => atom() | nil,
          required(:stage) => non_neg_integer() | nil,
          required(:attempt_id) => String.t(),
          required(:child_run_id) => String.t() | nil,
          required(:root_execution_group_id) => String.t()
        }

  @type execution_group_detail :: %{
          required(:summary) => execution_group_summary(),
          required(:root_run) => run_summary(),
          required(:child_runs) => [run_summary()],
          required(:windows) => [map()],
          required(:asset_attempts) => [asset_attempt_summary()],
          required(:timeline) => [timeline_entry()],
          required(:events) => [RunEvent.t()]
        }

  @type operator_run_detail :: %{
          required(:summary) => execution_group_summary(),
          required(:root_run) => run_summary(),
          required(:child_runs) => [run_summary()],
          required(:windows) => [map()],
          required(:asset_attempts) => [asset_attempt_summary()],
          required(:timeline) => [timeline_entry()],
          required(:steps) => [step_summary()],
          required(:progress) => progress_summary() | nil,
          required(:counts) => map(),
          required(:backfill_failures) => [backfill_failure()],
          required(:backfill_failure_count) => non_neg_integer(),
          required(:root_event_sequence) => non_neg_integer() | nil,
          required(:latest_global_event_sequence) => non_neg_integer() | nil,
          required(:latest_event) => RunEvent.t() | nil,
          optional(:events) => [RunEvent.t()]
        }

  @type asset_step_log_context :: %{
          required(:run) => run_summary(),
          required(:step) => step_summary() | nil,
          required(:title) => String.t(),
          required(:subtitle) => String.t(),
          required(:status) => atom() | nil,
          required(:facts) => [map()],
          required(:log_filter) => Filter.t(),
          required(:fallback?) => boolean(),
          required(:note) => String.t() | nil
        }

  @doc """
  Returns one public run detail from a persisted run snapshot.
  """
  @spec get_run_detail(String.t()) :: {:ok, run_detail()} | {:error, term()}
  def get_run_detail(run_id) when is_binary(run_id) do
    with {:ok, %RunState{} = run} <- Storage.get_run(run_id),
         {:ok, events} <- Storage.list_run_events(run_id),
         {:ok, backfill_windows} <- detail_backfill_windows(run),
         {:ok, runtime_input_pins} <- runtime_input_pins_for_read(run_id) do
      events = Enum.map(events, &RunEvent.from_map/1)
      public_run = with_public_status(run)
      {backfill_failures, backfill_failure_count} = backfill_failures(run, backfill_windows)

      {:ok,
       %{
         summary: summary(public_run, backfill_windows),
         params: run.params,
         trigger: run.trigger,
         metadata: run.metadata,
         result: run.result,
         error: run.error,
         runner_execution_id: run.runner_execution_id,
         event_seq: run.event_seq,
         retry: retry_detail(run),
         runtime_input_pins: Enum.map(runtime_input_pins, &Pin.lineage/1),
         steps: StepProjection.build(run, events),
         backfill_failures: backfill_failures,
         backfill_failure_count: backfill_failure_count,
         events: events
       }}
    end
  end

  @doc """
  Returns the bounded operator-facing run detail for a run detail page.

  The default response does not include full event streams. Pass
  `include: [:events]` with an optional `event_limit` to request a bounded event
  page explicitly.
  """
  @spec get_operator_run_detail(String.t(), keyword()) ::
          {:ok, operator_run_detail()} | {:error, term()}
  def get_operator_run_detail(run_id, opts \\ [])

  def get_operator_run_detail(run_id, opts) when is_binary(run_id) and is_list(opts) do
    with {:ok, event_opts} <- normalize_operator_event_opts(opts),
         {:ok, %RunState{} = run} <- Storage.get_run(run_id),
         group_id <- RunQuery.root_execution_group_id(run),
         {:ok, group} <- load_execution_group(group_id) do
      operator_run_detail(group, event_opts)
    end
  end

  @doc """
  Lists execution groups for run overview screens.

  An execution group is rooted at a submitted run request. Backfill child runs are
  grouped under their persisted `root_run_id`/`parent_run_id` so callers do not
  derive parent/child relationships from raw run rows.

  The overview path is intentionally bounded and uses run snapshots plus
  backfill-window ledger data. It does not hydrate per-run events; event-backed
  attempt detail is loaded by `get_execution_group_detail/2` and the focused
  asset-attempt/timeline helpers.
  """
  @spec list_execution_groups(keyword()) :: {:ok, [execution_group_summary()]} | {:error, term()}
  def list_execution_groups(filters \\ []) when is_list(filters) do
    with {:ok, page} <- page_execution_groups(filters) do
      {:ok, page.items}
    end
  end

  @doc """
  Returns a bounded page of execution groups for operator list screens.
  """
  @spec page_execution_groups(keyword()) ::
          {:ok, Page.t(execution_group_summary())} | {:error, term()}
  def page_execution_groups(filters \\ []) when is_list(filters) do
    normalized_filters = normalize_execution_group_filters(filters)

    case Storage.list_execution_group_summaries(normalized_filters) do
      {:ok, %Page{items: []} = page} ->
        maybe_rebuild_empty_execution_group_summary_page(page, normalized_filters, filters)

      {:ok, %Page{} = page} ->
        {:ok, page}

      {:error, _reason} = error ->
        error
    end
  end

  defp maybe_rebuild_empty_execution_group_summary_page(
         %Page{offset: 0, items: []} = page,
         normalized_filters,
         original_filters
       ) do
    case execution_group_summary_read_model_empty?() do
      true ->
        rebuild_empty_execution_group_summary_page(page, normalized_filters, original_filters)

      false ->
        {:ok, page}

      :unknown ->
        {:ok, page}
    end
  end

  defp maybe_rebuild_empty_execution_group_summary_page(
         page,
         _normalized_filters,
         _original_filters
       ) do
    {:ok, page}
  end

  defp execution_group_summary_read_model_empty? do
    case Storage.list_execution_group_summaries(limit: 1, offset: 0) do
      {:ok, %Page{items: []}} -> true
      {:ok, %Page{items: [_ | _]}} -> false
      {:error, _reason} -> :unknown
    end
  end

  defp rebuild_empty_execution_group_summary_page(page, normalized_filters, _original_filters) do
    case Storage.rebuild_execution_group_summaries() do
      {:ok, count} when count > 0 ->
        Storage.list_execution_group_summaries(normalized_filters)

      {:ok, 0} ->
        {:ok, page}

      {:error, _reason} = error ->
        error
    end
  end

  @doc """
  Returns one execution group detail including children, windows, attempts, and timeline entries.
  """
  @spec get_execution_group_detail(String.t(), keyword()) ::
          {:ok, execution_group_detail()} | {:error, term()}
  def get_execution_group_detail(group_id, filters \\ [])
      when is_binary(group_id) and is_list(filters) do
    with {:ok, group} <- load_execution_group(group_id),
         {:ok, events} <- list_execution_group_events(group_id),
         {:ok, backfill_windows} <- detail_backfill_windows(group.root) do
      events_by_run = Enum.group_by(events, & &1.run_id)
      windows = execution_group_window_summaries(group, backfill_windows)

      attempts =
        group
        |> execution_group_asset_attempts(:detail, events_by_run, windows)
        |> filter_asset_attempts(filters)

      {:ok,
       %{
         summary:
           execution_group_summary(Map.merge(group, %{attempts: attempts, windows: windows})),
         root_run: summary(group.root, backfill_windows),
         child_runs: Enum.map(group.children, &summary/1),
         windows: windows,
         asset_attempts: attempts,
         timeline: timeline_entries(attempts),
         events: events
       }}
    end
  end

  @doc """
  Returns execution group detail for any run id in the group.
  """
  @spec get_execution_group_detail_for_run(String.t(), keyword()) ::
          {:ok, execution_group_detail()} | {:error, term()}
  def get_execution_group_detail_for_run(run_id, filters \\ [])
      when is_binary(run_id) and is_list(filters) do
    with {:ok, %RunState{} = run} <- Storage.get_run(run_id) do
      run
      |> RunQuery.root_execution_group_id()
      |> get_execution_group_detail(filters)
    end
  end

  @doc """
  Lists persisted events for an execution group, including child/window runs.
  """
  @spec list_execution_group_events(String.t(), keyword()) ::
          {:ok, [RunEvent.t()]} | {:error, term()}
  def list_execution_group_events(group_id, filters \\ []) when is_binary(group_id) do
    with {:ok, events} <- Storage.list_execution_group_events(group_id, filters) do
      {:ok, Enum.map(events, &RunEvent.from_map/1)}
    end
  end

  @doc """
  Lists asset attempts for one execution group.
  """
  @spec list_execution_group_asset_attempts(String.t(), keyword()) ::
          {:ok, [asset_attempt_summary()]} | {:error, term()}
  def list_execution_group_asset_attempts(group_id, filters \\ [])
      when is_binary(group_id) and is_list(filters) do
    with {:ok, group} <- load_execution_group(group_id),
         {:ok, backfill_windows} <- detail_backfill_windows(group.root),
         {:ok, events} <- list_execution_group_events(group_id) do
      windows = execution_group_window_summaries(group, backfill_windows)
      events_by_run = Enum.group_by(events, & &1.run_id)

      attempts =
        group
        |> execution_group_asset_attempts(:detail, events_by_run, windows)
        |> filter_asset_attempts(filters)

      {:ok, attempts}
    end
  end

  @doc """
  Lists window summaries for one execution group.
  """
  @spec list_execution_group_windows(String.t(), keyword()) :: {:ok, [map()]} | {:error, term()}
  def list_execution_group_windows(group_id, filters \\ [])
      when is_binary(group_id) and is_list(filters) do
    with {:ok, group} <- load_execution_group(group_id),
         {:ok, backfill_windows} <- detail_backfill_windows(group.root) do
      {:ok, execution_group_window_summaries(group, backfill_windows)}
    end
  end

  @doc """
  Lists execution wall-clock timeline entries for one execution group.
  """
  @spec list_execution_group_timeline(String.t(), keyword()) ::
          {:ok, [timeline_entry()]} | {:error, term()}
  def list_execution_group_timeline(group_id, filters \\ [])
      when is_binary(group_id) and is_list(filters) do
    with {:ok, attempts} <- list_execution_group_asset_attempts(group_id, filters) do
      {:ok, timeline_entries(attempts)}
    end
  end

  @doc """
  Returns public log-page context for one asset step in a persisted run.
  """
  @spec get_asset_step_log_context(String.t(), String.t()) ::
          {:ok, asset_step_log_context()} | {:error, term()}
  def get_asset_step_log_context(run_id, asset_step_id)
      when is_binary(run_id) and is_binary(asset_step_id) do
    with {:ok, detail} <- get_run_detail(run_id),
         exact_filter <- %Filter{run_id: run_id, asset_step_id: asset_step_id},
         {:ok, exact?} <- log_filter_has_entries?(exact_filter) do
      step = Enum.find(detail.steps, &(&1.id == asset_step_id))
      {log_filter, fallback?} = asset_step_log_filter(run_id, step, exact_filter, exact?)

      {:ok, asset_step_log_context(detail, step, asset_step_id, log_filter, fallback?)}
    end
  end

  defp summary(%RunState{} = run) do
    summary(run, nil)
  end

  defp summary(%RunState{} = run, backfill_windows) do
    run = with_public_status(run)
    role = classify(run)
    progress = progress(run, role, backfill_windows)
    window = window(run, role)

    %{
      id: run.id,
      kind: role,
      role: role,
      status: run.status,
      submit_kind: run.submit_kind,
      manifest_version_id: run.manifest_version_id,
      asset_ref: run.asset_ref,
      target_refs: run.target_refs,
      parent_run_id: run.parent_run_id,
      root_run_id: run.root_run_id,
      rerun_of_run_id: run.rerun_of_run_id,
      window: window,
      progress_unit: if(progress, do: progress.unit),
      progress: progress,
      started_at: run.inserted_at,
      finished_at: finished_at(run),
      updated_at: run.updated_at,
      duration_ms: duration_ms(run)
    }
  end

  defp load_execution_group(group_id) do
    with {:ok, runs} <- Storage.list_execution_group_runs(group_id) do
      find_execution_group(runs, group_id)
    end
  end

  defp normalize_execution_group_filters(filters) do
    filters
    |> Keyword.update(:sort, :started_desc, &normalize_execution_group_sort/1)
    |> normalize_execution_group_filter(:status, &normalize_existing_atom/1)
    |> normalize_execution_group_filter(:trigger_type, &normalize_existing_atom/1)
    |> normalize_execution_group_filter(:window, &normalize_existing_atom/1)
  end

  defp normalize_execution_group_filter(filters, key, fun) do
    case Keyword.fetch(filters, key) do
      {:ok, value} -> Keyword.put(filters, key, fun.(value))
      :error -> filters
    end
  end

  defp normalize_execution_group_sort(value)
       when value in [:started_desc, :failed_first, :running_first, :status_priority], do: value

  defp normalize_execution_group_sort(value)
       when value in ["started_desc", "failed_first", "running_first", "status_priority"],
       do: String.to_existing_atom(value)

  defp normalize_execution_group_sort(_value), do: :started_desc

  defp normalize_existing_atom(value) when is_atom(value), do: value

  defp normalize_existing_atom(value) when is_binary(value) do
    String.to_existing_atom(value)
  rescue
    ArgumentError -> value
  end

  defp normalize_existing_atom(value), do: value

  defp find_execution_group(runs, group_id) do
    case Enum.find(runs, &(&1.id == group_id)) do
      nil ->
        {:error, :not_found}

      root ->
        children =
          runs
          |> Enum.reject(&(&1.id == group_id))
          |> Enum.sort_by(&run_started_sort_key/1)

        {:ok, %{id: group_id, root: root, children: children, runs: [root | children]}}
    end
  end

  defp execution_group_summary(group) do
    windows = Map.fetch!(group, :windows)
    attempts = Map.fetch!(group, :attempts)

    attempt_counts = attempt_counts(attempts)
    window_counts = window_counts(windows)
    root = with_public_status(group.root)
    timing = execution_group_timing(group, attempts, windows, attempt_counts)
    active? = execution_group_active?(group, windows, attempt_counts)
    status = execution_group_status(root.status, attempt_counts, window_counts, active?)
    failure_count = attempt_counts.failed + window_counts.failed

    %{
      id: root.id,
      root_execution_group_id: root.id,
      status: status,
      health: execution_group_health(status, failure_count, active?),
      active?: active?,
      trigger_type: trigger_type(root),
      target_assets: target_assets(root),
      root_status: root.status,
      started_at: timing.started_at,
      finished_at: timing.finished_at,
      duration_ms: timing.duration_ms,
      total_windows: window_counts.total,
      completed_windows: window_counts.completed,
      failed_windows: window_counts.failed,
      total_asset_attempts: attempt_counts.total,
      completed_asset_attempts: attempt_counts.completed,
      failed_asset_attempts: attempt_counts.failed,
      running_asset_attempts: attempt_counts.running,
      queued_asset_attempts: attempt_counts.queued,
      failure_count: failure_count,
      progress: execution_group_progress(attempt_counts),
      summary_totals: %{
        windows: window_counts,
        asset_attempts: attempt_counts
      },
      last_activity_at:
        latest_datetime(Enum.flat_map(group.runs, &[&1.updated_at, &1.inserted_at])),
      currently_running_asset_attempts:
        Enum.filter(attempts, &ExecutionStatus.running?(&1.status)),
      child_run_ids: Enum.map(group.children, & &1.id)
    }
  end

  defp operator_run_detail(group, event_opts) do
    with {:ok, backfill_windows} <- detail_backfill_windows(group.root),
         {:ok, events} <- operator_events(group.id, event_opts),
         {:ok, latest_event} <- latest_operator_event(group.id),
         {:ok, step_events_by_run} <- operator_step_events_by_run(group.id) do
      windows = execution_group_window_summaries(group, backfill_windows)
      attempts = execution_group_asset_attempts(group, :operator, step_events_by_run, windows)
      root_events = Map.get(step_events_by_run, group.root.id, [])
      root_steps = StepProjection.build(group.root, root_events)

      group_summary =
        execution_group_summary(Map.merge(group, %{attempts: attempts, windows: windows}))

      {backfill_failures, backfill_failure_count} =
        backfill_failures(group.root, backfill_windows)

      detail = %{
        summary: group_summary,
        root_run: summary(group.root, backfill_windows),
        child_runs: Enum.map(group.children, &summary/1),
        windows: windows,
        asset_attempts: attempts,
        timeline: timeline_entries(attempts),
        steps: root_steps,
        progress: group_summary.progress,
        counts: group_summary.summary_totals,
        backfill_failures: backfill_failures,
        backfill_failure_count: backfill_failure_count,
        root_event_sequence: group.root.event_seq,
        latest_global_event_sequence: latest_event && latest_event.global_sequence,
        latest_event: latest_event,
        retry: retry_detail(group.root)
      }

      if event_opts.include_events? do
        {:ok, Map.put(detail, :events, events)}
      else
        {:ok, detail}
      end
    end
  end

  defp retry_detail(%RunState{} = run) do
    %{
      input_mode: metadata_value(run.metadata, :runtime_input_mode),
      next_retry_at: retry_datetime(metadata_value(run.metadata, :next_retry_at)),
      retrying?: metadata_value(run.metadata, :retrying) == true,
      nodes: retry_nodes(run.plan)
    }
  end

  defp retry_nodes(%Favn.Plan{nodes: nodes}) do
    nodes
    |> Map.values()
    |> Enum.map(fn node ->
      %{
        asset_ref: node.ref,
        policy: Map.get(node, :retry_policy) || Favn.Retry.Policy.default(),
        source: Map.get(node, :retry_policy_source) || :default
      }
    end)
    |> Enum.sort_by(& &1.asset_ref)
  end

  defp retry_nodes(_plan), do: []

  defp metadata_value(metadata, key) when is_map(metadata),
    do: Map.get(metadata, key, Map.get(metadata, Atom.to_string(key)))

  defp metadata_value(_metadata, _key), do: nil

  defp retry_datetime(value) when is_integer(value), do: DateTime.from_unix!(value, :millisecond)
  defp retry_datetime(value), do: value

  defp runtime_input_pins_for_read(run_id) do
    case Storage.list_runtime_input_pins(run_id) do
      {:ok, pins} -> {:ok, pins}
      {:error, :runtime_input_pins_not_supported} -> {:ok, []}
      {:error, _reason} = error -> error
    end
  end

  defp normalize_operator_event_opts(opts) do
    include = opts |> Keyword.get(:include, []) |> List.wrap()
    include_events? = :events in include or "events" in include

    limit =
      Keyword.get(opts, :event_limit, Keyword.get(opts, :limit, @operator_event_default_limit))

    after_global_sequence = Keyword.get(opts, :after_global_sequence)

    cond do
      not is_integer(limit) or limit <= 0 ->
        {:error, :invalid_opts}

      limit > @operator_event_max_limit ->
        {:error, :invalid_opts}

      Keyword.has_key?(opts, :after_sequence) ->
        {:error, :invalid_opts}

      not is_nil(after_global_sequence) and
          (not is_integer(after_global_sequence) or after_global_sequence < 0) ->
        {:error, :invalid_opts}

      true ->
        {:ok,
         %{
           include_events?: include_events?,
           limit: limit,
           after_global_sequence: after_global_sequence
         }}
    end
  end

  defp operator_events(_group_id, %{include_events?: false}), do: {:ok, []}

  defp operator_events(group_id, event_opts) do
    opts =
      [limit: event_opts.limit]
      |> maybe_put_event_opt(:after_global_sequence, event_opts.after_global_sequence)

    list_execution_group_events(group_id, opts)
  end

  defp latest_operator_event(group_id) do
    case list_execution_group_events(group_id, limit: 1, order: :desc) do
      {:ok, [event]} -> {:ok, event}
      {:ok, []} -> {:ok, nil}
      {:error, _reason} = error -> error
    end
  end

  defp maybe_put_event_opt(opts, _key, nil), do: opts
  defp maybe_put_event_opt(opts, key, value), do: Keyword.put(opts, key, value)

  defp execution_group_active?(group, windows, attempt_counts) do
    attempt_counts.running > 0 or attempt_counts.queued > 0 or
      Enum.any?(group.runs, &(with_public_status(&1).status in [:pending, :running])) or
      Enum.any?(windows, &(Map.get(&1, :status) in [:pending, :queued, :running]))
  end

  defp execution_group_status(root_status, attempt_counts, window_counts, active?) do
    cond do
      attempt_counts.failed > 0 or window_counts.failed > 0 -> :error
      active? -> :running
      root_status -> root_status
      true -> :pending
    end
  end

  defp execution_group_health(_status, failure_count, _active?) when failure_count > 0, do: :error
  defp execution_group_health(_status, _failure_count, true), do: :active
  defp execution_group_health(:partial, _failure_count, _active?), do: :warning
  defp execution_group_health(_status, _failure_count, _active?), do: :ok

  defp execution_group_progress(%{total: 0}), do: nil

  defp execution_group_progress(attempt_counts) do
    %{
      unit: :assets,
      label: "#{attempt_counts.completed} / #{attempt_counts.total} asset attempts",
      counts: attempt_counts
    }
  end

  defp target_assets(%RunState{target_refs: refs}) when is_list(refs) and refs != [],
    do: Enum.map(refs, &RunQuery.public_ref/1)

  defp target_assets(%RunState{asset_ref: ref}), do: [RunQuery.public_ref(ref)]

  defp trigger_type(%RunState{} = run), do: RunQuery.trigger_type(run)

  defp execution_group_asset_attempts(group, mode, events_by_run, windows) do
    windows_by_child_run_id =
      windows
      |> Map.new(fn window -> {Map.get(window, :child_run_id), window} end)

    Enum.flat_map(group.runs, fn run ->
      case classify(run) do
        :backfill_parent ->
          []

        _role ->
          events = Map.get(events_by_run, run.id, [])
          window = Map.get(windows_by_child_run_id, run.id)

          run
          |> StepProjection.build(events)
          |> Enum.map(&asset_attempt_summary(group.root.id, run, &1, window, mode))
      end
    end)
  end

  defp operator_step_events_by_run(group_id) do
    with {:ok, recent_events} <-
           Storage.list_execution_group_events(group_id,
             per_run_limit: @operator_step_event_limit
           ),
         {:ok, current_step_events} <-
           Storage.list_execution_group_events(group_id,
             event_types: @operator_step_state_event_types,
             latest_per_step: true
           ) do
      events =
        (recent_events ++ current_step_events)
        |> Enum.uniq_by(&{Map.get(&1, :run_id), Map.get(&1, :sequence)})
        |> Enum.sort_by(
          &{Map.get(&1, :global_sequence) || 0, Map.get(&1, :run_id) || "",
           Map.get(&1, :sequence) || 0}
        )

      {:ok, events |> Enum.map(&RunEvent.from_map/1) |> Enum.group_by(& &1.run_id)}
    end
  end

  defp asset_attempt_summary(root_id, %RunState{} = run, step, window_hint, mode) do
    window = step.window || window_hint || window(run, classify(run))
    status = attempt_status(step.status, window_hint, mode)

    %{
      id: step.id,
      root_execution_group_id: root_id,
      child_run_id: if(run.id == root_id, do: nil, else: run.id),
      run_id: run.id,
      status: status,
      asset_key: step.asset_ref,
      asset_ref: step.asset_ref,
      stage: step.stage,
      execution_pool: step.execution_pool,
      queue_reason: step.queue_reason,
      attempt_number: step.attempt,
      started_at: step.started_at,
      finished_at: step.finished_at,
      duration_ms: step.duration_ms || duration_ms(step.started_at, step.finished_at),
      error_summary: error_summary(step.error),
      output_metadata: step.output_metadata,
      window: window,
      window_start_at: window && window.start_at,
      window_end_at: window && window.end_at
    }
  end

  defp attempt_status(status, %{status: window_status}, :overview)
       when status in [:pending, nil] and window_status in [:running, :pending, :queued],
       do: ExecutionStatus.normalize(window_status)

  defp attempt_status(status, _window_hint, _mode), do: ExecutionStatus.normalize(status)

  defp execution_group_timing(group, attempts, windows, attempt_counts) do
    started_at =
      group.runs
      |> Enum.map(& &1.inserted_at)
      |> Kernel.++(Enum.map(attempts, & &1.started_at))
      |> Kernel.++(Enum.map(windows, &Map.get(&1, :started_at)))
      |> earliest_datetime()

    active? =
      attempt_counts.running > 0 or attempt_counts.queued > 0 or
        Enum.any?(group.runs, &(with_public_status(&1).status in [:pending, :running])) or
        Enum.any?(windows, &(Map.get(&1, :status) in [:pending, :queued, :running]))

    finished_at =
      if active? do
        nil
      else
        group.runs
        |> Enum.map(&(with_public_status(&1) |> finished_at()))
        |> Kernel.++(Enum.map(attempts, & &1.finished_at))
        |> Kernel.++(Enum.map(windows, &Map.get(&1, :finished_at)))
        |> latest_datetime()
      end

    %{
      started_at: started_at,
      finished_at: finished_at,
      duration_ms: duration_ms(started_at, finished_at)
    }
  end

  defp earliest_datetime(values), do: datetime_extreme(values, &(DateTime.compare(&1, &2) == :lt))
  defp latest_datetime(values), do: datetime_extreme(values, &(DateTime.compare(&1, &2) == :gt))

  defp datetime_extreme(values, compare_fun) do
    values
    |> Enum.reject(&is_nil/1)
    |> Enum.reduce(nil, fn
      %DateTime{} = value, nil ->
        value

      %DateTime{} = value, %DateTime{} = current ->
        if(compare_fun.(value, current), do: value, else: current)

      _value, current ->
        current
    end)
  end

  defp execution_group_window_summaries(_group, [_ | _] = backfill_windows),
    do: Enum.map(backfill_windows, &execution_window_summary/1)

  defp execution_group_window_summaries(%{runs: runs}, _backfill_windows),
    do: run_window_summaries(runs)

  defp run_window_summaries(runs) do
    runs
    |> Enum.map(fn run -> window(run, classify(run)) end)
    |> Enum.reject(&WindowSummary.empty?/1)
    |> Enum.uniq_by(&{&1.key, &1.start_at, &1.end_at})
    |> Enum.map(fn window ->
      Map.merge(window, %{
        status: nil,
        child_run_id: nil,
        attempt_count: nil,
        started_at: nil,
        finished_at: nil,
        duration_ms: nil
      })
    end)
  end

  defp execution_window_summary(%BackfillWindow{} = window) do
    public = WindowSummary.from_backfill(window)

    %{
      key: public.key,
      label: public.label,
      kind: public.kind,
      start_at: public.start_at,
      end_at: public.end_at,
      timezone: public.timezone,
      status: window.status,
      child_run_id: window.latest_attempt_run_id || window.child_run_id,
      attempt_count: window.attempt_count,
      started_at: window.started_at,
      finished_at: window.finished_at,
      duration_ms: duration_ms(window.started_at, window.finished_at)
    }
  end

  defp attempt_counts(attempts) do
    %{
      total: length(attempts),
      completed: Enum.count(attempts, &ExecutionStatus.terminal?(&1.status)),
      failed: Enum.count(attempts, &ExecutionStatus.failed?(&1.status)),
      running: Enum.count(attempts, &ExecutionStatus.running?(&1.status)),
      queued: Enum.count(attempts, &ExecutionStatus.queued?(&1.status))
    }
  end

  defp window_counts(windows) do
    %{
      total: length(windows),
      completed: Enum.count(windows, &ExecutionStatus.terminal?(&1.status)),
      failed: Enum.count(windows, &ExecutionStatus.failed?(&1.status))
    }
  end

  defp timeline_entries(attempts) do
    now = DateTime.utc_now()

    attempts
    |> Enum.map(fn attempt ->
      %{
        started_at: attempt.started_at,
        finished_at:
          if(ExecutionStatus.running?(attempt.status), do: now, else: attempt.finished_at),
        asset_key: attempt.asset_key,
        window: attempt.window,
        status: attempt.status,
        stage: attempt.stage,
        attempt_id: attempt.id,
        child_run_id: attempt.child_run_id,
        root_execution_group_id: attempt.root_execution_group_id
      }
    end)
    |> Enum.sort_by(&timeline_sort_key/1)
  end

  defp filter_asset_attempts(attempts, filters) do
    Enum.filter(attempts, fn attempt ->
      matches_filter?(attempt.status, Keyword.get(filters, :status)) and
        matches_filter?(attempt.asset_key, Keyword.get(filters, :target_asset)) and
        matches_window_range?(attempt, filters) and
        matches_attempt_only_filters?(attempt, filters)
    end)
  end

  defp matches_filter?(_value, nil), do: true
  defp matches_filter?(value, expected), do: value == expected

  defp matches_attempt_only_filters?(attempt, filters) do
    (not Keyword.get(filters, :only_failed, false) or ExecutionStatus.failed?(attempt.status)) and
      (not Keyword.get(filters, :only_running, false) or
         ExecutionStatus.running?(attempt.status)) and
      (not Keyword.get(filters, :only_incomplete, false) or
         not ExecutionStatus.terminal?(attempt.status))
  end

  defp matches_window_range?(attempt, filters) do
    {from, until} = window_range(filters)

    after_from? =
      is_nil(from) or is_nil(attempt.window_end_at) or
        DateTime.compare(attempt.window_end_at, from) == :gt

    before_until? =
      is_nil(until) or is_nil(attempt.window_start_at) or
        DateTime.compare(attempt.window_start_at, until) == :lt

    after_from? and before_until?
  end

  defp window_range(filters) do
    case Keyword.get(filters, :window_range) do
      {%DateTime{} = from, %DateTime{} = until} -> {from, until}
      _other -> {Keyword.get(filters, :window_start_at), Keyword.get(filters, :window_end_at)}
    end
  end

  defp error_summary(nil), do: nil

  defp error_summary(%{message: message}) when is_binary(message), do: message
  defp error_summary(%{"message" => message}) when is_binary(message), do: message
  defp error_summary(error) when is_binary(error), do: error
  defp error_summary(_error), do: "Failure details are available in server logs."

  defp timeline_sort_key(%{started_at: %DateTime{} = started_at}),
    do: DateTime.to_unix(started_at, :microsecond)

  defp timeline_sort_key(_entry), do: 0

  defp run_started_sort_key(%RunState{inserted_at: %DateTime{} = inserted_at}),
    do: DateTime.to_unix(inserted_at, :microsecond)

  defp run_started_sort_key(_run), do: 0

  defp with_public_status(%RunState{} = run) do
    %{run | status: public_status(run)}
  end

  defp public_status(%RunState{submit_kind: :pipeline, status: :ok} = run) do
    if incomplete_pipeline_success?(run), do: :running, else: :ok
  end

  defp public_status(%RunState{status: status}), do: status

  defp incomplete_pipeline_success?(%RunState{} = run) do
    StepProjection.incomplete?(run)
  end

  defp classify(%RunState{submit_kind: :rerun}), do: :rerun

  defp classify(%RunState{parent_run_id: parent_run_id}) when is_binary(parent_run_id),
    do: :backfill_child

  defp classify(%RunState{submit_kind: submit_kind})
       when submit_kind in [:backfill_asset, :backfill_pipeline],
       do: :backfill_parent

  defp classify(%RunState{submit_kind: :pipeline}), do: :pipeline
  defp classify(%RunState{}), do: :asset

  defp progress(%RunState{}, :backfill_parent, nil), do: nil

  defp progress(%RunState{}, :backfill_parent, []), do: nil

  defp progress(%RunState{}, :backfill_parent, windows) when is_list(windows) do
    window_progress(windows)
  end

  defp progress(%RunState{} = run, role, _backfill_windows) do
    step_progress = StepProjection.progress(run)
    unit = if(role in [:pipeline, :backfill_child], do: :steps, else: step_progress.unit)

    cond do
      step_progress.empty? and ExecutionStatus.active?(run.status) ->
        %{unit: unit, label: "Waiting", counts: %{total: step_progress.total, completed: 0}}

      step_progress.total == 0 ->
        nil

      true ->
        %{
          unit: unit,
          label:
            "#{step_progress.completed}/#{step_progress.total} #{unit_label(unit, step_progress.total)}",
          counts: %{total: step_progress.total, completed: step_progress.completed}
        }
    end
  end

  defp list_all_backfill_windows(backfill_run_id, cursor \\ nil, acc \\ []) do
    case Storage.scan_backfill_windows(
           [backfill_run_id: backfill_run_id],
           [{:limit, Page.max_limit()}, {:after, cursor}]
         ) do
      {:ok, %{items: items, has_more?: true, next_cursor: next_cursor}}
      when is_map(next_cursor) ->
        list_all_backfill_windows(backfill_run_id, next_cursor, prepend_page_items(items, acc))

      {:ok, %{items: items}} ->
        {:ok, Enum.reverse(prepend_page_items(items, acc))}

      {:error, _reason} = error ->
        error
    end
  end

  defp prepend_page_items(items, acc), do: Enum.reduce(items, acc, &[&1 | &2])

  defp detail_backfill_windows(%RunState{} = run) do
    case classify(run) do
      :backfill_parent ->
        list_all_backfill_windows(run.id)

      _role ->
        {:ok, nil}
    end
  end

  defp backfill_failures(%RunState{}, nil), do: {[], 0}

  defp backfill_failures(%RunState{}, windows) when is_list(windows) do
    failed = Enum.filter(windows, &failed_backfill_window?/1)

    failures =
      failed
      |> Enum.take(@backfill_failure_detail_limit)
      |> Enum.map(&backfill_failure/1)

    {failures, length(failed)}
  end

  defp failed_backfill_window?(%BackfillWindow{status: status}),
    do: status in [:error, :timed_out, :cancelled]

  defp backfill_failure(%BackfillWindow{} = window) do
    child_context = child_failure_context(window)
    error = child_context.error || window_error(window)

    %{
      child_run_id: window.latest_attempt_run_id || window.child_run_id,
      status: window.status,
      window: WindowSummary.from_backfill(window),
      asset_ref: child_context.asset_ref,
      error: error,
      attempt_count: window.attempt_count,
      started_at: window.started_at,
      finished_at: window.finished_at,
      duration_ms: duration_ms(window.started_at, window.finished_at)
    }
  end

  defp child_failure_context(%BackfillWindow{} = window) do
    case window.latest_attempt_run_id || window.child_run_id do
      run_id when is_binary(run_id) ->
        with {:ok, %RunState{} = child} <- Storage.get_run(run_id),
             {:ok, events} <- Storage.list_run_events(run_id) do
          child_failure_context(child, Enum.map(events, &RunEvent.from_map/1))
        else
          _other -> %{asset_ref: nil, error: nil}
        end

      _other ->
        %{asset_ref: nil, error: nil}
    end
  end

  defp child_failure_context(%RunState{} = child, events) do
    failed_step =
      child
      |> StepProjection.build(events)
      |> Enum.find(&(failure_step_status?(&1.status) and not is_nil(&1.error)))

    %{
      asset_ref: failed_step && failed_step.asset_ref,
      error: (failed_step && failed_step.error) || child.error
    }
  end

  defp failure_step_status?(status),
    do: status in [:error, :timed_out, :blocked, "error", "timed_out", "blocked"]

  defp window_error(%BackfillWindow{last_error: last_error}) when not is_nil(last_error),
    do: last_error

  defp window_error(%BackfillWindow{errors: errors}) when is_list(errors), do: List.last(errors)
  defp window_error(_window), do: nil

  defp duration_ms(%DateTime{} = started_at, %DateTime{} = finished_at),
    do: DateTime.diff(finished_at, started_at, :millisecond)

  defp duration_ms(_started_at, _finished_at), do: nil

  defp log_filter_has_entries?(%Filter{} = filter) do
    case Storage.list_logs(filter, limit: 1) do
      {:ok, %{items: [_entry | _rest]}} -> {:ok, true}
      {:ok, %{items: []}} -> {:ok, false}
      {:error, _reason} = error -> error
    end
  end

  defp asset_step_log_filter(_run_id, _step, exact_filter, true), do: {exact_filter, false}

  defp asset_step_log_filter(run_id, %{canonical_asset_ref: asset_ref}, _exact_filter, false)
       when is_tuple(asset_ref) do
    {%Filter{run_id: run_id, asset_ref: asset_ref}, true}
  end

  defp asset_step_log_filter(_run_id, _step, exact_filter, false), do: {exact_filter, false}

  defp asset_step_log_context(detail, step, asset_step_id, log_filter, fallback?) do
    %{
      run: detail.summary,
      step: step,
      title: (step && step.asset_ref) || "Asset logs",
      subtitle: "Run #{short_id(detail.summary.id)} · Asset step #{asset_step_id}",
      status: step && step.status,
      facts: step_facts(step),
      log_filter: log_filter,
      fallback?: fallback?,
      note: asset_step_log_note(step, fallback?)
    }
  end

  defp asset_step_log_note(_step, true) do
    "Exact asset-step logs were not found; showing run logs for this asset instead."
  end

  defp asset_step_log_note(nil, false), do: "Asset step context not found, showing matching logs."
  defp asset_step_log_note(_step, false), do: nil

  defp step_facts(nil), do: []

  defp step_facts(step) do
    [
      %{label: "Started", value: step.started_at},
      %{label: "Duration", value: step.duration_ms},
      %{label: "Attempt", value: step.attempt}
    ]
  end

  defp window_progress(windows) do
    counts = %{
      total: length(windows),
      pending: count_status(windows, [:pending]),
      running: count_status(windows, [:running]),
      succeeded: count_status(windows, [:ok, :partial]),
      failed: count_status(windows, [:error]),
      cancelled: count_status(windows, [:cancelled]),
      timed_out: count_status(windows, [:timed_out])
    }

    completed = counts.succeeded + counts.failed + counts.cancelled + counts.timed_out
    counts = Map.put(counts, :completed, completed)

    %{
      unit: :windows,
      label: "#{completed}/#{counts.total} windows complete",
      counts: counts
    }
  end

  defp count_status(windows, statuses) do
    Enum.count(windows, &(&1.status in statuses))
  end

  defp unit_label(:assets, 1), do: "asset"
  defp unit_label(:assets, _total), do: "assets"
  defp unit_label(:steps, 1), do: "step"
  defp unit_label(:steps, _total), do: "steps"

  defp window(%RunState{} = run, :backfill_child) do
    WindowSummary.from_run(run) || backfill_child_window(run)
  end

  defp window(%RunState{} = run, _role), do: WindowSummary.from_run(run)

  defp backfill_child_window(%RunState{parent_run_id: parent_run_id, trigger: trigger})
       when is_binary(parent_run_id) and is_map(trigger) do
    with pipeline_module when is_atom(pipeline_module) <- pipeline_module(trigger),
         window_key when is_binary(window_key) <-
           Map.get(trigger, :window_key) || Map.get(trigger, "window_key"),
         {:ok, %BackfillWindow{} = window} <-
           Storage.get_backfill_window(parent_run_id, pipeline_module, window_key) do
      WindowSummary.from_backfill(window)
    else
      _other -> nil
    end
  end

  defp backfill_child_window(_run), do: nil

  defp pipeline_module(trigger) do
    Map.get(trigger, :pipeline_module) || Map.get(trigger, "pipeline_module")
  end

  defp short_id(id) when is_binary(id) and byte_size(id) > 18 do
    binary_part(id, 0, 9) <> "..." <> binary_part(id, byte_size(id) - 6, 6)
  end

  defp short_id(id) when is_binary(id), do: id

  defp finished_at(%RunState{status: status, updated_at: updated_at})
       when status in [:ok, :partial, :error, :cancelled, :timed_out],
       do: updated_at

  defp finished_at(_run), do: nil

  defp duration_ms(%RunState{inserted_at: %DateTime{} = started_at} = run) do
    case finished_at(run) do
      %DateTime{} = finished_at -> DateTime.diff(finished_at, started_at, :millisecond)
      _other -> nil
    end
  end

  defp duration_ms(_run), do: nil
end
