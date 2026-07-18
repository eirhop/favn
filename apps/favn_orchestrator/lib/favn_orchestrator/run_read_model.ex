defmodule FavnOrchestrator.RunReadModel do
  @moduledoc """
  Public orchestrator-owned read models for persisted runs.

  This module turns internal run snapshots and operational backfill ledgers into
  boring maps for thin callers. Callers should use the `FavnOrchestrator`
  facade functions rather than reaching into storage or run snapshot structs.
  """

  alias Favn.Log.Filter
  alias Favn.RuntimeInput.Pin
  alias FavnOrchestrator.Backfills
  alias FavnOrchestrator.ExecutionStatus
  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.Queries.GetExecutionGroup
  alias FavnOrchestrator.Persistence.Results.Backfill, as: PersistedBackfill
  alias FavnOrchestrator.Persistence.Results.BackfillWindow, as: PersistedBackfillWindow
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.RunEvent
  alias FavnOrchestrator.RunReadModel.StepProjection
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Runs
  alias FavnOrchestrator.Storage.RunQuery
  alias FavnOrchestrator.WindowSummary

  @backfill_failure_detail_limit 10
  @operator_step_event_limit 200
  @operator_snapshot_run_limit 4
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

  @doc "Returns one public run detail under an explicit workspace authority."
  @spec get_run_detail(WorkspaceContext.t(), String.t()) ::
          {:ok, run_detail()} | {:error, term()}
  def get_run_detail(%WorkspaceContext{} = context, run_id) when is_binary(run_id) do
    with {:ok, %RunState{} = run} <- Runs.get(context, run_id),
         {:ok, event_page} <- Runs.page_events(context, run_id, limit: 200),
         events <- Enum.map(event_page.items, &RunEvent.from_map/1),
         {:ok, events} <- ensure_retry_checkpoint_events(context, [run], events),
         {:ok, runtime_input_pins} <- Runs.get_runtime_inputs(context, run_id) do
      public_run = with_public_status(run)

      {:ok,
       %{
         summary: summary(public_run),
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
         backfill_failures: [],
         backfill_failure_count: 0,
         events: events
       }}
    end
  end

  @doc "Returns bounded operator run detail under an explicit workspace authority."
  @spec get_operator_run_detail(WorkspaceContext.t(), String.t(), keyword()) ::
          {:ok, operator_run_detail()} | {:error, term()}
  def get_operator_run_detail(%WorkspaceContext{} = context, run_id, opts)
      when is_binary(run_id) and is_list(opts) do
    with {:ok, event_opts} <- normalize_operator_event_opts(opts),
         {:ok, selected_run} <- Runs.get(context, run_id),
         root_run_id <- selected_run.root_run_id || selected_run.id,
         {:ok, root} <- load_root_run(context, selected_run, root_run_id),
         {:ok, group_page} <-
           Runs.page(
             context,
             root_execution_group_id: root_run_id,
             limit: @operator_snapshot_run_limit
           ),
         {:ok, projection} <-
           Persistence.stores().operator_reads.get_execution_group(%GetExecutionGroup{
             workspace_context: context,
             root_run_id: root_run_id,
             detail_limit: 200
           }),
         {:ok, backfill} <- persisted_backfill(context, root),
         {:ok, event_page} <-
           Runs.page_group_events(context, root_run_id,
             order: :desc,
             limit: @operator_step_event_limit
           ) do
      children =
        [selected_run | group_page.items]
        |> Enum.uniq_by(& &1.id)
        |> Enum.reject(&(&1.id == root.id))
        |> Enum.sort_by(&run_started_sort_key/1)

      events = event_page.items |> Enum.map(&RunEvent.from_map/1) |> Enum.reverse()

      with {:ok, events} <- ensure_retry_checkpoint_events(context, [root | children], events) do
        operator_run_detail_v2(
          %{
            id: root_run_id,
            root: root,
            children: children,
            runs: [root | children],
            runs_truncated?: group_page.has_more?
          },
          projection,
          backfill,
          events,
          event_opts
        )
      end
    end
  end

  @doc "Returns public asset-step log context under an explicit workspace authority."
  @spec get_asset_step_log_context(WorkspaceContext.t(), String.t(), String.t()) ::
          {:ok, asset_step_log_context()} | {:error, term()}
  def get_asset_step_log_context(%WorkspaceContext{} = context, run_id, asset_step_id)
      when is_binary(run_id) and is_binary(asset_step_id) do
    with {:ok, detail} <- get_run_detail(context, run_id) do
      step = Enum.find(detail.steps, &(&1.id == asset_step_id))
      filter = %Filter{run_id: run_id, asset_step_id: asset_step_id}
      {:ok, asset_step_log_context(detail, step, asset_step_id, filter, false)}
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

  defp operator_run_detail_v2(group, projection, backfill, events, event_opts) do
    windows = Enum.map(projection.windows.items, &persisted_window_summary/1)
    events_by_run = Enum.group_by(events, & &1.run_id)
    attempts = execution_group_asset_attempts(group, :operator, events_by_run, windows)
    root_steps = StepProjection.build(group.root, Map.get(events_by_run, group.root.id, []))

    summary =
      group
      |> Map.merge(%{attempts: attempts, windows: windows})
      |> execution_group_summary()
      |> merge_persisted_overview(projection.overview, backfill, attempts)

    {backfill_failures, backfill_failure_count} = persisted_backfill_failures(backfill)
    latest_event = List.last(events)

    detail = %{
      summary: summary,
      root_run: summary(group.root),
      child_runs: Enum.map(group.children, &summary/1),
      child_run_details_truncated?: group.runs_truncated?,
      windows: windows,
      asset_attempts: attempts,
      timeline: timeline_entries(attempts),
      steps: root_steps,
      progress: summary.progress,
      counts: summary.summary_totals,
      backfill_failures: backfill_failures,
      backfill_failure_count: backfill_failure_count,
      root_event_sequence: group.root.event_seq,
      latest_global_event_sequence: latest_event && latest_event.global_sequence,
      latest_event: latest_event,
      retry: retry_detail(group.root)
    }

    if event_opts.include_events? do
      {:ok, Map.put(detail, :events, bounded_operator_events(events, event_opts))}
    else
      {:ok, detail}
    end
  end

  defp load_root_run(_context, %RunState{id: root_run_id} = run, root_run_id), do: {:ok, run}
  defp load_root_run(context, _selected_run, root_run_id), do: Runs.get(context, root_run_id)

  defp ensure_retry_checkpoint_events(context, runs, events) do
    runs
    |> Enum.reduce_while({:ok, events}, fn run, {:ok, acc} ->
      case retry_checkpoint_sequence(run) do
        nil ->
          {:cont, {:ok, acc}}

        sequence ->
          if Enum.any?(acc, &(&1.run_id == run.id and &1.sequence == sequence)) do
            {:cont, {:ok, acc}}
          else
            case fetch_retry_checkpoint_event(context, run.id, sequence) do
              {:ok, nil} -> {:cont, {:ok, acc}}
              {:ok, event} -> {:cont, {:ok, merge_checkpoint_event(acc, event)}}
              {:error, _reason} = error -> {:halt, error}
            end
          end
      end
    end)
  end

  defp retry_checkpoint_sequence(%RunState{plan: %Favn.Plan{}, metadata: metadata})
       when is_map(metadata) do
    retry_state = metadata_value(metadata, :retry_state)

    if metadata_value(metadata, :retrying) == true and is_map(retry_state) and
         metadata_value(retry_state, :kind) in [:pipeline, "pipeline"] do
      case metadata_value(retry_state, :checkpoint_sequence) do
        sequence when is_integer(sequence) and sequence > 0 -> sequence
        _invalid -> nil
      end
    end
  end

  defp retry_checkpoint_sequence(%RunState{}), do: nil

  defp fetch_retry_checkpoint_event(context, run_id, sequence) do
    with {:ok, page} <-
           Runs.page_events(context, run_id,
             after_sequence: sequence - 1,
             event_types: [:pipeline_retry_checkpointed],
             limit: 1
           ) do
      case page.items do
        [event] ->
          decoded = RunEvent.from_map(event)
          if decoded.sequence == sequence, do: {:ok, decoded}, else: {:ok, nil}

        [] ->
          {:ok, nil}
      end
    end
  end

  defp merge_checkpoint_event(events, checkpoint) do
    events
    |> Kernel.++([checkpoint])
    |> Enum.uniq_by(&{&1.run_id, &1.sequence})
    |> Enum.sort_by(&{&1.global_sequence || 0, &1.run_id, &1.sequence})
  end

  defp persisted_backfill(context, root) do
    case metadata_value(root.metadata, :backfill_id) do
      backfill_id when is_binary(backfill_id) and backfill_id != "" ->
        with {:ok, header} <- Backfills.get(context, backfill_id),
             {:ok, failures} <-
               Backfills.page_windows(context, backfill_id,
                 status: :failed,
                 limit: @backfill_failure_detail_limit
               ) do
          {:ok, %{header: header, failures: failures.items}}
        end

      _no_backfill ->
        {:ok, nil}
    end
  end

  defp persisted_window_summary(%PersistedBackfillWindow{} = window) do
    kind = persisted_window_kind(window.payload)
    timezone = persisted_window_value(window.payload, "timezone")

    public =
      WindowSummary.public(%{
        key: window.window_key,
        kind: kind,
        start_at: window.window_start,
        end_at: window.window_end,
        timezone: timezone
      })

    Map.merge(public, %{
      status: persisted_window_status(window.status),
      child_run_id: window.run_id,
      attempt_count: window.attempt_count,
      started_at: nil,
      finished_at: nil,
      duration_ms: nil
    })
  end

  defp persisted_window_kind(payload) do
    case persisted_window_value(payload, "kind") do
      "hour" -> :hour
      "day" -> :day
      "month" -> :month
      "year" -> :year
      _unknown -> nil
    end
  end

  defp persisted_window_value(payload, key) when is_map(payload),
    do: Map.get(payload, key) || Map.get(payload, String.to_existing_atom(key))

  defp persisted_window_value(_payload, _key), do: nil

  defp persisted_window_status(:succeeded), do: :ok
  defp persisted_window_status(:failed), do: :error
  defp persisted_window_status(:claimed), do: :queued
  defp persisted_window_status(:ready), do: :pending
  defp persisted_window_status(:planned), do: :pending
  defp persisted_window_status(status), do: status

  defp merge_persisted_overview(summary, overview, backfill, attempts) do
    root_terminal_offset = if summary.root_status in [:pending, :running], do: 0, else: 1
    total_attempts = max(length(attempts), max(overview.run_count - 1, 0))
    succeeded = max(overview.succeeded_count - root_terminal_offset, 0)
    failed = overview.failed_count
    running = overview.running_count
    queued = overview.pending_count
    completed = min(total_attempts, succeeded + failed)
    window_counts = persisted_window_counts(backfill)
    active? = overview.status in [:pending, :running]
    status = persisted_group_status(overview.status)
    progress = persisted_group_progress(backfill, total_attempts, completed)

    summary
    |> Map.merge(%{
      status: status,
      health: execution_group_health(status, failed + window_counts.failed, active?),
      active?: active?,
      root_status: status,
      finished_at: if(active?, do: nil, else: summary.finished_at),
      total_windows: window_counts.total,
      completed_windows: window_counts.completed,
      failed_windows: window_counts.failed,
      total_asset_attempts: total_attempts,
      completed_asset_attempts: completed,
      failed_asset_attempts: failed,
      running_asset_attempts: running,
      queued_asset_attempts: queued,
      failure_count: failed + window_counts.failed,
      progress: progress,
      summary_totals: %{
        windows: window_counts,
        asset_attempts: %{
          total: total_attempts,
          completed: completed,
          failed: failed,
          running: running,
          queued: queued
        }
      },
      last_activity_at: overview.updated_at
    })
  end

  defp persisted_group_status(:succeeded), do: :ok
  defp persisted_group_status(:failed), do: :error
  defp persisted_group_status(status), do: status

  defp persisted_window_counts(%{header: %PersistedBackfill{} = header}) do
    progress = header.progress || %{}
    failed = Map.get(progress, :failed_count, Map.get(progress, "failed_count", 0))
    cancelled = Map.get(progress, :cancelled_count, Map.get(progress, "cancelled_count", 0))
    succeeded = Map.get(progress, :succeeded_count, Map.get(progress, "succeeded_count", 0))

    %{
      total: header.expected_window_count,
      completed: succeeded + failed + cancelled,
      failed: failed
    }
  end

  defp persisted_window_counts(_backfill), do: %{total: 0, completed: 0, failed: 0}

  defp persisted_group_progress(%{header: %PersistedBackfill{}} = backfill, _total, _completed) do
    counts = persisted_window_counts(backfill)

    %{
      unit: :windows,
      label: "#{counts.completed}/#{counts.total} windows complete",
      counts: counts
    }
  end

  defp persisted_group_progress(_backfill, 0, _completed), do: nil

  defp persisted_group_progress(_backfill, total, completed) do
    %{
      unit: :assets,
      label: "#{completed} / #{total} asset attempts",
      counts: %{total: total, completed: completed}
    }
  end

  defp persisted_backfill_failures(%{header: header, failures: failures}) do
    details = Enum.map(failures, &persisted_backfill_failure/1)
    progress = header.progress || %{}
    count = Map.get(progress, :failed_count, Map.get(progress, "failed_count", length(details)))
    {details, count}
  end

  defp persisted_backfill_failures(_backfill), do: {[], 0}

  defp persisted_backfill_failure(%PersistedBackfillWindow{} = window) do
    %{
      child_run_id: window.run_id,
      status: persisted_window_status(window.status),
      window:
        persisted_window_summary(window)
        |> Map.take([:key, :label, :kind, :start_at, :end_at, :timezone]),
      asset_ref: nil,
      error: window.last_error,
      attempt_count: window.attempt_count,
      started_at: nil,
      finished_at: nil,
      duration_ms: nil
    }
  end

  defp bounded_operator_events(events, event_opts) do
    events
    |> Enum.filter(fn event ->
      is_nil(event_opts.after_global_sequence) or
        (is_integer(event.global_sequence) and
           event.global_sequence > event_opts.after_global_sequence)
    end)
    |> Enum.take(-event_opts.limit)
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

  defp window(%RunState{} = run, _role), do: WindowSummary.from_run(run)

  defp short_id(id) when is_binary(id) and byte_size(id) > 18 do
    binary_part(id, 0, 9) <> "..." <> binary_part(id, byte_size(id) - 6, 6)
  end

  defp short_id(id) when is_binary(id), do: id

  defp duration_ms(%DateTime{} = started_at, %DateTime{} = finished_at),
    do: max(DateTime.diff(finished_at, started_at, :millisecond), 0)

  defp duration_ms(_started_at, _finished_at), do: nil

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
