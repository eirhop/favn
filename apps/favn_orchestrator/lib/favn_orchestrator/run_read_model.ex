defmodule FavnOrchestrator.RunReadModel do
  @moduledoc """
  Public orchestrator-owned read models for persisted runs.

  This module turns internal run snapshots and operational backfill ledgers into
  boring maps for thin callers. Callers should use the `FavnOrchestrator`
  facade functions rather than reaching into storage or run snapshot structs.
  """

  alias Favn.Log.Filter
  alias Favn.Window.Anchor
  alias FavnOrchestrator.AssetStepIdentity
  alias FavnOrchestrator.Backfill.BackfillWindow
  alias FavnOrchestrator.Page
  alias FavnOrchestrator.RunEvent
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage
  alias FavnOrchestrator.Storage.JsonSafe
  alias FavnOrchestrator.Storage.RunQuery

  @backfill_failure_detail_limit 10
  @execution_group_overview_default_scan_limit 500
  @execution_group_overview_max_scan_limit 2_000
  @operator_step_event_limit 200
  @operator_event_default_limit 50
  @operator_event_max_limit 500

  @type run_role :: :asset | :pipeline | :backfill_parent | :backfill_child | :rerun

  @type window_summary :: %{
          required(:key) => String.t() | nil,
          required(:label) => String.t() | nil,
          required(:kind) => atom() | nil,
          required(:start_at) => DateTime.t() | nil,
          required(:end_at) => DateTime.t() | nil,
          required(:timezone) => String.t() | nil
        }

  @type progress_summary :: %{
          required(:unit) => :assets | :steps | :windows,
          required(:label) => String.t(),
          required(:counts) => map()
        }

  @type step_summary :: %{
          required(:id) => String.t(),
          required(:node_key) => Favn.Plan.node_key() | nil,
          required(:asset_ref) => String.t(),
          required(:canonical_asset_ref) => Favn.Ref.t() | nil,
          required(:status) => atom() | nil,
          required(:stage) => non_neg_integer() | nil,
          required(:execution_pool) => atom() | String.t() | nil,
          required(:queue_reason) => atom() | String.t() | nil,
          required(:window) => window_summary() | nil,
          required(:duration_ms) => non_neg_integer() | nil,
          required(:started_at) => DateTime.t() | nil,
          required(:finished_at) => DateTime.t() | nil,
          required(:attempt) => non_neg_integer() | nil,
          required(:error) => term(),
          required(:output) => term(),
          required(:output_metadata) => map() | nil,
          required(:explanation) => String.t() | nil,
          required(:failure_role) => :primary | :cascade | nil,
          required(:root_failure_asset_ref) => String.t() | nil
        }

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
          required(:events) => [RunEvent.t()]
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
  Lists public run summaries from persisted run snapshots.
  """
  @spec list_run_summaries(keyword()) :: {:ok, [run_summary()]} | {:error, term()}
  def list_run_summaries(opts \\ []) when is_list(opts) do
    with {:ok, runs} <- Storage.list_runs(opts) do
      {:ok, Enum.map(runs, &summary/1)}
    end
  end

  @doc """
  Returns one public run detail from a persisted run snapshot.
  """
  @spec get_run_detail(String.t()) :: {:ok, run_detail()} | {:error, term()}
  def get_run_detail(run_id) when is_binary(run_id) do
    with {:ok, %RunState{} = run} <- Storage.get_run(run_id),
         {:ok, events} <- Storage.list_run_events(run_id) do
      events = Enum.map(events, &RunEvent.from_map/1)
      public_run = with_public_status(run)
      backfill_windows = detail_backfill_windows(run)
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
         steps: step_summaries(run, events),
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
    case Storage.list_execution_groups(normalize_execution_group_filters(filters)) do
      {:ok, %Page{} = page} ->
        hydrate_execution_group_page(page)

      {:error, :execution_group_reads_not_supported} ->
        page_execution_groups_from_run_scan(filters)

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
    with {:ok, group} <- load_execution_group(group_id) do
      attempts =
        group |> execution_group_asset_attempts(:detail) |> filter_asset_attempts(filters)

      windows = execution_group_window_summaries(group)

      {:ok,
       %{
         summary:
           execution_group_summary(
             Map.merge(group, %{attempts: attempts, windows: windows}),
             :detail
           ),
         root_run: summary(group.root),
         child_runs: Enum.map(group.children, &summary/1),
         windows: windows,
         asset_attempts: attempts,
         timeline: timeline_entries(attempts),
         events: execution_group_events(group)
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
    case Storage.list_execution_group_events(group_id, filters) do
      {:ok, events} ->
        {:ok, Enum.map(events, &RunEvent.from_map/1)}

      {:error, :execution_group_reads_not_supported} ->
        with {:ok, group} <- load_execution_group(group_id) do
          {:ok, execution_group_events(group)}
        end

      {:error, _reason} = error ->
        error
    end
  end

  @doc """
  Lists asset attempts for one execution group.
  """
  @spec list_execution_group_asset_attempts(String.t(), keyword()) ::
          {:ok, [asset_attempt_summary()]} | {:error, term()}
  def list_execution_group_asset_attempts(group_id, filters \\ [])
      when is_binary(group_id) and is_list(filters) do
    with {:ok, group} <- load_execution_group(group_id) do
      attempts =
        group
        |> execution_group_asset_attempts(:operator)
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
    with {:ok, group} <- load_execution_group(group_id) do
      {:ok, execution_group_window_summaries(group)}
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

  @spec summary(RunState.t()) :: run_summary()
  def summary(%RunState{} = run) do
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

  defp execution_groups(runs) when is_list(runs) do
    runs_by_id = Map.new(runs, &{&1.id, &1})

    runs
    |> Enum.group_by(&execution_group_id/1)
    |> Enum.map(fn {group_id, group_runs} ->
      root = Map.get(runs_by_id, group_id) || Enum.min_by(group_runs, &run_started_sort_key/1)

      children =
        group_runs
        |> Enum.reject(&(&1.id == root.id))
        |> Enum.sort_by(&run_started_sort_key/1)

      %{id: root.id, root: root, children: children, runs: [root | children]}
    end)
    |> Enum.sort_by(&group_run_activity_sort_key/1, :desc)
  end

  defp hydrate_execution_group_page(%Page{} = page) do
    page.items
    |> Enum.reduce_while({:ok, []}, fn group_id, {:ok, acc} ->
      case get_execution_group_summary(group_id) do
        {:ok, summary} -> {:cont, {:ok, [summary | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, summaries} -> {:ok, %{page | items: Enum.reverse(summaries)}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp page_execution_groups_from_run_scan(filters) do
    with {:ok, runs} <- Storage.list_runs(limit: execution_group_scan_limit(filters)),
         {:ok, page_opts} <- Page.normalize_opts(filters) do
      runs = include_missing_execution_group_roots(runs)

      page =
        runs
        |> execution_groups()
        |> Enum.map(&execution_group_summary(&1, :overview))
        |> filter_execution_group_summaries(filters)
        |> Enum.drop(Keyword.fetch!(page_opts, :offset))
        |> Enum.take(Keyword.fetch!(page_opts, :limit) + 1)
        |> Page.from_fetched(page_opts)

      {:ok, page}
    end
  end

  defp get_execution_group_summary(group_id) do
    with {:ok, group} <- load_execution_group(group_id) do
      {:ok, execution_group_summary(group, :overview)}
    end
  end

  defp load_execution_group(group_id) do
    case Storage.list_execution_group_runs(group_id) do
      {:ok, [_ | _] = runs} ->
        find_execution_group(runs, group_id)

      {:ok, []} ->
        {:error, :not_found}

      {:error, :execution_group_reads_not_supported} ->
        with {:ok, runs} <- Storage.list_runs() do
          find_execution_group(runs, group_id)
        end

      {:error, _reason} = error ->
        error
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
    try do
      String.to_existing_atom(value)
    rescue
      ArgumentError -> value
    end
  end

  defp normalize_existing_atom(value), do: value

  defp include_missing_execution_group_roots(runs) do
    runs_by_id = Map.new(runs, &{&1.id, &1})

    missing_root_ids =
      runs
      |> Enum.map(&execution_group_id/1)
      |> Enum.reject(&Map.has_key?(runs_by_id, &1))
      |> Enum.uniq()

    roots =
      missing_root_ids
      |> Enum.flat_map(fn root_id ->
        case Storage.get_run(root_id) do
          {:ok, %RunState{} = root} -> [root]
          {:error, _reason} -> []
        end
      end)

    (roots ++ runs)
    |> Enum.uniq_by(& &1.id)
  end

  defp execution_group_id(%RunState{root_run_id: root_run_id}) when is_binary(root_run_id),
    do: root_run_id

  defp execution_group_id(%RunState{parent_run_id: parent_run_id}) when is_binary(parent_run_id),
    do: parent_run_id

  defp execution_group_id(%RunState{id: id}), do: id

  defp find_execution_group(runs, group_id) do
    runs
    |> execution_groups()
    |> Enum.find(&(&1.id == group_id))
    |> case do
      nil -> {:error, :not_found}
      group -> {:ok, group}
    end
  end

  defp execution_group_summary(group, mode) do
    attempts = Map.get(group, :attempts) || execution_group_asset_attempts(group, mode)
    windows = Map.get(group, :windows) || execution_group_window_summaries(group)
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
      currently_running_asset_attempts: Enum.filter(attempts, &running_status?(&1.status)),
      child_run_ids: Enum.map(group.children, & &1.id)
    }
  end

  defp operator_run_detail(group, event_opts) do
    attempts = execution_group_asset_attempts(group, :operator)
    windows = execution_group_window_summaries(group)
    root_events = run_events(group.root.id, :operator)
    root_steps = step_summaries(group.root, root_events)

    group_summary =
      execution_group_summary(Map.merge(group, %{attempts: attempts, windows: windows}), :detail)

    {backfill_failures, backfill_failure_count} =
      backfill_failures(group.root, detail_backfill_windows(group.root))

    events = operator_events(group.id, event_opts)
    latest_event = latest_operator_event(group.id)

    detail = %{
      summary: group_summary,
      root_run: summary(group.root),
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
      latest_event: latest_event
    }

    if event_opts.include_events? do
      {:ok, Map.put(detail, :events, events)}
    else
      {:ok, detail}
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

  defp operator_events(_group_id, %{include_events?: false}), do: []

  defp operator_events(group_id, event_opts) do
    opts =
      [limit: event_opts.limit]
      |> maybe_put_event_opt(:after_global_sequence, event_opts.after_global_sequence)

    case list_execution_group_events(group_id, opts) do
      {:ok, events} -> events
      {:error, _reason} -> []
    end
  end

  defp latest_operator_event(group_id) do
    case list_execution_group_events(group_id, limit: 1, order: :desc) do
      {:ok, [event]} -> event
      {:ok, []} -> nil
      {:error, _reason} -> nil
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
    do: Enum.map(refs, &public_ref/1)

  defp target_assets(%RunState{asset_ref: ref}), do: [public_ref(ref)]

  defp trigger_type(%RunState{} = run), do: RunQuery.trigger_type(run)

  defp execution_group_asset_attempts(group, mode) do
    windows_by_child_run_id =
      group
      |> execution_group_window_summaries()
      |> Map.new(fn window -> {Map.get(window, :child_run_id), window} end)

    Enum.flat_map(group.runs, fn run ->
      case classify(run) do
        :backfill_parent ->
          []

        _role ->
          events = if(mode in [:detail, :operator], do: run_events(run.id, mode), else: [])
          window = Map.get(windows_by_child_run_id, run.id)

          run
          |> step_summaries(events)
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
       do: status_name(window_status)

  defp attempt_status(status, _window_hint, _mode), do: status_name(status)

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

  defp execution_group_window_summaries(%{root: root, runs: runs}) do
    case list_all_backfill_windows(root.id) do
      {:ok, [_ | _] = windows} -> Enum.map(windows, &execution_window_summary/1)
      _other -> run_window_summaries(runs)
    end
  end

  defp run_window_summaries(runs) do
    runs
    |> Enum.map(fn run -> window(run, classify(run)) end)
    |> Enum.reject(&empty_window?/1)
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
    public = backfill_window(window)

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
      completed: Enum.count(attempts, &terminal_status?(&1.status)),
      failed: Enum.count(attempts, &failed_status?(&1.status)),
      running: Enum.count(attempts, &running_status?(&1.status)),
      queued: Enum.count(attempts, &queued_status?(&1.status))
    }
  end

  defp window_counts(windows) do
    %{
      total: length(windows),
      completed: Enum.count(windows, &terminal_status?(&1.status)),
      failed: Enum.count(windows, &failed_status?(&1.status))
    }
  end

  defp timeline_entries(attempts) do
    now = DateTime.utc_now()

    attempts
    |> Enum.map(fn attempt ->
      %{
        started_at: attempt.started_at,
        finished_at: if(running_status?(attempt.status), do: now, else: attempt.finished_at),
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

  defp filter_execution_group_summaries(groups, filters) do
    Enum.filter(groups, fn group ->
      matches_filter?(group.root_status, Keyword.get(filters, :status)) and
        matches_filter?(group.trigger_type, Keyword.get(filters, :trigger_type)) and
        matches_target_asset?(group, Keyword.get(filters, :target_asset)) and
        matches_group_only_filters?(group, filters)
    end)
  end

  defp execution_group_scan_limit(filters) do
    case Keyword.get(filters, :limit) do
      limit when is_integer(limit) and limit > 0 ->
        limit
        |> Kernel.*(5)
        |> max(limit)
        |> min(@execution_group_overview_max_scan_limit)

      _other ->
        @execution_group_overview_default_scan_limit
    end
  end

  defp group_run_activity_sort_key(group) do
    group.runs
    |> Enum.flat_map(fn run -> [run.updated_at, run.inserted_at] end)
    |> latest_datetime()
    |> datetime_sort_key()
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

  defp matches_target_asset?(_group, nil), do: true
  defp matches_target_asset?(group, asset), do: asset in group.target_assets

  defp matches_group_only_filters?(group, filters) do
    (not Keyword.get(filters, :only_failed, false) or group.failed_asset_attempts > 0 or
       group.failed_windows > 0) and
      (not Keyword.get(filters, :only_running, false) or group.running_asset_attempts > 0 or
         group.root_status in [:pending, :running]) and
      (not Keyword.get(filters, :only_incomplete, false) or incomplete_group?(group))
  end

  defp matches_attempt_only_filters?(attempt, filters) do
    (not Keyword.get(filters, :only_failed, false) or failed_status?(attempt.status)) and
      (not Keyword.get(filters, :only_running, false) or running_status?(attempt.status)) and
      (not Keyword.get(filters, :only_incomplete, false) or not terminal_status?(attempt.status))
  end

  defp incomplete_group?(group) do
    group.root_status in [:pending, :running] or
      group.running_asset_attempts > 0 or group.queued_asset_attempts > 0
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

  defp run_events(run_id, mode \\ :detail)

  defp run_events(run_id, :operator) do
    case Storage.list_run_events(run_id, limit: @operator_step_event_limit, order: :desc) do
      {:ok, events} -> events |> Enum.map(&RunEvent.from_map/1) |> Enum.reverse()
      {:error, _reason} -> []
    end
  end

  defp run_events(run_id, _mode) do
    case Storage.list_run_events(run_id) do
      {:ok, events} -> Enum.map(events, &RunEvent.from_map/1)
      {:error, _reason} -> []
    end
  end

  defp execution_group_events(group) do
    group.runs
    |> Enum.flat_map(&run_events(&1.id))
    |> Enum.sort_by(&event_sort_key/1)
  end

  defp event_sort_key(%RunEvent{} = event) do
    {datetime_sort_key(event.occurred_at), event.run_id || "", event.sequence || 0}
  end

  defp event_step_finished_at(latest, events) do
    cond do
      event_step_status(latest.event_type, latest.status) |> terminal_status?() ->
        latest.occurred_at

      true ->
        events |> Enum.find(&terminal_step_event?/1) |> then(&(&1 && &1.occurred_at))
    end
  end

  defp event_window(events) do
    events
    |> Enum.find_value(fn event ->
      data = event.data || %{}
      window = public_window(Map.get(data, :window) || Map.get(data, "window") || %{})
      if empty_window?(window), do: nil, else: window
    end)
  end

  defp terminal_step_event?(event) do
    event_step_status(event.event_type, event.status) |> terminal_status?()
  end

  defp error_summary(nil), do: nil

  defp error_summary(%{message: message}) when is_binary(message), do: message
  defp error_summary(%{"message" => message}) when is_binary(message), do: message
  defp error_summary(error) when is_binary(error), do: error
  defp error_summary(error), do: inspect(error)

  defp timeline_sort_key(%{started_at: %DateTime{} = started_at}),
    do: DateTime.to_unix(started_at, :microsecond)

  defp timeline_sort_key(_entry), do: 0

  defp run_started_sort_key(%RunState{inserted_at: %DateTime{} = inserted_at}),
    do: DateTime.to_unix(inserted_at, :microsecond)

  defp run_started_sort_key(_run), do: 0

  defp datetime_sort_key(%DateTime{} = datetime), do: DateTime.to_unix(datetime, :microsecond)
  defp datetime_sort_key(_datetime), do: 0

  defp with_public_status(%RunState{} = run) do
    %{run | status: public_status(run)}
  end

  defp public_status(%RunState{submit_kind: :pipeline, status: :ok} = run) do
    if incomplete_pipeline_success?(run), do: :running, else: :ok
  end

  defp public_status(%RunState{status: status}), do: status

  defp incomplete_pipeline_success?(%RunState{} = run) do
    incomplete_step_results?(run, persisted_steps(run))
  end

  defp incomplete_step_results?(%RunState{} = run, persisted_steps) do
    expected = expected_step_count(run)

    pipeline_like_run?(run) and expected > 0 and terminal_step_count(persisted_steps) < expected
  end

  defp pipeline_like_run?(%RunState{submit_kind: :pipeline}), do: true

  defp pipeline_like_run?(%RunState{submit_kind: :rerun, metadata: metadata})
       when is_map(metadata),
       do: Map.get(metadata, :replay_submit_kind) == :pipeline

  defp pipeline_like_run?(_run), do: false

  defp expected_step_count(%RunState{plan: %Favn.Plan{nodes: nodes}})
       when is_map(nodes) and map_size(nodes) > 0,
       do: map_size(nodes)

  defp expected_step_count(%RunState{target_refs: refs}) when is_list(refs), do: length(refs)
  defp expected_step_count(_run), do: 0

  defp terminal_step_count(steps) when is_list(steps),
    do: Enum.count(steps, &terminal_status?(&1.status))

  defp classify(%RunState{submit_kind: :rerun}), do: :rerun

  defp classify(%RunState{parent_run_id: parent_run_id}) when is_binary(parent_run_id),
    do: :backfill_child

  defp classify(%RunState{submit_kind: submit_kind})
       when submit_kind in [:backfill_asset, :backfill_pipeline],
       do: :backfill_parent

  defp classify(%RunState{submit_kind: :pipeline}), do: :pipeline
  defp classify(%RunState{}), do: :asset

  defp progress(%RunState{} = run, :backfill_parent, nil) do
    case list_all_backfill_windows(run.id) do
      {:ok, []} -> nil
      {:ok, windows} -> window_progress(windows)
      {:error, _reason} -> nil
    end
  end

  defp progress(%RunState{}, :backfill_parent, []), do: nil

  defp progress(%RunState{}, :backfill_parent, windows) when is_list(windows) do
    window_progress(windows)
  end

  defp progress(%RunState{} = run, role, _backfill_windows) do
    steps = persisted_steps(run)
    total = progress_total(run, steps)

    unit =
      if(role in [:pipeline, :backfill_child] or has_node_results?(run),
        do: :steps,
        else: :assets
      )

    cond do
      steps == [] and active?(run.status) ->
        %{unit: unit, label: "Waiting", counts: %{total: total, completed: 0}}

      total == 0 ->
        nil

      true ->
        completed = Enum.count(steps, &terminal_status?(&1.status))

        %{
          unit: unit,
          label: "#{completed}/#{total} #{unit_label(unit, total)}",
          counts: %{total: total, completed: completed}
        }
    end
  end

  defp progress_total(%RunState{} = run, steps) do
    expected = expected_step_count(run)

    cond do
      pipeline_like_run?(run) and expected > 0 -> max(length(steps), expected)
      true -> max(length(steps), length(run.target_refs || []))
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
        case list_all_backfill_windows(run.id) do
          {:ok, windows} -> windows
          {:error, _reason} -> nil
        end

      _role ->
        nil
    end
  end

  defp backfill_failures(%RunState{} = run, nil) do
    case classify(run) do
      :backfill_parent ->
        case list_all_backfill_windows(run.id) do
          {:ok, windows} -> backfill_failures(run, windows)
          {:error, _reason} -> {[], 0}
        end

      _role ->
        {[], 0}
    end
  end

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
      window: backfill_window(window),
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
      |> step_summaries(events)
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

  defp step_summaries(%RunState{} = run, events) do
    persisted_steps = persisted_steps(run)
    event_steps = event_steps(run, events)
    settling? = incomplete_step_results?(run, persisted_steps)

    persisted_steps
    |> merge_event_steps(event_steps, run, settling?)
    |> append_waiting_steps(run, event_steps, settling?)
    |> normalize_step_timings()
    |> mark_cascade_failures(events)
    |> Enum.sort_by(&{&1.stage || 999_999, &1.asset_ref})
  end

  defp persisted_steps(%RunState{result: result} = run) when is_map(result) do
    node_steps =
      result
      |> result_entries(:node_results)
      |> Enum.map(&step_summary(&1, run))

    asset_steps =
      result
      |> result_entries(:asset_results)
      |> Enum.map(&step_summary(&1, run))

    merge_persisted_result_steps(node_steps, asset_steps)
  end

  defp persisted_steps(_run), do: []

  defp result_entries(result, field) when is_map(result) and is_atom(field) do
    result
    |> Map.get(field, Map.get(result, Atom.to_string(field), []))
    |> result_values()
  end

  defp merge_persisted_result_steps([], asset_steps), do: asset_steps
  defp merge_persisted_result_steps(node_steps, []), do: node_steps

  defp merge_persisted_result_steps(node_steps, asset_steps) do
    if terminal_step_count(asset_steps) > terminal_step_count(node_steps) do
      node_steps ++ missing_asset_steps(node_steps, asset_steps)
    else
      node_steps
    end
  end

  defp missing_asset_steps(node_steps, asset_steps) do
    node_ids = node_steps |> Enum.map(& &1.id) |> Enum.reject(&is_nil/1) |> MapSet.new()
    unique_refs = unique_asset_refs(node_steps, asset_steps)
    node_refs = MapSet.new(node_steps, & &1.asset_ref)

    Enum.reject(asset_steps, fn step ->
      MapSet.member?(node_ids, step.id) ||
        (MapSet.member?(unique_refs, step.asset_ref) && MapSet.member?(node_refs, step.asset_ref))
    end)
  end

  defp event_steps(%RunState{} = run, events) do
    events
    |> Enum.filter(&step_event?/1)
    |> Enum.group_by(&event_step_id(run.id, &1))
    |> Enum.map(fn {_id, grouped} -> event_step_summary(run.id, grouped) end)
  end

  defp merge_event_steps(persisted_steps, _event_steps, %RunState{status: status}, false)
       when persisted_steps != [] and status in [:ok, :partial, :error, :cancelled, :timed_out],
       do: persisted_steps

  defp merge_event_steps([], event_steps, _run, _settling?), do: event_steps

  defp merge_event_steps(persisted_steps, event_steps, _run, _settling?) do
    event_steps_by_id = Map.new(event_steps, &{&1.id, &1})
    unique_asset_refs = unique_asset_refs(persisted_steps, event_steps)

    event_steps_by_asset =
      event_steps
      |> Enum.filter(&MapSet.member?(unique_asset_refs, &1.asset_ref))
      |> Map.new(&{&1.asset_ref, &1})

    merged_persisted_steps =
      Enum.map(persisted_steps, fn step ->
        case Map.get(event_steps_by_id, step.id) || Map.get(event_steps_by_asset, step.asset_ref) do
          nil ->
            step

          event_step ->
            Map.merge(event_step, step, fn _key, event_value, persisted_value ->
              persisted_value || event_value
            end)
        end
      end)

    persisted_ids = MapSet.new(merged_persisted_steps, & &1.id)
    persisted_refs = MapSet.new(merged_persisted_steps, & &1.asset_ref)

    new_event_steps =
      Enum.reject(event_steps, fn step ->
        MapSet.member?(persisted_ids, step.id) ||
          (MapSet.member?(unique_asset_refs, step.asset_ref) &&
             MapSet.member?(persisted_refs, step.asset_ref))
      end)

    merged_persisted_steps ++ new_event_steps
  end

  defp append_waiting_steps(steps, %RunState{status: status} = run, event_steps, settling?)
       when status in [:pending, :running] or settling? do
    known = known_waiting_identities(steps, event_steps)

    candidates = planned_waiting_candidates(run)
    candidate_ref_counts = Enum.frequencies_by(candidates, & &1.asset_ref)

    waiting_steps =
      candidates
      |> Enum.reject(&known_waiting_candidate?(run.id, known, &1, candidate_ref_counts))
      |> Enum.map(&waiting_step(run.id, &1))

    steps ++ waiting_steps
  end

  defp append_waiting_steps(steps, _run, _event_steps, _settling?), do: steps

  defp known_waiting_identities(steps, event_steps) do
    all_steps = steps ++ event_steps

    %{
      ids: all_steps |> Enum.map(& &1.id) |> Enum.reject(&is_nil/1) |> MapSet.new(),
      node_keys:
        all_steps |> Enum.map(&Map.get(&1, :node_key)) |> Enum.reject(&is_nil/1) |> MapSet.new(),
      asset_refs: all_steps |> Enum.map(& &1.asset_ref) |> Enum.reject(&is_nil/1) |> MapSet.new()
    }
  end

  defp known_waiting_candidate?(run_id, known, %{node_key: node_key} = candidate, ref_counts)
       when not is_nil(node_key) do
    MapSet.member?(known.node_keys, node_key) ||
      MapSet.member?(known.ids, candidate_step_id(run_id, candidate)) ||
      (Map.get(ref_counts, candidate.asset_ref) == 1 and
         MapSet.member?(known.asset_refs, candidate.asset_ref))
  end

  defp known_waiting_candidate?(_run_id, known, candidate, _ref_counts) do
    MapSet.member?(known.asset_refs, candidate.asset_ref)
  end

  defp planned_waiting_candidates(
         %RunState{plan: %Favn.Plan{nodes: nodes, node_stages: stages}} = run
       )
       when is_map(nodes) and map_size(nodes) > 0 do
    ordered_node_keys = List.flatten(stages || [])
    remaining_node_keys = Map.keys(nodes) -- ordered_node_keys

    (ordered_node_keys ++ remaining_node_keys)
    |> Enum.uniq()
    |> Enum.flat_map(fn node_key ->
      case Map.fetch(nodes, node_key) do
        {:ok, node} ->
          [
            %{
              node_key: node_key,
              asset_ref: public_ref(Map.get(node, :ref)),
              canonical_asset_ref: Map.get(node, :ref),
              stage: Map.get(node, :stage),
              execution_pool: effective_execution_pool(run, node),
              queue_reason: waiting_queue_reason(run, node),
              window: public_window(Map.get(node, :window) || %{})
            }
          ]

        :error ->
          []
      end
    end)
  end

  defp planned_waiting_candidates(%RunState{} = run) do
    run.target_refs
    |> List.wrap()
    |> Enum.map(fn ref ->
      %{
        node_key: nil,
        asset_ref: public_ref(ref),
        canonical_asset_ref: ref,
        stage: nil,
        execution_pool: nil,
        queue_reason: nil,
        window: nil
      }
    end)
  end

  defp effective_execution_pool(%RunState{} = run, node) when is_map(node) do
    Map.get(node, :execution_pool) || pipeline_default_execution_pool(run)
  end

  defp effective_execution_pool(%RunState{} = run, node_key, asset_ref) do
    case run.plan do
      %Favn.Plan{nodes: nodes} when is_map(nodes) ->
        case Map.fetch(nodes, node_key) do
          {:ok, node} -> effective_execution_pool(run, node)
          :error -> effective_execution_pool_from_ref(run, asset_ref)
        end

      _other ->
        pipeline_default_execution_pool(run)
    end
  end

  defp effective_execution_pool_from_ref(%RunState{} = run, asset_ref) do
    with %Favn.Plan{nodes: nodes} <- run.plan,
         true <- is_map(nodes),
         {_node_key, node} <-
           Enum.find(nodes, fn {_key, node} -> Map.get(node, :ref) == asset_ref end) do
      effective_execution_pool(run, node)
    else
      _other -> pipeline_default_execution_pool(run)
    end
  end

  defp pipeline_default_execution_pool(%RunState{metadata: %{pipeline_execution_policy: policy}})
       when is_map(policy) do
    Map.get(policy, :execution_pool) || Map.get(policy, "execution_pool")
  end

  defp pipeline_default_execution_pool(%RunState{}), do: nil

  defp waiting_queue_reason(_run, %{upstream: [_ | _]}), do: :waiting_dependencies
  defp waiting_queue_reason(_run, _node), do: nil

  defp candidate_step_id(run_id, candidate) do
    AssetStepIdentity.asset_step_id(run_id, candidate.node_key, candidate.canonical_asset_ref)
  end

  defp waiting_step(run_id, candidate) do
    %{
      id: candidate_step_id(run_id, candidate),
      node_key: candidate.node_key,
      asset_ref: candidate.asset_ref,
      canonical_asset_ref: candidate.canonical_asset_ref,
      status: :pending,
      stage: candidate.stage,
      execution_pool: candidate.execution_pool,
      queue_reason: candidate.queue_reason,
      window: candidate.window,
      duration_ms: nil,
      started_at: nil,
      finished_at: nil,
      attempt: nil,
      error: nil,
      output: nil,
      output_metadata: nil,
      explanation: "Asset has not started yet for this run.",
      failure_role: nil,
      root_failure_asset_ref: nil
    }
  end

  defp unique_asset_refs(persisted_steps, event_steps) do
    persisted_steps
    |> unique_refs()
    |> MapSet.intersection(unique_refs(event_steps))
  end

  defp unique_refs(steps) do
    steps
    |> Enum.frequencies_by(& &1.asset_ref)
    |> Enum.filter(fn {_asset_ref, count} -> count == 1 end)
    |> Enum.map(fn {asset_ref, _count} -> asset_ref end)
    |> MapSet.new()
  end

  defp step_summary(result, %RunState{} = run) when is_map(result) do
    run_id = run.id
    node_key = Map.get(result, :node_key) || Map.get(result, "node_key")
    canonical_asset_ref = Map.get(result, :ref) || Map.get(result, "ref") || node_ref(result)
    asset_ref = public_ref(canonical_asset_ref)

    step_id =
      Map.get(result, :asset_step_id) || Map.get(result, "asset_step_id") ||
        Map.get(result, :id) || Map.get(result, "id") || Map.get(result, :step_id) ||
        Map.get(result, "step_id") ||
        AssetStepIdentity.asset_step_id(run_id, node_key, canonical_asset_ref)

    meta = Map.get(result, :meta) || Map.get(result, "meta") || %{}
    output_metadata = output_metadata(meta)

    %{
      id: step_id,
      node_key: node_key,
      asset_ref: asset_ref,
      canonical_asset_ref: canonical_asset_ref,
      status: Map.get(result, :status) || Map.get(result, "status"),
      stage: Map.get(result, :stage) || Map.get(result, "stage"),
      execution_pool:
        Map.get(result, :execution_pool) || Map.get(result, "execution_pool") ||
          effective_execution_pool(run, node_key, canonical_asset_ref),
      queue_reason: Map.get(result, :queue_reason) || Map.get(result, "queue_reason"),
      window: public_window(Map.get(result, :window) || Map.get(result, "window") || %{}),
      duration_ms: Map.get(result, :duration_ms) || Map.get(result, "duration_ms"),
      started_at: Map.get(result, :started_at) || Map.get(result, "started_at"),
      finished_at: Map.get(result, :finished_at) || Map.get(result, "finished_at"),
      attempt:
        Map.get(result, :attempt) || Map.get(result, "attempt") || Map.get(result, :attempt_count) ||
          Map.get(result, "attempt_count"),
      error:
        Map.get(result, :error) || Map.get(result, "error") || Map.get(result, :reason) ||
          Map.get(result, "reason"),
      output: output_from_metadata(output_metadata),
      output_metadata: output_metadata,
      explanation: step_explanation(Map.get(result, :status) || Map.get(result, "status")),
      failure_role: nil,
      root_failure_asset_ref: nil
    }
  end

  defp event_step_summary(run_id, events) do
    latest = List.last(events)
    data = latest.data || %{}
    asset_ref = public_ref(latest.asset_ref)

    %{
      id: event_step_id(run_id, latest),
      node_key: Map.get(data, :node_key) || Map.get(data, "node_key"),
      asset_ref: asset_ref,
      canonical_asset_ref: latest.asset_ref,
      status: event_step_status(latest.event_type, latest.status),
      stage: latest.stage,
      execution_pool:
        Map.get(data, :execution_pool) || Map.get(data, "execution_pool") ||
          Map.get(data, :execution_pool_key) || Map.get(data, "execution_pool_key"),
      queue_reason: Map.get(data, :queue_reason) || Map.get(data, "queue_reason"),
      window: event_window(events),
      duration_ms: nil,
      started_at:
        events
        |> Enum.find(&step_event_type?(&1, "step_started"))
        |> then(&(&1 && &1.occurred_at)),
      finished_at: event_step_finished_at(latest, events),
      sequence: latest.sequence,
      attempt: Map.get(data, :attempt) || Map.get(data, "attempt"),
      error: Map.get(data, :error) || Map.get(data, "error"),
      output: nil,
      output_metadata: nil,
      explanation: event_step_explanation(latest.event_type),
      failure_role: nil,
      root_failure_asset_ref: nil
    }
  end

  defp normalize_step_timings(steps) do
    Enum.map(steps, fn step ->
      started_at = derived_step_started_at(step) || step.started_at
      duration_ms = step.duration_ms || duration_ms(started_at, step.finished_at)

      %{step | started_at: started_at, duration_ms: duration_ms}
    end)
  end

  defp derived_step_started_at(%{
         status: status,
         finished_at: %DateTime{} = finished_at,
         duration_ms: duration_ms
       })
       when is_integer(duration_ms) and duration_ms >= 0 do
    if terminal_status?(status),
      do: DateTime.add(finished_at, -duration_ms, :millisecond),
      else: nil
  end

  defp derived_step_started_at(_step), do: nil

  defp mark_cascade_failures(steps, events) do
    cascade = cascade_failure_context(events)

    Enum.map(steps, fn step ->
      cond do
        terminal_failure_status?(step.status) and Map.has_key?(cascade.by_step_id, step.id) ->
          root_failure_asset_ref = Map.fetch!(cascade.by_step_id, step.id)

          step
          |> Map.put(:failure_role, :cascade)
          |> Map.put(:root_failure_asset_ref, root_failure_asset_ref)
          |> Map.put(:explanation, cascade_failure_explanation(root_failure_asset_ref))

        terminal_failure_status?(step.status) ->
          Map.put(step, :failure_role, :primary)

        true ->
          step
      end
    end)
  end

  defp cascade_failure_context(events) do
    started_by_runner_execution_id = started_step_ids_by_runner_execution_id(events)

    by_step_id =
      events
      |> Enum.filter(&event_type?(&1, :stage_draining_after_failure))
      |> Enum.reduce(%{}, fn event, acc ->
        data = event.data || %{}

        root_failure_asset_ref =
          public_ref(Map.get(data, :failed_asset_ref) || Map.get(data, "failed_asset_ref"))

        data
        |> Map.get(:pending_execution_ids, Map.get(data, "pending_execution_ids", []))
        |> List.wrap()
        |> Enum.reduce(acc, fn execution_id, step_acc ->
          case Map.get(started_by_runner_execution_id, execution_id) do
            nil -> step_acc
            step_id -> Map.put(step_acc, step_id, root_failure_asset_ref)
          end
        end)
      end)

    %{by_step_id: by_step_id}
  end

  defp started_step_ids_by_runner_execution_id(events) do
    events
    |> Enum.filter(&event_type?(&1, :step_started))
    |> Map.new(fn event ->
      data = event.data || %{}

      runner_execution_id =
        Map.get(data, :runner_execution_id) || Map.get(data, "runner_execution_id")

      {runner_execution_id, event_step_id(event.run_id, event)}
    end)
    |> Map.delete(nil)
  end

  defp event_type?(%RunEvent{event_type: event_type}, expected) when is_atom(event_type),
    do: event_type == expected

  defp event_type?(%RunEvent{event_type: event_type}, expected) when is_binary(event_type),
    do: event_type == Atom.to_string(expected)

  defp event_type?(_event, _expected), do: false

  defp terminal_failure_status?(status), do: status in [:error, :timed_out, "error", "timed_out"]

  defp cascade_failure_explanation(root_failure_asset_ref) do
    "Failed while draining in-flight work after root failure in #{root_failure_asset_ref}."
  end

  defp result_values(results) when is_map(results), do: Map.values(results)
  defp result_values(results) when is_list(results), do: results
  defp result_values(_results), do: []

  defp output_metadata(metadata) when is_map(metadata), do: JsonSafe.data(metadata)
  defp output_metadata(_metadata), do: nil

  defp output_from_metadata(metadata) when is_map(metadata) do
    Map.get(metadata, "output") || Map.get(metadata, "outputs") ||
      Map.get(metadata, "materialization")
  end

  defp output_from_metadata(_metadata), do: nil

  defp node_ref(result) do
    case Map.get(result, :node_key) || Map.get(result, "node_key") do
      {ref, _window} -> ref
      _other -> nil
    end
  end

  defp step_event?(%RunEvent{event_type: event_type}) do
    event_type
    |> event_type_name()
    |> String.starts_with?("step_")
  end

  defp step_event?(_event), do: false

  defp step_event_type?(%RunEvent{event_type: event_type}, expected),
    do: event_type_name(event_type) == expected

  defp event_type_name(event_type) when is_atom(event_type), do: Atom.to_string(event_type)
  defp event_type_name(event_type) when is_binary(event_type), do: event_type
  defp event_type_name(_event_type), do: ""

  defp event_step_id(run_id, %RunEvent{} = event) do
    data = event.data || %{}

    Map.get(data, :asset_step_id) || Map.get(data, "asset_step_id") ||
      safe_id("#{run_id}:#{public_ref(event.asset_ref)}")
  end

  defp event_step_status(event_type, status) do
    case event_type_name(event_type) do
      "step_started" -> :running
      "step_queued" -> :queued
      "step_finished" -> :ok
      "step_failed" -> :error
      "step_timed_out" -> :timed_out
      "step_cancelled" -> :cancelled
      "step_retry_scheduled" -> :retrying
      "step_skipped_fresh" -> :skipped_fresh
      "step_blocked" -> :blocked
      _other -> status_name(status)
    end
  end

  defp status_name(nil), do: nil
  defp status_name(status) when is_atom(status), do: status
  defp status_name("running"), do: :running
  defp status_name("ok"), do: :ok
  defp status_name("partial"), do: :partial
  defp status_name("error"), do: :error
  defp status_name("timed_out"), do: :timed_out
  defp status_name("cancelled"), do: :cancelled
  defp status_name("retrying"), do: :retrying
  defp status_name("skipped_fresh"), do: :skipped_fresh
  defp status_name("blocked"), do: :blocked
  defp status_name(status), do: status

  defp event_step_explanation(event_type) do
    case event_type_name(event_type) do
      "step_started" -> "Execution has started; waiting for runner result."
      "step_queued" -> "Execution is queued by orchestrator admission."
      "step_retry_scheduled" -> "Retry has been scheduled for this asset."
      "step_finished" -> "Execution finished successfully."
      "step_failed" -> "Failed while executing this asset."
      _event_type -> nil
    end
  end

  defp step_explanation(status) when status in [:pending, "pending"],
    do: "Asset has not started yet for this run."

  defp step_explanation(status) when status in [:running, :retrying, "running", "retrying"],
    do: "Backend is still working on this asset."

  defp step_explanation(status) when status in [:ok, "ok"], do: "Ran as part of the backend plan."

  defp step_explanation(status) when status in [:error, "error"],
    do: "Failed while executing this asset."

  defp step_explanation(_status), do: nil

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

  defp active?(status), do: status in [:pending, :running]

  defp has_node_results?(%RunState{result: result}) when is_map(result) do
    case Map.get(result, :node_results) || Map.get(result, "node_results") do
      results when is_list(results) -> results != []
      results when is_map(results) -> map_size(results) > 0
      _other -> false
    end
  end

  defp has_node_results?(_run), do: false

  defp terminal_status?(status),
    do:
      status in [
        :ok,
        :partial,
        :error,
        :blocked,
        :cancelled,
        :timed_out,
        :skipped,
        :skipped_fresh,
        "ok",
        "partial",
        "error",
        "blocked",
        "cancelled",
        "timed_out",
        "skipped",
        "skipped_fresh"
      ]

  defp failed_status?(status),
    do:
      status in [
        :error,
        :timed_out,
        :cancelled,
        :blocked,
        "error",
        "timed_out",
        "cancelled",
        "blocked"
      ]

  defp running_status?(status), do: status in [:running, :retrying, "running", "retrying"]

  defp queued_status?(status), do: status in [:pending, :queued, nil, "pending", "queued"]

  defp empty_window?(nil), do: true

  defp empty_window?(window) when is_map(window) do
    is_nil(window.key) and is_nil(window.start_at) and is_nil(window.end_at)
  end

  defp empty_window?(_window), do: false

  defp unit_label(:assets, 1), do: "asset"
  defp unit_label(:assets, _total), do: "assets"
  defp unit_label(:steps, 1), do: "step"
  defp unit_label(:steps, _total), do: "steps"

  defp window(%RunState{} = run, :backfill_child) do
    backfill_child_window(run) || metadata_window(run.metadata) || params_window(run.params)
  end

  defp window(%RunState{} = run, _role) do
    metadata_window(run.metadata) || params_window(run.params)
  end

  defp backfill_child_window(%RunState{parent_run_id: parent_run_id, trigger: trigger})
       when is_binary(parent_run_id) and is_map(trigger) do
    with pipeline_module when is_atom(pipeline_module) <- pipeline_module(trigger),
         window_key when is_binary(window_key) <-
           Map.get(trigger, :window_key) || Map.get(trigger, "window_key"),
         {:ok, %BackfillWindow{} = window} <-
           Storage.get_backfill_window(parent_run_id, pipeline_module, window_key) do
      backfill_window(window)
    else
      _other -> nil
    end
  end

  defp backfill_child_window(_run), do: nil

  defp pipeline_module(trigger) do
    Map.get(trigger, :pipeline_module) || Map.get(trigger, "pipeline_module")
  end

  defp backfill_window(%BackfillWindow{} = window) do
    %{
      key: window.window_key,
      label: label(window.window_kind, window.window_start_at),
      kind: window.window_kind,
      start_at: window.window_start_at,
      end_at: window.window_end_at,
      timezone: window.timezone
    }
  end

  defp metadata_window(metadata) when is_map(metadata) do
    metadata
    |> Map.get(:pipeline_context, Map.get(metadata, "pipeline_context", %{}))
    |> anchor_window()
    |> then(fn window -> window || selected_window(metadata) end)
  end

  defp metadata_window(_metadata), do: nil

  defp anchor_window(%{anchor_window: %Anchor{} = anchor}), do: anchor_window(anchor)
  defp anchor_window(%{"anchor_window" => %Anchor{} = anchor}), do: anchor_window(anchor)

  defp anchor_window(%Anchor{} = anchor) do
    %{
      key: encoded_key(anchor),
      label: label(anchor.kind, anchor.start_at),
      kind: anchor.kind,
      start_at: anchor.start_at,
      end_at: anchor.end_at,
      timezone: anchor.timezone
    }
  end

  defp anchor_window(_value), do: nil

  defp selected_window(metadata) do
    case Map.get(metadata, :selected_window) || Map.get(metadata, "selected_window") ||
           Map.get(metadata, :window) || Map.get(metadata, "window") do
      value when is_map(value) ->
        public_window(value)

      value when is_binary(value) ->
        %{key: value, label: value, kind: nil, start_at: nil, end_at: nil, timezone: nil}

      _other ->
        nil
    end
  end

  defp params_window(params) when is_map(params) do
    case Map.get(params, :window) || Map.get(params, "window") do
      value when is_map(value) ->
        public_window(value)

      value when is_binary(value) ->
        %{key: value, label: value, kind: nil, start_at: nil, end_at: nil, timezone: nil}

      _other ->
        nil
    end
  end

  defp params_window(_params), do: nil

  defp public_window(window) when is_map(window) do
    kind = Map.get(window, :kind) || Map.get(window, "kind")
    start_at = Map.get(window, :start_at) || Map.get(window, "start_at")
    end_at = Map.get(window, :end_at) || Map.get(window, "end_at")

    %{
      key:
        Map.get(window, :key) || Map.get(window, "key") || Map.get(window, :id) ||
          Map.get(window, "id"),
      label: Map.get(window, :label) || Map.get(window, "label") || label(kind, start_at),
      kind: kind,
      start_at: start_at,
      end_at: end_at,
      timezone: Map.get(window, :timezone) || Map.get(window, "timezone")
    }
  end

  defp public_window(_window), do: nil

  defp public_ref({module, name}), do: "#{module_label(module)}.#{name}"
  defp public_ref(%{module: module, name: name}), do: "#{module_label(module)}.#{name}"
  defp public_ref(%{"module" => module, "name" => name}), do: "#{module_label(module)}.#{name}"
  defp public_ref(ref) when is_atom(ref), do: ref |> Atom.to_string() |> strip_elixir_prefix()
  defp public_ref(ref) when is_binary(ref), do: strip_elixir_prefix(ref)
  defp public_ref(nil), do: "Unknown asset"
  defp public_ref(ref), do: inspect(ref)

  defp module_label(module) when is_atom(module),
    do: module |> Atom.to_string() |> strip_elixir_prefix()

  defp module_label(module), do: module |> to_string() |> strip_elixir_prefix()

  defp strip_elixir_prefix("Elixir." <> module), do: module
  defp strip_elixir_prefix(module), do: module

  defp safe_id(value), do: value |> to_string() |> String.replace(~r/[^A-Za-z0-9_-]+/, "-")

  defp short_id(id) when is_binary(id) and byte_size(id) > 18 do
    binary_part(id, 0, 9) <> "..." <> binary_part(id, byte_size(id) - 6, 6)
  end

  defp short_id(id) when is_binary(id), do: id

  defp encoded_key(%Anchor{kind: kind, key: key}) when is_map(key) do
    case {Map.get(key, :timezone), Map.get(key, :start_at_us)} do
      {timezone, start_at_us} when is_binary(timezone) and is_integer(start_at_us) ->
        start_at = DateTime.from_unix!(start_at_us, :microsecond)
        "#{kind}:#{timezone}:#{DateTime.to_iso8601(start_at)}"

      _other ->
        nil
    end
  end

  defp label(kind, %DateTime{} = start_at) do
    case kind do
      :hour -> Calendar.strftime(start_at, "%b %-d %H:00")
      :day -> Calendar.strftime(start_at, "%b %-d")
      :month -> Calendar.strftime(start_at, "%b %Y")
      :year -> Calendar.strftime(start_at, "%Y")
      _other -> nil
    end
  end

  defp label(_kind, _start_at), do: nil

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
