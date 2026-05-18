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
          required(:asset_ref) => String.t(),
          required(:canonical_asset_ref) => Favn.Ref.t() | nil,
          required(:status) => atom() | nil,
          required(:stage) => non_neg_integer() | nil,
          required(:window) => window_summary() | nil,
          required(:duration_ms) => non_neg_integer() | nil,
          required(:started_at) => DateTime.t() | nil,
          required(:attempt) => non_neg_integer() | nil,
          required(:error) => term(),
          required(:output) => term(),
          required(:explanation) => String.t() | nil
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
          required(:events) => [RunEvent.t()]
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

      {:ok,
       %{
         summary: summary(run),
         params: run.params,
         trigger: run.trigger,
         metadata: run.metadata,
         result: run.result,
         error: run.error,
         runner_execution_id: run.runner_execution_id,
         event_seq: run.event_seq,
         steps: step_summaries(run, events),
         events: events
       }}
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
    role = classify(run)
    progress = progress(run, role)
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

  defp classify(%RunState{submit_kind: :rerun}), do: :rerun

  defp classify(%RunState{parent_run_id: parent_run_id}) when is_binary(parent_run_id),
    do: :backfill_child

  defp classify(%RunState{submit_kind: submit_kind})
       when submit_kind in [:backfill_asset, :backfill_pipeline],
       do: :backfill_parent

  defp classify(%RunState{submit_kind: :pipeline}), do: :pipeline
  defp classify(%RunState{}), do: :asset

  defp progress(%RunState{} = run, :backfill_parent) do
    case list_all_backfill_windows(run.id) do
      {:ok, []} -> nil
      {:ok, windows} -> window_progress(windows)
      {:error, _reason} -> nil
    end
  end

  defp progress(%RunState{} = run, role) do
    steps = persisted_steps(run)
    total = max(length(steps), length(run.target_refs || []))

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

  defp list_all_backfill_windows(backfill_run_id, offset \\ 0, acc \\ []) do
    case Storage.list_backfill_windows(
           backfill_run_id: backfill_run_id,
           limit: Page.max_limit(),
           offset: offset
         ) do
      {:ok, %Page{items: items, has_more?: true, next_offset: next_offset}}
      when is_integer(next_offset) ->
        list_all_backfill_windows(backfill_run_id, next_offset, acc ++ items)

      {:ok, %Page{items: items}} ->
        {:ok, acc ++ items}

      {:error, _reason} = error ->
        error
    end
  end

  defp step_summaries(%RunState{} = run, events) do
    persisted_steps = persisted_steps(run)
    event_steps = event_steps(run, events)

    persisted_steps
    |> merge_event_steps(event_steps, run)
    |> append_waiting_steps(run, event_steps)
    |> Enum.sort_by(&{&1.stage || 999_999, &1.asset_ref})
  end

  defp persisted_steps(%RunState{result: result} = run) when is_map(result) do
    results =
      Map.get(result, :node_results) || Map.get(result, "node_results") ||
        Map.get(result, :asset_results) || Map.get(result, "asset_results") || []

    results
    |> result_values()
    |> Enum.map(&step_summary(&1, run.id))
  end

  defp persisted_steps(_run), do: []

  defp event_steps(%RunState{} = run, events) do
    events
    |> Enum.filter(&step_event?/1)
    |> Enum.group_by(&event_step_id(run.id, &1))
    |> Enum.map(fn {_id, grouped} -> event_step_summary(run.id, grouped) end)
  end

  defp merge_event_steps(persisted_steps, _event_steps, %RunState{status: status})
       when persisted_steps != [] and status in [:ok, :partial, :error, :cancelled, :timed_out],
       do: persisted_steps

  defp merge_event_steps([], event_steps, _run), do: event_steps

  defp merge_event_steps(persisted_steps, event_steps, _run) do
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

  defp append_waiting_steps(steps, %RunState{status: status} = run, event_steps)
       when status in [:pending, :running] do
    known_refs =
      (Enum.map(steps, & &1.asset_ref) ++ Enum.map(event_steps, & &1.asset_ref))
      |> Enum.reject(&is_nil/1)
      |> MapSet.new()

    waiting_steps =
      run.target_refs
      |> List.wrap()
      |> Enum.map(&public_ref/1)
      |> Enum.reject(&MapSet.member?(known_refs, &1))
      |> Enum.map(&waiting_step(run.id, &1))

    steps ++ waiting_steps
  end

  defp append_waiting_steps(steps, _run, _event_steps), do: steps

  defp waiting_step(run_id, asset_ref) do
    %{
      id: AssetStepIdentity.asset_step_id(run_id, nil, asset_ref),
      asset_ref: asset_ref,
      canonical_asset_ref: nil,
      status: :pending,
      stage: nil,
      window: nil,
      duration_ms: nil,
      started_at: nil,
      attempt: nil,
      error: nil,
      output: nil,
      explanation: "Asset has not started yet for this run."
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

  defp step_summary(result, run_id) when is_map(result) do
    node_key = Map.get(result, :node_key) || Map.get(result, "node_key")
    canonical_asset_ref = Map.get(result, :ref) || Map.get(result, "ref") || node_ref(result)
    asset_ref = public_ref(canonical_asset_ref)

    step_id =
      Map.get(result, :asset_step_id) || Map.get(result, "asset_step_id") ||
        Map.get(result, :id) || Map.get(result, "id") || Map.get(result, :step_id) ||
        Map.get(result, "step_id") ||
        AssetStepIdentity.asset_step_id(run_id, node_key, canonical_asset_ref)

    meta = Map.get(result, :meta) || Map.get(result, "meta") || %{}

    %{
      id: step_id,
      asset_ref: asset_ref,
      canonical_asset_ref: canonical_asset_ref,
      status: Map.get(result, :status) || Map.get(result, "status"),
      stage: Map.get(result, :stage) || Map.get(result, "stage"),
      window: public_window(Map.get(result, :window) || Map.get(result, "window") || %{}),
      duration_ms: Map.get(result, :duration_ms) || Map.get(result, "duration_ms"),
      started_at: Map.get(result, :started_at) || Map.get(result, "started_at"),
      attempt:
        Map.get(result, :attempt) || Map.get(result, "attempt") || Map.get(result, :attempt_count) ||
          Map.get(result, "attempt_count"),
      error:
        Map.get(result, :error) || Map.get(result, "error") || Map.get(result, :reason) ||
          Map.get(result, "reason"),
      output:
        Map.get(meta, :output) || Map.get(meta, "output") || Map.get(meta, :outputs) ||
          Map.get(meta, "outputs") || Map.get(meta, :materialization) ||
          Map.get(meta, "materialization"),
      explanation: step_explanation(Map.get(result, :status) || Map.get(result, "status"))
    }
  end

  defp event_step_summary(run_id, events) do
    latest = List.last(events)
    data = latest.data || %{}
    asset_ref = public_ref(latest.asset_ref)

    %{
      id: event_step_id(run_id, latest),
      asset_ref: asset_ref,
      canonical_asset_ref: latest.asset_ref,
      status: event_step_status(latest.event_type, latest.status),
      stage: latest.stage,
      window: nil,
      duration_ms: nil,
      started_at:
        events
        |> Enum.find(&step_event_type?(&1, "step_started"))
        |> then(&(&1 && &1.occurred_at)),
      attempt: Map.get(data, :attempt) || Map.get(data, "attempt"),
      error: Map.get(data, :error) || Map.get(data, "error"),
      output: nil,
      explanation: event_step_explanation(latest.event_type)
    }
  end

  defp result_values(results) when is_map(results), do: Map.values(results)
  defp result_values(results) when is_list(results), do: results
  defp result_values(_results), do: []

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
    do: status in [:ok, :partial, :error, :blocked, :cancelled, :timed_out, :skipped_fresh]

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

  defp public_ref({module, name}), do: "#{inspect(module)}.#{name}"
  defp public_ref(%{module: module, name: name}), do: "#{module}.#{name}"
  defp public_ref(%{"module" => module, "name" => name}), do: "#{module}.#{name}"
  defp public_ref(ref) when is_atom(ref), do: Atom.to_string(ref)
  defp public_ref(ref) when is_binary(ref), do: ref
  defp public_ref(nil), do: "Unknown asset"
  defp public_ref(ref), do: inspect(ref)

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
