defmodule FavnOrchestrator.RunReadModel.StepProjection do
  @moduledoc """
  Reconstructs operator-facing step state from a run snapshot and its events.

  Persisted results are authoritative once a run is settled. While work is in
  flight, events fill gaps and the pinned plan supplies steps that have not
  started. Plan indexes are built once per projection so repeated asset results
  do not rescan the complete plan.
  """

  alias Favn.Plan
  alias FavnOrchestrator.AssetStepIdentity
  alias FavnOrchestrator.ExecutionStatus
  alias FavnOrchestrator.RunEvent
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage.JsonSafe
  alias FavnOrchestrator.Storage.RunQuery
  alias FavnOrchestrator.WindowSummary

  @type t :: %{
          required(:id) => String.t(),
          required(:node_key) => Plan.node_key() | nil,
          required(:asset_ref) => String.t(),
          required(:canonical_asset_ref) => Favn.Ref.t() | nil,
          required(:status) => ExecutionStatus.t(),
          required(:stage) => non_neg_integer() | nil,
          required(:execution_pool) => atom() | String.t() | nil,
          required(:queue_reason) => atom() | String.t() | nil,
          required(:window) => WindowSummary.t() | nil,
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

  @type progress :: %{
          required(:unit) => :assets | :steps,
          required(:total) => non_neg_integer(),
          required(:completed) => non_neg_integer(),
          required(:empty?) => boolean()
        }

  @typep context :: %{
           run: RunState.t(),
           nodes: map(),
           nodes_by_unique_ref: map(),
           default_execution_pool: atom() | String.t() | nil
         }

  @doc "Builds the complete step projection for one run."
  @spec build(RunState.t(), [RunEvent.t()]) :: [t()]
  def build(%RunState{} = run, events) when is_list(events) do
    context = context(run)
    persisted_steps = persisted(run, context)
    event_steps = event_steps(run, events)
    settling? = incomplete?(run, persisted_steps)

    persisted_steps
    |> merge_event_steps(event_steps, run, settling?)
    |> append_waiting_steps(context, event_steps, settling?)
    |> normalize_timings()
    |> mark_cascade_failures(events)
    |> Enum.sort_by(&{&1.stage || 999_999, &1.asset_ref})
  end

  @doc "Returns steps stored in the terminal or in-flight run result."
  @spec persisted(RunState.t()) :: [t()]
  def persisted(%RunState{} = run), do: persisted(run, context(run))

  @doc "Returns whether a pipeline snapshot is marked successful before every step settled."
  @spec incomplete?(RunState.t()) :: boolean()
  def incomplete?(%RunState{} = run), do: incomplete?(run, persisted(run))

  @doc "Returns bounded progress counts derived from the persisted snapshot and pinned plan."
  @spec progress(RunState.t()) :: progress()
  def progress(%RunState{} = run) do
    steps = persisted(run)
    expected = expected_count(run)

    total =
      if pipeline_like?(run) and expected > 0,
        do: max(length(steps), expected),
        else: max(length(steps), length(run.target_refs || []))

    %{
      unit: if(pipeline_like?(run) or has_node_results?(run), do: :steps, else: :assets),
      total: total,
      completed: Enum.count(steps, &ExecutionStatus.terminal?(&1.status)),
      empty?: steps == []
    }
  end

  defp persisted(%RunState{result: result}, context) when is_map(result) do
    node_steps = result |> result_entries(:node_results) |> project_results(context)
    asset_steps = result |> result_entries(:asset_results) |> project_results(context)

    merge_persisted_result_steps(node_steps, asset_steps)
  end

  defp persisted(_run, _context), do: []

  defp project_results(results, context), do: Enum.map(results, &step_summary(&1, context))

  defp incomplete?(%RunState{} = run, persisted_steps) do
    expected = expected_count(run)

    pipeline_like?(run) and expected > 0 and terminal_count(persisted_steps) < expected
  end

  defp pipeline_like?(%RunState{submit_kind: :pipeline}), do: true

  defp pipeline_like?(%RunState{submit_kind: :rerun, metadata: metadata})
       when is_map(metadata) do
    value(metadata, :replay_submit_kind) in [:pipeline, "pipeline"]
  end

  defp pipeline_like?(_run), do: false

  defp expected_count(%RunState{plan: %Plan{nodes: nodes}})
       when is_map(nodes) and map_size(nodes) > 0,
       do: map_size(nodes)

  defp expected_count(%RunState{target_refs: refs}) when is_list(refs), do: length(refs)
  defp expected_count(_run), do: 0

  defp terminal_count(steps), do: Enum.count(steps, &ExecutionStatus.terminal?(&1.status))

  defp result_entries(result, field) do
    result |> value(field, []) |> result_values()
  end

  defp result_values(results) when is_map(results), do: Map.values(results)
  defp result_values(results) when is_list(results), do: results
  defp result_values(_results), do: []

  defp merge_persisted_result_steps([], asset_steps), do: asset_steps
  defp merge_persisted_result_steps(node_steps, []), do: node_steps

  defp merge_persisted_result_steps(node_steps, asset_steps) do
    if terminal_count(asset_steps) > terminal_count(node_steps),
      do: node_steps ++ missing_asset_steps(node_steps, asset_steps),
      else: node_steps
  end

  defp missing_asset_steps(node_steps, asset_steps) do
    node_ids = node_steps |> Enum.map(& &1.id) |> Enum.reject(&is_nil/1) |> MapSet.new()
    unique_refs = unique_asset_refs(node_steps, asset_steps)
    node_refs = MapSet.new(node_steps, & &1.asset_ref)

    Enum.reject(asset_steps, fn step ->
      MapSet.member?(node_ids, step.id) or
        (MapSet.member?(unique_refs, step.asset_ref) and
           MapSet.member?(node_refs, step.asset_ref))
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

    merged =
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

    persisted_ids = MapSet.new(merged, & &1.id)
    persisted_refs = MapSet.new(merged, & &1.asset_ref)

    new_event_steps =
      Enum.reject(event_steps, fn step ->
        MapSet.member?(persisted_ids, step.id) or
          (MapSet.member?(unique_asset_refs, step.asset_ref) and
             MapSet.member?(persisted_refs, step.asset_ref))
      end)

    merged ++ new_event_steps
  end

  defp append_waiting_steps(
         steps,
         %{run: %RunState{status: status}} = context,
         event_steps,
         settling?
       )
       when status in [:pending, :running] or settling? do
    known = known_identities(steps, event_steps)
    candidates = planned_candidates(context)
    ref_counts = Enum.frequencies_by(candidates, & &1.asset_ref)

    missing =
      candidates
      |> Enum.reject(&known_candidate?(context.run.id, known, &1, ref_counts))
      |> Enum.map(&missing_step(context.run, &1))

    steps ++ missing
  end

  defp append_waiting_steps(steps, _context, _event_steps, _settling?), do: steps

  defp known_identities(steps, event_steps) do
    all_steps = steps ++ event_steps

    %{
      ids: all_steps |> Enum.map(& &1.id) |> Enum.reject(&is_nil/1) |> MapSet.new(),
      node_keys:
        all_steps |> Enum.map(&Map.get(&1, :node_key)) |> Enum.reject(&is_nil/1) |> MapSet.new(),
      asset_refs: all_steps |> Enum.map(& &1.asset_ref) |> Enum.reject(&is_nil/1) |> MapSet.new()
    }
  end

  defp known_candidate?(run_id, known, %{node_key: node_key} = candidate, ref_counts)
       when not is_nil(node_key) do
    MapSet.member?(known.node_keys, node_key) or
      MapSet.member?(known.ids, candidate_step_id(run_id, candidate)) or
      (Map.get(ref_counts, candidate.asset_ref) == 1 and
         MapSet.member?(known.asset_refs, candidate.asset_ref))
  end

  defp known_candidate?(_run_id, known, candidate, _ref_counts),
    do: MapSet.member?(known.asset_refs, candidate.asset_ref)

  defp planned_candidates(%{run: run, nodes: nodes} = context) when map_size(nodes) > 0 do
    ordered_node_keys = List.flatten(run.plan.node_stages || [])
    remaining_node_keys = Map.keys(nodes) -- ordered_node_keys

    (ordered_node_keys ++ remaining_node_keys)
    |> Enum.uniq()
    |> Enum.flat_map(fn node_key ->
      case Map.fetch(nodes, node_key) do
        {:ok, node} -> [planned_candidate(node_key, node, context)]
        :error -> []
      end
    end)
  end

  defp planned_candidates(%{run: run}) do
    run.target_refs
    |> List.wrap()
    |> Enum.map(fn ref ->
      %{
        node_key: nil,
        asset_ref: RunQuery.public_ref(ref),
        canonical_asset_ref: ref,
        stage: nil,
        execution_pool: nil,
        queue_reason: nil,
        window: nil
      }
    end)
  end

  defp planned_candidate(node_key, node, context) do
    ref = Map.get(node, :ref)

    %{
      node_key: node_key,
      asset_ref: RunQuery.public_ref(ref),
      canonical_asset_ref: ref,
      stage: Map.get(node, :stage),
      execution_pool: node_execution_pool(node, context),
      queue_reason: if(match?([_ | _], Map.get(node, :upstream)), do: :waiting_dependencies),
      window: WindowSummary.public(Map.get(node, :window) || %{})
    }
  end

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

  defp missing_step(%RunState{status: status} = run, candidate)
       when status in [:error, :partial, :cancelled, :timed_out] do
    {reason, explanation} = stopped_before_start_reason(status)

    run.id
    |> waiting_step(candidate)
    |> Map.merge(%{
      status: :blocked,
      queue_reason: reason,
      error: %{type: reason, status: status},
      explanation: explanation,
      failure_role: :cascade
    })
  end

  defp missing_step(%RunState{} = run, candidate), do: waiting_step(run.id, candidate)

  defp stopped_before_start_reason(status) when status in [:error, :partial] do
    {:pipeline_stopped_after_failure,
     "Asset was not started because the pipeline stopped after an earlier failure."}
  end

  defp stopped_before_start_reason(:cancelled) do
    {:pipeline_stopped_after_cancellation,
     "Asset was not started because the pipeline was cancelled before this step began."}
  end

  defp stopped_before_start_reason(:timed_out) do
    {:pipeline_stopped_after_timeout,
     "Asset was not started because the pipeline timed out before this step began."}
  end

  defp unique_asset_refs(left, right),
    do: MapSet.intersection(unique_refs(left), unique_refs(right))

  defp unique_refs(steps) do
    steps
    |> Enum.frequencies_by(& &1.asset_ref)
    |> Enum.filter(fn {_asset_ref, count} -> count == 1 end)
    |> Map.new(fn {asset_ref, _count} -> {asset_ref, true} end)
    |> Map.keys()
    |> MapSet.new()
  end

  defp step_summary(result, context) when is_map(result) do
    {step_id, node_key, canonical_asset_ref} = step_identity(result, context.run)
    status = value(result, :status)
    output_metadata = result |> value(:meta, %{}) |> output_metadata()

    %{
      id: step_id,
      node_key: node_key,
      asset_ref: RunQuery.public_ref(canonical_asset_ref),
      canonical_asset_ref: canonical_asset_ref,
      status: status,
      stage: value(result, :stage),
      execution_pool:
        value(result, :execution_pool) ||
          effective_execution_pool(context, node_key, canonical_asset_ref),
      queue_reason: value(result, :queue_reason),
      window: WindowSummary.public(value(result, :window, %{})),
      duration_ms: value(result, :duration_ms),
      started_at: value(result, :started_at),
      finished_at: value(result, :finished_at),
      attempt: first_value(result, [:attempt, :attempt_count]),
      error: first_value(result, [:error, :reason]),
      output: output_from_metadata(output_metadata),
      output_metadata: output_metadata,
      explanation: step_explanation(status),
      failure_role: nil,
      root_failure_asset_ref: nil
    }
  end

  defp step_identity(result, run) do
    node_key = value(result, :node_key)
    canonical_asset_ref = value(result, :ref) || node_ref(result)

    step_id =
      first_value(result, [:asset_step_id, :id, :step_id]) ||
        AssetStepIdentity.asset_step_id(run.id, node_key, canonical_asset_ref)

    {step_id, node_key, canonical_asset_ref}
  end

  defp effective_execution_pool(%{nodes: nodes} = context, node_key, asset_ref) do
    case Map.fetch(nodes, node_key) do
      {:ok, node} -> node_execution_pool(node, context)
      :error -> execution_pool_for_unique_ref(context, asset_ref)
    end
  end

  defp execution_pool_for_unique_ref(context, asset_ref) do
    case Map.fetch(context.nodes_by_unique_ref, asset_ref) do
      {:ok, node} -> node_execution_pool(node, context)
      :error -> context.default_execution_pool
    end
  end

  defp node_execution_pool(node, context),
    do: Map.get(node, :execution_pool) || context.default_execution_pool

  @spec context(RunState.t()) :: context()
  defp context(%RunState{} = run) do
    nodes =
      case run.plan do
        %Plan{nodes: nodes} when is_map(nodes) -> nodes
        _other -> %{}
      end

    %{
      run: run,
      nodes: nodes,
      nodes_by_unique_ref: unique_nodes_by_ref(nodes),
      default_execution_pool: default_execution_pool(run)
    }
  end

  defp unique_nodes_by_ref(nodes) do
    nodes
    |> Map.values()
    |> Enum.group_by(&Map.get(&1, :ref))
    |> Enum.reduce(%{}, fn
      {nil, _nodes}, acc -> acc
      {ref, [node]}, acc -> Map.put(acc, ref, node)
      {_ref, _nodes}, acc -> acc
    end)
  end

  defp default_execution_pool(%RunState{metadata: metadata}) when is_map(metadata) do
    metadata
    |> value(:pipeline_execution_policy, %{})
    |> case do
      policy when is_map(policy) -> value(policy, :execution_pool)
      _other -> nil
    end
  end

  defp default_execution_pool(_run), do: nil

  defp event_step_summary(run_id, events) do
    latest = List.last(events)
    data = latest.data || %{}
    result = event_result(latest, data)
    output_metadata = result |> value(:meta, %{}) |> output_metadata()

    %{
      id: event_step_id(run_id, latest),
      node_key: value(result, :node_key) || value(data, :node_key),
      asset_ref: RunQuery.public_ref(latest.asset_ref),
      canonical_asset_ref: latest.asset_ref,
      status: event_step_status(latest.event_type, latest.status),
      stage: value(result, :stage) || latest.stage,
      execution_pool:
        value(result, :execution_pool) || value(data, :execution_pool) ||
          value(data, :execution_pool_key),
      queue_reason: value(data, :queue_reason),
      window: result_window(result, events),
      duration_ms: value(result, :duration_ms),
      started_at: event_datetime(value(result, :started_at)) || started_at(events),
      finished_at: event_datetime(value(result, :finished_at)) || finished_at(latest, events),
      sequence: latest.sequence,
      attempt: first_value(result, [:attempt, :attempt_count]) || value(data, :attempt),
      error: first_value(result, [:error, :reason]) || value(data, :error),
      output: output_from_metadata(output_metadata),
      output_metadata: output_metadata,
      explanation: event_step_explanation(latest.event_type),
      failure_role: nil,
      root_failure_asset_ref: nil
    }
  end

  defp event_result(latest, data) do
    case value(data, :node_result) do
      result when is_map(result) -> result
      _other -> matching_asset_result(value(data, :asset_results, []), latest.asset_ref)
    end
  end

  defp matching_asset_result(results, asset_ref) when is_list(results) do
    Enum.find(results, %{}, fn result ->
      RunQuery.public_ref(value(result, :ref)) == RunQuery.public_ref(asset_ref)
    end)
  end

  defp matching_asset_result(_results, _asset_ref), do: %{}

  defp result_window(result, events) do
    window = WindowSummary.public(value(result, :window, %{}))
    if WindowSummary.empty?(window), do: event_window(events), else: window
  end

  defp event_datetime(%DateTime{} = value), do: value

  defp event_datetime(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> datetime
      _invalid -> nil
    end
  end

  defp event_datetime(_value), do: nil

  defp started_at(events) do
    case Enum.find(events, &event_type?(&1, :step_started)) do
      nil -> nil
      event -> event.occurred_at
    end
  end

  defp finished_at(latest, events) do
    if ExecutionStatus.terminal?(event_step_status(latest.event_type, latest.status)) do
      latest.occurred_at
    else
      case Enum.find(events, &terminal_step_event?/1) do
        nil -> nil
        event -> event.occurred_at
      end
    end
  end

  defp event_window(events) do
    Enum.find_value(events, fn event ->
      window = WindowSummary.public(value(event.data || %{}, :window, %{}))
      if WindowSummary.empty?(window), do: nil, else: window
    end)
  end

  defp terminal_step_event?(event),
    do: ExecutionStatus.terminal?(event_step_status(event.event_type, event.status))

  defp normalize_timings(steps) do
    Enum.map(steps, fn step ->
      started_at = derived_started_at(step) || step.started_at
      duration_ms = step.duration_ms || duration_ms(started_at, step.finished_at)
      %{step | started_at: started_at, duration_ms: duration_ms}
    end)
  end

  defp derived_started_at(%{
         status: status,
         finished_at: %DateTime{} = finished_at,
         duration_ms: duration_ms
       })
       when is_integer(duration_ms) and duration_ms >= 0 do
    if ExecutionStatus.terminal?(status),
      do: DateTime.add(finished_at, -duration_ms, :millisecond),
      else: nil
  end

  defp derived_started_at(_step), do: nil

  defp mark_cascade_failures(steps, events) do
    cascade_by_step_id = cascade_failure_steps(events)

    Enum.map(steps, fn step ->
      cond do
        terminal_failure?(step.status) and Map.has_key?(cascade_by_step_id, step.id) ->
          root_failure_asset_ref = Map.fetch!(cascade_by_step_id, step.id)

          step
          |> Map.put(:failure_role, :cascade)
          |> Map.put(:root_failure_asset_ref, root_failure_asset_ref)
          |> Map.put(:explanation, cascade_failure_explanation(root_failure_asset_ref))

        terminal_failure?(step.status) ->
          Map.put(step, :failure_role, :primary)

        true ->
          step
      end
    end)
  end

  defp cascade_failure_steps(events) do
    started_by_execution_id = started_step_ids_by_runner_execution_id(events)

    events
    |> Enum.filter(&event_type?(&1, :stage_draining_after_failure))
    |> Enum.reduce(%{}, fn event, acc ->
      data = event.data || %{}
      root_failure_asset_ref = RunQuery.public_ref(value(data, :failed_asset_ref))

      data
      |> value(:pending_execution_ids, [])
      |> List.wrap()
      |> Enum.reduce(acc, fn execution_id, step_acc ->
        case Map.get(started_by_execution_id, execution_id) do
          nil -> step_acc
          step_id -> Map.put(step_acc, step_id, root_failure_asset_ref)
        end
      end)
    end)
  end

  defp started_step_ids_by_runner_execution_id(events) do
    events
    |> Enum.filter(&event_type?(&1, :step_started))
    |> Map.new(fn event ->
      {value(event.data || %{}, :runner_execution_id), event_step_id(event.run_id, event)}
    end)
    |> Map.delete(nil)
  end

  defp event_type?(%RunEvent{event_type: event_type}, expected),
    do: event_type_name(event_type) == Atom.to_string(expected)

  defp event_type?(_event, _expected), do: false

  defp terminal_failure?(status),
    do: ExecutionStatus.normalize(status) in [:error, :timed_out]

  defp cascade_failure_explanation(root_failure_asset_ref) do
    "Failed while draining in-flight work after root failure in #{root_failure_asset_ref}."
  end

  defp output_metadata(metadata) when is_map(metadata), do: JsonSafe.output_metadata(metadata)
  defp output_metadata(_metadata), do: nil

  defp output_from_metadata(metadata) when is_map(metadata),
    do: value(metadata, :output) || value(metadata, :outputs) || value(metadata, :materialization)

  defp output_from_metadata(_metadata), do: nil

  defp node_ref(result) do
    case value(result, :node_key) do
      {ref, _window} -> ref
      _other -> nil
    end
  end

  defp step_event?(%RunEvent{event_type: event_type}),
    do: String.starts_with?(event_type_name(event_type), "step_")

  defp step_event?(_event), do: false

  defp event_type_name(event_type) when is_atom(event_type), do: Atom.to_string(event_type)
  defp event_type_name(event_type) when is_binary(event_type), do: event_type
  defp event_type_name(_event_type), do: ""

  defp event_step_id(run_id, %RunEvent{} = event) do
    value(event.data || %{}, :asset_step_id) ||
      safe_id("#{run_id}:#{RunQuery.public_ref(event.asset_ref)}")
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
      _other -> ExecutionStatus.normalize(status)
    end
  end

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

  defp step_explanation(status) when status in [:ok, "ok"],
    do: "Ran as part of the backend plan."

  defp step_explanation(status) when status in [:error, "error"],
    do: "Failed while executing this asset."

  defp step_explanation(_status), do: nil

  defp first_value(map, keys) do
    Enum.find_value(keys, fn key ->
      case value(map, key) do
        nil -> nil
        found -> {:found, found}
      end
    end)
    |> case do
      {:found, found} -> found
      nil -> nil
    end
  end

  defp value(map, key, default \\ nil) when is_map(map) and is_atom(key) do
    case Map.fetch(map, key) do
      {:ok, nil} -> Map.get(map, Atom.to_string(key), default)
      {:ok, found} -> found
      :error -> Map.get(map, Atom.to_string(key), default)
    end
  end

  defp duration_ms(%DateTime{} = started_at, %DateTime{} = finished_at),
    do: DateTime.diff(finished_at, started_at, :millisecond)

  defp duration_ms(_started_at, _finished_at), do: nil

  defp has_node_results?(%RunState{result: result}) when is_map(result),
    do: result |> value(:node_results, []) |> result_values() |> Enum.any?()

  defp has_node_results?(_run), do: false

  defp safe_id(value), do: value |> to_string() |> String.replace(~r/[^A-Za-z0-9_-]+/, "-")
end
