defmodule FavnOrchestrator.RunReadModel do
  @moduledoc """
  Public read models retained for run logs and execution-group lists.

  The operator run screen uses `FavnOrchestrator.OperatorRunView` instead. This
  module deliberately does not provide a second, broad run-screen projection.
  """

  alias Favn.Log.Filter
  alias Favn.RuntimeInput.Pin
  alias FavnOrchestrator.ExecutionStatus

  alias FavnOrchestrator.Persistence.Results.ExecutionGroupOverview,
    as: PersistedExecutionGroupOverview

  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.RunEvent
  alias FavnOrchestrator.RunReadModel.StepProjection
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Runs
  alias FavnOrchestrator.WindowSummary

  @type run_role :: :asset | :pipeline | :backfill_parent | :backfill_child | :rerun
  @type window_summary :: WindowSummary.t()
  @type step_summary :: StepProjection.t()

  @type progress_summary :: %{
          required(:unit) => :assets | :steps | :windows,
          required(:label) => String.t(),
          required(:counts) => map()
        }

  @type run_summary :: %{
          required(:id) => String.t(),
          required(:kind) => run_role(),
          required(:role) => run_role(),
          required(:status) => RunState.status(),
          required(:submit_kind) => FavnOrchestrator.Persistence.RunEnum.submit_kind(),
          required(:manifest_version_id) => String.t(),
          required(:runner_releases) => Favn.RunnerPool.releases(),
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
          required(:status) => ExecutionStatus.known(),
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
          required(:runner_task_id) => String.t() | nil,
          required(:event_seq) => pos_integer(),
          required(:steps) => [step_summary()],
          required(:backfill_failures) => [backfill_failure()],
          required(:backfill_failure_count) => non_neg_integer(),
          required(:events) => [RunEvent.t()],
          required(:retry) => map(),
          required(:runtime_input_pins) => [map()]
        }

  @type execution_group_summary :: %{
          required(:id) => String.t(),
          required(:root_execution_group_id) => String.t(),
          required(:status) => atom(),
          required(:health) => :ok | :warning | :error | :active,
          required(:active?) => boolean(),
          required(:trigger_type) => atom() | nil,
          required(:target_assets) => [String.t()],
          required(:target_pipelines) => [String.t()],
          required(:asset_counts) => map(),
          required(:root_status) => atom(),
          required(:started_at) => DateTime.t() | nil,
          required(:finished_at) => DateTime.t() | nil,
          required(:duration_ms) => non_neg_integer() | nil,
          required(:total_windows) => non_neg_integer(),
          required(:completed_windows) => non_neg_integer(),
          required(:failed_windows) => non_neg_integer(),
          required(:total_asset_attempts) => non_neg_integer(),
          required(:completed_asset_attempts) => non_neg_integer(),
          required(:succeeded_asset_attempts) => non_neg_integer(),
          required(:skipped_asset_attempts) => non_neg_integer(),
          required(:failed_asset_attempts) => non_neg_integer(),
          required(:running_asset_attempts) => non_neg_integer(),
          required(:queued_asset_attempts) => non_neg_integer(),
          required(:planned_asset_attempts) => non_neg_integer(),
          required(:failure_count) => non_neg_integer(),
          required(:progress) => progress_summary() | nil,
          required(:summary_totals) => map(),
          required(:last_activity_at) => DateTime.t() | nil,
          required(:currently_running_asset_attempts) => [],
          required(:child_run_ids) => []
        }

  @type execution_group_detail :: %{
          required(:overview) => execution_group_summary(),
          required(:child_runs) => [map()],
          required(:windows) => [map()],
          required(:failures) => [map()]
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

  @doc "Expands one compact execution-group row into the public list shape."
  @spec from_execution_group_overview(PersistedExecutionGroupOverview.t()) ::
          execution_group_summary()
  def from_execution_group_overview(%PersistedExecutionGroupOverview{} = group) do
    status = public_overview_status(group.status)
    completed = group.succeeded_count + group.failed_count

    attempt_counts = %{
      total: group.run_count,
      completed: completed,
      succeeded: group.succeeded_count,
      skipped: 0,
      failed: group.failed_count,
      running: group.running_count,
      queued: group.pending_count,
      planned: 0
    }

    active? =
      status in [:pending, :running] or group.running_count > 0 or group.pending_count > 0

    %{
      id: group.root_run_id,
      root_execution_group_id: group.root_run_id,
      status: status,
      health: execution_group_health(status, group.failed_count, active?),
      active?: active?,
      trigger_type: group.trigger_type,
      target_assets: group.target_refs,
      target_pipelines: group.pipeline_refs,
      asset_counts: asset_counts(group.asset_counts || %{}),
      root_status: status,
      started_at: group.started_at || group.updated_at,
      finished_at: overview_finished_at(group, active?),
      duration_ms: duration_ms(group.started_at, group.finished_at),
      total_windows: 0,
      completed_windows: 0,
      failed_windows: 0,
      total_asset_attempts: group.run_count,
      completed_asset_attempts: completed,
      succeeded_asset_attempts: group.succeeded_count,
      skipped_asset_attempts: 0,
      failed_asset_attempts: group.failed_count,
      running_asset_attempts: group.running_count,
      queued_asset_attempts: group.pending_count,
      planned_asset_attempts: 0,
      failure_count: group.failed_count,
      progress: execution_group_progress(attempt_counts),
      summary_totals: %{
        windows: %{total: 0, completed: 0, failed: 0},
        asset_attempts: attempt_counts
      },
      last_activity_at: group.updated_at,
      currently_running_asset_attempts: [],
      child_run_ids: []
    }
  end

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
         runner_task_id: run.runner_task_id,
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

  @doc "Returns public asset-step log context under an explicit workspace authority."
  @spec get_asset_step_log_context(WorkspaceContext.t(), String.t(), String.t()) ::
          {:ok, asset_step_log_context()} | {:error, term()}
  def get_asset_step_log_context(%WorkspaceContext{} = context, run_id, asset_step_id)
      when is_binary(run_id) and is_binary(asset_step_id) do
    with {:ok, detail} <- get_run_detail(context, run_id) do
      step = Enum.find(detail.steps, &(&1.id == asset_step_id))
      filter = %Filter{run_id: run_id, asset_step_id: asset_step_id}
      {:ok, asset_step_log_context(detail, step, asset_step_id, filter)}
    end
  end

  defp ensure_retry_checkpoint_events(context, runs, events) do
    Enum.reduce_while(runs, {:ok, events}, fn run, {:ok, acc} ->
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

  defp summary(%RunState{} = run) do
    role = classify(run)
    progress = progress(run, role)

    %{
      id: run.id,
      kind: role,
      role: role,
      status: run.status,
      submit_kind: run.submit_kind,
      manifest_version_id: run.manifest_version_id,
      runner_releases: run.runner_releases,
      asset_ref: run.asset_ref,
      target_refs: run.target_refs,
      parent_run_id: run.parent_run_id,
      root_run_id: run.root_run_id,
      rerun_of_run_id: run.rerun_of_run_id,
      window: WindowSummary.from_run(run),
      progress_unit: if(progress, do: progress.unit),
      progress: progress,
      started_at: run.inserted_at,
      finished_at: finished_at(run),
      updated_at: run.updated_at,
      duration_ms: duration_ms(run)
    }
  end

  defp with_public_status(%RunState{} = run), do: %{run | status: public_status(run)}

  defp public_status(%RunState{submit_kind: :pipeline, status: :ok} = run) do
    if StepProjection.incomplete?(run), do: :running, else: :ok
  end

  defp public_status(%RunState{status: status}), do: status

  defp classify(%RunState{submit_kind: :rerun}), do: :rerun

  defp classify(%RunState{parent_run_id: parent_run_id}) when is_binary(parent_run_id),
    do: :backfill_child

  defp classify(%RunState{submit_kind: submit_kind})
       when submit_kind in [:backfill_asset, :backfill_pipeline],
       do: :backfill_parent

  defp classify(%RunState{submit_kind: :pipeline}), do: :pipeline
  defp classify(%RunState{}), do: :asset

  defp progress(%RunState{} = run, role) do
    step_progress = StepProjection.progress(run)
    unit = if role in [:pipeline, :backfill_child], do: :steps, else: step_progress.unit

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

  defp asset_step_log_context(detail, step, asset_step_id, log_filter) do
    %{
      run: detail.summary,
      step: step,
      title: (step && step.asset_ref) || "Asset logs",
      subtitle: "Run #{short_id(detail.summary.id)} · Asset step #{asset_step_id}",
      status: step && step.status,
      facts: step_facts(step),
      log_filter: log_filter,
      fallback?: false,
      note: if(step, do: nil, else: "Asset step context not found, showing matching logs.")
    }
  end

  defp step_facts(nil), do: []

  defp step_facts(step) do
    [
      %{label: "Started", value: step.started_at},
      %{label: "Duration", value: step.duration_ms},
      %{label: "Attempt", value: step.attempt}
    ]
  end

  defp execution_group_health(_status, failure_count, _active?) when failure_count > 0,
    do: :error

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

  defp asset_counts(counts) do
    %{
      total: Map.get(counts, :total, 0),
      completed: Map.get(counts, :completed, 0),
      failed: Map.get(counts, :failed, 0),
      running: Map.get(counts, :running, 0),
      queued: Map.get(counts, :queued, 0)
    }
  end

  defp public_overview_status(:succeeded), do: :ok
  defp public_overview_status(:failed), do: :error
  defp public_overview_status(status), do: status

  defp overview_finished_at(_group, true), do: nil
  defp overview_finished_at(group, false), do: group.finished_at || group.updated_at

  defp unit_label(:assets, 1), do: "asset"
  defp unit_label(:assets, _total), do: "assets"
  defp unit_label(:steps, 1), do: "step"
  defp unit_label(:steps, _total), do: "steps"

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
