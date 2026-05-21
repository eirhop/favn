defmodule FavnOrchestrator.Repair.RuntimeState do
  @moduledoc """
  Repairs stale runtime state left behind by a stopped or crashed orchestrator.

  The repair workflow is safe to run as a dry-run first. Apply mode terminalizes
  orphaned active run snapshots, emits terminal step events for stale active step
  events, releases execution leases, expires stale materialization claims,
  reprojects affected backfill parents, and conservatively rebuilds missing
  freshness state for successful independent node results.

  ## Options

  - `:dry_run` - defaults to `true`; set to `false` to write repairs.
  - `:run_id` - limits repair to one run id.
  - `:backfill_id` - limits repair to a parent backfill and its children.
  - `:since` - limits repair to runs updated at or after the timestamp.
  - `:freshness` - defaults to `true`; set to `false` for startup recovery when
    only active runtime state should be reconciled.
  """

  alias FavnOrchestrator.Backfill.Projector, as: BackfillProjector
  alias FavnOrchestrator.ExecutionAdmission
  alias FavnOrchestrator.Repair.Passes.Freshness
  alias FavnOrchestrator.Repair.Report
  alias FavnOrchestrator.RunEvent
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage
  alias FavnOrchestrator.TransitionWriter

  @active_run_statuses [:pending, :running]
  @active_step_events [:step_started, :step_queued, :step_retry_scheduled]
  @terminal_step_events [
    :step_finished,
    :step_failed,
    :step_timed_out,
    :step_cancelled,
    :step_blocked,
    :step_skipped_fresh
  ]

  @type option ::
          {:dry_run, boolean()}
          | {:run_id, String.t() | nil}
          | {:backfill_id, String.t() | nil}
          | {:since, DateTime.t() | nil}
          | {:freshness, boolean()}

  @doc "Runs runtime-state repair and returns a summary report."
  @spec repair([option()]) :: {:ok, Report.t()} | {:error, Report.t()}
  def repair(opts \\ []) when is_list(opts) do
    mode = if Keyword.get(opts, :dry_run, true), do: :dry_run, else: :apply

    report =
      mode
      |> Report.new()
      |> expire_execution_leases(opts)
      |> expire_materialization_claims(opts)
      |> repair_active_runs(opts)
      |> maybe_repair_freshness(opts)

    if report.errors == [], do: {:ok, report}, else: {:error, report}
  end

  defp expire_execution_leases(%Report{mode: :dry_run} = report, _opts) do
    case Storage.list_execution_leases() do
      {:ok, leases} ->
        Report.bump(report, :execution_leases_expired, count_expired_leases(leases))

      {:error, reason} ->
        Report.error(report, {:execution_lease_expiry_failed, reason})
    end
  end

  defp expire_execution_leases(%Report{mode: :apply} = report, _opts) do
    case Storage.expire_execution_leases(DateTime.utc_now()) do
      {:ok, count} -> Report.bump(report, :execution_leases_expired, count)
      {:error, reason} -> Report.error(report, {:execution_lease_expiry_failed, reason})
    end
  end

  defp expire_materialization_claims(%Report{} = report, _opts) do
    if function_exported?(Storage, :expire_materialization_claims, 1) do
      expire_materialization_claims_for_mode(report, report.mode)
    else
      report
    end
  end

  defp expire_materialization_claims_for_mode(%Report{mode: :dry_run} = report, _mode) do
    if function_exported?(Storage, :list_materialization_claims, 1) do
      case apply(Storage, :list_materialization_claims, [[]]) do
        {:ok, claims} ->
          Report.bump(report, :materialization_claims_expired, count_expired_claims(claims))

        {:error, :materialization_claims_not_supported} ->
          report

        {:error, reason} ->
          Report.error(report, {:materialization_claim_expiry_failed, reason})
      end
    else
      report
    end
  end

  defp expire_materialization_claims_for_mode(%Report{mode: :apply} = report, _mode) do
    case apply(Storage, :expire_materialization_claims, [DateTime.utc_now()]) do
      {:ok, count} -> Report.bump(report, :materialization_claims_expired, count)
      {:error, :materialization_claims_not_supported} -> report
      {:error, reason} -> Report.error(report, {:materialization_claim_expiry_failed, reason})
    end
  end

  defp repair_active_runs(%Report{} = report, opts) do
    case active_runs(opts) do
      {:ok, runs} ->
        runs
        |> Enum.reduce(Report.bump(report, :runs_scanned, length(runs)), &repair_run(&1, &2))
        |> reproject_backfill_parents(runs)

      {:error, reason} ->
        Report.error(report, {:list_active_runs_failed, reason})
    end
  end

  defp active_runs(opts) do
    @active_run_statuses
    |> Enum.reduce_while({:ok, []}, fn status, {:ok, acc} ->
      case Storage.list_runs(status: status) do
        {:ok, runs} -> {:cont, {:ok, acc ++ Enum.filter(runs, &matches_filters?(&1, opts))}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp repair_run(%RunState{} = run, %Report{mode: :dry_run} = report) do
    with {:ok, events} <- run_events(run.id) do
      report
      |> Report.bump(:steps_terminalized, length(orphaned_step_events(run, events)))
      |> Report.bump(:runs_terminalized)
    else
      {:error, reason} -> Report.error(report, {:run_repair_failed, run.id, reason})
    end
  end

  defp repair_run(%RunState{} = run, %Report{mode: :apply} = report) do
    with {:ok, events} <- run_events(run.id),
         {:ok, repaired_run, step_count} <- terminalize_orphaned_steps(run, events),
         :ok <- terminalize_run(repaired_run),
         :ok <- release_execution_leases(repaired_run.id) do
      report
      |> Report.bump(:steps_terminalized, step_count)
      |> Report.bump(:runs_terminalized)
    else
      {:error, reason} -> Report.error(report, {:run_repair_failed, run.id, reason})
    end
  end

  defp terminalize_orphaned_steps(%RunState{} = run, events) do
    orphaned = orphaned_step_events(run, events)

    Enum.reduce_while(orphaned, {:ok, run, 0}, fn event, {:ok, current_run, count} ->
      next_run = RunState.transition(current_run, status: current_run.status)

      case TransitionWriter.persist_transition(
             next_run,
             :step_failed,
             step_failure_data(event, run)
           ) do
        :ok -> {:cont, {:ok, next_run, count + 1}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp terminalize_run(%RunState{} = run) do
    failed =
      RunState.transition(run,
        status: :error,
        error: orphaned_run_error(run),
        runner_execution_id: nil,
        metadata:
          run.metadata
          |> Map.put(:terminal_event_type, :run_failed)
          |> Map.put(:in_flight_execution_ids, [])
      )

    TransitionWriter.persist_transition(failed, :run_failed, %{
      status: failed.status,
      error: failed.error
    })
  end

  defp release_execution_leases(run_id) do
    ExecutionAdmission.release_run(run_id)
    :ok
  end

  defp run_events(run_id) do
    with {:ok, events} <- Storage.list_run_events(run_id) do
      {:ok, Enum.map(events, &RunEvent.from_map/1)}
    end
  end

  defp orphaned_step_events(%RunState{} = run, events) do
    events
    |> Enum.filter(&step_event?/1)
    |> Enum.group_by(&step_identity(run.id, &1))
    |> Enum.reject(fn {identity, _events} -> is_nil(identity) end)
    |> Enum.flat_map(fn {_identity, grouped} -> latest_orphaned_step_event(grouped) end)
  end

  defp latest_orphaned_step_event(events) do
    latest = Enum.max_by(events, &(&1.sequence || 0))

    if event_type(latest) in @active_step_events do
      [latest]
    else
      []
    end
  end

  defp step_failure_data(%RunEvent{} = event, %RunState{} = run) do
    data = event.data || %{}

    data
    |> copy_field(:asset_step_id)
    |> copy_field(:node_key)
    |> copy_field(:runner_execution_id)
    |> Map.put_new(:asset_ref, event.asset_ref || run.asset_ref)
    |> Map.put_new(:stage, event.stage)
    |> Map.put(:error, %{
      type: :orphaned_step_reconciled,
      run_id: run.id,
      previous_event_type: event_type(event),
      reconciled_at: DateTime.utc_now()
    })
  end

  defp copy_field(data, key) do
    case Map.get(data, key) || Map.get(data, Atom.to_string(key)) do
      nil -> data
      value -> Map.put(data, key, value)
    end
  end

  defp step_identity(run_id, %RunEvent{} = event) do
    data = event.data || %{}

    Map.get(data, :asset_step_id) || Map.get(data, "asset_step_id") ||
      case {Map.get(data, :node_key) || Map.get(data, "node_key"), event.asset_ref} do
        {nil, nil} -> nil
        {node_key, asset_ref} -> {run_id, node_key, asset_ref}
      end
  end

  defp step_event?(%RunEvent{} = event),
    do: event_type(event) in (@active_step_events ++ @terminal_step_events)

  defp event_type(%RunEvent{event_type: event_type}) when is_binary(event_type),
    do:
      Enum.find(@active_step_events ++ @terminal_step_events, &(Atom.to_string(&1) == event_type))

  defp event_type(%RunEvent{event_type: event_type}), do: event_type

  defp reproject_backfill_parents(%Report{} = report, runs) do
    parent_ids =
      runs
      |> Enum.map(&backfill_parent_id/1)
      |> Enum.reject(&is_nil/1)
      |> Enum.uniq()

    case report.mode do
      :dry_run ->
        Report.bump(report, :backfill_parents_reprojected, length(parent_ids))

      :apply ->
        Enum.reduce(parent_ids, report, fn backfill_run_id, acc ->
          case BackfillProjector.reproject_parent(backfill_run_id) do
            :ok ->
              Report.bump(acc, :backfill_parents_reprojected)

            {:error, reason} ->
              Report.error(acc, {:backfill_parent_reproject_failed, backfill_run_id, reason})
          end
        end)
    end
  end

  defp backfill_parent_id(%RunState{trigger: trigger}) when is_map(trigger) do
    case {Map.get(trigger, :kind), Map.get(trigger, :backfill_run_id)} do
      {:backfill, backfill_run_id} when is_binary(backfill_run_id) and backfill_run_id != "" ->
        backfill_run_id

      _other ->
        nil
    end
  end

  defp backfill_parent_id(_run), do: nil

  defp maybe_repair_freshness(%Report{} = report, opts) do
    if Keyword.get(opts, :freshness, true), do: Freshness.run(report, opts), else: report
  end

  defp matches_filters?(%RunState{} = run, opts) do
    matches_run_id?(run, Keyword.get(opts, :run_id)) and
      matches_backfill_id?(run, Keyword.get(opts, :backfill_id)) and
      matches_since?(run, Keyword.get(opts, :since))
  end

  defp matches_run_id?(%RunState{id: run_id}, run_id), do: true
  defp matches_run_id?(_run, nil), do: true
  defp matches_run_id?(_run, _run_id), do: false

  defp matches_backfill_id?(%RunState{} = run, backfill_id) when is_binary(backfill_id),
    do:
      run.id == backfill_id or run.parent_run_id == backfill_id or
        backfill_parent_id(run) == backfill_id

  defp matches_backfill_id?(_run, nil), do: true

  defp matches_since?(%RunState{updated_at: %DateTime{} = updated_at}, %DateTime{} = since),
    do: DateTime.compare(updated_at, since) in [:gt, :eq]

  defp matches_since?(_run, nil), do: true
  defp matches_since?(_run, %DateTime{}), do: false

  defp count_expired_leases(leases) when is_list(leases) do
    now = DateTime.utc_now()

    Enum.count(leases, fn lease ->
      case Map.get(lease, :expires_at) || Map.get(lease, "expires_at") do
        %DateTime{} = expires_at -> DateTime.compare(expires_at, now) != :gt
        _other -> false
      end
    end)
  end

  defp count_expired_claims(claims) when is_list(claims) do
    now = DateTime.utc_now()

    Enum.count(claims, fn claim ->
      status = Map.get(claim, :status) || Map.get(claim, "status")

      case Map.get(claim, :expires_at) || Map.get(claim, "expires_at") do
        %DateTime{} = expires_at ->
          status in [:claimed, "claimed"] and DateTime.compare(expires_at, now) != :gt

        _other ->
          false
      end
    end)
  end

  defp orphaned_run_error(%RunState{} = run) do
    %{
      type: :orphaned_run_reconciled,
      scope: :local_single_node,
      previous_status: run.status,
      reconciled_at: DateTime.utc_now()
    }
  end
end
