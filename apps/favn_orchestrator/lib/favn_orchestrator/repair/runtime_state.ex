defmodule FavnOrchestrator.Repair.RuntimeState do
  @moduledoc """
  Repairs stale runtime state left behind by a stopped or crashed orchestrator.

  The repair workflow is safe to run as a dry-run first. Apply mode terminalizes
  orphaned active run snapshots, emits terminal step events for stale active step
  events, releases execution leases, expires stale materialization claims,
  reconciles stale backfill windows, reprojects affected backfill parents, and
  conservatively rebuilds missing freshness state for successful independent
  node results.

  ## Options

  - `:dry_run` - defaults to `true`; set to `false` to write repairs.
  - `:run_id` - limits repair to one run id.
  - `:backfill_id` - limits repair to a parent backfill and its children.
  - `:since` - limits repair to runs updated at or after the timestamp.
  - `:freshness` - defaults to `true`; set to `false` for startup recovery when
    only active runtime state should be reconciled.
  """

  alias FavnOrchestrator.Backfill.BackfillWindow
  alias FavnOrchestrator.Backfill.Projector, as: BackfillProjector
  alias FavnOrchestrator.ExecutionAdmission
  alias FavnOrchestrator.Repair.Passes.Freshness
  alias FavnOrchestrator.Repair.Report
  alias FavnOrchestrator.RunEvent
  alias FavnOrchestrator.RunExecutionCleanup
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage
  alias FavnOrchestrator.TransitionWriter

  @active_run_statuses [:pending, :running]
  @active_backfill_window_statuses [:pending, :running]
  @terminal_run_statuses [:ok, :partial, :error, :cancelled, :timed_out]
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
    case validate_options(opts) do
      {:ok, opts} -> run_repair(opts)
      {:error, reason} -> {:error, Report.new(:dry_run) |> Report.error(reason)}
    end
  end

  defp run_repair(opts) do
    mode = if Keyword.fetch!(opts, :dry_run), do: :dry_run, else: :apply

    report =
      mode
      |> Report.new()
      |> expire_execution_leases(opts)
      |> expire_materialization_claims(opts)
      |> repair_active_runs(opts)
      |> repair_backfill_windows(opts)
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
    expire_materialization_claims_for_mode(report, report.mode)
  end

  defp expire_materialization_claims_for_mode(%Report{mode: :dry_run} = report, _mode) do
    case Storage.list_materialization_claims() do
      {:ok, claims} ->
        Report.bump(report, :materialization_claims_expired, count_expired_claims(claims))

      {:error, :materialization_claims_not_supported} ->
        report

      {:error, reason} ->
        Report.error(report, {:materialization_claim_expiry_failed, reason})
    end
  end

  defp expire_materialization_claims_for_mode(%Report{mode: :apply} = report, _mode) do
    case Storage.expire_materialization_claims(DateTime.utc_now()) do
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
    cond do
      is_binary(opts[:run_id]) ->
        one_active_run(opts[:run_id], opts)

      is_binary(opts[:backfill_id]) ->
        with {:ok, runs} <- Storage.list_execution_group_runs(opts[:backfill_id]) do
          {:ok, Enum.filter(runs, &(active_run?(&1) and matches_filters?(&1, opts)))}
        end

      true ->
        list_all_active_runs(opts)
    end
  end

  defp one_active_run(run_id, opts) do
    case Storage.get_run(run_id) do
      {:ok, run} -> {:ok, Enum.filter([run], &(active_run?(&1) and matches_filters?(&1, opts)))}
      {:error, _reason} = error -> error
    end
  end

  defp list_all_active_runs(opts) do
    @active_run_statuses
    |> Enum.reduce_while({:ok, []}, fn status, {:ok, acc} ->
      case Storage.list_runs(status: status) do
        {:ok, runs} ->
          matching = Enum.filter(runs, &matches_filters?(&1, opts))
          {:cont, {:ok, Enum.reduce(matching, acc, &[&1 | &2])}}

        {:error, reason} ->
          {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, runs} -> {:ok, Enum.reverse(runs)}
      {:error, _reason} = error -> error
    end
  end

  defp active_run?(%RunState{status: status}), do: status in @active_run_statuses

  defp repair_run(%RunState{} = run, %Report{mode: :dry_run} = report) do
    case run_events(run.id) do
      {:ok, events} ->
        report
        |> Report.bump(:steps_terminalized, length(orphaned_step_events(run, events)))
        |> Report.bump(:runs_terminalized)

      {:error, reason} ->
        Report.error(report, {:run_repair_failed, run.id, reason})
    end
  end

  defp repair_run(%RunState{} = run, %Report{mode: :apply} = report) do
    with {:ok, events} <- run_events(run.id),
         cleanup_statuses = RunExecutionCleanup.cancel_active(run, orphaned_run_error(run)),
         {:ok, repaired_run, step_count} <- terminalize_orphaned_steps(run, events),
         :ok <- terminalize_run(repaired_run, cleanup_statuses) do
      repaired_report =
        report
        |> Report.bump(:steps_terminalized, step_count)
        |> Report.bump(:runs_terminalized)

      cleanup_repaired_run(repaired_report, repaired_run.id, cleanup_statuses)
    else
      {:error, reason} -> Report.error(report, {:run_repair_failed, run.id, reason})
    end
  end

  defp cleanup_repaired_run(report, run_id, cleanup_statuses) do
    if RunExecutionCleanup.confirmed?(cleanup_statuses) do
      case release_execution_leases(run_id) do
        :ok -> report
        {:error, reason} -> Report.error(report, {:run_lease_release_failed, run_id, reason})
      end
    else
      Report.error(report, {:runner_cleanup_unconfirmed, run_id, cleanup_statuses})
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

  defp terminalize_run(%RunState{} = run, cleanup_statuses) do
    failed =
      RunState.transition(run,
        status: :error,
        error: Map.put(orphaned_run_error(run), :runner_cleanup, cleanup_statuses),
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

  defp backfill_parent_id(%RunState{} = run) do
    case {trigger_field(run, :kind), trigger_field(run, :backfill_run_id)} do
      {:backfill, backfill_run_id} when is_binary(backfill_run_id) and backfill_run_id != "" ->
        backfill_run_id

      {"backfill", backfill_run_id} when is_binary(backfill_run_id) and backfill_run_id != "" ->
        backfill_run_id

      _other ->
        nil
    end
  end

  defp repair_backfill_windows(%Report{} = report, opts) do
    case active_backfill_windows(opts) do
      {:ok, windows} ->
        Enum.reduce(windows, report, fn window, acc ->
          repair_backfill_window(window, acc, opts)
        end)

      {:error, reason} ->
        Report.error(report, {:list_active_backfill_windows_failed, reason})
    end
  end

  defp active_backfill_windows(opts) do
    @active_backfill_window_statuses
    |> Enum.reduce_while({:ok, []}, fn status, {:ok, acc} ->
      filters = window_filters(status, opts)

      case BackfillProjector.list_all_backfill_windows(filters) do
        {:ok, windows} -> {:cont, {:ok, Enum.reduce(windows, acc, &[&1 | &2])}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, windows} -> {:ok, Enum.reverse(windows)}
      {:error, _reason} = error -> error
    end
  end

  defp window_filters(status, opts) do
    filters = [status: status]

    case Keyword.get(opts, :backfill_id) do
      backfill_id when is_binary(backfill_id) ->
        Keyword.put(filters, :backfill_run_id, backfill_id)

      _other ->
        filters
    end
  end

  defp repair_backfill_window(%BackfillWindow{} = window, %Report{} = report, opts) do
    case terminal_window_child(window) do
      {:ok, %RunState{} = run} ->
        repair_backfill_window(window, run, report, opts)

      :ignore ->
        report

      {:error, reason} ->
        Report.error(report, {:backfill_window_repair_failed, window_id(window), reason})
    end
  end

  defp repair_backfill_window(
         %BackfillWindow{} = window,
         %RunState{} = run,
         %Report{mode: :dry_run} = report,
         opts
       ) do
    if matches_filters?(run, opts) and run_matches_window?(run, window) do
      Report.bump(report, :backfill_windows_reconciled)
    else
      report
    end
  end

  defp repair_backfill_window(
         %BackfillWindow{} = window,
         %RunState{} = run,
         %Report{mode: :apply} = report,
         opts
       ) do
    if matches_filters?(run, opts) and run_matches_window?(run, window) do
      case BackfillProjector.reproject_child_window(run) do
        :ok ->
          Report.bump(report, :backfill_windows_reconciled)

        :ignore ->
          report

        {:error, reason} ->
          Report.error(report, {:backfill_window_repair_failed, window_id(window), reason})
      end
    else
      report
    end
  end

  defp terminal_window_child(%BackfillWindow{} = window) do
    case window.latest_attempt_run_id || window.child_run_id do
      run_id when is_binary(run_id) and run_id != "" ->
        case Storage.get_run(run_id) do
          {:ok, %RunState{status: status} = run} when status in @terminal_run_statuses ->
            {:ok, run}

          {:ok, %RunState{}} ->
            :ignore

          {:error, :not_found} ->
            :ignore

          {:error, reason} ->
            {:error, reason}
        end

      _other ->
        :ignore
    end
  end

  defp run_matches_window?(%RunState{} = run, %BackfillWindow{} = window) do
    backfill_parent_id(run) == window.backfill_run_id and
      trigger_field(run, :window_key) == window.window_key
  end

  defp trigger_field(%RunState{trigger: trigger}, key) when is_map(trigger) do
    Map.get(trigger, key) || Map.get(trigger, Atom.to_string(key))
  end

  defp trigger_field(%RunState{}, _key), do: nil

  defp window_id(%BackfillWindow{} = window) do
    %{
      backfill_run_id: window.backfill_run_id,
      pipeline_module: window.pipeline_module,
      window_key: window.window_key
    }
  end

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

  defp validate_options(opts) do
    allowed = [:dry_run, :run_id, :backfill_id, :since, :freshness]

    cond do
      not Keyword.keyword?(opts) ->
        {:error, :invalid_runtime_repair_options}

      Keyword.keys(opts) -- allowed != [] ->
        {:error, {:invalid_runtime_repair_options, Keyword.keys(opts) -- allowed}}

      not is_boolean(Keyword.get(opts, :dry_run, true)) ->
        {:error, {:invalid_runtime_repair_option, :dry_run}}

      not valid_optional_id?(Keyword.get(opts, :run_id)) ->
        {:error, {:invalid_runtime_repair_option, :run_id}}

      not valid_optional_id?(Keyword.get(opts, :backfill_id)) ->
        {:error, {:invalid_runtime_repair_option, :backfill_id}}

      not valid_optional_datetime?(Keyword.get(opts, :since)) ->
        {:error, {:invalid_runtime_repair_option, :since}}

      not is_boolean(Keyword.get(opts, :freshness, true)) ->
        {:error, {:invalid_runtime_repair_option, :freshness}}

      true ->
        {:ok,
         opts
         |> Keyword.put_new(:dry_run, true)
         |> Keyword.put_new(:freshness, true)}
    end
  end

  defp valid_optional_id?(nil), do: true
  defp valid_optional_id?(value), do: is_binary(value) and byte_size(value) > 0

  defp valid_optional_datetime?(nil), do: true
  defp valid_optional_datetime?(%DateTime{}), do: true
  defp valid_optional_datetime?(_value), do: false
end
