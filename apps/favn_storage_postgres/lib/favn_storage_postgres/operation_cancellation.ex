defmodule FavnStoragePostgres.OperationCancellation do
  @moduledoc false
  import Ecto.Query
  alias FavnOrchestrator.Persistence.BackfillPlan
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Results.{CancellationScope, CancellationWork, CursorPage}
  alias FavnStoragePostgres.{CancellationOwnership, Repo}
  alias FavnStoragePostgres.Outbox.Writer, as: OutboxWriter

  alias FavnStoragePostgres.Schemas.{
    AssetTargetGeneration,
    Materialization,
    Backfill,
    BackfillWindow,
    ResourceRecoveryCandidate,
    Run,
    RunSubmission,
    RunnerTask
  }

  @batch_size 100
  @active_submissions ~w(queued preparing admitting)
  @active_tasks ~w(queued assigned preparing running cancelling)

  def scope!(workspace, run_id) do
    owner = CancellationOwnership.owner!(workspace, run_id)
    backfill = CancellationOwnership.backfill!(workspace, owner)
    root = Repo.get_by(Run, workspace_id: workspace, run_id: owner)
    submission = Repo.get_by(RunSubmission, workspace_id: workspace, run_id: owner)

    if is_nil(root) and is_nil(submission),
      do: Repo.rollback(Error.new(:not_found, "run not found"))

    status =
      cond do
        backfill && backfill.cancellation_requested_at ->
          backfill.status

        root && root.cancellation_status ->
          root.cancellation_status

        (is_nil(root) and submission) && submission.cancellation_requested_at ->
          cond do
            submission.status in @active_submissions -> "cancelling"
            submission.failure_kind == "unknown" -> "needs_attention"
            true -> "cancelled"
          end

        true ->
          nil
      end

    %CancellationScope{
      run_id: owner,
      kind: if(backfill, do: :backfill, else: :run),
      label: if(backfill, do: "Cancel full backfill", else: "Cancel run"),
      status: status_atom(status),
      cancellable?: is_nil(status) and unfinished?(workspace, owner, backfill)
    }
  end

  def request!(command, request_run) do
    workspace = command.workspace_context.workspace_id
    owner = CancellationOwnership.lock!(workspace, command.run_id)
    scope = scope!(workspace, owner)

    cond do
      not is_nil(scope.status) ->
        :ok

      not scope.cancellable? ->
        Repo.rollback(Error.new(:conflict, "operation is already terminal"))

      scope.kind == :backfill ->
        backfill = CancellationOwnership.backfill!(workspace, owner)
        update_backfill!(backfill, "cancelling", command.occurred_at, command.reason)

      root = Repo.get_by(Run, workspace_id: workspace, run_id: owner) ->
        request_run.(%{command | run_id: owner})
        update_run_status!(root, "cancelling", command.occurred_at)

      true ->
        submission = Repo.get_by!(RunSubmission, workspace_id: workspace, run_id: owner)
        cancel_submission!(submission, command.occurred_at)

        OutboxWriter.insert!(%{
          workspace_id: workspace,
          command_id: command.command_id,
          event_kind: "submission.cancellation.requested",
          aggregate_kind: "run_submission",
          aggregate_id: submission.submission_id,
          aggregate_version: submission.claim_generation + 1,
          occurred_at: command.occurred_at,
          payload: %{"run_id" => owner}
        })
    end

    :ok
  end

  def page!(query) do
    workspace = query.workspace_context.workspace_id

    queryable =
      case query.kind do
        :run ->
          runs =
            from(r in Run,
              where:
                r.workspace_id == ^workspace and
                  (r.cancellation_status == "cancelling" or
                     (not is_nil(r.cancellation_requested_at) and
                        r.status in ["pending", "running"])),
              select: %{run_id: r.run_id}
            )

          submissions =
            from(s in RunSubmission,
              where:
                s.workspace_id == ^workspace and
                  not is_nil(s.cancellation_requested_at) and s.status in @active_submissions,
              select: %{run_id: s.run_id}
            )

          subquery(union(runs, ^submissions))

        :backfill ->
          subquery(
            from(b in Backfill,
              where: b.workspace_id == ^workspace and b.status == "cancelling",
              select: %{run_id: b.root_run_id}
            )
          )
      end

    rows =
      Repo.all(
        from(r in queryable,
          where: r.run_id > ^(query.after || ""),
          order_by: [asc: r.run_id],
          select: r.run_id,
          limit: ^(query.limit + 1)
        )
      )

    ids = Enum.take(rows, query.limit)

    %CursorPage{
      items: ids,
      limit: query.limit,
      has_more?: length(rows) > query.limit,
      next_cursor: if(length(rows) > query.limit, do: List.last(ids))
    }
  end

  def reconcile!(workspace, run_id) do
    owner = CancellationOwnership.lock!(workspace, run_id)
    scope = scope!(workspace, owner)

    if scope.status == :cancelling do
      now = DateTime.utc_now()
      backfill = CancellationOwnership.backfill!(workspace, owner)
      cancel_windows!(backfill, now)

      submissions(workspace, owner)
      |> where([s], s.status in @active_submissions)
      |> order_by([s], asc: s.updated_at, asc: s.run_id)
      |> limit(@batch_size)
      |> lock("FOR UPDATE")
      |> Repo.all()
      |> Enum.each(&cancel_submission!(&1, now))

      candidates(workspace, owner)
      |> where([c], c.status in ["pending", "claimed"])
      |> order_by([c], asc: c.candidate_id)
      |> limit(@batch_size)
      |> lock("FOR UPDATE")
      |> Repo.all()
      |> Enum.each(fn candidate ->
        candidate
        |> Ecto.Changeset.change(
          status: "cancelled",
          claim_owner: nil,
          claim_expires_at: nil,
          updated_at: now
        )
        |> Repo.update!()
      end)

      work = %CancellationWork{
        run_ids:
          Repo.all(
            from(r in runs(workspace, owner),
              where: r.status in ["pending", "running"],
              order_by: [asc: r.updated_at, asc: r.run_id],
              limit: @batch_size,
              select: r.run_id
            )
          ),
        task_ids:
          Repo.all(
            from(t in tasks(workspace, owner),
              where: t.status in @active_tasks,
              order_by: [asc: t.updated_at, asc: t.task_id],
              limit: @batch_size,
              select: t.task_id
            )
          )
      }

      unless unfinished?(workspace, owner, backfill) do
        status = if uncertain?(workspace, owner), do: "needs_attention", else: "cancelled"

        if backfill do
          update_backfill!(backfill, status, now, backfill.cancellation_reason)
        else
          if root = Repo.get_by(Run, workspace_id: workspace, run_id: owner),
            do: update_run_status!(root, status, now)
        end
      end

      work
    else
      reconcile_exact!(workspace, run_id)
    end
  end

  defp reconcile_exact!(workspace, run_id) do
    run = Repo.get_by(Run, workspace_id: workspace, run_id: run_id)
    submission = Repo.get_by(RunSubmission, workspace_id: workspace, run_id: run_id)

    requested? =
      not is_nil(run && run.cancellation_requested_at) or
        not is_nil(submission && submission.cancellation_requested_at)

    if requested? do
      if submission && submission.status in @active_submissions,
        do: cancel_submission!(submission, DateTime.utc_now())

      %CancellationWork{
        run_ids: if(run && run.status in ["pending", "running"], do: [run_id], else: []),
        task_ids:
          Repo.all(
            from(t in RunnerTask,
              where:
                t.workspace_id == ^workspace and t.run_id == ^run_id and
                  t.status in @active_tasks,
              order_by: [asc: t.updated_at, asc: t.task_id],
              limit: @batch_size,
              select: t.task_id
            )
          )
      }
    else
      %CancellationWork{}
    end
  end

  defp cancel_submission!(submission, now) do
    submission =
      Repo.one!(
        from(s in RunSubmission,
          where:
            s.workspace_id == ^submission.workspace_id and
              s.run_id == ^submission.run_id,
          lock: "FOR UPDATE"
        )
      )

    run_exists? =
      Repo.exists?(
        from(r in Run,
          where: r.workspace_id == ^submission.workspace_id and r.run_id == ^submission.run_id
        )
      )

    status =
      cond do
        submission.status == "admitting" and run_exists? ->
          "submitted"

        submission.status == "queued" ->
          "cancelled"

        submission.claim_expires_at && DateTime.compare(submission.claim_expires_at, now) != :gt ->
          "cancelled"

        true ->
          submission.status
      end

    changes = %{
      status: status,
      cancellation_requested_at: submission.cancellation_requested_at || now,
      cancellation_reason: submission.cancellation_reason || "operation_cancelled",
      updated_at: now
    }

    changes =
      if status in ["submitted", "cancelled"],
        do:
          Map.merge(changes, %{
            claim_owner: nil,
            claim_expires_at: nil,
            terminal_at: submission.terminal_at || now
          }),
        else: changes

    changes =
      if status == "submitted",
        do: Map.put(changes, :outcome, %{"run_id" => submission.run_id}),
        else: changes

    submission |> Ecto.Changeset.change(changes) |> Repo.update!()
  end

  defp cancel_windows!(nil, _now), do: :ok

  defp cancel_windows!(backfill, now) do
    windows(backfill)
    |> where([w], w.status in ["planned", "ready", "claimed", "running"])
    |> order_by([w], asc: w.window_id)
    |> limit(@batch_size)
    |> lock("FOR UPDATE")
    |> Repo.all()
    |> Enum.each(fn window ->
      run_id =
        window.run_id ||
          BackfillPlan.child_run_id(window.backfill_id, window.window_id, window.payload)

      run = Repo.get_by(Run, workspace_id: window.workspace_id, run_id: run_id)
      submission = Repo.get_by(RunSubmission, workspace_id: window.workspace_id, run_id: run_id)

      unsettled? =
        (not is_nil(run) and run.status in ["pending", "running"]) or
          (not is_nil(submission) and submission.status in @active_submissions)

      status =
        case run do
          %{status: "ok"} -> "succeeded"
          %{status: status} when status in ["error", "partial", "timed_out"] -> "failed"
          _ -> "cancelled"
        end

      unless unsettled? do
        updated =
          window
          |> Ecto.Changeset.change(status: status, version: window.version + 1, updated_at: now)
          |> Repo.update!()

        OutboxWriter.insert!(%{
          workspace_id: window.workspace_id,
          command_id:
            transition_id(
              "cancel-window",
              {window.backfill_id, window.window_id, updated.version}
            ),
          event_kind: "backfill.window." <> status,
          aggregate_kind: "backfill_window",
          aggregate_id: window.backfill_id <> ":" <> window.window_id,
          aggregate_version: updated.version,
          occurred_at: now,
          payload: %{
            "backfill_id" => window.backfill_id,
            "window_id" => window.window_id,
            "run_id" => window.run_id,
            "previous_status" => if(window.status == "claimed", do: "ready", else: window.status),
            "status" => status
          }
        })
      end
    end)
  end

  defp update_run_status!(run, status, now) do
    run |> Ecto.Changeset.change(cancellation_status: status) |> Repo.update!()

    OutboxWriter.insert!(%{
      workspace_id: run.workspace_id,
      command_id: transition_id("cancel-operation", {run.run_id, status}),
      event_kind: "operation.cancellation." <> status,
      aggregate_kind: "run_cancellation",
      aggregate_id: run.run_id,
      aggregate_version: if(status == "cancelling", do: 1, else: 2),
      occurred_at: now,
      payload: %{"run_id" => run.run_id, "status" => status}
    })
  end

  defp update_backfill!(backfill, status, now, reason) do
    updated =
      backfill
      |> Ecto.Changeset.change(
        status: status,
        cancellation_requested_at: backfill.cancellation_requested_at || now,
        cancellation_reason: reason,
        version: backfill.version + 1,
        updated_at: now
      )
      |> Repo.update!()

    OutboxWriter.insert!(%{
      workspace_id: backfill.workspace_id,
      command_id: transition_id("cancel-backfill", {backfill.backfill_id, updated.version}),
      event_kind: "backfill.cancellation." <> status,
      aggregate_kind: "backfill",
      aggregate_id: backfill.backfill_id,
      aggregate_version: updated.version,
      occurred_at: now,
      payload: %{"backfill_id" => backfill.backfill_id, "status" => status}
    })
  end

  defp unfinished?(workspace, owner, backfill) do
    Repo.exists?(from(r in runs(workspace, owner), where: r.status in ["pending", "running"])) or
      Repo.exists?(
        from(s in submissions(workspace, owner), where: s.status in @active_submissions)
      ) or
      Repo.exists?(from(t in tasks(workspace, owner), where: t.status in @active_tasks)) or
      Repo.exists?(
        from(c in candidates(workspace, owner), where: c.status in ["pending", "claimed"])
      ) or
      (not is_nil(backfill) and
         (backfill.status == "planning" or
            Repo.exists?(
              from(w in windows(backfill),
                where: w.status in ["planned", "ready", "claimed", "running"]
              )
            )))
  end

  defp uncertain?(workspace, owner) do
    Repo.exists?(
      from(m in Materialization,
        join: r in Run,
        on: r.workspace_id == m.workspace_id and r.run_id == m.run_id,
        join: g in AssetTargetGeneration,
        on:
          g.workspace_id == m.workspace_id and g.target_id == m.target_id and
            g.target_generation_id == m.target_generation_id,
        where:
          r.workspace_id == ^workspace and r.cancellation_owner_run_id == ^owner and
            g.status == "building" and is_nil(g.creating_rebuild_operation_id)
      )
    ) or
      Repo.exists?(from(t in tasks(workspace, owner), where: t.status == "unknown")) or
      Repo.exists?(from(s in submissions(workspace, owner), where: s.failure_kind == "unknown")) or
      Repo.exists?(
        from(r in runs(workspace, owner),
          where: fragment("?->'metadata'->>'cancellation_needs_attention' = 'true'", r.snapshot)
        )
      )
  end

  defp runs(workspace, owner) do
    from(r in Run, where: r.workspace_id == ^workspace and r.cancellation_owner_run_id == ^owner)
  end

  defp submissions(workspace, owner) do
    from(s in RunSubmission,
      where: s.workspace_id == ^workspace and s.cancellation_owner_run_id == ^owner
    )
  end

  defp tasks(workspace, owner) do
    from(t in RunnerTask,
      join: r in Run,
      on: r.workspace_id == t.workspace_id and r.run_id == t.run_id,
      where: r.workspace_id == ^workspace and r.cancellation_owner_run_id == ^owner
    )
  end

  defp candidates(workspace, owner) do
    from(c in ResourceRecoveryCandidate,
      join: r in Run,
      on: r.workspace_id == c.workspace_id and r.run_id == c.source_run_id,
      where: r.workspace_id == ^workspace and r.cancellation_owner_run_id == ^owner
    )
  end

  defp windows(backfill) do
    from(w in BackfillWindow,
      where: w.workspace_id == ^backfill.workspace_id and w.backfill_id == ^backfill.backfill_id
    )
  end

  defp transition_id(prefix, identity) do
    hash =
      identity
      |> :erlang.term_to_binary([:deterministic])
      |> then(&:crypto.hash(:sha256, &1))
      |> Base.url_encode64(padding: false)

    prefix <> ":" <> hash
  end

  defp status_atom(nil), do: nil
  defp status_atom("cancelling"), do: :cancelling
  defp status_atom("cancelled"), do: :cancelled
  defp status_atom("needs_attention"), do: :needs_attention
end
