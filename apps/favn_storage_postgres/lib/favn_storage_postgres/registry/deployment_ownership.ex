defmodule FavnStoragePostgres.Registry.DeploymentOwnership do
  @moduledoc false
  import Ecto.Query
  alias Ecto.Adapters.SQL
  alias FavnOrchestrator.Persistence.Commands, as: C
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnStoragePostgres.ErrorMapper
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.Registry.Store
  alias FavnStoragePostgres.RunnerTasks.Store, as: TaskStore
  alias FavnStoragePostgres.Schemas.ManifestDeploymentOperation, as: Operation
  alias FavnStoragePostgres.Schemas.RunnerTask

  @terminal ~w(succeeded failed cancelled unknown)
  @closed ~w(succeeded needs_attention failed unknown cancelling cancelled)

  def accept(%C.AcceptLocalManifestDeployment{} = command) do
    transact(fn ->
      authorize!(command.workspace_context)
      validate_identity!(command.operation_id)
      validate_identity!(command.session_id)
      validate_times!(command.occurred_at, command.expires_at)
      workspace = command.workspace_context.workspace_id

      SQL.query!(Repo, "SELECT pg_advisory_xact_lock(hashtext($1))", [
        "local-deployment:" <> workspace
      ])

      case Repo.get_by(Operation, workspace_id: workspace, operation_id: command.operation_id) do
        %Operation{source: "local", local_session_id: session, manifest_version_id: manifest} =
            row
        when session == command.session_id and manifest == command.manifest_version_id ->
          {:replay, Store.manifest_deployment_result(row)}

        %Operation{} ->
          fail!(:deployment_operation_conflict)

        nil ->
          if SQL.query!(
               Repo,
               "SELECT 1 FROM favn_control.local_deployment_cancellations WHERE workspace_id = $1 AND operation_id = $2",
               [workspace, command.operation_id]
             ).num_rows > 0, do: fail!(:local_deployment_cancelled_before_acceptance)

          if Repo.exists?(
               from(o in Operation,
                 where:
                   o.workspace_id == ^workspace and
                     (o.state in ["accepted", "activating", "cancelling", "unknown"] or
                        o.cleanup_state != "settled")
               )
             ),
             do: fail!(:local_deployment_pending)

          if Repo.exists?(
               from(t in RunnerTask,
                 where:
                   t.workspace_id == ^workspace and
                     t.task_kind == "relation_inspection" and is_nil(t.deployment_operation_id) and
                     is_nil(t.operation_id) and is_nil(t.run_id) and
                     t.status not in ["succeeded", "failed", "cancelled"]
               )
             ),
             do: fail!(:legacy_deployment_inspections_pending)

          %{rows: rows} =
            SQL.query!(
              Repo,
              "SELECT content_hash, runner_releases FROM favn_control.manifest_versions WHERE manifest_version_id = $1 AND NOT retiring",
              [command.manifest_version_id]
            )

          {hash, releases} =
            case rows do
              [[hash, releases]] -> {hash, releases}
              _ -> fail!(:manifest_not_found)
            end

          request = %{
            "ownership_version" => 1,
            "selection" =>
              FavnOrchestrator.ManifestDeployments.fixed_selection()
              |> Jason.encode!()
              |> Jason.decode!(),
            "configuration" => %{},
            "approve_manifest_defaults" => true
          }

          fingerprint =
            :crypto.hash(
              :sha256,
              :erlang.term_to_binary(
                {workspace, command.operation_id, command.session_id, command.manifest_version_id,
                 hash, releases, request},
                [:deterministic]
              )
            )

          row =
            Repo.insert!(%Operation{
              workspace_id: workspace,
              operation_id: command.operation_id,
              source: "local",
              local_session_id: command.session_id,
              local_expires_at: command.expires_at,
              request_fingerprint: fingerprint,
              service_identity: "favn-local",
              manifest_version_id: command.manifest_version_id,
              manifest_content_hash: hash,
              runner_releases: releases,
              request: request,
              state: "accepted",
              claim_fence: 0,
              accepted_at: command.occurred_at,
              inserted_at: command.occurred_at,
              updated_at: command.occurred_at
            })

          {:accepted, Store.manifest_deployment_result(row)}
      end
    end)
    |> case do
      {:ok, {status, row}} -> {:ok, status, row}
      error -> error
    end
  end

  def renew(%C.RenewLocalManifestDeployment{} = command) do
    transact(fn ->
      authorize!(command.workspace_context)
      validate_times!(command.occurred_at, command.expires_at)
      row = lock!(command.workspace_context.workspace_id, command.operation_id)
      now = later_time(command.occurred_at, DateTime.utc_now())

      if row.source != "local" or row.local_session_id != command.session_id or
           row.state in @closed or DateTime.compare(row.local_expires_at, now) != :gt or
           DateTime.compare(command.expires_at, now) != :gt,
         do: fail!(:local_deployment_session_expired)

      Repo.update_all(query(row),
        set: [local_expires_at: command.expires_at, updated_at: command.occurred_at]
      )

      :ok
    end)
    |> case do
      {:ok, :ok} -> :ok
      error -> error
    end
  end

  def cancel(%C.CancelManifestDeployment{} = command) do
    transact(fn ->
      authorize!(command.workspace_context)

      unless command.reason in [:local_stop, :startup_timeout, :session_expired, :owner_stopped],
        do: fail!(:invalid_deployment_cancellation)

      workspace = command.workspace_context.workspace_id
      validate_identity!(command.operation_id)

      SQL.query!(Repo, "SELECT pg_advisory_xact_lock(hashtext($1))", [
        "local-deployment:" <> workspace
      ])

      SQL.query!(
        Repo,
        "INSERT INTO favn_control.local_deployment_cancellations (workspace_id, operation_id, cancelled_at, reason) VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING",
        [workspace, command.operation_id, command.occurred_at, Atom.to_string(command.reason)]
      )

      case Repo.get_by(Operation, workspace_id: workspace, operation_id: command.operation_id) do
        nil ->
          :cancelled_before_acceptance

        _ ->
          row = lock!(workspace, command.operation_id)
          row = close!(row, command.occurred_at, Atom.to_string(command.reason))
          Store.manifest_deployment_result(row)
      end
    end)
  end

  def reconcile(%C.ReconcileManifestDeployments{} = command) do
    transact(fn ->
      unless :platform_operator in command.platform_context.roles, do: fail!(:forbidden)

      rows =
        Repo.all(
          from(o in Operation,
            where:
              (o.source == "local" and o.state in ["accepted", "activating"] and
                 o.local_expires_at <= ^command.occurred_at) or
                (o.state in ^@closed and o.cleanup_state in ["pending", "settling", "unknown"]),
            order_by: [asc: o.updated_at, asc: o.workspace_id, asc: o.operation_id],
            limit: 10,
            lock: "FOR UPDATE SKIP LOCKED"
          )
        )

      Enum.map(rows, fn row ->
        row =
          if row.source == "local" and row.state in ["accepted", "activating"],
            do: close!(row, command.occurred_at, "session_expired"),
            else: row

        row =
          if row.state == "unknown" and is_nil(row.activation_receipt) and
               row.request["ownership_version"] == 1 do
            changes = [state: "failed", failure_class: "activation_reconciled_not_committed"]
            Repo.update_all(query(row), set: changes)
            struct!(row, changes)
          else
            row
          end

        %{task_ids: task_ids, cursor: cursor} =
          TaskStore.settle_deployment_in_transaction(
            row.workspace_id,
            row.operation_id,
            command.occurred_at,
            row.cleanup_cursor
          )

        counts = counts(row.workspace_id, row.operation_id)

        cleanup =
          cond do
            Map.get(counts, "unknown", 0) > 0 ->
              "unknown"

            Enum.any?(counts, fn {state, count} -> state not in @terminal and count > 0 end) ->
              "settling"

            true ->
              "settled"
          end

        changes = [
          cleanup_state: cleanup,
          cleanup_cursor: cursor,
          updated_at: command.occurred_at
        ]

        changes =
          if row.state == "cancelling" and cleanup == "settled",
            do: changes ++ [state: "cancelled", terminal_at: command.occurred_at],
            else: changes

        Repo.update_all(query(row), set: changes)

        %{
          workspace_id: row.workspace_id,
          operation_id: row.operation_id,
          task_ids: task_ids,
          cleanup_state: cleanup,
          counts: counts
        }
      end)
    end)
  end

  def inspections(query) do
    transact(fn ->
      authorize!(query.workspace_context)
      unless is_integer(query.limit) and query.limit in 1..100, do: fail!(:invalid_page_limit)
      workspace = query.workspace_context.workspace_id
      _ = lock!(workspace, query.operation_id)

      rows =
        from(t in RunnerTask,
          where:
            t.workspace_id == ^workspace and t.deployment_operation_id == ^query.operation_id,
          order_by: t.task_id,
          limit: ^query.limit,
          select: %{task_id: t.task_id, status: t.status}
        )

      rows =
        if query.after_task_id, do: where(rows, [t], t.task_id > ^query.after_task_id), else: rows

      %{counts: counts(workspace, query.operation_id), tasks: Repo.all(rows)}
    end)
  end

  def pin_base(%C.PinDeploymentInspectionBase{} = command) do
    transact(fn ->
      authorize!(command.workspace_context)

      unless is_binary(command.binding_hash) and byte_size(command.binding_hash) == 32,
        do: fail!(:invalid_binding_hash)

      row = lock!(command.workspace_context.workspace_id, command.operation_id)
      unless live?(row, DateTime.utc_now()), do: fail!(:deployment_inspection_admission_closed)

      case row.inspection_binding_hash do
        nil -> Repo.update_all(query(row), set: [inspection_binding_hash: command.binding_hash])
        hash when hash == command.binding_hash -> :ok
        _changed -> fail!(:deployment_inspection_base_changed)
      end

      :ok
    end)
    |> case do
      {:ok, :ok} -> :ok
      error -> error
    end
  end

  def resolve(%C.ResolveDeploymentInspections{} = command) do
    transact(fn ->
      authorize!(command.workspace_context)

      unless command.runner_stopped == true and command.backend_stopped == true and
               is_binary(command.evidence_reference) and
               byte_size(command.evidence_reference) in 1..255 and
               is_map(command.task_assignments) and map_size(command.task_assignments) in 1..100,
             do: fail!(:inspection_quiescence_evidence_required)

      workspace = command.workspace_context.workspace_id

      if command.operation_id do
        row = lock!(workspace, command.operation_id)
        if live?(row, DateTime.utc_now()), do: fail!(:deployment_still_live)
      end

      resolved = TaskStore.resolve_deployment_inspections_in_transaction(command)

      if command.operation_id do
        Repo.update_all(
          from(o in Operation,
            where: o.workspace_id == ^workspace and o.operation_id == ^command.operation_id
          ),
          set: [cleanup_state: "settling", updated_at: command.occurred_at]
        )
      end

      resolved
    end)
  end

  def counts(workspace, operation) do
    Repo.all(
      from(t in RunnerTask,
        where: t.workspace_id == ^workspace and t.deployment_operation_id == ^operation,
        group_by: t.status,
        select: {t.status, count(t.task_id)}
      )
    )
    |> Map.new()
  end

  # All owned-task writers acquire this lock before any task row lock.
  def lock!(_workspace, nil), do: nil

  def lock!(workspace, operation) do
    Repo.one(
      from(o in Operation,
        where: o.workspace_id == ^workspace and o.operation_id == ^operation,
        lock: "FOR UPDATE"
      )
    ) ||
      fail!(:deployment_owner_not_found)
  end

  def try_lock(_workspace, nil), do: {:ok, nil}

  def try_lock(workspace, operation) do
    case Repo.one(
           from(o in Operation,
             where: o.workspace_id == ^workspace and o.operation_id == ^operation,
             lock: "FOR UPDATE SKIP LOCKED"
           )
         ) do
      nil -> :busy
      row -> {:ok, row}
    end
  end

  def live?(nil, _now), do: true

  def live?(row, now) do
    row.state in ["accepted", "activating"] and is_nil(row.cancellation_requested_at) and
      (row.source != "local" or DateTime.compare(row.local_expires_at, now) == :gt)
  end

  def admit!(nil, _now), do: :ok

  def admit!(row, now) do
    unless live?(row, now) and
             (is_nil(row.inspection_deadline_at) or
                DateTime.compare(row.inspection_deadline_at, now) == :gt),
           do: fail!(:deployment_inspection_admission_closed)

    :ok
  end

  def activation!(%{deployment_claim: nil}), do: nil

  def activation!(command) do
    %{operation_id: id, owner: owner, fence: fence} = command.deployment_claim
    row = lock!(command.workspace_context.workspace_id, id)

    unless live?(row, DateTime.utc_now()) and row.state == "activating" and
             row.manifest_version_id == command.manifest_version_id and
             row.claim_owner == owner and row.claim_fence == fence and
             DateTime.compare(row.claim_expires_at, DateTime.utc_now()) == :gt,
           do: fail!(:deployment_activation_fenced)

    row
  end

  def receipt!(nil, _runtime), do: :ok

  def receipt!(row, runtime) do
    diagnostics =
      FavnOrchestrator.ManifestActivationDiagnostics.to_map(runtime.activation_diagnostics)

    receipt = %{
      "deployment_id" => runtime.deployment_id,
      "runtime_revision" => runtime.revision,
      "manifest_version_id" => runtime.manifest_version_id,
      "runner_releases" => runtime.runner_releases
    }

    Repo.update_all(query(row),
      set: [
        activation_receipt: receipt,
        cleanup_state:
          if(
            Enum.all?(counts(row.workspace_id, row.operation_id), fn {state, _count} ->
              state in ["succeeded", "failed", "cancelled"]
            end),
            do: "settled",
            else: "pending"
          ),
        deployment_id: runtime.deployment_id,
        state:
          if(diagnostics.unresolved_inspection_count > 0,
            do: "needs_attention",
            else: "succeeded"
          ),
        activation_diagnostics: diagnostics,
        terminal_at: DateTime.utc_now(),
        updated_at: DateTime.utc_now()
      ]
    )

    :ok
  end

  defp close!(%{activation_receipt: receipt} = row, _now, _reason) when not is_nil(receipt),
    do: row

  defp close!(%{state: state} = row, _now, _reason) when state in @closed, do: row

  defp close!(row, now, reason) do
    changes = [
      state: "cancelling",
      cancellation_requested_at: now,
      failure_class: reason,
      cleanup_state: "settling",
      updated_at: now
    ]

    Repo.update_all(query(row), set: changes)
    struct!(row, changes)
  end

  defp query(row),
    do:
      from(o in Operation,
        where: o.workspace_id == ^row.workspace_id and o.operation_id == ^row.operation_id
      )

  defp authorize!(%WorkspaceContext{roles: roles} = context) do
    unless WorkspaceContext.valid?(context) and :platform_operator in roles, do: fail!(:forbidden)
  end

  defp authorize!(_), do: fail!(:forbidden)
  defp later_time(a, b), do: if(DateTime.compare(a, b) == :gt, do: a, else: b)

  defp validate_identity!(value) do
    unless is_binary(value) and byte_size(value) in 1..128 and
             Regex.match?(~r/\A[A-Za-z0-9][A-Za-z0-9._-]*\z/, value),
           do: fail!(:invalid_deployment_identity)
  end

  defp validate_times!(%DateTime{} = now, %DateTime{} = expires) do
    unless DateTime.diff(expires, now, :second) in 1..45, do: fail!(:invalid_local_lease)
  end

  defp validate_times!(_, _), do: fail!(:invalid_local_lease)

  defp fail!(reason),
    do:
      Repo.rollback(
        Error.new(:conflict, "deployment ownership rejected", details: %{reason: reason})
      )

  defp transact(fun) do
    Repo.transaction(fun)
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end
end
