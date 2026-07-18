defmodule FavnStoragePostgres.Admission.Store do
  @moduledoc false

  @behaviour FavnOrchestrator.Persistence.AdmissionStore

  import Ecto.Query

  alias Ecto.Adapters.SQL
  alias FavnOrchestrator.Persistence.Commands.AdmitExecution
  alias FavnOrchestrator.Persistence.Commands.CapacityRequest
  alias FavnOrchestrator.Persistence.Commands.ClaimAdmissionWaiters
  alias FavnOrchestrator.Persistence.Commands.ExpireAdmission
  alias FavnOrchestrator.Persistence.Commands.ReleaseExecutionLease
  alias FavnOrchestrator.Persistence.Commands.ReleaseRunLeases
  alias FavnOrchestrator.Persistence.Commands.RenewExecutionLease
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Results.Admission
  alias FavnOrchestrator.Persistence.Results.AdmissionWaiter, as: AdmissionWaiterResult
  alias FavnOrchestrator.Persistence.Results.CapacityRelease
  alias FavnOrchestrator.Persistence.Results.ExecutionLease, as: ExecutionLeaseResult
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnStoragePostgres.CanonicalJSON
  alias FavnStoragePostgres.ErrorMapper
  alias FavnStoragePostgres.Outbox.Writer, as: OutboxWriter
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.Schemas.AdmissionWaiter
  alias FavnStoragePostgres.Schemas.CapacityScope
  alias FavnStoragePostgres.Schemas.ExecutionLease
  alias FavnStoragePostgres.Schemas.ExecutionLeaseScope

  @max_requests 32

  @impl true
  def admit(%AdmitExecution{} = command) do
    with :ok <- validate_admit(command) do
      transaction(fn -> admit!(command) end)
    end
  end

  @impl true
  def renew_lease(%RenewExecutionLease{} = command) do
    with :ok <- validate_renew(command) do
      transaction(fn -> renew_lease!(command) end)
    end
  end

  @impl true
  def release_lease(%ReleaseExecutionLease{} = command) do
    with :ok <- validate_release(command) do
      transaction(fn -> release_lease!(command) end)
    end
  end

  @impl true
  def release_run_leases(%ReleaseRunLeases{} = command) do
    with :ok <- validate_release_run(command) do
      transaction(fn -> release_run_leases!(command) end)
    end
  end

  @impl true
  def claim_waiters(%ClaimAdmissionWaiters{} = command) do
    with :ok <- validate_claim_waiters(command) do
      transaction(fn -> claim_waiters!(command) end)
    end
  end

  @impl true
  def expire(%ExpireAdmission{} = command) do
    with :ok <- validate_expire(command) do
      transaction(fn -> expire!(command) end)
    end
  end

  defp admit!(command) do
    workspace_id = command.workspace_context.workspace_id
    requests = normalize_requests(command.requests)
    request_hash = request_hash!(command, requests)

    case existing_decision(workspace_id, command, request_hash) do
      {:ok, result} ->
        result

      :new ->
        scopes = lock_scopes!(workspace_id, requests)

        case blocking_scope(scopes, requests) do
          nil -> admit_available!(command, requests, request_hash)
          scope_id -> persist_waiter!(command, requests, request_hash, scope_id)
        end
    end
  end

  defp existing_decision(workspace_id, command, request_hash) do
    lease =
      from(lease in ExecutionLease,
        where:
          lease.workspace_id == ^workspace_id and
            (lease.command_id == ^command.command_id or lease.lease_id == ^command.lease_id),
        lock: "FOR UPDATE"
      )
      |> Repo.one()

    waiter =
      from(waiter in AdmissionWaiter,
        where:
          waiter.workspace_id == ^workspace_id and
            (waiter.command_id == ^command.command_id or waiter.waiter_id == ^command.waiter_id),
        lock: "FOR UPDATE"
      )
      |> Repo.one()

    decide_existing(lease, waiter, command, request_hash)
  end

  defp decide_existing(%ExecutionLease{} = lease, _waiter, command, request_hash) do
    if exact_lease_replay?(lease, command, request_hash) and lease.status == "active" and
         future?(lease.expires_at) do
      {:ok, %Admission{status: :admitted, lease: lease_result(lease)}}
    else
      Repo.rollback(Error.new(:conflict, "admission lease identity has different content"))
    end
  end

  defp decide_existing(nil, %AdmissionWaiter{} = waiter, command, request_hash) do
    cond do
      exact_waiter_replay?(waiter, command, request_hash) ->
        {:ok,
         %Admission{
           status: :waiting,
           waiter: waiter_result(waiter),
           blocking_scope_id: waiter.blocking_scope_id
         }}

      reusable_waiter?(waiter, command, request_hash) ->
        :new

      true ->
        Repo.rollback(
          Error.new(:conflict, "admission waiter identity has different content",
            details: %{
              same_waiter_id: waiter.waiter_id == command.waiter_id,
              same_run_id: waiter.run_id == command.run_id,
              same_step_id: waiter.step_id == command.step_id,
              same_request: waiter.request_hash == request_hash
            }
          )
        )
    end
  end

  defp decide_existing(nil, nil, _command, _request_hash), do: :new

  defp admit_available!(command, requests, request_hash) do
    workspace_id = command.workspace_context.workspace_id
    increment_capacity!(requests)

    lease =
      %ExecutionLease{
        workspace_id: workspace_id,
        lease_id: command.lease_id,
        run_id: command.run_id,
        step_id: command.step_id,
        command_id: command.command_id,
        request_hash: request_hash,
        owner_id: command.owner_id,
        owner_generation: command.owner_generation,
        status: "active",
        expires_at: database_deadline!(command.lease_duration_ms),
        inserted_at: command.occurred_at,
        updated_at: command.occurred_at
      }
      |> Repo.insert!()

    rows =
      Enum.map(requests, fn request ->
        %{
          workspace_id: workspace_id,
          lease_id: command.lease_id,
          scope_id: request.scope_id,
          units: request.units,
          inserted_at: command.occurred_at
        }
      end)

    {_count, nil} = Repo.insert_all(ExecutionLeaseScope, rows)

    from(waiter in AdmissionWaiter,
      where:
        waiter.workspace_id == ^workspace_id and
          (waiter.waiter_id == ^command.waiter_id or
             (waiter.run_id == ^command.run_id and waiter.step_id == ^command.step_id))
    )
    |> Repo.delete_all()

    OutboxWriter.insert!(%{
      workspace_id: workspace_id,
      command_id: command.command_id,
      event_kind: "admission.lease.admitted",
      aggregate_kind: "execution_lease",
      aggregate_id: command.lease_id,
      aggregate_version: command.owner_generation,
      occurred_at: command.occurred_at,
      payload: %{
        "lease_id" => command.lease_id,
        "run_id" => command.run_id,
        "step_id" => command.step_id,
        "scope_ids" => Enum.map(requests, & &1.scope_id)
      }
    })

    %Admission{status: :admitted, lease: lease_result(lease, requests)}
  end

  defp persist_waiter!(command, requests, request_hash, blocking_scope_id) do
    workspace_id = command.workspace_context.workspace_id
    expires_at = database_deadline!(command.waiter_ttl_ms)

    attrs = %{
      command_id: command.command_id,
      request_hash: request_hash,
      requested_scopes: request_maps(requests),
      blocking_scope_id: blocking_scope_id,
      priority: command.priority,
      status: "waiting",
      available_at: command.occurred_at,
      expires_at: expires_at,
      claim_owner: nil,
      claim_command_id: nil,
      claim_expires_at: nil,
      updated_at: command.occurred_at
    }

    waiter =
      case Repo.get_by(AdmissionWaiter,
             workspace_id: workspace_id,
             waiter_id: command.waiter_id
           ) do
        nil ->
          attrs
          |> Map.merge(%{
            workspace_id: workspace_id,
            waiter_id: command.waiter_id,
            run_id: command.run_id,
            step_id: command.step_id,
            claim_generation: 0,
            inserted_at: command.occurred_at
          })
          |> then(&struct!(AdmissionWaiter, &1))
          |> Repo.insert!()

        existing ->
          existing
          |> Ecto.Changeset.change(attrs)
          |> Repo.update!()
      end

    %Admission{
      status: :waiting,
      waiter: waiter_result(waiter),
      blocking_scope_id: blocking_scope_id
    }
  end

  defp renew_lease!(command) do
    workspace_id = command.workspace_context.workspace_id
    lease = lock_lease!(workspace_id, command.lease_id)

    cond do
      lease.last_renewal_id == command.renewal_id and matching_lease_owner?(lease, command) and
        lease.status == "active" and future?(lease.expires_at) ->
        lease_result(lease)

      not matching_lease_owner?(lease, command) or lease.status != "active" or
          not future?(lease.expires_at) ->
        Repo.rollback(Error.new(:fenced, "execution lease cannot be renewed"))

      true ->
        %{rows: [row]} =
          SQL.query!(
            Repo,
            """
            UPDATE favn_control.execution_leases
            SET last_renewal_id = $5,
                expires_at = clock_timestamp() + ($6 * interval '1 millisecond'),
                updated_at = clock_timestamp()
            WHERE workspace_id = $1 AND lease_id = $2 AND owner_id = $3
              AND owner_generation = $4 AND status = 'active'
              AND expires_at > clock_timestamp()
            RETURNING workspace_id, lease_id, run_id, step_id, owner_id,
                      owner_generation, status, expires_at, released_at
            """,
            [
              workspace_id,
              command.lease_id,
              command.owner_id,
              command.owner_generation,
              command.renewal_id,
              command.lease_duration_ms
            ]
          )

        lease_result(row)
    end
  end

  defp release_lease!(command) do
    workspace_id = command.workspace_context.workspace_id
    lease = lock_lease!(workspace_id, command.lease_id)

    if matching_lease_owner?(lease, command) do
      if lease.status == "active" do
        requests = lease_requests(workspace_id, [lease.lease_id])
        lock_scopes_for_release!(Map.keys(requests))
        decrement_capacity!(requests)

        now = database_now!()

        lease
        |> Ecto.Changeset.change(%{
          status: "released",
          released_at: now,
          expires_at: now,
          updated_at: now
        })
        |> Repo.update!()

        release_outbox!(workspace_id, lease, "released", now)
      end

      result = %CapacityRelease{
        released_lease_ids: [lease.lease_id],
        expired_waiter_ids: [],
        freed_scope_ids: lease_scope_ids(workspace_id, lease.lease_id)
      }

      notify_admission_changed!()
      result
    else
      Repo.rollback(Error.new(:fenced, "execution lease cannot be released"))
    end
  end

  defp release_run_leases!(command) do
    workspace_id = command.workspace_context.workspace_id

    leases =
      from(lease in ExecutionLease,
        where:
          lease.workspace_id == ^workspace_id and lease.run_id == ^command.run_id and
            lease.status == "active",
        order_by: [asc: lease.lease_id],
        limit: ^command.limit,
        lock: "FOR UPDATE SKIP LOCKED"
      )
      |> Repo.all()

    release = release_batch!(workspace_id, leases, "released", "admission.release_run")

    waiters =
      from(waiter in AdmissionWaiter,
        where:
          waiter.workspace_id == ^workspace_id and waiter.run_id == ^command.run_id and
            waiter.status in ["waiting", "claimed"],
        order_by: [asc: waiter.waiter_id],
        limit: ^command.limit,
        lock: "FOR UPDATE SKIP LOCKED"
      )
      |> Repo.all()

    waiter_ids = Enum.map(waiters, & &1.waiter_id)

    if waiter_ids != [] do
      now = database_now!()

      from(waiter in AdmissionWaiter,
        where: waiter.workspace_id == ^workspace_id and waiter.waiter_id in ^waiter_ids
      )
      |> Repo.update_all(set: [status: "cancelled", claim_expires_at: now, updated_at: now])
    end

    result = %{release | expired_waiter_ids: waiter_ids}
    if result.released_lease_ids != [] or waiter_ids != [], do: notify_admission_changed!()
    result
  end

  defp claim_waiters!(command) do
    workspace_id = command.workspace_context.workspace_id

    replay =
      from(waiter in AdmissionWaiter,
        where:
          waiter.workspace_id == ^workspace_id and
            waiter.claim_command_id == ^command.batch_id and
            waiter.blocking_scope_id == ^command.scope_id,
        order_by: [desc: waiter.priority, asc: waiter.inserted_at, asc: waiter.waiter_id]
      )
      |> Repo.all()

    if replay == [] do
      %{rows: rows} =
        SQL.query!(
          Repo,
          """
          WITH candidates AS (
            SELECT workspace_id, waiter_id
            FROM favn_control.admission_waiters
            WHERE workspace_id = $1 AND blocking_scope_id = $2
              AND status = 'waiting' AND available_at <= clock_timestamp()
              AND expires_at > clock_timestamp()
            ORDER BY priority DESC, inserted_at, waiter_id
            LIMIT $3
            FOR UPDATE SKIP LOCKED
          )
          UPDATE favn_control.admission_waiters waiter
          SET status = 'claimed', claim_owner = $4,
              claim_generation = waiter.claim_generation + 1,
              claim_command_id = $5,
              claim_expires_at = clock_timestamp() + ($6 * interval '1 millisecond'),
              updated_at = clock_timestamp()
          FROM candidates
          WHERE waiter.workspace_id = candidates.workspace_id
            AND waiter.waiter_id = candidates.waiter_id
          RETURNING waiter.workspace_id, waiter.waiter_id, waiter.run_id, waiter.step_id,
                    waiter.blocking_scope_id, waiter.status, waiter.priority,
                    waiter.expires_at, waiter.claim_owner, waiter.claim_generation,
                    waiter.claim_expires_at, waiter.requested_scopes
          """,
          [
            workspace_id,
            command.scope_id,
            command.limit,
            command.owner_id,
            command.batch_id,
            command.lease_duration_ms
          ]
        )

      rows |> Enum.map(&waiter_result/1) |> sort_waiters()
    else
      unless live_waiter_replay?(replay, command) do
        Repo.rollback(Error.new(:fenced, "admission waiter claim batch is no longer live"))
      end

      Enum.map(replay, &waiter_result/1)
    end
  end

  defp live_waiter_replay?(waiters, command) do
    now = database_now!()

    Enum.all?(waiters, fn waiter ->
      waiter.status == "claimed" and waiter.claim_owner == command.owner_id and
        match?(%DateTime{}, waiter.claim_expires_at) and
        DateTime.compare(waiter.claim_expires_at, now) == :gt and
        DateTime.compare(waiter.expires_at, now) == :gt
    end)
  end

  defp expire!(command) do
    workspace_id = command.workspace_context.workspace_id

    leases =
      from(lease in ExecutionLease,
        where:
          lease.workspace_id == ^workspace_id and lease.status == "active" and
            lease.expires_at <= fragment("clock_timestamp()"),
        order_by: [asc: lease.expires_at, asc: lease.lease_id],
        limit: ^command.limit,
        lock: "FOR UPDATE SKIP LOCKED"
      )
      |> Repo.all()

    released = release_batch!(workspace_id, leases, "expired", "admission.expire")
    remaining = max(command.limit - length(leases), 0)

    waiter_ids =
      if remaining == 0 do
        []
      else
        %{rows: rows} =
          SQL.query!(
            Repo,
            """
            WITH candidates AS (
              SELECT workspace_id, waiter_id
              FROM favn_control.admission_waiters
              WHERE workspace_id = $1 AND status IN ('waiting', 'claimed')
                AND expires_at <= clock_timestamp()
              ORDER BY expires_at, waiter_id
              LIMIT $2
              FOR UPDATE SKIP LOCKED
            )
            UPDATE favn_control.admission_waiters waiter
            SET status = 'expired', claim_owner = NULL, claim_expires_at = NULL,
                updated_at = clock_timestamp()
            FROM candidates
            WHERE waiter.workspace_id = candidates.workspace_id
              AND waiter.waiter_id = candidates.waiter_id
            RETURNING waiter.waiter_id
            """,
            [workspace_id, remaining]
          )

        List.flatten(rows)
      end

    result = %{released | expired_waiter_ids: waiter_ids}

    if result.released_lease_ids != [] or waiter_ids != [],
      do: notify_admission_changed!()

    result
  end

  defp release_batch!(_workspace_id, [], _status, _event_kind),
    do: %CapacityRelease{released_lease_ids: [], expired_waiter_ids: [], freed_scope_ids: []}

  defp release_batch!(workspace_id, leases, status, event_kind) do
    lease_ids = Enum.map(leases, & &1.lease_id)
    requests = lease_requests(workspace_id, lease_ids)
    scope_ids = requests |> Map.keys() |> Enum.sort()
    lock_scopes_for_release!(scope_ids)
    decrement_capacity!(requests)
    now = database_now!()

    from(lease in ExecutionLease,
      where: lease.workspace_id == ^workspace_id and lease.lease_id in ^lease_ids
    )
    |> Repo.update_all(set: [status: status, released_at: now, expires_at: now, updated_at: now])

    command_id = event_kind <> ":" <> batch_identity(lease_ids)

    OutboxWriter.insert!(%{
      workspace_id: workspace_id,
      command_id: command_id,
      event_kind: event_kind,
      aggregate_kind: "execution_lease_batch",
      aggregate_id: batch_identity(lease_ids),
      aggregate_version: 1,
      occurred_at: now,
      payload: %{"lease_ids" => lease_ids, "scope_ids" => scope_ids, "status" => status}
    })

    %CapacityRelease{
      released_lease_ids: lease_ids,
      expired_waiter_ids: [],
      freed_scope_ids: scope_ids
    }
  end

  defp lock_scopes!(workspace_id, requests) do
    ids = Enum.map(requests, & &1.scope_id)

    scopes =
      from(scope in CapacityScope,
        where: scope.scope_id in ^ids,
        order_by: [asc: scope.scope_id],
        lock: "FOR UPDATE"
      )
      |> Repo.all()

    if length(scopes) != length(ids) or
         Enum.any?(scopes, &(&1.workspace_id not in [nil, workspace_id])) do
      Repo.rollback(Error.new(:not_found, "one or more capacity scopes are unavailable"))
    end

    scopes
  end

  defp lock_scopes_for_release!([]), do: []

  defp lock_scopes_for_release!(scope_ids) do
    scopes =
      from(scope in CapacityScope,
        where: scope.scope_id in ^scope_ids,
        order_by: [asc: scope.scope_id],
        lock: "FOR UPDATE"
      )
      |> Repo.all()

    if length(scopes) != length(scope_ids),
      do: Repo.rollback(Error.new(:constraint, "capacity lease references a missing scope"))

    scopes
  end

  defp blocking_scope(scopes, requests) do
    units_by_scope = Map.new(requests, &{&1.scope_id, &1.units})

    Enum.find_value(scopes, fn scope ->
      if scope.active_count + Map.fetch!(units_by_scope, scope.scope_id) > scope.capacity_limit,
        do: scope.scope_id
    end)
  end

  defp increment_capacity!(requests) do
    {ids, units} = request_arrays(requests)

    %{num_rows: count} =
      SQL.query!(
        Repo,
        """
        WITH requested AS (
          SELECT * FROM unnest($1::text[], $2::integer[]) AS item(scope_id, units)
        )
        UPDATE favn_control.capacity_scopes scope
        SET active_count = scope.active_count + requested.units,
            version = scope.version + 1,
            updated_at = clock_timestamp()
        FROM requested
        WHERE scope.scope_id = requested.scope_id
          AND scope.active_count + requested.units <= scope.capacity_limit
        """,
        [ids, units]
      )

    if count != length(requests),
      do:
        Repo.rollback(Error.new(:conflict, "capacity changed during admission", retryable?: true))
  end

  defp decrement_capacity!(requests) when map_size(requests) == 0, do: :ok

  defp decrement_capacity!(requests) do
    {ids, units} = requests |> Enum.sort() |> Enum.unzip()

    %{num_rows: count} =
      SQL.query!(
        Repo,
        """
        WITH requested AS (
          SELECT * FROM unnest($1::text[], $2::integer[]) AS item(scope_id, units)
        )
        UPDATE favn_control.capacity_scopes scope
        SET active_count = scope.active_count - requested.units,
            version = scope.version + 1,
            updated_at = clock_timestamp()
        FROM requested
        WHERE scope.scope_id = requested.scope_id
          AND scope.active_count >= requested.units
        """,
        [ids, units]
      )

    if count != map_size(requests),
      do: Repo.rollback(Error.new(:constraint, "capacity counter underflow prevented"))
  end

  defp lease_requests(workspace_id, lease_ids) do
    from(membership in ExecutionLeaseScope,
      where: membership.workspace_id == ^workspace_id and membership.lease_id in ^lease_ids,
      select: {membership.scope_id, sum(membership.units)},
      group_by: membership.scope_id
    )
    |> Repo.all()
    |> Map.new()
  end

  defp lease_scope_ids(workspace_id, lease_id) do
    from(membership in ExecutionLeaseScope,
      where: membership.workspace_id == ^workspace_id and membership.lease_id == ^lease_id,
      order_by: [asc: membership.scope_id],
      select: membership.scope_id
    )
    |> Repo.all()
  end

  defp lock_lease!(workspace_id, lease_id) do
    from(lease in ExecutionLease,
      where: lease.workspace_id == ^workspace_id and lease.lease_id == ^lease_id,
      lock: "FOR UPDATE"
    )
    |> Repo.one()
    |> case do
      nil -> Repo.rollback(Error.new(:not_found, "execution lease not found"))
      lease -> lease
    end
  end

  defp release_outbox!(workspace_id, lease, status, now) do
    OutboxWriter.insert!(%{
      workspace_id: workspace_id,
      command_id: "admission.lease.#{status}:#{lease.lease_id}:#{lease.owner_generation}",
      event_kind: "admission.lease." <> status,
      aggregate_kind: "execution_lease",
      aggregate_id: lease.lease_id,
      aggregate_version: lease.owner_generation,
      occurred_at: now,
      payload: %{"lease_id" => lease.lease_id, "run_id" => lease.run_id, "status" => status}
    })
  end

  defp notify_admission_changed! do
    SQL.query!(Repo, "SELECT pg_notify('favn_admission_changed', '')", [])
    :ok
  end

  defp lease_result(%ExecutionLease{} = lease) do
    lease_result(lease, lease_scope_ids(lease.workspace_id, lease.lease_id))
  end

  defp lease_result([
         workspace_id,
         lease_id,
         run_id,
         step_id,
         owner_id,
         owner_generation,
         status,
         expires_at,
         released_at
       ]) do
    %ExecutionLeaseResult{
      workspace_id: workspace_id,
      lease_id: lease_id,
      run_id: run_id,
      step_id: step_id,
      owner_id: owner_id,
      owner_generation: owner_generation,
      status: String.to_existing_atom(status),
      expires_at: expires_at,
      released_at: released_at,
      scope_ids: lease_scope_ids(workspace_id, lease_id)
    }
  end

  defp lease_result(%ExecutionLease{} = lease, requests) do
    scope_ids =
      Enum.map(requests, fn
        %CapacityRequest{scope_id: scope_id} -> scope_id
        scope_id when is_binary(scope_id) -> scope_id
      end)

    %ExecutionLeaseResult{
      workspace_id: lease.workspace_id,
      lease_id: lease.lease_id,
      run_id: lease.run_id,
      step_id: lease.step_id,
      owner_id: lease.owner_id,
      owner_generation: lease.owner_generation,
      status: String.to_existing_atom(lease.status),
      expires_at: lease.expires_at,
      released_at: lease.released_at,
      scope_ids: scope_ids
    }
  end

  defp waiter_result(%AdmissionWaiter{} = waiter) do
    %AdmissionWaiterResult{
      workspace_id: waiter.workspace_id,
      waiter_id: waiter.waiter_id,
      run_id: waiter.run_id,
      step_id: waiter.step_id,
      blocking_scope_id: waiter.blocking_scope_id,
      status: String.to_existing_atom(waiter.status),
      priority: waiter.priority,
      expires_at: waiter.expires_at,
      claim_owner: waiter.claim_owner,
      claim_generation: waiter.claim_generation,
      claim_expires_at: waiter.claim_expires_at,
      requests: waiter.requested_scopes
    }
  end

  defp waiter_result([
         workspace_id,
         waiter_id,
         run_id,
         step_id,
         blocking_scope_id,
         status,
         priority,
         expires_at,
         claim_owner,
         claim_generation,
         claim_expires_at,
         requests
       ]) do
    %AdmissionWaiterResult{
      workspace_id: workspace_id,
      waiter_id: waiter_id,
      run_id: run_id,
      step_id: step_id,
      blocking_scope_id: blocking_scope_id,
      status: String.to_existing_atom(status),
      priority: priority,
      expires_at: expires_at,
      claim_owner: claim_owner,
      claim_generation: claim_generation,
      claim_expires_at: claim_expires_at,
      requests: requests
    }
  end

  defp exact_lease_replay?(lease, command, request_hash) do
    lease.command_id == command.command_id and lease.lease_id == command.lease_id and
      lease.run_id == command.run_id and lease.step_id == command.step_id and
      lease.owner_id == command.owner_id and
      lease.owner_generation == command.owner_generation and
      lease.request_hash == request_hash
  end

  defp exact_waiter_replay?(waiter, command, request_hash) do
    waiter.command_id == command.command_id and waiter.waiter_id == command.waiter_id and
      waiter.run_id == command.run_id and waiter.step_id == command.step_id and
      waiter.request_hash == request_hash
  end

  defp reusable_waiter?(waiter, command, request_hash) do
    waiter.waiter_id == command.waiter_id and waiter.run_id == command.run_id and
      waiter.step_id == command.step_id and waiter.request_hash == request_hash
  end

  defp matching_lease_owner?(lease, command),
    do:
      lease.owner_id == command.owner_id and
        lease.owner_generation == command.owner_generation

  defp normalize_requests(requests), do: Enum.sort_by(requests, & &1.scope_id)

  defp request_maps(requests),
    do: Enum.map(requests, &%{"scope_id" => &1.scope_id, "units" => &1.units})

  defp request_arrays(requests),
    do: requests |> Enum.map(&{&1.scope_id, &1.units}) |> Enum.unzip()

  defp request_hash!(command, requests) do
    {:ok, hash} =
      CanonicalJSON.hash(%{
        lease_id: command.lease_id,
        waiter_id: command.waiter_id,
        run_id: command.run_id,
        step_id: command.step_id,
        owner_id: command.owner_id,
        owner_generation: command.owner_generation,
        requests: request_maps(requests)
      })

    hash
  end

  defp database_deadline!(duration_ms) do
    %{rows: [[deadline]]} =
      SQL.query!(
        Repo,
        "SELECT clock_timestamp() + ($1 * interval '1 millisecond')",
        [duration_ms]
      )

    deadline
  end

  defp database_now! do
    %{rows: [[now]]} = SQL.query!(Repo, "SELECT clock_timestamp()", [])
    now
  end

  defp future?(timestamp) do
    %{rows: [[future?]]} =
      SQL.query!(Repo, "SELECT $1::timestamptz > clock_timestamp()", [timestamp])

    future?
  end

  defp batch_identity(ids) do
    ids
    |> Enum.sort()
    |> Enum.join("\0")
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.url_encode64(padding: false)
  end

  defp sort_waiters(waiters),
    do: Enum.sort_by(waiters, &{-&1.priority, &1.waiter_id})

  defp transaction(fun) do
    case Repo.transaction(fun) do
      {:ok, result} -> {:ok, result}
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, ErrorMapper.map(reason)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  defp validate_admit(command) do
    if valid_admit_identity?(command) and valid_admit_options?(command) and
         valid_admit_requests?(command.requests),
       do: :ok,
       else: {:error, ErrorMapper.map(:invalid)}
  end

  defp valid_admit_identity?(command) do
    workspace_context?(command.workspace_context) and
      Enum.all?(
        [
          command.command_id,
          command.lease_id,
          command.waiter_id,
          command.run_id,
          command.step_id,
          command.owner_id
        ],
        &valid_id?/1
      )
  end

  defp valid_admit_options?(command) do
    is_integer(command.owner_generation) and command.owner_generation > 0 and
      valid_duration?(command.lease_duration_ms) and valid_duration?(command.waiter_ttl_ms) and
      is_integer(command.priority) and match?(%DateTime{}, command.occurred_at)
  end

  defp valid_admit_requests?(requests) do
    is_list(requests) and requests != [] and length(requests) <= @max_requests and
      Enum.all?(requests, &valid_request?/1) and unique_scope_ids?(requests)
  end

  defp validate_renew(command) do
    if workspace_context?(command.workspace_context) and
         Enum.all?([command.renewal_id, command.lease_id, command.owner_id], &valid_id?/1) and
         is_integer(command.owner_generation) and command.owner_generation > 0 and
         valid_duration?(command.lease_duration_ms),
       do: :ok,
       else: {:error, ErrorMapper.map(:invalid)}
  end

  defp validate_release(command) do
    if workspace_context?(command.workspace_context) and
         Enum.all?([command.lease_id, command.owner_id], &valid_id?/1) and
         is_integer(command.owner_generation) and command.owner_generation > 0,
       do: :ok,
       else: {:error, ErrorMapper.map(:invalid)}
  end

  defp validate_release_run(command) do
    if workspace_context?(command.workspace_context) and valid_id?(command.run_id) and
         valid_limit?(command.limit),
       do: :ok,
       else: {:error, ErrorMapper.map(:invalid)}
  end

  defp validate_claim_waiters(command) do
    if workspace_context?(command.workspace_context) and
         Enum.all?([command.batch_id, command.scope_id, command.owner_id], &valid_id?/1) and
         valid_duration?(command.lease_duration_ms) and valid_limit?(command.limit),
       do: :ok,
       else: {:error, ErrorMapper.map(:invalid)}
  end

  defp validate_expire(command) do
    if workspace_context?(command.workspace_context) and valid_id?(command.batch_id) and
         valid_limit?(command.limit),
       do: :ok,
       else: {:error, ErrorMapper.map(:invalid)}
  end

  defp workspace_context?(context), do: WorkspaceContext.valid?(context)

  defp valid_request?(%CapacityRequest{scope_id: scope_id, units: units}),
    do: valid_id?(scope_id) and is_integer(units) and units > 0

  defp valid_request?(_other), do: false

  defp unique_scope_ids?(requests) do
    ids = Enum.map(requests, & &1.scope_id)
    length(ids) == length(Enum.uniq(ids))
  end

  defp valid_duration?(duration), do: is_integer(duration) and duration > 0
  defp valid_limit?(limit), do: is_integer(limit) and limit >= 1 and limit <= 500
  defp valid_id?(value), do: is_binary(value) and value != "" and byte_size(value) <= 255
end
