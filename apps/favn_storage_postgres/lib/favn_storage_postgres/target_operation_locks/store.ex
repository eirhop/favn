defmodule FavnStoragePostgres.TargetOperationLocks.Store do
  @moduledoc false

  @behaviour FavnOrchestrator.Persistence.TargetOperationLockStore

  import Ecto.Query

  alias Ecto.Adapters.SQL
  alias FavnOrchestrator.Persistence.Commands.AcquireTargetOperationLocks
  alias FavnOrchestrator.Persistence.Commands.ReleaseTargetOperationLocks
  alias FavnOrchestrator.Persistence.Commands.RenewTargetOperationLocks
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Results.TargetOperationLock, as: LockResult
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnStoragePostgres.ErrorMapper
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.Schemas.TargetOperationLock
  alias FavnStoragePostgres.Schemas.MaterializationClaim

  @max_targets 10_000
  @max_lease_ms 3_600_000

  @impl true
  def acquire_many(%AcquireTargetOperationLocks{} = command) do
    with :ok <- validate_acquire(command) do
      transaction(fn -> acquire_many!(command) end)
    end
  end

  @impl true
  def renew_many(%RenewTargetOperationLocks{} = command) do
    with :ok <- validate_renew(command) do
      transaction(fn -> renew_many!(command) end)
    end
  end

  @impl true
  def release_many(%ReleaseTargetOperationLocks{} = command) do
    with :ok <- validate_release(command) do
      case transaction(fn -> release_many!(command) end) do
        {:ok, :ok} -> :ok
        {:error, %Error{} = error} -> {:error, error}
      end
    end
  end

  defp acquire_many!(command) do
    workspace_id = command.workspace_context.workspace_id
    lock_identities!(workspace_id, command.target_ids)
    now = database_now!()
    expires_at = DateTime.add(now, command.lease_duration_ms, :millisecond)

    ensure_no_conflicting_materializations!(workspace_id, command, now)

    Enum.map(command.target_ids, fn target_id ->
      case lock_row(workspace_id, target_id) do
        nil ->
          %TargetOperationLock{
            workspace_id: workspace_id,
            target_id: target_id,
            operation_id: command.operation_id,
            operation_type: Atom.to_string(command.operation_type),
            fencing_token: 1,
            lease_owner: command.lease_owner,
            lease_expires_at: expires_at,
            version: 1,
            inserted_at: now,
            updated_at: now
          }
          |> Repo.insert!()
          |> lock_result()

        lock ->
          acquire_existing!(lock, command, now, expires_at)
      end
    end)
  end

  defp ensure_no_conflicting_materializations!(workspace_id, command, now) do
    conflict =
      from(claim in MaterializationClaim,
        where:
          claim.workspace_id == ^workspace_id and claim.target_id in ^command.target_ids and
            claim.status == "claimed" and claim.expires_at > ^now and
            (is_nil(claim.operation_id) or claim.operation_id != ^command.operation_id),
        order_by: [asc: claim.target_id, asc: claim.claim_key],
        limit: 1,
        lock: "FOR SHARE"
      )
      |> Repo.one()

    if conflict do
      Repo.rollback(
        Error.new(:conflict, "target materialization is already in progress",
          details: %{
            reason_code: "target_operation_in_progress",
            target_id: conflict.target_id
          }
        )
      )
    end
  end

  defp acquire_existing!(lock, command, now, expires_at) do
    same_owner? =
      lock.operation_id == command.operation_id and lock.lease_owner == command.lease_owner and
        lock.operation_type == Atom.to_string(command.operation_type)

    cond do
      same_owner? and DateTime.compare(lock.lease_expires_at, now) == :gt ->
        lock
        |> Ecto.Changeset.change(%{lease_expires_at: expires_at, updated_at: now})
        |> Repo.update!()
        |> lock_result()

      DateTime.compare(lock.lease_expires_at, now) != :gt ->
        lock
        |> Ecto.Changeset.change(%{
          operation_id: command.operation_id,
          operation_type: Atom.to_string(command.operation_type),
          fencing_token: lock.fencing_token + 1,
          lease_owner: command.lease_owner,
          lease_expires_at: expires_at,
          version: lock.version + 1,
          updated_at: now
        })
        |> Repo.update!()
        |> lock_result()

      true ->
        Repo.rollback(
          Error.new(:conflict, "target operation is already in progress",
            details: %{
              reason_code: "target_operation_in_progress",
              target_id: lock.target_id,
              operation_id: lock.operation_id,
              operation_type: lock.operation_type
            }
          )
        )
    end
  end

  defp renew_many!(command) do
    workspace_id = command.workspace_context.workspace_id
    target_ids = Enum.map(command.locks, & &1.target_id)
    lock_identities!(workspace_id, target_ids)
    now = database_now!()
    expires_at = DateTime.add(now, command.lease_duration_ms, :millisecond)

    rows = Enum.map(target_ids, &lock_row(workspace_id, &1))

    Enum.zip(rows, command.locks)
    |> Enum.each(fn
      {nil, _lock_ref} ->
        Repo.rollback(Error.new(:fenced, "target operation lock no longer exists"))

      {lock, lock_ref} ->
        unless exact_live_lock?(lock, command, lock_ref, now) do
          Repo.rollback(
            Error.new(:fenced, "target operation lock fence is stale",
              details: %{target_id: lock.target_id}
            )
          )
        end
    end)

    Enum.map(rows, fn lock ->
      lock
      |> Ecto.Changeset.change(%{
        lease_expires_at: expires_at,
        version: lock.version + 1,
        updated_at: now
      })
      |> Repo.update!()
      |> lock_result()
    end)
  end

  defp release_many!(command) do
    workspace_id = command.workspace_context.workspace_id
    target_ids = Enum.map(command.locks, & &1.target_id)
    lock_identities!(workspace_id, target_ids)
    rows = Enum.map(target_ids, &lock_row(workspace_id, &1))

    Enum.zip(rows, command.locks)
    |> Enum.each(fn
      {nil, _lock_ref} ->
        :ok

      {lock, lock_ref} ->
        unless lock.operation_id == command.operation_id and
                 lock.lease_owner == command.lease_owner and
                 lock.fencing_token == lock_ref.fencing_token do
          Repo.rollback(
            Error.new(:fenced, "target operation lock fence is stale",
              details: %{target_id: lock.target_id}
            )
          )
        end
    end)

    Enum.each(rows, fn
      nil -> :ok
      lock -> Repo.delete!(lock)
    end)

    :ok
  end

  defp exact_live_lock?(lock, command, lock_ref, now) do
    lock.operation_id == command.operation_id and lock.lease_owner == command.lease_owner and
      lock.fencing_token == lock_ref.fencing_token and
      DateTime.compare(lock.lease_expires_at, now) == :gt
  end

  defp lock_identities!(workspace_id, target_ids) do
    Enum.each(target_ids, fn target_id ->
      SQL.query!(
        Repo,
        "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
        ["favn:target-operation:" <> workspace_id <> ":" <> target_id]
      )
    end)
  end

  defp lock_row(workspace_id, target_id) do
    from(lock in TargetOperationLock,
      where: lock.workspace_id == ^workspace_id and lock.target_id == ^target_id,
      lock: "FOR UPDATE"
    )
    |> Repo.one()
  end

  defp lock_result(lock) do
    %LockResult{
      workspace_id: lock.workspace_id,
      target_id: lock.target_id,
      operation_id: lock.operation_id,
      operation_type: String.to_existing_atom(lock.operation_type),
      fencing_token: lock.fencing_token,
      lease_owner: lock.lease_owner,
      lease_expires_at: lock.lease_expires_at,
      version: lock.version,
      inserted_at: lock.inserted_at,
      updated_at: lock.updated_at
    }
  end

  defp validate_acquire(command) do
    if valid_context?(command.workspace_context) and valid_id?(command.command_id) and
         valid_targets?(command.target_ids) and valid_id?(command.operation_id) and
         command.operation_type == :rebuild and valid_id?(command.lease_owner) and
         valid_duration?(command.lease_duration_ms) and match?(%DateTime{}, command.occurred_at),
       do: :ok,
       else: {:error, ErrorMapper.map(:invalid)}
  end

  defp validate_renew(command) do
    if valid_context?(command.workspace_context) and valid_id?(command.command_id) and
         valid_id?(command.operation_id) and valid_id?(command.lease_owner) and
         valid_lock_refs?(command.locks) and valid_duration?(command.lease_duration_ms) and
         match?(%DateTime{}, command.occurred_at),
       do: :ok,
       else: {:error, ErrorMapper.map(:invalid)}
  end

  defp validate_release(command) do
    if valid_context?(command.workspace_context) and valid_id?(command.command_id) and
         valid_id?(command.operation_id) and valid_id?(command.lease_owner) and
         valid_lock_refs?(command.locks) and match?(%DateTime{}, command.occurred_at),
       do: :ok,
       else: {:error, ErrorMapper.map(:invalid)}
  end

  defp valid_lock_refs?(locks) when is_list(locks) and locks != [] do
    target_ids = Enum.map(locks, &Map.get(&1, :target_id))

    valid_targets?(target_ids) and
      Enum.all?(locks, fn lock ->
        match?(
          %{target_id: target_id, fencing_token: token}
          when is_binary(target_id) and
                 is_integer(token) and token > 0,
          lock
        )
      end)
  end

  defp valid_lock_refs?(_locks), do: false

  defp valid_targets?(target_ids) when is_list(target_ids) do
    target_ids != [] and length(target_ids) <= @max_targets and
      target_ids == Enum.sort(Enum.uniq(target_ids)) and Enum.all?(target_ids, &valid_id?/1)
  end

  defp valid_targets?(_target_ids), do: false

  defp valid_context?(context), do: WorkspaceContext.valid?(context)
  defp valid_id?(value), do: is_binary(value) and byte_size(value) in 1..255

  defp valid_duration?(duration),
    do: is_integer(duration) and duration > 0 and duration <= @max_lease_ms

  defp database_now! do
    %{rows: [[now]]} = SQL.query!(Repo, "SELECT clock_timestamp()", [])
    now
  end

  defp transaction(fun) do
    case Repo.transaction(fun) do
      {:ok, result} -> {:ok, result}
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, ErrorMapper.map(reason)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end
end
