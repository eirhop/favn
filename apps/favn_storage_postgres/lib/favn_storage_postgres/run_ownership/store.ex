defmodule FavnStoragePostgres.RunOwnership.Store do
  @moduledoc false

  @behaviour FavnOrchestrator.Persistence.RunOwnershipStore

  import Ecto.Query

  alias Ecto.Adapters.SQL
  alias FavnOrchestrator.Persistence.Commands.ClaimRecoveryBatch
  alias FavnOrchestrator.Persistence.Commands.ClaimRun
  alias FavnOrchestrator.Persistence.Commands.ReleaseRunOwnership
  alias FavnOrchestrator.Persistence.Commands.RenewRunOwnership
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Identity
  alias FavnOrchestrator.Persistence.Results.RunOwnership, as: RunOwnershipResult
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnStoragePostgres.ErrorMapper
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.Schemas.RunOwnership

  @impl true
  def claim_run(%ClaimRun{} = command) do
    with :ok <- validate_claim(command),
         {:ok, ownership} <- Repo.transaction(fn -> claim_run!(command) end) do
      {:ok, ownership}
    else
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, ErrorMapper.map(reason)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def claim_recovery_batch(%ClaimRecoveryBatch{} = command) do
    with :ok <- validate_recovery(command),
         {:ok, rows} <- Repo.transaction(fn -> claim_recovery!(command) end) do
      {:ok, rows}
    else
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, ErrorMapper.map(reason)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def renew_run(%RenewRunOwnership{} = command) do
    with :ok <- validate_renew(command),
         {:ok, ownership} <- Repo.transaction(fn -> renew_run!(command) end) do
      {:ok, ownership}
    else
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, ErrorMapper.map(reason)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def release_run(%ReleaseRunOwnership{} = command) do
    with :ok <- validate_release(command),
         {:ok, :ok} <- Repo.transaction(fn -> release_run!(command) end) do
      :ok
    else
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, ErrorMapper.map(reason)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  defp claim_run!(command) do
    workspace_id = command.workspace_context.workspace_id
    ownership = lock_ownership!(workspace_id, command.run_id)

    cond do
      ownership.claim_command_id == command.command_id and ownership.owner_id == command.owner_id ->
        if future?(ownership.expires_at) and is_nil(ownership.released_at) do
          ownership_result(ownership)
        else
          Repo.rollback(Error.new(:fenced, "replayed run claim has expired"))
        end

      available?(ownership) ->
        %{rows: [row]} =
          SQL.query!(
            Repo,
            """
            UPDATE favn_control.run_ownerships
            SET owner_id = $3,
                fencing_token = fencing_token + 1,
                claim_command_id = $4,
                last_renewal_id = NULL,
                expires_at = clock_timestamp() + ($5 * interval '1 millisecond'),
                released_at = NULL,
                updated_at = clock_timestamp()
            WHERE workspace_id = $1 AND run_id = $2
            RETURNING workspace_id, run_id, owner_id, fencing_token, expires_at
            """,
            [
              workspace_id,
              command.run_id,
              command.owner_id,
              command.command_id,
              command.lease_duration_ms
            ]
          )

        ownership_result(row)

      true ->
        Repo.rollback(
          Error.new(:conflict, "run is owned by another active worker", retryable?: true)
        )
    end
  end

  defp claim_recovery!(command) do
    workspace_id = command.workspace_context.workspace_id

    %{rows: rows} =
      SQL.query!(
        Repo,
        """
        WITH candidates AS (
          SELECT ownership.workspace_id, ownership.run_id
          FROM favn_control.run_ownerships ownership
          JOIN favn_control.runs run
            ON run.workspace_id = ownership.workspace_id AND run.run_id = ownership.run_id
          WHERE ownership.workspace_id = $1
            AND run.status IN ('pending', 'running')
            AND (
              (ownership.owner_id IS NULL
               AND ownership.updated_at <=
                 clock_timestamp() - ($6 * interval '1 millisecond'))
              OR ownership.released_at IS NOT NULL
              OR ownership.expires_at <= clock_timestamp()
              OR (ownership.owner_id IS NOT NULL AND ownership.expires_at IS NULL)
            )
          ORDER BY ownership.updated_at, ownership.run_id
          LIMIT $2
          FOR UPDATE OF ownership SKIP LOCKED
        )
        UPDATE favn_control.run_ownerships ownership
        SET owner_id = $3,
            fencing_token = ownership.fencing_token + 1,
            claim_command_id = $4 || ':' || ownership.run_id,
            last_renewal_id = NULL,
            expires_at = clock_timestamp() + ($5 * interval '1 millisecond'),
            released_at = NULL,
            updated_at = clock_timestamp()
        FROM candidates
        WHERE ownership.workspace_id = candidates.workspace_id
          AND ownership.run_id = candidates.run_id
        RETURNING ownership.workspace_id, ownership.run_id, ownership.owner_id,
                  ownership.fencing_token, ownership.expires_at
        """,
        [
          workspace_id,
          command.limit,
          command.owner_id,
          command.batch_id,
          command.lease_duration_ms,
          command.unowned_grace_period_ms
        ]
      )

    rows |> Enum.map(&ownership_result/1) |> Enum.sort_by(& &1.run_id)
  end

  defp renew_run!(command) do
    workspace_id = command.workspace_context.workspace_id
    ownership = lock_ownership!(workspace_id, command.run_id)

    cond do
      ownership.last_renewal_id == command.renewal_id and matching_owner?(ownership, command) and
          future?(ownership.expires_at) ->
        ownership_result(ownership)

      not matching_owner?(ownership, command) or not is_nil(ownership.released_at) or
          not future?(ownership.expires_at) ->
        Repo.rollback(Error.new(:fenced, "run ownership cannot be renewed"))

      true ->
        %{rows: [row]} =
          SQL.query!(
            Repo,
            """
            UPDATE favn_control.run_ownerships
            SET last_renewal_id = $5,
                expires_at = clock_timestamp() + ($6 * interval '1 millisecond'),
                updated_at = clock_timestamp()
            WHERE workspace_id = $1 AND run_id = $2 AND owner_id = $3 AND fencing_token = $4
              AND released_at IS NULL AND expires_at > clock_timestamp()
            RETURNING workspace_id, run_id, owner_id, fencing_token, expires_at
            """,
            [
              workspace_id,
              command.run_id,
              command.owner_id,
              command.fencing_token,
              command.renewal_id,
              command.lease_duration_ms
            ]
          )

        ownership_result(row)
    end
  end

  defp release_run!(command) do
    workspace_id = command.workspace_context.workspace_id
    ownership = lock_ownership!(workspace_id, command.run_id)

    if matching_owner?(ownership, command) do
      if is_nil(ownership.released_at) do
        SQL.query!(
          Repo,
          """
          UPDATE favn_control.run_ownerships
          SET released_at = clock_timestamp(), expires_at = clock_timestamp(),
              updated_at = clock_timestamp()
          WHERE workspace_id = $1 AND run_id = $2 AND owner_id = $3 AND fencing_token = $4
          """,
          [workspace_id, command.run_id, command.owner_id, command.fencing_token]
        )
      end

      :ok
    else
      Repo.rollback(Error.new(:fenced, "run ownership cannot be released"))
    end
  end

  defp lock_ownership!(workspace_id, run_id) do
    from(ownership in RunOwnership,
      where: ownership.workspace_id == ^workspace_id and ownership.run_id == ^run_id,
      lock: "FOR UPDATE"
    )
    |> Repo.one()
    |> case do
      nil -> Repo.rollback(Error.new(:not_found, "run ownership root not found"))
      ownership -> ownership
    end
  end

  defp available?(ownership) do
    is_nil(ownership.owner_id) or not is_nil(ownership.released_at) or
      not future?(ownership.expires_at)
  end

  defp future?(nil), do: false

  defp future?(expires_at) do
    %{rows: [[future?]]} =
      SQL.query!(Repo, "SELECT $1::timestamptz > clock_timestamp()", [expires_at])

    future?
  end

  defp matching_owner?(ownership, command),
    do:
      ownership.owner_id == command.owner_id and ownership.fencing_token == command.fencing_token

  defp ownership_result(%RunOwnership{} = ownership) do
    %RunOwnershipResult{
      workspace_id: ownership.workspace_id,
      run_id: ownership.run_id,
      owner_id: ownership.owner_id,
      fencing_token: ownership.fencing_token,
      expires_at: ownership.expires_at
    }
  end

  defp ownership_result([workspace_id, run_id, owner_id, fencing_token, expires_at]) do
    %RunOwnershipResult{
      workspace_id: workspace_id,
      run_id: run_id,
      owner_id: owner_id,
      fencing_token: fencing_token,
      expires_at: expires_at
    }
  end

  defp validate_claim(command),
    do:
      validate_owner_command(
        command.workspace_context,
        command.command_id,
        command.run_id,
        command.owner_id,
        command.lease_duration_ms
      )

  defp validate_recovery(command) do
    with :ok <-
           validate_owner_command(
             command.workspace_context,
             command.batch_id,
             "batch",
             command.owner_id,
             command.lease_duration_ms
           ),
         true <- is_integer(command.limit) and command.limit >= 1 and command.limit <= 500,
         true <- valid_unowned_grace_period?(command.unowned_grace_period_ms) do
      :ok
    else
      _value -> {:error, :invalid}
    end
  end

  defp validate_renew(command) do
    with :ok <-
           validate_owner_command(
             command.workspace_context,
             command.renewal_id,
             command.run_id,
             command.owner_id,
             command.lease_duration_ms
           ),
         true <- is_integer(command.fencing_token) and command.fencing_token > 0 do
      :ok
    else
      _value -> {:error, :invalid}
    end
  end

  defp validate_release(command) do
    if workspace_context?(command.workspace_context) and valid_id?(command.run_id) and
         valid_id?(command.owner_id) and is_integer(command.fencing_token) and
         command.fencing_token > 0,
       do: :ok,
       else: {:error, :invalid}
  end

  defp valid_unowned_grace_period?(value),
    do: is_integer(value) and value >= 0 and value <= 3_600_000

  defp validate_owner_command(context, command_id, run_id, owner_id, duration) do
    if workspace_context?(context) and Enum.all?([command_id, run_id, owner_id], &valid_id?/1) and
         is_integer(duration) and duration >= 1_000 and duration <= 3_600_000,
       do: :ok,
       else: {:error, :invalid}
  end

  defp workspace_context?(%WorkspaceContext{roles: roles} = context),
    do:
      WorkspaceContext.valid?(context) and
        Enum.any?(roles, &(&1 in [:customer_operator, :workspace_admin, :platform_operator]))

  defp workspace_context?(_context), do: false
  defp valid_id?(value), do: Identity.valid?(value)
end
