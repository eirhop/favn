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
  alias FavnStoragePostgres.RunLeaseRepo
  alias FavnStoragePostgres.RunTransaction
  alias FavnStoragePostgres.Schemas.RunOwnership

  def validate_execution!(authority, kind \\ :asset_attempt) do
    held = lock_ownership!(authority.workspace_id, authority.run_id)

    cleanup_read =
      held.claim_purpose == "cleanup" and
        kind in [:relation_inspection, :generation_capabilities, :generation_marker_read] and
        FavnStoragePostgres.CancellationOwnership.cancelled?(
          authority.workspace_id,
          authority.run_id
        )

    unless matching_owner?(held, authority) and is_nil(held.released_at) and
             future?(held.expires_at) and
             (cleanup_read or
                (held.claim_purpose == "execution" and held.recovery_disposition == "automatic")),
           do: Repo.rollback(Error.new(:fenced, "run execution authority unavailable"))

    :ok
  end

  @impl true
  def require_diagnosis(command) do
    with true <- workspace_context?(command.workspace_context) and valid_id?(command.reason_code),
         :ok <- validate_release(command),
         {:ok, :ok} <-
           RunTransaction.transaction(fn ->
             workspace = command.workspace_context.workspace_id
             FavnStoragePostgres.RunIdentity.lock!(workspace, command.run_id)
             held = lock_ownership!(workspace, command.run_id)

             unless matching_owner?(held, command) and is_nil(held.released_at) and
                      future?(held.expires_at),
                    do:
                      Repo.rollback(
                        Error.new(:fenced, "diagnosis requires current run authority")
                      )

             SQL.query!(
               Repo,
               """
               UPDATE favn_control.run_ownerships SET claim_purpose='diagnosis', recovery_attempts=3,
                 diagnosis_reason=COALESCE(diagnosis_reason,$3) WHERE workspace_id=$1 AND run_id=$2 AND recovery_disposition='automatic'
               """,
               [workspace, command.run_id, command.reason_code]
             )

             pace!(workspace, command.run_id)
             :ok
           end) do
      :ok
    else
      false -> {:error, Error.new(:invalid, "invalid run diagnosis command")}
      error -> error
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def check_resume(command) do
    context = command.workspace_context

    if workspace_context?(context) and valid_id?(command.run_id) and valid_id?(command.command_id) do
      Repo.transaction(
        fn ->
          if resume_replayed?(command) do
            :already_resumed
          else
            %{rows: rows} =
              SQL.query!(
                Repo,
                """
                SELECT 1 FROM favn_control.run_ownerships o JOIN favn_control.runs r USING(workspace_id, run_id)
                WHERE o.workspace_id=$1 AND o.run_id=$2 AND o.recovery_disposition='attention'
                  AND o.attention_revision=$3 AND r.cancellation_requested_at IS NULL
                  AND r.status IN ('pending','running')
                """,
                [context.workspace_id, command.run_id, command.expected_revision]
              )

            if rows == [] or
                 FavnStoragePostgres.CancellationOwnership.cancelled?(
                   context.workspace_id,
                   command.run_id
                 ),
               do:
                 Repo.rollback(
                   Error.new(:conflict, "recovery attention revision or cancellation changed")
                 )

            :ready
          end
        end,
        timeout: 2_000
      )
    else
      {:error, Error.new(:invalid, "invalid recovery resume command")}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def resume_recovery(command) do
    context = command.workspace_context

    with true <-
           workspace_context?(context) and valid_id?(command.run_id) and
             valid_id?(command.command_id) and
             is_integer(command.expected_revision) and command.expected_revision > 0,
         {:ok, :ok} <-
           RunTransaction.transaction(fn ->
             FavnStoragePostgres.CancellationOwnership.lock!(context.workspace_id, command.run_id)

             FavnStoragePostgres.Idempotency.Transaction.execute!(
               context.workspace_id,
               command.idempotency,
               fn -> resume_recovery!(command) end,
               fn :ok -> {:ok, %{response: %{"resumed" => true}, response_status: 200}} end,
               fn %{response: %{"resumed" => true}} -> {:ok, :ok} end
             )
           end),
         do: :ok
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  defp resume_replayed?(command) do
    context = command.workspace_context

    %{rows: replay} =
      SQL.query!(
        Repo,
        """
        SELECT e.event FROM favn_control.outbox_events o JOIN favn_control.run_events e USING(workspace_id, outbox_event_id) WHERE o.workspace_id=$1 AND o.command_id=$2
        """,
        [context.workspace_id, command.command_id]
      )

    if replay == [] do
      false
    else
      [[event]] = replay

      with {:ok, decoded} <- FavnOrchestrator.Storage.RunEventCodec.decode(Jason.encode!(event)),
           true <-
             decoded.run_id == command.run_id and decoded.event_type == :run_recovery_resumed and
               (decoded.data[:revision] || decoded.data["revision"]) == command.expected_revision do
        true
      else
        _ -> Repo.rollback(Error.new(:conflict, "recovery resume command identity changed"))
      end
    end
  end

  defp resume_recovery!(command) do
    context = command.workspace_context
    ownership = lock_ownership!(context.workspace_id, command.run_id)

    if resume_replayed?(command) do
      :ok
    else
      unless ownership.recovery_disposition == "attention" and
               ownership.attention_revision == command.expected_revision,
             do: Repo.rollback(Error.new(:conflict, "recovery attention revision changed"))

      if FavnStoragePostgres.CancellationOwnership.cancelled?(
           context.workspace_id,
           command.run_id
         ),
         do: Repo.rollback(Error.new(:conflict, "run cancellation owns recovery"))

      {:ok, run} =
        FavnStoragePostgres.Runs.Store.locked_snapshot(context.workspace_id, command.run_id)

      if run.status not in [:pending, :running],
        do: Repo.rollback(Error.new(:conflict, "terminal run cannot resume recovery"))

      resumed =
        FavnOrchestrator.RunState.transition(run,
          metadata: Map.drop(run.metadata, [:recovery_attention, "recovery_attention"])
        )

      event =
        FavnOrchestrator.Projector.run_event(resumed, :run_recovery_resumed, %{
          revision: command.expected_revision,
          principal_id: context.principal_id,
          request_id: context.request_id
        })

      transition = %FavnOrchestrator.Persistence.Commands.CommitRunTransition{
        workspace_context: context,
        command_id: command.command_id,
        expected_sequence: run.event_seq,
        run: resumed,
        event: event
      }

      case FavnStoragePostgres.Runs.Store.commit_transition(transition) do
        {:ok, _} -> :ok
        {:error, reason} -> Repo.rollback(reason)
      end

      SQL.query!(
        Repo,
        """
        UPDATE favn_control.run_ownerships SET released_at=clock_timestamp(), expires_at=clock_timestamp(),
          recovery_disposition='automatic', recovery_attempts=0, next_recovery_at=clock_timestamp(),
          attention_revision=NULL, diagnosis_reason=NULL, updated_at=clock_timestamp()
        WHERE workspace_id=$1 AND run_id=$2
        """,
        [context.workspace_id, command.run_id]
      )

      :ok
    end
  end

  @impl true
  def maintain_targets(context, ownership, task_ids, renewal_id) do
    with true <-
           workspace_context?(context) and context.workspace_id == ownership.workspace_id and
             is_list(task_ids) and length(task_ids) <= 512 and valid_id?(renewal_id),
         {:ok, result} <-
           Repo.transaction(
             fn ->
               SQL.query!(Repo, "SET LOCAL transaction_timeout = '4s'", [])

               FavnStoragePostgres.Maintenance.History.guard!(
                 context.workspace_id,
                 ownership.run_id
               )

               held =
                 lock_ownership!(
                   context.workspace_id,
                   ownership.run_id,
                   Repo,
                   "FOR UPDATE NOWAIT"
                 )

               unless matching_owner?(held, ownership) and is_nil(held.released_at) and
                        future?(held.expires_at),
                      do:
                        Repo.rollback(Error.new(:fenced, "run target maintenance lost ownership"))

               %{rows: rows} =
                 SQL.query!(
                   Repo,
                   """
                   SELECT t.task_id, t.status,
                     t.orchestration_context->>'format',
                     claim.value IS NOT NULL,
                     claim.value = 'null'::jsonb OR (claim.value->>0 = 'map' AND (
                       (projection.purpose = '["atom","ownership_only"]'::jsonb
                         AND (projection.lock IS NULL OR projection.lock = 'null'::jsonb)) OR
                       (projection.purpose = '["atom","materialization"]'::jsonb
                         AND projection.lock IS NOT NULL))),
                     projection.lock,
                     t.write_target_id
                   FROM favn_control.runner_tasks t
                   LEFT JOIN LATERAL (
                     SELECT jsonb_path_query_first(t.orchestration_context,
                       'strict $.data[1][*] ? (@[0][0] == "atom" && @[0][1] == "materialization_claim")[1]', '{}'::jsonb, true) AS value
                   ) claim ON TRUE
                   LEFT JOIN LATERAL (
                     SELECT
                       jsonb_path_query_first(claim.value,
                         'strict $[1][*] ? (@[0][0] == "atom" && @[0][1] == "purpose")[1]', '{}'::jsonb, true) AS purpose,
                       jsonb_path_query_first(claim.value,
                         'strict $[1][*] ? (@[0][0] == "atom" && @[0][1] == "target_operation_lock")[1]', '{}'::jsonb, true) AS lock
                   ) projection ON TRUE
                   WHERE t.workspace_id=$1 AND t.run_id=$2 AND t.task_kind='asset_attempt'
                     AND (t.task_id=ANY($3::text[]) OR t.status NOT IN ('succeeded','failed','cancelled','unknown'))
                   ORDER BY t.task_id LIMIT 513
                   """,
                   [context.workspace_id, ownership.run_id, task_ids]
                 )

               if length(rows) > 512,
                 do:
                   Repo.rollback(
                     Error.new(:limit_exceeded, "target maintenance capacity exceeded")
                   )

               Map.new(rows, fn
                 [task, status, _, _, _, _, _]
                 when status in ["succeeded", "failed", "cancelled", "unknown"] ->
                   {task, :terminal}

                 [task, _, "task-data-v1", true, true, encoded, write_target] ->
                   case FavnOrchestrator.RunnerTaskContext.decode_target_lock(encoded) do
                     {:ok, nil} ->
                       {task, :terminal}

                     {:ok, lock}
                     when lock.workspace_id == context.workspace_id and
                            lock.target_id == write_target and
                            lock.operation_type == :materialization ->
                       renew_target!(context.workspace_id, task, lock, renewal_id)

                     _ ->
                       terminal_or_lost!(context.workspace_id, task)
                   end

                 [task | _] ->
                   terminal_or_lost!(context.workspace_id, task)
               end)
             end,
             timeout: 5_000
           ),
         do: {:ok, result}
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  defp renew_target!(workspace, task, lock, renewal_id) do
    %{rows: updated} =
      SQL.query!(
        Repo,
        """
        UPDATE favn_control.target_operation_locks SET
          lease_expires_at=CASE WHEN last_renewal_id=$6 THEN lease_expires_at ELSE clock_timestamp()+interval '60 seconds' END,
          last_renewal_id=$6, updated_at=clock_timestamp()
        WHERE workspace_id=$1 AND target_id=$2 AND fencing_token=$3
          AND operation_id=$4 AND lease_owner=$5 AND operation_type='materialization'
          AND lease_expires_at>clock_timestamp()
        RETURNING lease_expires_at, clock_timestamp()
        """,
        [
          workspace,
          lock.target_id,
          lock.fencing_token,
          lock.operation_id,
          lock.lease_owner,
          renewal_id
        ]
      )

    case updated do
      [[expiry, observed]] -> {task, %{expires_at: expiry, database_observed_at: observed}}
      [] -> terminal_or_lost!(workspace, task)
    end
  end

  defp terminal_or_lost!(workspace, task) do
    %{rows: rows} =
      SQL.query!(
        Repo,
        """
        SELECT 1 FROM favn_control.runner_tasks WHERE workspace_id=$1 AND task_id=$2
          AND status IN ('succeeded','failed','cancelled','unknown')
        """,
        [workspace, task]
      )

    if rows != [],
      do: {task, :terminal},
      else:
        Repo.rollback(
          Error.new(
            :conflict,
            "original target lease requires reconciliation",
            details: %{reason_code: "target_lease_lost", task_id: task}
          )
        )
  end

  @impl true
  def recovery_candidates(context, limit) do
    if workspace_context?(context) and is_integer(limit) and limit in 1..64 do
      %{rows: rows} =
        SQL.query!(
          Repo,
          """
          SELECT o.run_id FROM favn_control.run_ownerships o
          JOIN favn_control.runs r USING(workspace_id, run_id)
          WHERE o.workspace_id=$1 AND r.status IN ('pending','running')
            AND o.recovery_disposition='automatic'
            AND (o.next_recovery_at IS NULL OR o.next_recovery_at <= clock_timestamp())
            AND ((o.owner_id IS NULL AND o.updated_at < clock_timestamp()-interval '30 seconds')
              OR o.released_at IS NOT NULL OR o.expires_at <= clock_timestamp())
          ORDER BY o.next_recovery_at NULLS FIRST, o.run_id LIMIT $2
          """,
          [context.workspace_id, limit]
        )

      {:ok, Enum.map(rows, &hd/1)}
    else
      {:error, Error.new(:invalid, "invalid recovery candidates query")}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def claim_run(%ClaimRun{} = command) do
    with :ok <- validate_claim(command),
         {:ok, ownership} <- RunTransaction.transaction(fn -> claim_run!(command) end) do
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
         {:ok, rows} <- RunTransaction.transaction(fn -> claim_recovery!(command) end) do
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
         {:ok, ownership} <-
           RunLeaseRepo.transaction(fn -> renew_run!(command) end, timeout: 2_000) do
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
         {:ok, :ok} <- RunTransaction.transaction(fn -> release_run!(command) end) do
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
    FavnStoragePostgres.RunIdentity.lock!(workspace_id, command.run_id)
    ownership = lock_ownership!(workspace_id, command.run_id)

    cond do
      ownership.claim_command_id == command.command_id and ownership.owner_id == command.owner_id ->
        if future?(ownership.expires_at) and is_nil(ownership.released_at) and
             (ownership.recovery_disposition == "automatic" or command.purpose == :cleanup) and
             ((command.purpose == :execution and
                 ownership.claim_purpose in ["execution", "diagnosis"]) or
                (command.purpose == :cleanup and ownership.claim_purpose == "cleanup" and
                   FavnStoragePostgres.CancellationOwnership.cancelled?(
                     workspace_id,
                     command.run_id
                   ))) do
          ownership_result(ownership)
        else
          Repo.rollback(Error.new(:fenced, "replayed run claim has expired"))
        end

      available?(ownership) and eligible?(ownership, command) ->
        %{rows: [row]} =
          SQL.query!(
            Repo,
            """
            UPDATE favn_control.run_ownerships
            SET owner_id = $3,
                fencing_token = fencing_token + 1,
                claim_command_id = $4,
                recovery_attempts = CASE WHEN $6 = 'cleanup' THEN recovery_attempts
                  WHEN fencing_token = 0 THEN 0 ELSE LEAST(recovery_attempts + 1, 3) END,
                claim_purpose = CASE WHEN $6 = 'cleanup' THEN 'cleanup'
                  WHEN recovery_attempts >= 3 THEN 'diagnosis' ELSE $6 END,
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
              command.lease_duration_ms,
              Atom.to_string(command.purpose)
            ]
          )

        pace!(workspace_id, command.run_id)
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
            AND ownership.recovery_disposition = 'automatic'
            AND (ownership.next_recovery_at IS NULL OR ownership.next_recovery_at <= clock_timestamp())
            AND ($7::text[] IS NULL OR ownership.run_id = ANY($7))
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
            recovery_attempts = CASE WHEN ownership.fencing_token = 0 THEN 0
              ELSE LEAST(ownership.recovery_attempts + 1, 3) END,
            claim_purpose = CASE WHEN ownership.recovery_attempts >= 3 THEN 'diagnosis' ELSE 'execution' END,
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
          command.unowned_grace_period_ms,
          command.run_ids
        ]
      )

    Enum.each(rows, fn [workspace, run | _] -> pace!(workspace, run) end)
    rows |> Enum.map(&ownership_result/1) |> Enum.sort_by(& &1.run_id)
  end

  defp renew_run!(command) do
    workspace_id = command.workspace_context.workspace_id
    FavnStoragePostgres.Maintenance.History.guard!(workspace_id, command.run_id, RunLeaseRepo)
    ownership = lock_ownership!(workspace_id, command.run_id, RunLeaseRepo, "FOR UPDATE NOWAIT")

    cond do
      is_nil(ownership.released_at) and ownership.last_renewal_id == command.renewal_id and
        matching_owner?(ownership, command) and
          future?(ownership.expires_at, RunLeaseRepo) ->
        ownership_result(ownership, RunLeaseRepo)

      not matching_owner?(ownership, command) or not is_nil(ownership.released_at) or
          not future?(ownership.expires_at, RunLeaseRepo) ->
        RunLeaseRepo.rollback(Error.new(:fenced, "run ownership cannot be renewed"))

      true ->
        %{rows: [row]} =
          SQL.query!(
            RunLeaseRepo,
            """
            UPDATE favn_control.run_ownerships
            SET last_renewal_id = $5,
                last_renewed_at = clock_timestamp(),
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

        pace!(workspace_id, command.run_id, RunLeaseRepo)
        ownership_result(row, RunLeaseRepo)
    end
  end

  defp release_run!(command) do
    workspace_id = command.workspace_context.workspace_id
    FavnStoragePostgres.RunIdentity.lock!(workspace_id, command.run_id)
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

      pace!(workspace_id, command.run_id)
      :ok
    else
      Repo.rollback(Error.new(:fenced, "run ownership cannot be released"))
    end
  end

  defp lock_ownership!(workspace_id, run_id, repo \\ Repo, lock \\ "FOR UPDATE") do
    from(ownership in RunOwnership,
      where: ownership.workspace_id == ^workspace_id and ownership.run_id == ^run_id
    )
    |> then(fn query ->
      if lock == "FOR UPDATE NOWAIT",
        do: lock(query, "FOR UPDATE NOWAIT"),
        else: lock(query, "FOR UPDATE")
    end)
    |> repo.one()
    |> case do
      nil -> repo.rollback(Error.new(:not_found, "run ownership root not found"))
      ownership -> ownership
    end
  end

  defp available?(ownership) do
    is_nil(ownership.owner_id) or not is_nil(ownership.released_at) or
      not future?(ownership.expires_at)
  end

  defp future?(expires_at, repo \\ Repo)
  defp future?(nil, _repo), do: false

  defp future?(expires_at, repo) do
    %{rows: [[future?]]} =
      SQL.query!(repo, "SELECT $1::timestamptz > clock_timestamp()", [expires_at])

    future?
  end

  defp matching_owner?(ownership, command),
    do:
      ownership.owner_id == command.owner_id and ownership.fencing_token == command.fencing_token

  defp ownership_result(ownership, repo \\ Repo)

  defp ownership_result(%RunOwnership{} = ownership, repo) do
    %RunOwnershipResult{
      workspace_id: ownership.workspace_id,
      run_id: ownership.run_id,
      owner_id: ownership.owner_id,
      fencing_token: ownership.fencing_token,
      expires_at: ownership.expires_at,
      claim_purpose: String.to_existing_atom(ownership.claim_purpose),
      recovery_attempts: ownership.recovery_attempts,
      diagnosis_reason: ownership.diagnosis_reason,
      database_observed_at: observed_at(repo)
    }
  end

  defp ownership_result([workspace_id, run_id, _owner_id, _fencing_token, _expires_at], repo) do
    ownership = repo.get_by!(RunOwnership, workspace_id: workspace_id, run_id: run_id)
    ownership_result(ownership, repo)
  end

  defp observed_at(repo) do
    %{rows: [[now]]} = SQL.query!(repo, "SELECT clock_timestamp()", [])
    now
  end

  defp eligible?(ownership, command) do
    due? = is_nil(ownership.next_recovery_at) or not future?(ownership.next_recovery_at)

    if command.purpose == :cleanup do
      %{rows: rows} =
        SQL.query!(
          Repo,
          "SELECT 1 FROM favn_control.runs WHERE workspace_id=$1 AND run_id=$2 AND cancellation_requested_at IS NOT NULL",
          [ownership.workspace_id, ownership.run_id]
        )

      rows != []
    else
      ownership.recovery_disposition == "automatic" and due?
    end
  end

  defp pace!(workspace, run, repo \\ Repo) do
    SQL.query!(
      repo,
      """
      UPDATE favn_control.run_ownerships SET next_recovery_at =
        COALESCE(released_at, expires_at) +
        ((CASE recovery_attempts WHEN 0 THEN 0 WHEN 1 THEN 5000 WHEN 2 THEN 15000 ELSE 60000 END
          + CASE WHEN recovery_attempts = 0 THEN 0 ELSE mod(fencing_token * 137, 1001) END) * interval '1 millisecond')
      WHERE workspace_id=$1 AND run_id=$2
      """,
      [workspace, run]
    )
  end

  defp validate_claim(command) when command.purpose in [:execution, :diagnosis, :cleanup],
    do:
      validate_owner_command(
        command.workspace_context,
        command.command_id,
        command.run_id,
        command.owner_id,
        command.lease_duration_ms
      )

  defp validate_claim(_command), do: {:error, :invalid}

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
