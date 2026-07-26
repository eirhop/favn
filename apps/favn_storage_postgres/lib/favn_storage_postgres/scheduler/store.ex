defmodule FavnStoragePostgres.Scheduler.Store do
  @moduledoc false

  @behaviour FavnOrchestrator.Persistence.SchedulerStore

  import Ecto.Query

  alias Ecto.Adapters.SQL
  alias FavnOrchestrator.Persistence.Commands.ClaimDueSchedules
  alias FavnOrchestrator.Persistence.Commands.AuthorizeScheduleOccurrenceDispatch
  alias FavnOrchestrator.Persistence.Commands.ClaimScheduleOccurrences
  alias FavnOrchestrator.Persistence.Commands.CommitScheduleEvaluation
  alias FavnOrchestrator.Persistence.Commands.CompleteScheduleOccurrence
  alias FavnOrchestrator.Persistence.Commands.EnqueueRunSubmission
  alias FavnOrchestrator.Persistence.Commands.ScheduleOccurrenceIntent
  alias FavnOrchestrator.Persistence.Commands.SetScheduleActivation
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Queries.PageScheduleOccurrences
  alias FavnOrchestrator.Persistence.Queries.PageSchedules
  alias FavnOrchestrator.Persistence.Results.CursorPage
  alias FavnOrchestrator.Persistence.Results.Schedule, as: ScheduleResult
  alias FavnOrchestrator.Persistence.Results.ScheduleClaim
  alias FavnOrchestrator.Persistence.Results.ScheduleActivation, as: ScheduleActivationResult
  alias FavnOrchestrator.Persistence.Results.ScheduleOccurrence, as: ScheduleOccurrenceResult
  alias FavnOrchestrator.Persistence.Results.RunSubmission, as: RunSubmissionResult
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnStoragePostgres.CanonicalJSON
  alias FavnStoragePostgres.ErrorMapper
  alias FavnStoragePostgres.Outbox.Writer, as: OutboxWriter
  alias FavnStoragePostgres.Payload
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.RunSubmissions.Store, as: RunSubmissionStore
  alias FavnStoragePostgres.RunSubmissions.Validation, as: RunSubmissionValidation
  alias FavnStoragePostgres.Schemas.ScheduleCursor
  alias FavnStoragePostgres.Schemas.ScheduleActivation
  alias FavnStoragePostgres.Schemas.ScheduleActivationCommand
  alias FavnStoragePostgres.Schemas.ScheduleOccurrence
  alias FavnStoragePostgres.Schemas.WorkspaceRuntimeState

  @impl true
  def claim_due_schedules(%ClaimDueSchedules{} = command) do
    with :ok <- validate_claim(command),
         {:ok, claims} <- Repo.transaction(fn -> claim_due!(command) end) do
      {:ok, claims}
    else
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, ErrorMapper.map(reason)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def commit_evaluation(%CommitScheduleEvaluation{} = command) do
    with :ok <- validate_evaluation(command),
         {:ok, occurrences} <- Repo.transaction(fn -> commit_evaluation!(command) end) do
      {:ok, occurrences}
    else
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, ErrorMapper.map(reason)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def claim_occurrences(%ClaimScheduleOccurrences{} = command) do
    with :ok <- validate_occurrence_claim(command),
         {:ok, occurrences} <- Repo.transaction(fn -> claim_occurrences!(command) end) do
      {:ok, occurrences}
    else
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, ErrorMapper.map(reason)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def authorize_occurrence_dispatch(%AuthorizeScheduleOccurrenceDispatch{} = command) do
    with :ok <- validate_dispatch_authorization(command),
         {:ok, occurrence} <-
           Repo.transaction(fn -> authorize_occurrence_dispatch!(command) end) do
      {:ok, occurrence}
    else
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, ErrorMapper.map(reason)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def complete_occurrence(%CompleteScheduleOccurrence{} = command) do
    with :ok <- validate_completion(command),
         {:ok, occurrence} <- Repo.transaction(fn -> complete_occurrence!(command) end) do
      {:ok, occurrence}
    else
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, ErrorMapper.map(reason)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def set_activation(%SetScheduleActivation{} = command) do
    with :ok <- validate_activation(command),
         {:ok, activation} <- Repo.transaction(fn -> set_activation!(command) end) do
      {:ok, activation}
    else
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, ErrorMapper.map(reason)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def page_schedules(%PageSchedules{} = page) do
    with :ok <- validate_schedule_page(page) do
      query =
        from(cursor in ScheduleCursor,
          join: runtime in WorkspaceRuntimeState,
          on:
            runtime.workspace_id == cursor.workspace_id and
              runtime.active_deployment_id == cursor.deployment_id,
          left_join: activation in ScheduleActivation,
          on:
            activation.workspace_id == cursor.workspace_id and
              activation.pipeline_target_id == cursor.pipeline_target_id and
              activation.schedule_id == cursor.schedule_id,
          where: cursor.workspace_id == ^page.workspace_context.workspace_id,
          order_by: [asc: cursor.pipeline_target_id, asc: cursor.schedule_id],
          limit: ^(page.limit + 1),
          select: {cursor, activation}
        )
        |> schedule_filter(page)
        |> schedule_after(page.after)

      rows = Repo.all(query)
      page_rows = Enum.take(rows, page.limit)
      has_more? = length(rows) > page.limit
      last = List.last(page_rows)

      {:ok,
       %CursorPage{
         items: Enum.map(page_rows, &schedule_result/1),
         limit: page.limit,
         has_more?: has_more?,
         next_cursor:
           if(has_more? and last,
             do: %{
               pipeline_target_id: elem(last, 0).pipeline_target_id,
               schedule_id: elem(last, 0).schedule_id
             }
           )
       }}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def page_occurrences(%PageScheduleOccurrences{} = page) do
    with :ok <- validate_occurrence_page(page) do
      query =
        from(occurrence in ScheduleOccurrence,
          join: runtime in WorkspaceRuntimeState,
          on:
            runtime.workspace_id == occurrence.workspace_id and
              runtime.active_deployment_id == occurrence.deployment_id,
          where:
            occurrence.workspace_id == ^page.workspace_context.workspace_id and
              occurrence.pipeline_target_id == ^page.pipeline_target_id and
              occurrence.schedule_id == ^page.schedule_id,
          order_by: [desc: occurrence.due_at, desc: occurrence.occurrence_id],
          limit: ^(page.limit + 1),
          select: occurrence
        )
        |> occurrence_after(page.after)

      rows = Repo.all(query)
      page_rows = Enum.take(rows, page.limit)
      has_more? = length(rows) > page.limit
      last = List.last(page_rows)

      {:ok,
       %CursorPage{
         items: Enum.map(page_rows, &occurrence_result/1),
         limit: page.limit,
         has_more?: has_more?,
         next_cursor:
           if(has_more? and last,
             do: %{due_at: last.due_at, occurrence_id: last.occurrence_id}
           )
       }}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  defp claim_due!(command) do
    workspace_id = command.workspace_context.workspace_id

    replayed =
      from(cursor in ScheduleCursor,
        where:
          cursor.workspace_id == ^workspace_id and
            cursor.claim_command_id == ^command.batch_id,
        order_by: [asc: cursor.pipeline_target_id, asc: cursor.schedule_id]
      )
      |> Repo.all()

    if replayed != [] do
      now = database_now!()

      if Enum.all?(replayed, &live_schedule_replay?(&1, command, now)) do
        Enum.map(replayed, &schedule_claim/1)
      else
        Repo.rollback(Error.new(:fenced, "replayed schedule claim is stale"))
      end
    else
      %{rows: rows} =
        SQL.query!(
          Repo,
          """
          WITH candidates AS (
            SELECT cursor.workspace_id, cursor.deployment_id,
                   cursor.pipeline_target_id, cursor.schedule_id
            FROM favn_control.schedule_cursors cursor
            JOIN favn_control.workspace_runtime_state runtime
              ON runtime.workspace_id = cursor.workspace_id
             AND runtime.active_deployment_id = cursor.deployment_id
            JOIN favn_control.schedule_activations activation
              ON activation.workspace_id = cursor.workspace_id
             AND activation.pipeline_target_id = cursor.pipeline_target_id
             AND activation.schedule_id = cursor.schedule_id
             AND activation.enabled = TRUE
             AND activation.approved_schedule_fingerprint = cursor.schedule_fingerprint
            WHERE cursor.workspace_id = $1
              AND cursor.next_due_at <= clock_timestamp()
              AND (cursor.claim_owner IS NULL OR cursor.claim_expires_at IS NULL
                   OR cursor.claim_expires_at <= clock_timestamp())
            ORDER BY cursor.next_due_at, cursor.pipeline_target_id, cursor.schedule_id
            LIMIT $2
            FOR UPDATE OF cursor SKIP LOCKED
          )
          UPDATE favn_control.schedule_cursors cursor
          SET claim_owner = $3,
              claim_generation = cursor.claim_generation + 1,
              claim_command_id = $4,
              claim_expires_at = clock_timestamp() + ($5 * interval '1 millisecond'),
              updated_at = clock_timestamp()
          FROM candidates
          WHERE cursor.workspace_id = candidates.workspace_id
            AND cursor.deployment_id = candidates.deployment_id
            AND cursor.pipeline_target_id = candidates.pipeline_target_id
            AND cursor.schedule_id = candidates.schedule_id
          RETURNING cursor.workspace_id, cursor.deployment_id, cursor.pipeline_target_id,
                    cursor.schedule_id, cursor.next_due_at, cursor.cursor, cursor.version,
                    cursor.claim_owner, cursor.claim_generation, cursor.claim_expires_at
          """,
          [
            workspace_id,
            command.limit,
            command.owner_id,
            command.batch_id,
            command.lease_duration_ms
          ]
        )

      rows
      |> Enum.map(&schedule_claim/1)
      |> Enum.sort_by(&{&1.next_due_at, &1.pipeline_target_id, &1.schedule_id})
    end
  end

  defp commit_evaluation!(command) do
    cursor = lock_cursor!(command)

    cond do
      cursor.last_command_id == command.command_id ->
        occurrences_for_evaluation(command)

      not valid_cursor_claim?(cursor, command) ->
        Repo.rollback(Error.new(:fenced, "schedule cursor claim is stale"))

      cursor.version != command.expected_version ->
        Repo.rollback(Error.new(:conflict, "schedule cursor version changed"))

      true ->
        rows = occurrence_rows(command)
        {_inserted, _rows} = Repo.insert_all(ScheduleOccurrence, rows, on_conflict: :nothing)
        occurrences = validate_committed_occurrences!(command, rows)

        cursor
        |> Ecto.Changeset.change(%{
          next_due_at: database_datetime(command.next_due_at),
          cursor: command.cursor,
          version: cursor.version + 1,
          last_command_id: command.command_id,
          claim_owner: nil,
          claim_command_id: nil,
          claim_expires_at: nil,
          updated_at: command.occurred_at
        })
        |> Repo.update!()

        OutboxWriter.insert!(%{
          workspace_id: command.workspace_context.workspace_id,
          command_id: command.command_id,
          event_kind: "schedule.evaluated",
          aggregate_kind: "schedule_cursor",
          aggregate_id: schedule_aggregate_id(command),
          aggregate_version: cursor.version + 1,
          occurred_at: command.occurred_at,
          payload: %{
            "deployment_id" => command.deployment_id,
            "pipeline_target_id" => command.pipeline_target_id,
            "schedule_id" => command.schedule_id,
            "occurrence_count" => length(occurrences)
          }
        })

        occurrences
    end
  end

  defp claim_occurrences!(command) do
    workspace_id = command.workspace_context.workspace_id

    replayed =
      from(occurrence in ScheduleOccurrence,
        where:
          occurrence.workspace_id == ^workspace_id and
            occurrence.claim_command_id == ^command.batch_id,
        order_by: [asc: occurrence.due_at, asc: occurrence.occurrence_id]
      )
      |> Repo.all()

    if replayed != [] do
      now = database_now!()

      if Enum.all?(replayed, &live_occurrence_replay?(&1, command, now)) do
        Enum.map(replayed, &occurrence_result/1)
      else
        Repo.rollback(Error.new(:fenced, "replayed occurrence claim is stale"))
      end
    else
      %{rows: rows} =
        SQL.query!(
          Repo,
          """
          WITH candidates AS (
            SELECT occurrence.workspace_id, occurrence.occurrence_id
            FROM favn_control.schedule_occurrences occurrence
            JOIN favn_control.workspace_runtime_state runtime
              ON runtime.workspace_id = occurrence.workspace_id
             AND runtime.active_deployment_id = occurrence.deployment_id
            JOIN favn_control.schedule_cursors cursor
              ON cursor.workspace_id = occurrence.workspace_id
             AND cursor.deployment_id = occurrence.deployment_id
             AND cursor.pipeline_target_id = occurrence.pipeline_target_id
             AND cursor.schedule_id = occurrence.schedule_id
            JOIN favn_control.schedule_activations activation
              ON activation.workspace_id = occurrence.workspace_id
             AND activation.pipeline_target_id = occurrence.pipeline_target_id
             AND activation.schedule_id = occurrence.schedule_id
            WHERE occurrence.workspace_id = $1
              AND occurrence.status IN ('pending', 'claimed', 'dispatching')
              AND (
                occurrence.status = 'dispatching'
                OR (
                  activation.enabled = TRUE
                  AND activation.approved_schedule_fingerprint = cursor.schedule_fingerprint
                )
              )
              AND occurrence.due_at <= clock_timestamp()
              AND (occurrence.status = 'pending' OR occurrence.claim_expires_at <= clock_timestamp())
            ORDER BY occurrence.due_at, occurrence.occurrence_id
            LIMIT $2
            FOR UPDATE OF occurrence SKIP LOCKED
          )
          UPDATE favn_control.schedule_occurrences occurrence
          SET status = CASE
                WHEN occurrence.status = 'dispatching' THEN 'dispatching'
                ELSE 'claimed'
              END,
              claim_owner = $3,
              claim_generation = occurrence.claim_generation + 1,
              claim_command_id = $4,
              claim_expires_at = clock_timestamp() + ($5 * interval '1 millisecond'),
              attempt_count = occurrence.attempt_count + 1,
              updated_at = clock_timestamp()
          FROM candidates
          WHERE occurrence.workspace_id = candidates.workspace_id
            AND occurrence.occurrence_id = candidates.occurrence_id
          RETURNING occurrence.workspace_id, occurrence.occurrence_id,
                    occurrence.deployment_id, occurrence.pipeline_target_id,
                    occurrence.schedule_id, occurrence.due_at, occurrence.payload,
                    occurrence.status, occurrence.claim_owner, occurrence.claim_generation,
                    occurrence.claim_expires_at, occurrence.run_id,
                    occurrence.attempt_count, occurrence.last_error
          """,
          [
            workspace_id,
            command.limit,
            command.owner_id,
            command.batch_id,
            command.lease_duration_ms
          ]
        )

      rows |> Enum.map(&occurrence_result/1) |> Enum.sort_by(&{&1.due_at, &1.occurrence_id})
    end
  end

  defp complete_occurrence!(command) do
    occurrence = lock_occurrence!(command.workspace_context.workspace_id, command.occurrence_id)

    cond do
      occurrence.last_command_id == command.command_id ->
        occurrence_result(occurrence)

      not valid_occurrence_claim?(occurrence, command) ->
        Repo.rollback(Error.new(:fenced, "schedule occurrence claim is stale"))

      true ->
        {status, run_id, error} = occurrence_completion(command)

        updated =
          occurrence
          |> Ecto.Changeset.change(%{
            status: status,
            run_id: run_id,
            last_error: error,
            last_command_id: command.command_id,
            claim_expires_at: command.occurred_at,
            updated_at: command.occurred_at
          })
          |> Repo.update!()

        OutboxWriter.insert!(%{
          workspace_id: command.workspace_context.workspace_id,
          command_id: command.command_id,
          event_kind: "schedule.occurrence." <> status,
          aggregate_kind: "schedule_occurrence",
          aggregate_id: command.occurrence_id,
          aggregate_version: updated.attempt_count,
          occurred_at: command.occurred_at,
          payload: %{
            "occurrence_id" => command.occurrence_id,
            "status" => status,
            "run_id" => run_id
          }
        })

        occurrence_result(updated)
    end
  end

  defp occurrence_completion(%{status: :suppressed}), do: {"suppressed", nil, nil}

  defp occurrence_completion(%{run_id: run_id}) when is_binary(run_id),
    do: {"completed", run_id, nil}

  defp occurrence_completion(command), do: {"failed", nil, command.error}

  defp set_activation!(command) do
    workspace_id = command.workspace_context.workspace_id
    lock_activation_command!(workspace_id, command.command_id)

    replayed =
      from(stored_command in ScheduleActivationCommand,
        where:
          stored_command.workspace_id == ^workspace_id and
            stored_command.command_id == ^command.command_id,
        lock: "FOR UPDATE"
      )
      |> Repo.one()

    case replayed do
      %ScheduleActivationCommand{request_hash: request_hash, result: result}
      when request_hash == command.request_hash ->
        decode_activation_result!(result)

      %ScheduleActivationCommand{} ->
        Repo.rollback(Error.new(:conflict, "schedule activation idempotency conflict"))

      nil ->
        lock_activation_schedule!(
          workspace_id,
          command.pipeline_target_id,
          command.schedule_id
        )

        existing =
          from(activation in ScheduleActivation,
            where:
              activation.workspace_id == ^workspace_id and
                activation.pipeline_target_id == ^command.pipeline_target_id and
                activation.schedule_id == ^command.schedule_id,
            lock: "FOR UPDATE"
          )
          |> Repo.one()

        previous_state = activation_state(existing, command.schedule_fingerprint)
        version = if(existing, do: existing.version + 1, else: 1)
        now = database_datetime(command.occurred_at)

        attrs = %{
          workspace_id: workspace_id,
          pipeline_target_id: command.pipeline_target_id,
          schedule_id: command.schedule_id,
          enabled: command.enabled,
          approved_schedule_fingerprint:
            if(command.enabled, do: command.schedule_fingerprint, else: nil),
          version: version,
          actor_id: command.actor_id,
          reason: command.reason,
          last_command_id: command.command_id,
          request_hash: command.request_hash,
          decided_at: now,
          inserted_at: (existing && existing.inserted_at) || now,
          updated_at: now
        }

        activation =
          if existing do
            existing |> Ecto.Changeset.change(attrs) |> Repo.update!()
          else
            struct!(ScheduleActivation, attrs) |> Repo.insert!()
          end

        reset_cursor_for_activation!(command)

        OutboxWriter.insert!(%{
          workspace_id: workspace_id,
          command_id: command.command_id,
          event_kind: if(command.enabled, do: "schedule.activated", else: "schedule.deactivated"),
          aggregate_kind: "schedule_activation",
          aggregate_id: Enum.join([command.pipeline_target_id, command.schedule_id], ":"),
          aggregate_version: version,
          occurred_at: command.occurred_at,
          payload: %{
            "pipeline_target_id" => command.pipeline_target_id,
            "schedule_id" => command.schedule_id,
            "schedule_fingerprint" => command.schedule_fingerprint,
            "enabled" => command.enabled,
            "actor_id" => command.actor_id,
            "reason" => command.reason
          }
        })

        result = activation_result(activation, command, previous_state)

        %ScheduleActivationCommand{
          workspace_id: workspace_id,
          command_id: command.command_id,
          request_hash: command.request_hash,
          result: encode_activation_result(result),
          inserted_at: now,
          updated_at: now
        }
        |> Repo.insert!()

        result
    end
  end

  defp lock_activation_command!(workspace_id, command_id) do
    SQL.query!(
      Repo,
      """
      SELECT pg_advisory_xact_lock(
        hashtextextended(jsonb_build_array($1::text, $2::text)::text, 0)
      )
      """,
      [workspace_id, command_id]
    )
  end

  defp lock_activation_schedule!(workspace_id, pipeline_target_id, schedule_id) do
    SQL.query!(
      Repo,
      """
      SELECT pg_advisory_xact_lock(
        hashtextextended(
          jsonb_build_array($1::text, $2::text, $3::text)::text,
          0
        )
      )
      """,
      [workspace_id, pipeline_target_id, schedule_id]
    )
  end

  defp authorize_occurrence_dispatch!(command) do
    workspace_id = command.workspace_context.workspace_id

    activation =
      from(activation in ScheduleActivation,
        where:
          activation.workspace_id == ^workspace_id and
            activation.pipeline_target_id == ^command.pipeline_target_id and
            activation.schedule_id == ^command.schedule_id,
        lock: "FOR UPDATE"
      )
      |> Repo.one()

    occurrence = lock_occurrence!(workspace_id, command.occurrence_id)

    cond do
      occurrence.pipeline_target_id != command.pipeline_target_id or
          occurrence.schedule_id != command.schedule_id ->
        Repo.rollback(Error.new(:fenced, "schedule occurrence identity does not match command"))

      occurrence.last_command_id == command.command_id and
          occurrence.status in ["dispatching", "completed"] ->
        occurrence_result(occurrence)

      not valid_occurrence_claim?(occurrence, command) ->
        Repo.rollback(Error.new(:fenced, "schedule occurrence claim is stale"))

      activation_enabled_for?(activation, command.schedule_fingerprint) ->
        submission = enqueue_occurrence_submission!(command, occurrence)

        updated =
          occurrence
          |> Ecto.Changeset.change(%{
            status: "completed",
            run_id: submission.run_id,
            last_command_id: command.command_id,
            claim_expires_at: command.occurred_at,
            updated_at: command.occurred_at
          })
          |> Repo.update!()

        OutboxWriter.insert!(%{
          workspace_id: workspace_id,
          command_id: command.command_id,
          event_kind: "schedule.occurrence.submission_queued",
          aggregate_kind: "schedule_occurrence",
          aggregate_id: command.occurrence_id,
          aggregate_version: updated.claim_generation,
          occurred_at: command.occurred_at,
          payload: %{
            "occurrence_id" => command.occurrence_id,
            "pipeline_target_id" => command.pipeline_target_id,
            "schedule_id" => command.schedule_id,
            "schedule_fingerprint" => command.schedule_fingerprint,
            "run_id" => submission.run_id,
            "submission_id" => submission.submission_id
          }
        })

        occurrence_result(updated)

      true ->
        occurrence
        |> Ecto.Changeset.change(%{
          status: "suppressed",
          claim_owner: nil,
          claim_command_id: nil,
          claim_expires_at: nil,
          last_command_id: command.command_id,
          updated_at: command.occurred_at
        })
        |> Repo.update!()
        |> occurrence_result()
    end
  end

  defp activation_enabled_for?(
         %ScheduleActivation{
           enabled: true,
           approved_schedule_fingerprint: fingerprint
         },
         fingerprint
       ),
       do: true

  defp activation_enabled_for?(_activation, _fingerprint), do: false

  defp reset_cursor_for_activation!(command) do
    workspace_id = command.workspace_context.workspace_id

    cursor_query =
      from(cursor in ScheduleCursor,
        join: runtime in WorkspaceRuntimeState,
        on:
          runtime.workspace_id == cursor.workspace_id and
            runtime.active_deployment_id == cursor.deployment_id,
        where:
          cursor.workspace_id == ^workspace_id and
            cursor.pipeline_target_id == ^command.pipeline_target_id and
            cursor.schedule_id == ^command.schedule_id
      )

    if command.enabled do
      Repo.update_all(cursor_query,
        set: [
          next_due_at: database_datetime(command.next_due_at),
          cursor: %{
            "schedule_fingerprint" => command.schedule_fingerprint,
            "in_flight_run_id" => nil,
            "queued_due_at" => nil
          },
          claim_owner: nil,
          claim_command_id: nil,
          claim_expires_at: nil,
          updated_at: database_datetime(command.occurred_at)
        ],
        inc: [version: 1]
      )
    else
      Repo.update_all(cursor_query,
        set: [
          next_due_at: nil,
          claim_owner: nil,
          claim_command_id: nil,
          claim_expires_at: nil,
          updated_at: database_datetime(command.occurred_at)
        ],
        inc: [version: 1]
      )

      from(occurrence in ScheduleOccurrence,
        join: runtime in WorkspaceRuntimeState,
        on:
          runtime.workspace_id == occurrence.workspace_id and
            runtime.active_deployment_id == occurrence.deployment_id,
        where:
          occurrence.workspace_id == ^workspace_id and
            occurrence.pipeline_target_id == ^command.pipeline_target_id and
            occurrence.schedule_id == ^command.schedule_id and
            occurrence.status in ["pending", "claimed"]
      )
      |> Repo.update_all(
        set: [
          status: "suppressed",
          claim_owner: nil,
          claim_command_id: nil,
          claim_expires_at: nil,
          updated_at: database_datetime(command.occurred_at)
        ]
      )
    end
  end

  defp lock_cursor!(command) do
    from(cursor in ScheduleCursor,
      where:
        cursor.workspace_id == ^command.workspace_context.workspace_id and
          cursor.deployment_id == ^command.deployment_id and
          cursor.pipeline_target_id == ^command.pipeline_target_id and
          cursor.schedule_id == ^command.schedule_id,
      lock: "FOR UPDATE"
    )
    |> Repo.one()
    |> case do
      nil -> Repo.rollback(Error.new(:not_found, "schedule cursor not found"))
      cursor -> cursor
    end
  end

  defp lock_occurrence!(workspace_id, occurrence_id) do
    from(occurrence in ScheduleOccurrence,
      where:
        occurrence.workspace_id == ^workspace_id and occurrence.occurrence_id == ^occurrence_id,
      lock: "FOR UPDATE"
    )
    |> Repo.one()
    |> case do
      nil -> Repo.rollback(Error.new(:not_found, "schedule occurrence not found"))
      occurrence -> occurrence
    end
  end

  defp valid_cursor_claim?(cursor, command) do
    cursor.claim_owner == command.owner_id and
      cursor.claim_generation == command.claim_generation and future?(cursor.claim_expires_at)
  end

  defp valid_occurrence_claim?(occurrence, command) do
    occurrence.status in ["claimed", "dispatching"] and occurrence.claim_owner == command.owner_id and
      occurrence.claim_generation == command.claim_generation and
      future?(occurrence.claim_expires_at)
  end

  defp occurrence_rows(command) do
    now = command.occurred_at

    Enum.map(command.occurrences, fn occurrence ->
      {:ok, key} =
        CanonicalJSON.hash(%{
          "workspace_id" => command.workspace_context.workspace_id,
          "deployment_id" => command.deployment_id,
          "pipeline_target_id" => command.pipeline_target_id,
          "schedule_id" => command.schedule_id,
          "due_at" => occurrence.due_at,
          "payload" => occurrence.payload
        })

      %{
        workspace_id: command.workspace_context.workspace_id,
        occurrence_id: occurrence.occurrence_id,
        occurrence_key: key,
        evaluation_command_id: command.command_id,
        deployment_id: command.deployment_id,
        pipeline_target_id: command.pipeline_target_id,
        schedule_id: command.schedule_id,
        due_at: database_datetime(occurrence.due_at),
        payload: occurrence.payload,
        status: "pending",
        claim_generation: 0,
        attempt_count: 0,
        inserted_at: now,
        updated_at: now
      }
    end)
  end

  defp validate_committed_occurrences!(command, expected_rows) do
    ids = Enum.map(expected_rows, & &1.occurrence_id)

    stored =
      from(occurrence in ScheduleOccurrence,
        where:
          occurrence.workspace_id == ^command.workspace_context.workspace_id and
            occurrence.occurrence_id in ^ids,
        order_by: [asc: occurrence.occurrence_id]
      )
      |> Repo.all()

    expected_by_id = Map.new(expected_rows, &{&1.occurrence_id, &1})

    committed? =
      committed_occurrences_match?(stored, expected_rows, expected_by_id, command.command_id)

    if committed? do
      Enum.map(stored, &occurrence_result/1)
    else
      Repo.rollback(Error.new(:conflict, "schedule occurrence identity has different content"))
    end
  end

  defp committed_occurrences_match?(stored, expected_rows, expected_by_id, command_id) do
    length(stored) == length(expected_rows) and
      Enum.all?(stored, fn occurrence ->
        expected = Map.fetch!(expected_by_id, occurrence.occurrence_id)

        occurrence.occurrence_key == expected.occurrence_key and
          occurrence.evaluation_command_id == command_id
      end)
  end

  defp occurrences_for_evaluation(command) do
    from(occurrence in ScheduleOccurrence,
      where:
        occurrence.workspace_id == ^command.workspace_context.workspace_id and
          occurrence.evaluation_command_id == ^command.command_id,
      order_by: [asc: occurrence.occurrence_id]
    )
    |> Repo.all()
    |> Enum.map(&occurrence_result/1)
  end

  defp schedule_claim(%ScheduleCursor{} = cursor) do
    %ScheduleClaim{
      workspace_id: cursor.workspace_id,
      deployment_id: cursor.deployment_id,
      pipeline_target_id: cursor.pipeline_target_id,
      schedule_id: cursor.schedule_id,
      next_due_at: cursor.next_due_at,
      cursor: cursor.cursor,
      version: cursor.version,
      owner_id: cursor.claim_owner,
      claim_generation: cursor.claim_generation,
      claim_expires_at: cursor.claim_expires_at
    }
  end

  defp schedule_claim([
         workspace_id,
         deployment_id,
         pipeline_target_id,
         schedule_id,
         next_due_at,
         cursor,
         version,
         owner_id,
         generation,
         expires_at
       ]) do
    %ScheduleClaim{
      workspace_id: workspace_id,
      deployment_id: deployment_id,
      pipeline_target_id: pipeline_target_id,
      schedule_id: schedule_id,
      next_due_at: next_due_at,
      cursor: cursor,
      version: version,
      owner_id: owner_id,
      claim_generation: generation,
      claim_expires_at: expires_at
    }
  end

  defp occurrence_result(%ScheduleOccurrence{} = occurrence) do
    %ScheduleOccurrenceResult{
      workspace_id: occurrence.workspace_id,
      occurrence_id: occurrence.occurrence_id,
      deployment_id: occurrence.deployment_id,
      pipeline_target_id: occurrence.pipeline_target_id,
      schedule_id: occurrence.schedule_id,
      due_at: occurrence.due_at,
      payload: occurrence.payload,
      status: String.to_existing_atom(occurrence.status),
      claim_owner: occurrence.claim_owner,
      claim_generation: occurrence.claim_generation,
      claim_expires_at: occurrence.claim_expires_at,
      run_id: occurrence.run_id,
      attempt_count: occurrence.attempt_count,
      last_error: occurrence.last_error
    }
  end

  defp occurrence_result([
         workspace_id,
         occurrence_id,
         deployment_id,
         pipeline_target_id,
         schedule_id,
         due_at,
         payload,
         status,
         owner,
         generation,
         expires_at,
         run_id,
         attempts,
         error
       ]) do
    %ScheduleOccurrenceResult{
      workspace_id: workspace_id,
      occurrence_id: occurrence_id,
      deployment_id: deployment_id,
      pipeline_target_id: pipeline_target_id,
      schedule_id: schedule_id,
      due_at: due_at,
      payload: payload,
      status: String.to_existing_atom(status),
      claim_owner: owner,
      claim_generation: generation,
      claim_expires_at: expires_at,
      run_id: run_id,
      attempt_count: attempts,
      last_error: error
    }
  end

  defp database_datetime(%DateTime{} = datetime),
    do: DateTime.add(datetime, 0, :microsecond)

  defp database_datetime(nil), do: nil

  defp schedule_result({%ScheduleCursor{} = cursor, activation}) do
    %ScheduleResult{
      workspace_id: cursor.workspace_id,
      deployment_id: cursor.deployment_id,
      pipeline_target_id: cursor.pipeline_target_id,
      schedule_id: cursor.schedule_id,
      schedule_fingerprint: cursor.schedule_fingerprint,
      definition: cursor.definition,
      next_due_at: cursor.next_due_at,
      cursor: cursor.cursor,
      version: cursor.version,
      activation_enabled: activation && activation.enabled,
      approved_schedule_fingerprint: activation && activation.approved_schedule_fingerprint,
      activation_version: activation && activation.version,
      activation_actor_id: activation && activation.actor_id,
      activation_reason: activation && activation.reason,
      activation_decided_at: activation && activation.decided_at,
      claim_owner: cursor.claim_owner,
      claim_expires_at: cursor.claim_expires_at,
      updated_at: cursor.updated_at
    }
  end

  defp activation_result(
         %ScheduleActivation{} = activation,
         %SetScheduleActivation{} = command,
         previous_state
       ) do
    %ScheduleActivationResult{
      workspace_id: activation.workspace_id,
      schedule_entry_id:
        persisted_schedule_entry_id(activation.pipeline_target_id, activation.schedule_id),
      pipeline_target_id: activation.pipeline_target_id,
      schedule_id: activation.schedule_id,
      schedule_fingerprint: command.schedule_fingerprint,
      previous_state: previous_state,
      effective_state: if(activation.enabled, do: :enabled, else: :disabled),
      enabled: activation.enabled,
      approved_schedule_fingerprint: activation.approved_schedule_fingerprint,
      version: activation.version,
      actor_id: activation.actor_id,
      reason: activation.reason,
      command_id: activation.last_command_id,
      decided_at: activation.decided_at,
      next_due_at: database_datetime(command.next_due_at)
    }
  end

  defp activation_state(
         %ScheduleActivation{enabled: true, approved_schedule_fingerprint: fingerprint},
         fingerprint
       ),
       do: :enabled

  defp activation_state(%ScheduleActivation{enabled: true}, _fingerprint), do: :needs_review
  defp activation_state(_activation, _fingerprint), do: :disabled

  defp encode_activation_result(%ScheduleActivationResult{} = result) do
    %{
      "workspace_id" => result.workspace_id,
      "schedule_entry_id" => result.schedule_entry_id,
      "pipeline_target_id" => result.pipeline_target_id,
      "schedule_id" => result.schedule_id,
      "schedule_fingerprint" => result.schedule_fingerprint,
      "previous_state" => Atom.to_string(result.previous_state),
      "effective_state" => Atom.to_string(result.effective_state),
      "enabled" => result.enabled,
      "approved_schedule_fingerprint" => result.approved_schedule_fingerprint,
      "version" => result.version,
      "actor_id" => result.actor_id,
      "reason" => result.reason,
      "command_id" => result.command_id,
      "decided_at" => DateTime.to_iso8601(result.decided_at),
      "next_due_at" => encode_datetime(result.next_due_at)
    }
  end

  defp decode_activation_result!(result) when is_map(result) do
    %ScheduleActivationResult{
      workspace_id: Map.fetch!(result, "workspace_id"),
      schedule_entry_id: Map.fetch!(result, "schedule_entry_id"),
      pipeline_target_id: Map.fetch!(result, "pipeline_target_id"),
      schedule_id: Map.fetch!(result, "schedule_id"),
      schedule_fingerprint: Map.fetch!(result, "schedule_fingerprint"),
      previous_state: decode_activation_state!(Map.fetch!(result, "previous_state")),
      effective_state: decode_effective_state!(Map.fetch!(result, "effective_state")),
      enabled: Map.fetch!(result, "enabled"),
      approved_schedule_fingerprint: Map.get(result, "approved_schedule_fingerprint"),
      version: Map.fetch!(result, "version"),
      actor_id: Map.fetch!(result, "actor_id"),
      reason: Map.fetch!(result, "reason"),
      command_id: Map.fetch!(result, "command_id"),
      decided_at: decode_datetime!(Map.fetch!(result, "decided_at")),
      next_due_at: decode_optional_datetime!(Map.get(result, "next_due_at"))
    }
  rescue
    _error -> Repo.rollback(Error.new(:internal, "schedule activation replay is invalid"))
  end

  defp decode_activation_state!("disabled"), do: :disabled
  defp decode_activation_state!("enabled"), do: :enabled
  defp decode_activation_state!("needs_review"), do: :needs_review
  defp decode_activation_state!(_state), do: raise(ArgumentError, "invalid activation state")

  defp decode_effective_state!("disabled"), do: :disabled
  defp decode_effective_state!("enabled"), do: :enabled
  defp decode_effective_state!(_state), do: raise(ArgumentError, "invalid effective state")

  defp encode_datetime(nil), do: nil
  defp encode_datetime(%DateTime{} = datetime), do: DateTime.to_iso8601(datetime)

  defp decode_optional_datetime!(nil), do: nil
  defp decode_optional_datetime!(value), do: decode_datetime!(value)

  defp decode_datetime!(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, 0} -> datetime
      _invalid -> raise ArgumentError, "invalid activation datetime"
    end
  end

  defp persisted_schedule_entry_id(pipeline_target_id, schedule_id) do
    target = Base.url_encode64(pipeline_target_id, padding: false)
    name = Base.url_encode64(schedule_id, padding: false)
    "schedule-v2:" <> target <> ":" <> name
  end

  defp schedule_filter(query, %PageSchedules{} = page) do
    query
    |> then(fn query ->
      if is_binary(page.pipeline_target_id),
        do:
          where(
            query,
            [cursor, _runtime, _activation],
            cursor.pipeline_target_id == ^page.pipeline_target_id
          ),
        else: query
    end)
    |> then(fn query ->
      if is_binary(page.schedule_id),
        do:
          where(query, [cursor, _runtime, _activation], cursor.schedule_id == ^page.schedule_id),
        else: query
    end)
  end

  defp schedule_after(query, nil), do: query

  defp schedule_after(query, %{pipeline_target_id: target_id, schedule_id: schedule_id}) do
    where(
      query,
      [cursor, _runtime, _activation],
      cursor.pipeline_target_id > ^target_id or
        (cursor.pipeline_target_id == ^target_id and cursor.schedule_id > ^schedule_id)
    )
  end

  defp occurrence_after(query, nil), do: query

  defp occurrence_after(query, %{due_at: due_at, occurrence_id: occurrence_id}) do
    where(
      query,
      [occurrence, _runtime],
      occurrence.due_at < ^due_at or
        (occurrence.due_at == ^due_at and occurrence.occurrence_id < ^occurrence_id)
    )
  end

  defp future?(nil), do: false

  defp future?(timestamp) do
    %{rows: [[future?]]} =
      SQL.query!(Repo, "SELECT $1::timestamptz > clock_timestamp()", [timestamp])

    future?
  end

  defp live_schedule_replay?(cursor, command, now) do
    cursor.claim_owner == command.owner_id and
      DateTime.compare(cursor.claim_expires_at, now) == :gt
  end

  defp live_occurrence_replay?(occurrence, command, now) do
    occurrence.status in ["claimed", "dispatching"] and occurrence.claim_owner == command.owner_id and
      DateTime.compare(occurrence.claim_expires_at, now) == :gt
  end

  defp database_now! do
    %{rows: [[now]]} = SQL.query!(Repo, "SELECT clock_timestamp()", [])
    now
  end

  defp schedule_aggregate_id(command) do
    Enum.join([command.deployment_id, command.pipeline_target_id, command.schedule_id], ":")
  end

  defp validate_claim(command),
    do:
      validate_batch(
        command.workspace_context,
        command.batch_id,
        command.owner_id,
        command.lease_duration_ms,
        command.limit
      )

  defp validate_activation(%SetScheduleActivation{} = command) do
    valid? =
      writer?(command.workspace_context) and
        valid_id?(command.pipeline_target_id) and valid_id?(command.schedule_id) and
        valid_id?(command.schedule_fingerprint) and is_boolean(command.enabled) and
        valid_id?(command.actor_id) and is_binary(command.reason) and
        String.trim(command.reason) != "" and byte_size(command.reason) <= 2_048 and
        valid_id?(command.command_id) and match?(%DateTime{}, command.occurred_at) and
        is_binary(command.request_hash) and byte_size(command.request_hash) == 32 and
        (not command.enabled or match?(%DateTime{}, command.next_due_at))

    if valid?,
      do: :ok,
      else: {:error, Error.new(:invalid, "invalid schedule activation command")}
  end

  defp validate_dispatch_authorization(%AuthorizeScheduleOccurrenceDispatch{} = command) do
    submission = command.submission

    valid? =
      writer?(command.workspace_context) and
        match?(%EnqueueRunSubmission{}, submission) and
        RunSubmissionValidation.command(submission) == :ok and
        submission.workspace_context == command.workspace_context and
        submission.source == :scheduler and submission.target_kind == "pipeline" and
        submission.target_id == command.pipeline_target_id and
        Enum.all?(
          [
            command.command_id,
            command.occurrence_id,
            command.pipeline_target_id,
            command.schedule_id,
            command.schedule_fingerprint,
            command.owner_id
          ],
          &valid_id?/1
        ) and
        is_integer(command.claim_generation) and command.claim_generation > 0 and
        match?(%DateTime{}, command.occurred_at)

    if valid?, do: :ok, else: {:error, :invalid}
  end

  defp enqueue_occurrence_submission!(command, occurrence) do
    submission = command.submission

    if submission.deployment_id != occurrence.deployment_id or
         submission.target_id != occurrence.pipeline_target_id do
      Repo.rollback(Error.new(:fenced, "schedule submission identity does not match occurrence"))
    end

    case RunSubmissionStore.enqueue(submission) do
      {:ok, %RunSubmissionResult{} = result} -> result
      {:error, %Error{} = error} -> Repo.rollback(error)
      {:error, reason} -> Repo.rollback(ErrorMapper.map(reason))
    end
  end

  defp validate_occurrence_claim(command),
    do:
      validate_batch(
        command.workspace_context,
        command.batch_id,
        command.owner_id,
        command.lease_duration_ms,
        command.limit
      )

  defp validate_batch(context, batch_id, owner_id, duration, limit) do
    if writer?(context) and Enum.all?([batch_id, owner_id], &valid_id?/1) and
         is_integer(duration) and duration >= 1_000 and duration <= 3_600_000 and
         is_integer(limit) and limit >= 1 and limit <= 500,
       do: :ok,
       else: {:error, :invalid}
  end

  defp validate_evaluation(command) do
    if valid_evaluation_identity?(command) and valid_evaluation_cursor?(command) and
         valid_evaluation_occurrences?(command.occurrences),
       do: :ok,
       else: {:error, :invalid}
  end

  defp valid_evaluation_identity?(command) do
    identities = [
      command.command_id,
      command.deployment_id,
      command.pipeline_target_id,
      command.schedule_id,
      command.owner_id
    ]

    writer?(command.workspace_context) and Enum.all?(identities, &valid_id?/1) and
      is_integer(command.claim_generation) and command.claim_generation > 0 and
      is_integer(command.expected_version) and command.expected_version > 0
  end

  defp valid_evaluation_cursor?(command) do
    match?(%DateTime{}, command.next_due_at) and match?(%DateTime{}, command.occurred_at) and
      is_map(command.cursor) and Payload.validate(command.cursor, 64 * 1_024) == :ok
  end

  defp valid_evaluation_occurrences?(occurrences) do
    is_list(occurrences) and length(occurrences) <= 500 and
      Enum.all?(occurrences, &valid_evaluation_occurrence?/1)
  end

  defp valid_evaluation_occurrence?(%ScheduleOccurrenceIntent{} = occurrence) do
    valid_id?(occurrence.occurrence_id) and match?(%DateTime{}, occurrence.due_at) and
      is_map(occurrence.payload) and Payload.validate(occurrence.payload, 64 * 1_024) == :ok
  end

  defp valid_evaluation_occurrence?(_occurrence), do: false

  defp validate_completion(command) do
    outcome? =
      case {command.status, command.run_id, command.error} do
        {:suppressed, nil, nil} -> true
        {_status, run_id, nil} -> valid_id?(run_id)
        {_status, nil, error} when is_map(error) -> Payload.validate(error, 64 * 1_024) == :ok
        _invalid -> false
      end

    if writer?(command.workspace_context) and
         Enum.all?([command.command_id, command.occurrence_id, command.owner_id], &valid_id?/1) and
         is_integer(command.claim_generation) and command.claim_generation > 0 and
         match?(%DateTime{}, command.occurred_at) and outcome?,
       do: :ok,
       else: {:error, :invalid}
  end

  defp validate_schedule_page(page) do
    cursor? =
      is_nil(page.after) or
        match?(
          %{pipeline_target_id: target, schedule_id: schedule}
          when is_binary(target) and is_binary(schedule),
          page.after
        )

    if reader?(page.workspace_context) and valid_optional_id?(page.pipeline_target_id) and
         valid_optional_id?(page.schedule_id) and cursor? and valid_limit?(page.limit),
       do: :ok,
       else: {:error, :invalid}
  end

  defp validate_occurrence_page(page) do
    cursor? =
      is_nil(page.after) or
        match?(%{due_at: %DateTime{}, occurrence_id: id} when is_binary(id), page.after)

    if reader?(page.workspace_context) and valid_id?(page.pipeline_target_id) and
         valid_id?(page.schedule_id) and cursor? and valid_limit?(page.limit),
       do: :ok,
       else: {:error, :invalid}
  end

  defp reader?(%WorkspaceContext{roles: roles} = context),
    do:
      WorkspaceContext.valid?(context) and
        Enum.any?(roles, fn role ->
          role in [
            :customer_reader,
            :customer_operator,
            :workspace_admin,
            :platform_reader,
            :platform_operator
          ]
        end)

  defp reader?(_context), do: false

  defp valid_optional_id?(nil), do: true
  defp valid_optional_id?(value), do: valid_id?(value)
  defp valid_limit?(value), do: is_integer(value) and value >= 1 and value <= 500

  defp writer?(%WorkspaceContext{roles: roles} = context),
    do:
      WorkspaceContext.valid?(context) and
        Enum.any?(roles, &(&1 in [:customer_operator, :workspace_admin, :platform_operator]))

  defp writer?(_context), do: false
  defp valid_id?(value), do: is_binary(value) and value != "" and byte_size(value) <= 255
end
