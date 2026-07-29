defmodule FavnStoragePostgres.RunSubmissions.Store do
  @moduledoc false

  @behaviour FavnOrchestrator.Persistence.RunSubmissionStore

  import Ecto.Query

  alias Ecto.Adapters.SQL
  alias FavnOrchestrator.Persistence.Commands.AcknowledgeRunSubmissionCancellation
  alias FavnOrchestrator.Persistence.Commands.ClaimRunSubmissions
  alias FavnOrchestrator.Persistence.Commands.ClaimStaleRunSubmissions
  alias FavnOrchestrator.Persistence.Commands.EnqueueRunSubmission
  alias FavnOrchestrator.Persistence.Commands.MarkRunSubmissionAdmitting
  alias FavnOrchestrator.Persistence.Commands.MarkRunSubmissionFailed
  alias FavnOrchestrator.Persistence.Commands.MarkRunSubmissionSubmitted
  alias FavnOrchestrator.Persistence.Commands.RenewRunSubmissionClaim
  alias FavnOrchestrator.Persistence.Commands.RequestRunSubmissionCancellation
  alias FavnOrchestrator.Persistence.Commands.RequeueRunSubmission
  alias FavnOrchestrator.Persistence.Commands.RetryFailedRunSubmission
  alias FavnOrchestrator.Persistence.Commands.SupersedeRunSubmission
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Queries.GetRunSubmission
  alias FavnOrchestrator.Persistence.Queries.GetRunSubmissionByRunId
  alias FavnOrchestrator.Persistence.Queries.GetRunSubmissionStats
  alias FavnOrchestrator.Persistence.Queries.PageClaimableRunSubmissionWorkspaces
  alias FavnOrchestrator.Persistence.Queries.PageRunSubmissions
  alias FavnOrchestrator.Persistence.Results.RunSubmissionPage
  alias FavnOrchestrator.Persistence.Results.RunSubmissionStats
  alias FavnOrchestrator.Persistence.Results.RunSubmissionWorkspacePage
  alias FavnOrchestrator.Persistence.RunSubmissionAuthority
  alias FavnStoragePostgres.CanonicalJSON
  alias FavnStoragePostgres.ErrorMapper
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.RunIdentity
  alias FavnStoragePostgres.RunSubmissions.Codec
  alias FavnStoragePostgres.RunSubmissions.Validation
  alias FavnStoragePostgres.Schemas.Run
  alias FavnStoragePostgres.Schemas.RunSubmission
  alias FavnStoragePostgres.Schemas.RunSubmissionCommand
  alias FavnStoragePostgres.Schemas.RunTarget

  @active_statuses ["preparing", "admitting"]
  @fenced_result_kinds ["claim", "claim_stale", "renew", "mark_admitting", "requeue"]
  @receipt_retention_ms :timer.hours(24) * 7
  @maximum_future_clock_skew_ms :timer.minutes(5)
  @receipt_prune_limit 100

  @impl true
  def enqueue(%EnqueueRunSubmission{} = command) do
    execute(command, "enqueue", fn -> enqueue_request(command) end, fn -> enqueue!(command) end)
  end

  @impl true
  def claim(%ClaimRunSubmissions{} = command) do
    execute(command, "claim", fn -> claim_request(command) end, fn -> claim!(command) end)
  end

  @impl true
  def claim_stale(%ClaimStaleRunSubmissions{} = command) do
    execute(command, "claim_stale", fn -> claim_request(command) end, fn ->
      claim_stale!(command)
    end)
  end

  @impl true
  def renew(%RenewRunSubmissionClaim{} = command) do
    execute(command, "renew", fn -> fenced_request(command) end, fn -> renew!(command) end)
  end

  @impl true
  def mark_admitting(%MarkRunSubmissionAdmitting{} = command) do
    execute(
      command,
      "mark_admitting",
      fn -> Map.put(fenced_request(command), "preparation", command.preparation) end,
      fn -> mark_admitting!(command) end
    )
  end

  @impl true
  def mark_submitted(%MarkRunSubmissionSubmitted{} = command) do
    execute(
      command,
      "mark_submitted",
      fn ->
        command
        |> fenced_request()
        |> Map.put("run_id", command.run_id)
        |> Map.put("outcome", command.outcome)
      end,
      fn -> mark_submitted!(command) end
    )
  end

  @impl true
  def mark_failed(%MarkRunSubmissionFailed{} = command) do
    execute(
      command,
      "mark_failed",
      fn ->
        command
        |> fenced_request()
        |> Map.put("failure_kind", Atom.to_string(command.failure_kind))
        |> Map.put("error", command.error)
      end,
      fn -> mark_failed!(command) end
    )
  end

  @impl true
  def requeue(%RequeueRunSubmission{} = command) do
    execute(
      command,
      "requeue",
      fn ->
        command
        |> fenced_request()
        |> Map.put("reason", command.reason)
        |> Map.put("available_at", encode_datetime(command.available_at))
      end,
      fn -> requeue!(command) end
    )
  end

  @impl true
  def request_cancellation(%RequestRunSubmissionCancellation{} = command) do
    execute(
      command,
      "request_cancellation",
      fn ->
        %{
          "submission_id" => command.submission_id,
          "reason" => command.reason,
          "occurred_at" => encode_datetime(command.occurred_at)
        }
      end,
      fn -> request_cancellation!(command) end
    )
  end

  @impl true
  def acknowledge_cancellation(%AcknowledgeRunSubmissionCancellation{} = command) do
    execute(command, "acknowledge_cancellation", fn -> fenced_request(command) end, fn ->
      acknowledge_cancellation!(command)
    end)
  end

  @impl true
  def supersede(%SupersedeRunSubmission{} = command) do
    execute(
      command,
      "supersede",
      fn ->
        %{
          "submission_id" => command.submission_id,
          "replacement_submission_id" => command.replacement_submission_id,
          "occurred_at" => encode_datetime(command.occurred_at)
        }
      end,
      fn -> supersede!(command) end
    )
  end

  @impl true
  def retry_failed(%RetryFailedRunSubmission{} = command) do
    execute(
      command,
      "retry_failed",
      fn ->
        %{
          "failed_submission_id" => command.failed_submission_id,
          "submission_id" => command.submission_id,
          "idempotency_key" => command.idempotency_key,
          "run_id" => command.run_id,
          "occurred_at" => encode_datetime(command.occurred_at),
          "available_at" => encode_datetime(command.available_at)
        }
      end,
      fn -> retry_failed!(command) end
    )
  end

  @impl true
  def get(%GetRunSubmission{} = query) do
    with :ok <- Validation.query(query) do
      case Repo.get_by(RunSubmission,
             workspace_id: query.workspace_context.workspace_id,
             submission_id: query.submission_id
           ) do
        %RunSubmission{} = submission -> {:ok, Codec.result(submission)}
        nil -> {:error, ErrorMapper.map(:not_found)}
      end
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def get_by_run_id(%GetRunSubmissionByRunId{} = query) do
    with :ok <- Validation.query(query),
         %RunSubmission{} = submission <-
           Repo.get_by(RunSubmission,
             workspace_id: query.workspace_context.workspace_id,
             run_id: query.run_id
           ) do
      {:ok, Codec.result(submission)}
    else
      nil -> {:error, Error.new(:not_found, "run submission not found")}
      {:error, %Error{} = error} -> {:error, error}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def stats(%GetRunSubmissionStats{} = query) do
    with :ok <- Validation.query(query) do
      observed_at = database_now!()

      rows =
        RunSubmission
        |> where([submission], submission.workspace_id == ^query.workspace_context.workspace_id)
        |> group_by([submission], [submission.status, submission.failure_kind])
        |> select([submission], %{
          status: submission.status,
          failure_kind: submission.failure_kind,
          count: count(submission.submission_id),
          retrying:
            fragment(
              "count(*) FILTER (WHERE ? = 'queued' AND ? > 0)",
              submission.status,
              submission.attempt
            ),
          cancellation_requested:
            fragment(
              "count(*) FILTER (WHERE ? IS NOT NULL AND ? IN ('queued', 'preparing', 'admitting'))",
              submission.cancellation_requested_at,
              submission.status
            ),
          oldest_queued_at:
            fragment(
              "min(?) FILTER (WHERE ? = 'queued')",
              submission.enqueued_at,
              submission.status
            )
        })
        |> Repo.all()

      {:ok, stats_result(rows, observed_at)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def page(%PageRunSubmissions{} = query) do
    with :ok <- Validation.query(query) do
      status = query.status && Atom.to_string(query.status)

      rows =
        RunSubmission
        |> where([submission], submission.workspace_id == ^query.workspace_context.workspace_id)
        |> maybe_status(status)
        |> after_cursor(query.after)
        |> order_by([submission],
          desc: submission.inserted_at,
          desc: submission.submission_id
        )
        |> limit(^(query.limit + 1))
        |> Repo.all()

      {items, next} = page_rows(rows, query.limit)
      {:ok, %RunSubmissionPage{items: Enum.map(items, &Codec.result/1), next: next}}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def page_claimable_workspaces(%PageClaimableRunSubmissionWorkspaces{} = query) do
    with :ok <- Validation.query(query) do
      now = database_now!()

      queued =
        RunSubmission
        |> where(
          [submission],
          submission.status == "queued" and submission.available_at <= ^now
        )
        |> after_workspace_cursor(query.after)
        |> select([submission], %{workspace_id: submission.workspace_id})

      stale =
        RunSubmission
        |> where(
          [submission],
          submission.status in @active_statuses and submission.claim_expires_at <= ^now
        )
        |> after_workspace_cursor(query.after)
        |> select([submission], %{workspace_id: submission.workspace_id})

      candidates = union_all(queued, ^stale)

      rows =
        candidates
        |> subquery()
        |> group_by([candidate], candidate.workspace_id)
        |> order_by([candidate], asc: candidate.workspace_id)
        |> select([candidate], candidate.workspace_id)
        |> limit(^(query.limit + 1))
        |> Repo.all()

      workspace_ids = Enum.take(rows, query.limit)
      has_more? = length(rows) > query.limit
      next = if has_more?, do: List.last(workspace_ids)

      {:ok,
       %RunSubmissionWorkspacePage{
         workspace_ids: workspace_ids,
         has_more?: has_more?,
         next: next
       }}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  defp stats_result(rows, observed_at) do
    empty = %RunSubmissionStats{
      total: 0,
      counts: %{},
      failure_counts: %{},
      queued_depth: 0,
      active_depth: 0,
      retrying_depth: 0,
      cancellation_requested_depth: 0,
      observed_at: observed_at
    }

    rows
    |> Enum.reduce(empty, fn row, stats ->
      status = status_atom(row.status)
      failure_kind = failure_kind_atom(row.failure_kind)

      %{
        stats
        | total: stats.total + row.count,
          counts: Map.update(stats.counts, status, row.count, &(&1 + row.count)),
          failure_counts: put_failure_count(stats.failure_counts, failure_kind, row.count),
          queued_depth: stats.queued_depth + if(status == :queued, do: row.count, else: 0),
          active_depth:
            stats.active_depth +
              if(status in [:preparing, :admitting], do: row.count, else: 0),
          retrying_depth: stats.retrying_depth + row.retrying,
          cancellation_requested_depth:
            stats.cancellation_requested_depth + row.cancellation_requested,
          oldest_queued_at: earliest(stats.oldest_queued_at, row.oldest_queued_at)
      }
    end)
    |> then(fn stats ->
      %{
        stats
        | oldest_queued_age_ms: queued_age_ms(stats.observed_at, stats.oldest_queued_at)
      }
    end)
  end

  defp queued_age_ms(_observed_at, nil), do: nil

  defp queued_age_ms(observed_at, oldest_queued_at),
    do: max(DateTime.diff(observed_at, oldest_queued_at, :millisecond), 0)

  defp put_failure_count(counts, nil, _count), do: counts
  defp put_failure_count(counts, kind, count), do: Map.update(counts, kind, count, &(&1 + count))

  defp earliest(nil, timestamp), do: timestamp
  defp earliest(timestamp, nil), do: timestamp

  defp earliest(left, right) do
    if DateTime.compare(left, right) == :gt, do: right, else: left
  end

  defp status_atom("queued"), do: :queued
  defp status_atom("preparing"), do: :preparing
  defp status_atom("admitting"), do: :admitting
  defp status_atom("submitted"), do: :submitted
  defp status_atom("failed"), do: :failed
  defp status_atom("cancelled"), do: :cancelled
  defp status_atom("superseded"), do: :superseded

  defp failure_kind_atom(nil), do: nil
  defp failure_kind_atom("safe"), do: :safe
  defp failure_kind_atom("permanent"), do: :permanent
  defp failure_kind_atom("unknown"), do: :unknown

  defp enqueue!(command) do
    workspace_id = command.workspace_context.workspace_id
    advisory_lock!("run-submission-idempotency", workspace_id, command.idempotency_key)
    RunIdentity.lock!(workspace_id, command.run_id)

    case lock_by_idempotency(workspace_id, command.idempotency_key) do
      %RunSubmission{request_hash: request_hash} = submission
      when request_hash == command.request_hash ->
        Codec.result(submission)

      %RunSubmission{} ->
        rollback(:conflict, "run submission idempotency key has different intent")

      nil ->
        reject_run_identity_collision!(workspace_id, command.run_id)
        now = database_now!()

        %RunSubmission{}
        |> Ecto.Changeset.change(%{
          workspace_id: workspace_id,
          submission_id: command.submission_id,
          source: Atom.to_string(command.source),
          idempotency_key: command.idempotency_key,
          request_hash: command.request_hash,
          authority:
            command.workspace_context
            |> RunSubmissionAuthority.from_context()
            |> RunSubmissionAuthority.dump(),
          deployment_id: command.deployment_id,
          manifest_version_id: command.manifest_version_id,
          target_kind: command.target_kind,
          target_id: command.target_id,
          run_id: command.run_id,
          intent: command.intent,
          status: "queued",
          attempt: 0,
          claim_generation: 0,
          retry_root_id: command.submission_id,
          enqueued_at: now,
          available_at: command.available_at || now,
          inserted_at: now,
          updated_at: now
        })
        |> Repo.insert!()
        |> Codec.result()
    end
  end

  defp claim!(command) do
    now = database_now!()

    from(submission in RunSubmission,
      where:
        submission.workspace_id == ^command.workspace_context.workspace_id and
          submission.status == "queued" and submission.available_at <= ^now,
      order_by: [
        asc: submission.available_at,
        asc: submission.enqueued_at,
        asc: submission.submission_id
      ],
      limit: ^command.limit,
      lock: "FOR UPDATE SKIP LOCKED"
    )
    |> Repo.all()
    |> Enum.map(fn submission ->
      submission
      |> Ecto.Changeset.change(%{
        status: "preparing",
        attempt: submission.attempt + 1,
        claim_owner: command.owner_id,
        claim_generation: submission.claim_generation + 1,
        claim_expires_at: lease_expiry(now, command.lease_duration_ms),
        preparing_at: now,
        admitting_at: nil,
        updated_at: now
      })
      |> Repo.update!()
      |> Codec.result()
    end)
  end

  defp claim_stale!(command) do
    now = database_now!()

    from(submission in RunSubmission,
      where:
        submission.workspace_id == ^command.workspace_context.workspace_id and
          submission.status in @active_statuses and submission.claim_expires_at <= ^now,
      order_by: [
        asc: submission.claim_expires_at,
        asc: submission.submission_id
      ],
      limit: ^command.limit,
      lock: "FOR UPDATE SKIP LOCKED"
    )
    |> Repo.all()
    |> Enum.map(fn submission ->
      submission
      |> Ecto.Changeset.change(%{
        claim_owner: command.owner_id,
        claim_generation: submission.claim_generation + 1,
        claim_expires_at: lease_expiry(now, command.lease_duration_ms),
        updated_at: now
      })
      |> Repo.update!()
      |> Codec.result()
    end)
  end

  defp renew!(command) do
    {submission, now} = lock_live_claim!(command, @active_statuses)

    submission
    |> Ecto.Changeset.change(%{
      claim_expires_at: lease_expiry(now, command.lease_duration_ms),
      updated_at: now
    })
    |> Repo.update!()
    |> Codec.result()
  end

  defp mark_admitting!(command) do
    {submission, now} = lock_live_claim!(command, ["preparing"])

    if submission.cancellation_requested_at do
      rollback(:conflict, "run submission cancellation is already requested")
    end

    submission
    |> Ecto.Changeset.change(%{
      status: "admitting",
      preparation: command.preparation,
      admitting_at: now,
      updated_at: now
    })
    |> Repo.update!()
    |> Codec.result()
  end

  defp mark_submitted!(command) do
    {submission, now} = lock_live_claim!(command, ["admitting"])

    if submission.run_id != command.run_id or not matching_durable_run?(submission) do
      rollback(:constraint, "submitted run identity is not durable")
    end

    submission
    |> Ecto.Changeset.change(%{
      status: "submitted",
      claim_owner: nil,
      claim_expires_at: nil,
      outcome: command.outcome,
      terminal_at: now,
      updated_at: now
    })
    |> Repo.update!()
    |> Codec.result()
  end

  defp mark_failed!(command) do
    {submission, now} = lock_live_claim!(command, @active_statuses)

    submission
    |> Ecto.Changeset.change(%{
      status: "failed",
      claim_owner: nil,
      claim_expires_at: nil,
      error: command.error,
      failure_kind: Atom.to_string(command.failure_kind),
      terminal_at: now,
      updated_at: now
    })
    |> Repo.update!()
    |> Codec.result()
  end

  defp requeue!(command) do
    {submission, now} = lock_live_claim!(command, @active_statuses)

    if submission.cancellation_requested_at do
      rollback(:conflict, "run submission cancellation is already requested")
    end

    submission
    |> Ecto.Changeset.change(%{
      status: "queued",
      claim_owner: nil,
      claim_expires_at: nil,
      preparation: nil,
      error: command.reason,
      failure_kind: nil,
      available_at: command.available_at,
      preparing_at: nil,
      admitting_at: nil,
      updated_at: now
    })
    |> Repo.update!()
    |> Codec.result()
  end

  defp request_cancellation!(command) do
    submission =
      lock_submission!(
        command.workspace_context.workspace_id,
        command.submission_id
      )

    now = database_now!()

    changes =
      case submission.status do
        "queued" ->
          %{
            status: "cancelled",
            cancellation_requested_at: now,
            cancellation_reason: command.reason,
            terminal_at: now,
            updated_at: now
          }

        "preparing" ->
          %{
            cancellation_requested_at: now,
            cancellation_reason: command.reason,
            updated_at: now
          }

        "admitting" ->
          rollback(
            :conflict,
            "admitting run submission must reconcile through run cancellation"
          )

        _terminal ->
          rollback(:conflict, "terminal run submission cannot be cancelled")
      end

    submission
    |> Ecto.Changeset.change(changes)
    |> Repo.update!()
    |> Codec.result()
  end

  defp acknowledge_cancellation!(command) do
    {submission, now} = lock_live_claim!(command, ["preparing"])

    if is_nil(submission.cancellation_requested_at) do
      rollback(:conflict, "run submission cancellation was not requested")
    end

    submission
    |> Ecto.Changeset.change(%{
      status: "cancelled",
      claim_owner: nil,
      claim_expires_at: nil,
      terminal_at: now,
      updated_at: now
    })
    |> Repo.update!()
    |> Codec.result()
  end

  defp supersede!(command) do
    workspace_id = command.workspace_context.workspace_id

    submissions =
      lock_submissions!(
        workspace_id,
        [command.submission_id, command.replacement_submission_id]
      )

    submission = Map.fetch!(submissions, command.submission_id)
    replacement = Map.fetch!(submissions, command.replacement_submission_id)

    if submission.status != "queued" do
      rollback(:conflict, "only queued run submissions can be superseded")
    end

    if replacement.status not in ["queued", "preparing", "admitting", "submitted"] do
      rollback(:conflict, "replacement run submission is no longer active")
    end

    now = database_now!()

    submission
    |> Ecto.Changeset.change(%{
      status: "superseded",
      superseded_by_submission_id: command.replacement_submission_id,
      terminal_at: now,
      updated_at: now
    })
    |> Repo.update!()
    |> Codec.result()
  end

  defp retry_failed!(command) do
    workspace_id = command.workspace_context.workspace_id
    failed = lock_submission!(workspace_id, command.failed_submission_id)

    if failed.status != "failed" or failed.failure_kind != "safe" do
      rollback(:conflict, "only a proven-safe failed submission can be retried")
    end

    if command.run_id == failed.run_id do
      rollback(:conflict, "retry must use a new run identity")
    end

    advisory_lock!("run-submission-idempotency", workspace_id, command.idempotency_key)
    RunIdentity.lock!(workspace_id, command.run_id)

    case lock_by_idempotency(workspace_id, command.idempotency_key) do
      %RunSubmission{} ->
        rollback(:conflict, "retry idempotency key already belongs to another submission")

      nil ->
        reject_run_identity_collision!(workspace_id, command.run_id)
        now = database_now!()

        %RunSubmission{}
        |> Ecto.Changeset.change(%{
          workspace_id: workspace_id,
          submission_id: command.submission_id,
          source: "operator",
          idempotency_key: command.idempotency_key,
          request_hash: failed.request_hash,
          authority:
            command.workspace_context
            |> RunSubmissionAuthority.from_context()
            |> RunSubmissionAuthority.dump(),
          deployment_id: failed.deployment_id,
          manifest_version_id: failed.manifest_version_id,
          target_kind: failed.target_kind,
          target_id: failed.target_id,
          run_id: command.run_id,
          intent: failed.intent,
          status: "queued",
          attempt: 0,
          claim_generation: 0,
          retry_root_id: failed.retry_root_id,
          retry_of_submission_id: failed.submission_id,
          retry_command_id: command.command_id,
          enqueued_at: now,
          available_at: command.available_at || now,
          inserted_at: now,
          updated_at: now
        })
        |> Repo.insert!()
        |> Codec.result()
    end
  end

  defp execute(command, kind, request_builder, fun) do
    with :ok <- Validation.command(command),
         request <- request_builder.(),
         {:ok, request_hash} <- CanonicalJSON.hash(request) do
      transaction(fn ->
        now = database_now!()
        validate_command_window!(command.occurred_at, now)
        prune_command_receipts!(now)

        workspace_id = command.workspace_context.workspace_id
        command_lock!(workspace_id, command.command_id)

        case command_receipt(workspace_id, command.command_id) do
          %RunSubmissionCommand{command_kind: ^kind, request_hash: ^request_hash} = receipt ->
            replay_receipt!(receipt)

          %RunSubmissionCommand{} ->
            rollback(:conflict, "run submission command id has different content")

          nil ->
            result = fun.()
            insert_receipt!(workspace_id, command.command_id, kind, request_hash, result)
            result
        end
      end)
    else
      {:error, %Error{} = error} -> {:error, error}
      {:error, _reason} -> {:error, ErrorMapper.map(:invalid)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  defp insert_receipt!(workspace_id, command_id, kind, request_hash, result) do
    {result_kind, submission_ids} =
      case result do
        %{submission_id: submission_id} ->
          {"one", [submission_id]}

        submissions when is_list(submissions) ->
          {"many", Enum.map(submissions, & &1.submission_id)}
      end

    receipt_result =
      %{"kind" => result_kind, "submission_ids" => submission_ids}
      |> put_result_fences(kind, result)

    %RunSubmissionCommand{}
    |> Ecto.Changeset.change(%{
      workspace_id: workspace_id,
      command_id: command_id,
      submission_id: List.first(submission_ids),
      command_kind: kind,
      request_hash: request_hash,
      result: receipt_result,
      inserted_at: database_now!()
    })
    |> Repo.insert!()
  end

  defp replay_receipt!(
         %RunSubmissionCommand{
           workspace_id: workspace_id,
           command_kind: command_kind,
           result: %{"kind" => kind, "submission_ids" => submission_ids}
         } = receipt
       )
       when kind in ["one", "many"] and is_list(submission_ids) do
    submissions =
      from(submission in RunSubmission,
        where:
          submission.workspace_id == ^workspace_id and
            submission.submission_id in ^submission_ids
      )
      |> Repo.all()
      |> Map.new(&{&1.submission_id, &1})

    results =
      Enum.map(submission_ids, fn submission_id ->
        case Map.fetch(submissions, submission_id) do
          {:ok, submission} -> Codec.result(submission)
          :error -> rollback(:internal, "run submission command receipt is dangling")
        end
      end)

    verify_result_fences!(command_kind, receipt.result, results)

    case {kind, results} do
      {"one", [result]} -> result
      {"many", results} -> results
      _invalid -> rollback(:internal, "run submission command receipt has an invalid shape")
    end
  end

  defp replay_receipt!(_receipt),
    do: rollback(:internal, "run submission command receipt is invalid")

  defp put_result_fences(result, kind, submissions) when kind in @fenced_result_kinds do
    submissions = List.wrap(submissions)

    fences =
      Enum.map(submissions, fn submission ->
        %{
          "submission_id" => submission.submission_id,
          "status" => Atom.to_string(submission.status),
          "owner_id" => submission.claim_owner,
          "generation" => submission.claim_generation
        }
      end)

    Map.put(result, "result_fences", fences)
  end

  defp put_result_fences(result, _kind, _result), do: result

  defp verify_result_fences!(kind, %{"result_fences" => fences}, results)
       when kind in @fenced_result_kinds and is_list(fences) and
              length(fences) == length(results) do
    valid? =
      Enum.zip(fences, results)
      |> Enum.all?(fn {fence, result} ->
        fence == %{
          "submission_id" => result.submission_id,
          "status" => Atom.to_string(result.status),
          "owner_id" => result.claim_owner,
          "generation" => result.claim_generation
        }
      end)

    if not valid?, do: rollback(:fenced, "run submission command receipt has a stale fence")
  end

  defp verify_result_fences!(kind, _result, _results) when kind in @fenced_result_kinds,
    do: rollback(:internal, "run submission command receipt has an invalid fence")

  defp verify_result_fences!(_kind, _result, _results), do: :ok

  defp lock_live_claim!(command, statuses) do
    submission =
      lock_submission!(
        command.workspace_context.workspace_id,
        command.submission_id
      )

    now = database_now!()

    valid? =
      submission.status in statuses and submission.claim_owner == command.owner_id and
        submission.claim_generation == command.claim_generation and
        DateTime.compare(submission.claim_expires_at, now) == :gt

    if valid?, do: {submission, now}, else: rollback(:fenced, "run submission claim is stale")
  end

  defp lock_submission!(workspace_id, submission_id) do
    case from(submission in RunSubmission,
           where:
             submission.workspace_id == ^workspace_id and
               submission.submission_id == ^submission_id,
           lock: "FOR UPDATE"
         )
         |> Repo.one() do
      %RunSubmission{} = submission -> submission
      nil -> rollback(:not_found, "run submission was not found")
    end
  end

  defp lock_submissions!(workspace_id, submission_ids) do
    submissions =
      from(submission in RunSubmission,
        where:
          submission.workspace_id == ^workspace_id and
            submission.submission_id in ^submission_ids,
        order_by: [asc: submission.submission_id],
        lock: "FOR UPDATE"
      )
      |> Repo.all()

    if length(submissions) != length(submission_ids) do
      rollback(:not_found, "run submission or replacement was not found")
    end

    Map.new(submissions, &{&1.submission_id, &1})
  end

  defp lock_by_idempotency(workspace_id, idempotency_key) do
    from(submission in RunSubmission,
      where:
        submission.workspace_id == ^workspace_id and
          submission.idempotency_key == ^idempotency_key,
      lock: "FOR UPDATE"
    )
    |> Repo.one()
  end

  defp reject_run_identity_collision!(workspace_id, run_id) do
    submission_exists? =
      Repo.exists?(
        from(submission in RunSubmission,
          where: submission.workspace_id == ^workspace_id and submission.run_id == ^run_id
        )
      )

    run_exists? =
      Repo.exists?(
        from(run in Run,
          where: run.workspace_id == ^workspace_id and run.run_id == ^run_id
        )
      )

    if submission_exists? or run_exists? do
      rollback(:conflict, "run identity is already owned in this workspace")
    end
  end

  defp matching_durable_run?(submission) do
    Repo.exists?(
      from(run in Run,
        join: target in RunTarget,
        on:
          target.workspace_id == run.workspace_id and target.run_id == run.run_id and
            target.deployment_id == run.deployment_id and
            target.manifest_version_id == run.manifest_version_id,
        where:
          run.workspace_id == ^submission.workspace_id and run.run_id == ^submission.run_id and
            run.deployment_id == ^submission.deployment_id and
            run.manifest_version_id == ^submission.manifest_version_id and
            target.target_kind == ^submission.target_kind and
            target.target_id == ^submission.target_id and
            (^submission.target_kind == "pipeline" or target.is_primary == true)
      )
    )
  end

  defp command_receipt(workspace_id, command_id) do
    Repo.get_by(RunSubmissionCommand, workspace_id: workspace_id, command_id: command_id)
  end

  defp command_lock!(workspace_id, command_id),
    do: advisory_lock!("run-submission-command", workspace_id, command_id)

  defp advisory_lock!(namespace, workspace_id, identity) do
    SQL.query!(
      Repo,
      """
      SELECT pg_advisory_xact_lock(
        hashtextextended(jsonb_build_array($1::text, $2::text, $3::text)::text, 0)
      )
      """,
      [namespace, workspace_id, identity]
    )
  end

  defp validate_command_window!(occurred_at, now) do
    oldest = DateTime.add(now, -@receipt_retention_ms, :millisecond)
    newest = DateTime.add(now, @maximum_future_clock_skew_ms, :millisecond)

    if DateTime.compare(occurred_at, oldest) == :lt or
         DateTime.compare(occurred_at, newest) == :gt do
      rollback(:invalid, "run submission command is outside the idempotency window")
    end
  end

  defp prune_command_receipts!(now) do
    cutoff =
      DateTime.add(
        now,
        -(@receipt_retention_ms + @maximum_future_clock_skew_ms),
        :millisecond
      )

    SQL.query!(
      Repo,
      """
      DELETE FROM favn_control.run_submission_commands
      WHERE ctid IN (
        SELECT ctid
        FROM favn_control.run_submission_commands
        WHERE inserted_at < $1
        ORDER BY inserted_at
        LIMIT $2
        FOR UPDATE SKIP LOCKED
      )
      """,
      [cutoff, @receipt_prune_limit]
    )
  end

  defp page_rows(rows, limit) when length(rows) > limit do
    items = Enum.take(rows, limit)
    last = List.last(items)
    {items, %{inserted_at: last.inserted_at, submission_id: last.submission_id}}
  end

  defp page_rows(rows, _limit), do: {rows, nil}

  defp maybe_status(query, nil), do: query

  defp maybe_status(query, status),
    do: where(query, [submission], submission.status == ^status)

  defp after_cursor(query, nil), do: query

  defp after_cursor(query, %{inserted_at: inserted_at, submission_id: submission_id}) do
    where(
      query,
      [submission],
      submission.inserted_at < ^inserted_at or
        (submission.inserted_at == ^inserted_at and
           submission.submission_id < ^submission_id)
    )
  end

  defp after_workspace_cursor(query, nil), do: query

  defp after_workspace_cursor(query, workspace_id),
    do: where(query, [submission], submission.workspace_id > ^workspace_id)

  defp enqueue_request(command) do
    authority =
      command.workspace_context
      |> RunSubmissionAuthority.from_context()
      |> RunSubmissionAuthority.dump()

    %{
      "submission_id" => command.submission_id,
      "source" => Atom.to_string(command.source),
      "idempotency_key" => command.idempotency_key,
      "request_hash" => Base.encode64(command.request_hash),
      "authority" => authority,
      "deployment_id" => command.deployment_id,
      "manifest_version_id" => command.manifest_version_id,
      "target_kind" => command.target_kind,
      "target_id" => command.target_id,
      "run_id" => command.run_id,
      "intent" => command.intent,
      "occurred_at" => encode_datetime(command.occurred_at),
      "available_at" => encode_datetime(command.available_at)
    }
  end

  defp claim_request(command) do
    %{
      "owner_id" => command.owner_id,
      "lease_duration_ms" => command.lease_duration_ms,
      "occurred_at" => encode_datetime(command.occurred_at),
      "limit" => command.limit
    }
  end

  defp fenced_request(command) do
    %{
      "submission_id" => command.submission_id,
      "owner_id" => command.owner_id,
      "claim_generation" => command.claim_generation,
      "occurred_at" => encode_datetime(command.occurred_at)
    }
  end

  defp encode_datetime(nil), do: nil
  defp encode_datetime(%DateTime{} = datetime), do: DateTime.to_iso8601(datetime)

  defp lease_expiry(now, duration_ms),
    do: DateTime.add(now, duration_ms, :millisecond)

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

  defp rollback(kind, message) do
    Repo.rollback(Error.new(kind, message))
  end
end
