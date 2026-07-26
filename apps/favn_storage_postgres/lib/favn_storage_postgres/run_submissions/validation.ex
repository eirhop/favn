defmodule FavnStoragePostgres.RunSubmissions.Validation do
  @moduledoc false

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
  alias FavnOrchestrator.Persistence.Queries.PageRunSubmissions
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnStoragePostgres.ErrorMapper
  alias FavnStoragePostgres.Payload

  @sources [:api, :operator, :scheduler, :backfill, :rebuild, :recovery, :child_run]
  @statuses [:queued, :preparing, :admitting, :submitted, :failed, :cancelled, :superseded]

  @spec command(struct()) :: :ok | {:error, Error.t()}
  def command(%EnqueueRunSubmission{} = command) do
    valid? =
      Enum.all?(
        [
          command.command_id,
          command.submission_id,
          command.idempotency_key,
          command.deployment_id,
          command.manifest_version_id,
          command.target_kind,
          command.target_id,
          command.run_id
        ],
        &valid_id?/1
      ) and command.source in @sources and command.target_kind in ["asset", "pipeline"] and
        hash?(command.request_hash) and
        payload?(command.intent, 262_144) and
        datetime?(command.occurred_at) and optional_datetime?(command.available_at)

    with :ok <- authorize_writer(command.workspace_context), do: valid(valid?)
  end

  def command(%ClaimRunSubmissions{} = command), do: claim(command)
  def command(%ClaimStaleRunSubmissions{} = command), do: claim(command)

  def command(%RenewRunSubmissionClaim{} = command) do
    with :ok <- authorize_writer(command.workspace_context) do
      valid(
        fenced?(command) and valid_lease?(command.lease_duration_ms) and
          datetime?(command.occurred_at)
      )
    end
  end

  def command(%MarkRunSubmissionAdmitting{} = command) do
    with :ok <- authorize_writer(command.workspace_context) do
      valid(
        fenced?(command) and payload?(command.preparation, 262_144) and
          datetime?(command.occurred_at)
      )
    end
  end

  def command(%MarkRunSubmissionSubmitted{} = command) do
    with :ok <- authorize_writer(command.workspace_context) do
      valid(
        fenced?(command) and valid_id?(command.run_id) and payload?(command.outcome, 65_536) and
          datetime?(command.occurred_at)
      )
    end
  end

  def command(%MarkRunSubmissionFailed{} = command) do
    with :ok <- authorize_writer(command.workspace_context) do
      valid(
        fenced?(command) and command.failure_kind in [:safe, :permanent, :unknown] and
          payload?(command.error, 65_536) and datetime?(command.occurred_at)
      )
    end
  end

  def command(%RequeueRunSubmission{} = command) do
    with :ok <- authorize_writer(command.workspace_context) do
      valid(
        fenced?(command) and payload?(command.reason, 65_536) and
          datetime?(command.available_at) and datetime?(command.occurred_at)
      )
    end
  end

  def command(%RequestRunSubmissionCancellation{} = command) do
    with :ok <- authorize_writer(command.workspace_context) do
      valid(
        valid_id?(command.command_id) and valid_id?(command.submission_id) and
          valid_reason?(command.reason) and datetime?(command.occurred_at)
      )
    end
  end

  def command(%AcknowledgeRunSubmissionCancellation{} = command) do
    with :ok <- authorize_writer(command.workspace_context),
         do: valid(fenced?(command) and datetime?(command.occurred_at))
  end

  def command(%SupersedeRunSubmission{} = command) do
    with :ok <- authorize_writer(command.workspace_context) do
      valid(
        valid_id?(command.command_id) and valid_id?(command.submission_id) and
          valid_id?(command.replacement_submission_id) and
          command.submission_id != command.replacement_submission_id and
          datetime?(command.occurred_at)
      )
    end
  end

  def command(%RetryFailedRunSubmission{} = command) do
    with :ok <- authorize_writer(command.workspace_context) do
      valid(
        Enum.all?(
          [
            command.command_id,
            command.failed_submission_id,
            command.submission_id,
            command.idempotency_key,
            command.run_id
          ],
          &valid_id?/1
        ) and command.failed_submission_id != command.submission_id and
          datetime?(command.occurred_at) and optional_datetime?(command.available_at)
      )
    end
  end

  def command(_command), do: invalid()

  @spec query(struct()) :: :ok | {:error, Error.t()}
  def query(%GetRunSubmission{} = query) do
    with :ok <- authorize_reader(query.workspace_context),
         do: valid(valid_id?(query.submission_id))
  end

  def query(%PageRunSubmissions{} = query) do
    status? = is_nil(query.status) or query.status in @statuses

    cursor? =
      case query.after do
        nil -> true
        %{inserted_at: %DateTime{}, submission_id: submission_id} -> valid_id?(submission_id)
        _invalid -> false
      end

    with :ok <- authorize_reader(query.workspace_context) do
      valid(status? and cursor? and is_integer(query.limit) and query.limit in 1..200)
    end
  end

  def query(_query), do: invalid()

  defp claim(command) do
    with :ok <- authorize_writer(command.workspace_context) do
      valid(
        valid_id?(command.command_id) and valid_id?(command.owner_id) and
          valid_lease?(command.lease_duration_ms) and datetime?(command.occurred_at) and
          is_integer(command.limit) and command.limit in 1..50
      )
    end
  end

  defp fenced?(command) do
    valid_id?(command.command_id) and valid_id?(command.submission_id) and
      valid_id?(command.owner_id) and
      is_integer(command.claim_generation) and command.claim_generation > 0
  end

  defp authorize_writer(context) do
    if writer?(context),
      do: :ok,
      else: {:error, Error.new(:forbidden, "workspace run-submission write role required")}
  end

  defp authorize_reader(context) do
    if reader?(context),
      do: :ok,
      else: {:error, Error.new(:forbidden, "workspace run-submission read role required")}
  end

  defp writer?(%WorkspaceContext{roles: roles} = context),
    do:
      WorkspaceContext.valid?(context) and
        Enum.any?(roles, &(&1 in [:customer_operator, :workspace_admin, :platform_operator]))

  defp writer?(_context), do: false

  defp reader?(%WorkspaceContext{roles: roles} = context),
    do:
      WorkspaceContext.valid?(context) and
        Enum.any?(
          roles,
          &(&1 in [
              :customer_reader,
              :customer_operator,
              :workspace_admin,
              :platform_operator
            ])
        )

  defp reader?(_context), do: false

  defp valid_id?(value), do: is_binary(value) and byte_size(value) in 1..255
  defp valid_reason?(value), do: is_binary(value) and byte_size(value) in 1..2_048
  defp valid_lease?(value), do: is_integer(value) and value in 1_000..3_600_000
  defp hash?(value), do: is_binary(value) and byte_size(value) == 32
  defp datetime?(%DateTime{}), do: true
  defp datetime?(_value), do: false
  defp optional_datetime?(nil), do: true
  defp optional_datetime?(value), do: datetime?(value)
  defp payload?(value, maximum), do: is_map(value) and Payload.validate(value, maximum) == :ok
  defp valid(true), do: :ok
  defp valid(false), do: invalid()
  defp invalid, do: {:error, ErrorMapper.map(:invalid)}
end
