defmodule FavnStoragePostgres.RunOwnership.Store do
  @moduledoc false

  @behaviour FavnOrchestrator.Persistence.RunOwnershipStore

  import Ecto.Query

  alias Ecto.Adapters.SQL
  alias FavnOrchestrator.Persistence.Commands.AdvanceRunnerExecution
  alias FavnOrchestrator.Persistence.Commands.ClaimRecoveryBatch
  alias FavnOrchestrator.Persistence.Commands.ClaimRun
  alias FavnOrchestrator.Persistence.Commands.RecordRunnerDispatch
  alias FavnOrchestrator.Persistence.Commands.ReleaseRunOwnership
  alias FavnOrchestrator.Persistence.Commands.RenewRunOwnership
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Identity
  alias FavnOrchestrator.Persistence.Queries.PageRunnerExecutions
  alias FavnOrchestrator.Persistence.Results.CursorPage
  alias FavnOrchestrator.Persistence.Results.RunnerExecution, as: RunnerExecutionResult
  alias FavnOrchestrator.Persistence.Results.RunOwnership, as: RunOwnershipResult
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnStoragePostgres.ErrorMapper
  alias FavnStoragePostgres.Outbox.Writer, as: OutboxWriter
  alias FavnStoragePostgres.Payload
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.Schemas.Run
  alias FavnStoragePostgres.Schemas.RunnerExecution
  alias FavnStoragePostgres.Schemas.RunOwnership

  @terminal_execution_statuses ~w(ok error cancelled timed_out)

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

  @impl true
  def record_dispatch(%RecordRunnerDispatch{} = command) do
    with :ok <- validate_dispatch(command),
         {:ok, execution} <- Repo.transaction(fn -> record_dispatch!(command) end) do
      {:ok, execution}
    else
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, ErrorMapper.map(reason)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def advance_execution(%AdvanceRunnerExecution{} = command) do
    with :ok <- validate_advance(command),
         {:ok, execution} <- Repo.transaction(fn -> advance_execution!(command) end) do
      {:ok, execution}
    else
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, ErrorMapper.map(reason)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def page_executions(%PageRunnerExecutions{} = page) do
    case validate_page(page) do
      :ok -> page_executions!(page)
      {:error, reason} -> {:error, ErrorMapper.map(reason)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  defp page_executions!(page) do
    query =
      RunnerExecution
      |> where(
        [execution],
        execution.workspace_id == ^page.workspace_context.workspace_id
      )
      |> maybe_filter_active(page.active_only?)
      |> optional_filter(:run_id, page.run_id)
      |> optional_filter(:owner_id, page.owner_id)
      |> then(fn query ->
        if match?(%{runner_execution_id: id} when is_binary(id), page.after),
          do:
            where(
              query,
              [execution],
              execution.runner_execution_id > ^page.after.runner_execution_id
            ),
          else: query
      end)
      |> order_by([execution], asc: execution.runner_execution_id)
      |> limit(^(page.limit + 1))

    rows = Repo.all(query)
    items = rows |> Enum.take(page.limit) |> Enum.map(&execution_result/1)
    has_more? = length(rows) > page.limit

    {:ok,
     %CursorPage{
       items: items,
       limit: page.limit,
       has_more?: has_more?,
       next_cursor:
         if(has_more? and items != [],
           do: %{runner_execution_id: List.last(items).runner_execution_id}
         )
     }}
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

  defp record_dispatch!(command) do
    lock_run!(command.workspace_context.workspace_id, command.run_id)
    validate_fence!(command.workspace_context.workspace_id, command)

    attrs = %{
      workspace_id: command.workspace_context.workspace_id,
      runner_execution_id: command.runner_execution_id,
      run_id: command.run_id,
      dispatch_id: command.dispatch_id,
      last_command_id: command.command_id,
      owner_id: command.owner_id,
      run_fencing_token: command.fencing_token,
      status: "dispatching",
      version: 1,
      dispatch_payload: command.payload,
      dispatched_at: command.occurred_at,
      inserted_at: command.occurred_at,
      updated_at: command.occurred_at
    }

    case Repo.insert_all(RunnerExecution, [attrs], on_conflict: :nothing) do
      {1, _rows} ->
        OutboxWriter.insert!(%{
          workspace_id: command.workspace_context.workspace_id,
          command_id: command.command_id,
          event_kind: "runner.dispatch.recorded",
          aggregate_kind: "runner_execution",
          aggregate_id: command.runner_execution_id,
          aggregate_version: 1,
          occurred_at: command.occurred_at,
          payload: %{
            "runner_execution_id" => command.runner_execution_id,
            "run_id" => command.run_id,
            "status" => "dispatching"
          }
        })

        attrs |> then(&struct!(RunnerExecution, &1)) |> execution_result()

      {0, _rows} ->
        existing =
          get_execution!(command.workspace_context.workspace_id, command.runner_execution_id)

        if existing.dispatch_id == command.dispatch_id and
             existing.last_command_id == command.command_id and
             existing.run_id == command.run_id and existing.owner_id == command.owner_id and
             existing.run_fencing_token == command.fencing_token and
             existing.dispatch_payload == command.payload and
             same_datetime?(existing.dispatched_at, command.occurred_at) do
          execution_result(existing)
        else
          Repo.rollback(Error.new(:conflict, "runner dispatch identity has different content"))
        end
    end
  end

  defp advance_execution!(command) do
    workspace_id = command.workspace_context.workspace_id
    lock_run!(workspace_id, command.run_id)
    validate_fence!(workspace_id, command)
    execution = lock_execution!(workspace_id, command.runner_execution_id)

    cond do
      execution.run_id != command.run_id ->
        Repo.rollback(Error.new(:conflict, "runner execution belongs to another run"))

      execution.last_command_id == command.command_id and
          replayed_transition_matches?(execution, command) ->
        execution_result(execution)

      execution.last_command_id == command.command_id ->
        Repo.rollback(Error.new(:conflict, "runner execution replay has different content"))

      execution.version != command.expected_version ->
        Repo.rollback(Error.new(:conflict, "runner execution version changed"))

      not valid_execution_transition?(execution.status, command.status) ->
        Repo.rollback(Error.new(:invalid, "invalid runner execution transition"))

      true ->
        status = Atom.to_string(command.status)
        terminal_at = if status in @terminal_execution_statuses, do: command.occurred_at

        updated =
          execution
          |> Ecto.Changeset.change(%{
            last_command_id: command.command_id,
            status: status,
            version: execution.version + 1,
            result: command.result,
            error: command.error,
            terminal_at: terminal_at,
            updated_at: command.occurred_at
          })
          |> Repo.update!()

        OutboxWriter.insert!(%{
          workspace_id: workspace_id,
          command_id: command.command_id,
          event_kind: "runner.execution." <> status,
          aggregate_kind: "runner_execution",
          aggregate_id: command.runner_execution_id,
          aggregate_version: updated.version,
          occurred_at: command.occurred_at,
          payload: %{
            "runner_execution_id" => command.runner_execution_id,
            "run_id" => command.run_id,
            "status" => status,
            "version" => updated.version
          }
        })

        execution_result(updated)
    end
  end

  defp validate_fence!(workspace_id, command) do
    ownership = lock_ownership!(workspace_id, command.run_id)

    if matching_owner?(ownership, command) and is_nil(ownership.released_at) and
         future?(ownership.expires_at) do
      :ok
    else
      Repo.rollback(Error.new(:fenced, "run ownership fencing token is stale"))
    end
  end

  defp lock_run!(workspace_id, run_id) do
    from(run in Run,
      where: run.workspace_id == ^workspace_id and run.run_id == ^run_id,
      lock: "FOR UPDATE"
    )
    |> Repo.one()
    |> case do
      nil -> Repo.rollback(Error.new(:not_found, "run not found"))
      run -> run
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

  defp lock_execution!(workspace_id, execution_id) do
    from(execution in RunnerExecution,
      where:
        execution.workspace_id == ^workspace_id and
          execution.runner_execution_id == ^execution_id,
      lock: "FOR UPDATE"
    )
    |> Repo.one()
    |> case do
      nil -> Repo.rollback(Error.new(:not_found, "runner execution not found"))
      execution -> execution
    end
  end

  defp get_execution!(workspace_id, execution_id) do
    Repo.get_by!(RunnerExecution,
      workspace_id: workspace_id,
      runner_execution_id: execution_id
    )
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

  defp execution_result(%RunnerExecution{} = execution) do
    %RunnerExecutionResult{
      workspace_id: execution.workspace_id,
      runner_execution_id: execution.runner_execution_id,
      run_id: execution.run_id,
      dispatch_id: execution.dispatch_id,
      owner_id: execution.owner_id,
      fencing_token: execution.run_fencing_token,
      status: String.to_existing_atom(execution.status),
      version: execution.version,
      payload: execution.dispatch_payload,
      result: execution.result,
      error: execution.error,
      dispatched_at: execution.dispatched_at,
      terminal_at: execution.terminal_at
    }
  end

  defp optional_filter(query, _field, nil), do: query

  defp optional_filter(query, :run_id, value),
    do: where(query, [execution], execution.run_id == ^value)

  defp optional_filter(query, :owner_id, value),
    do: where(query, [execution], execution.owner_id == ^value)

  defp valid_execution_transition?("dispatching", status),
    do: status in [:running, :error, :cancelled]

  defp valid_execution_transition?("running", status),
    do: status in [:cancelling, :ok, :error, :cancelled, :timed_out]

  defp valid_execution_transition?("cancelling", status),
    do: status in [:cancelled, :ok, :error, :timed_out]

  defp valid_execution_transition?(_status, _next), do: false

  defp replayed_transition_matches?(execution, command) do
    status = Atom.to_string(command.status)

    expected_terminal_at =
      if status in @terminal_execution_statuses, do: command.occurred_at

    execution.run_id == command.run_id and execution.owner_id == command.owner_id and
      execution.run_fencing_token == command.fencing_token and
      execution.version == command.expected_version + 1 and execution.status == status and
      execution.result == command.result and execution.error == command.error and
      same_datetime?(execution.updated_at, command.occurred_at) and
      same_datetime?(execution.terminal_at, expected_terminal_at)
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

  defp validate_dispatch(command) do
    if workspace_context?(command.workspace_context) and
         Enum.all?(
           [
             command.command_id,
             command.run_id,
             command.runner_execution_id,
             command.dispatch_id,
             command.owner_id
           ],
           &valid_id?/1
         ) and is_integer(command.fencing_token) and command.fencing_token > 0 and
         is_map(command.payload) and Payload.validate(command.payload, 256 * 1_024) == :ok and
         match?(%DateTime{}, command.occurred_at),
       do: :ok,
       else: {:error, :invalid}
  end

  defp validate_advance(command) do
    valid_statuses = [:dispatching, :running, :cancelling, :ok, :error, :cancelled, :timed_out]

    if workspace_context?(command.workspace_context) and
         Enum.all?(
           [command.command_id, command.run_id, command.runner_execution_id, command.owner_id],
           &valid_id?/1
         ) and is_integer(command.fencing_token) and command.fencing_token > 0 and
         is_integer(command.expected_version) and command.expected_version > 0 and
         command.status in valid_statuses and valid_execution_payload?(command) and
         match?(%DateTime{}, command.occurred_at),
       do: :ok,
       else: {:error, :invalid}
  end

  defp valid_execution_payload?(%{status: status, result: nil, error: nil})
       when status in [:dispatching, :running, :cancelling, :cancelled],
       do: true

  defp valid_execution_payload?(%{status: :ok, result: result, error: nil}) when is_map(result),
    do: Payload.validate(result, 256 * 1_024) == :ok

  defp valid_execution_payload?(%{status: status, result: nil, error: error})
       when status in [:error, :timed_out, :cancelled] and is_map(error),
       do: Payload.validate(error, 64 * 1_024) == :ok

  defp valid_execution_payload?(_command), do: false

  defp validate_page(page) do
    if workspace_context?(page.workspace_context) and is_integer(page.limit) and page.limit >= 1 and
         page.limit <= 500 and
         is_boolean(page.active_only?) and
         (is_nil(page.run_id) or valid_id?(page.run_id)) and
         (is_nil(page.owner_id) or valid_id?(page.owner_id)) and
         (page.active_only? or valid_id?(page.run_id)) and
         (is_nil(page.after) or
            (match?(%{runner_execution_id: _id}, page.after) and
               valid_id?(page.after.runner_execution_id))),
       do: :ok,
       else: {:error, :invalid}
  end

  defp maybe_filter_active(query, true),
    do: where(query, [execution], is_nil(execution.terminal_at))

  defp maybe_filter_active(query, false), do: query

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
  defp same_datetime?(nil, nil), do: true

  defp same_datetime?(%DateTime{} = left, %DateTime{} = right),
    do: DateTime.compare(left, right) == :eq

  defp same_datetime?(_left, _right), do: false
  defp valid_id?(value), do: Identity.valid?(value)
end
