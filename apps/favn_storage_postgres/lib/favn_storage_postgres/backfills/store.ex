defmodule FavnStoragePostgres.Backfills.Store do
  @moduledoc false

  @behaviour FavnOrchestrator.Persistence.BackfillStore

  import Ecto.Query

  alias Ecto.Adapters.SQL
  alias FavnOrchestrator.Persistence.BackfillPlan
  alias FavnOrchestrator.Persistence.Commands.ActivateBackfillPlan
  alias FavnOrchestrator.Persistence.Commands.AppendBackfillPlanBatch
  alias FavnOrchestrator.Persistence.Commands.BackfillPlanWindow
  alias FavnOrchestrator.Persistence.Commands.ClaimBackfillWindows
  alias FavnOrchestrator.Persistence.Commands.StartBackfillPlan
  alias FavnOrchestrator.Persistence.Commands.TransitionBackfillWindow
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Queries.GetBackfill
  alias FavnOrchestrator.Persistence.Queries.PageAssetWindows
  alias FavnOrchestrator.Persistence.Queries.PageBackfillWindows
  alias FavnOrchestrator.Persistence.Results.Backfill, as: BackfillResult
  alias FavnOrchestrator.Persistence.Results.BackfillWindow, as: BackfillWindowResult
  alias FavnOrchestrator.Persistence.Results.CursorPage
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnStoragePostgres.CanonicalJSON
  alias FavnStoragePostgres.ErrorMapper
  alias FavnStoragePostgres.Idempotency.Transaction, as: IdempotencyTransaction
  alias FavnStoragePostgres.Outbox.Writer, as: OutboxWriter
  alias FavnStoragePostgres.Payload
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.Schemas.Backfill
  alias FavnStoragePostgres.Schemas.BackfillOverview
  alias FavnStoragePostgres.Schemas.BackfillPlanBatch
  alias FavnStoragePostgres.Schemas.BackfillWindow

  @max_batch_windows 500
  @max_plan_windows 10_000
  @max_plan_batches 100
  @backfill_statuses ~w(planning ready running completed failed cancelled)a
  @window_statuses ~w(planned ready claimed running succeeded failed cancelled)

  @impl true
  def start_plan(%StartBackfillPlan{} = command) do
    with :ok <- validate_start(command) do
      transaction(fn ->
        IdempotencyTransaction.execute!(
          command.workspace_context.workspace_id,
          command.idempotency,
          fn -> start_plan!(command) end,
          &encode_idempotent_backfill/1,
          &decode_idempotent_backfill/1
        )
      end)
    end
  end

  @impl true
  def append_plan_batch(%AppendBackfillPlanBatch{} = command) do
    with :ok <- validate_append(command) do
      transaction(fn -> append_plan_batch!(command) end)
    end
  end

  @impl true
  def activate_plan(%ActivateBackfillPlan{} = command) do
    with :ok <- validate_activate(command) do
      transaction(fn -> activate_plan!(command) end)
    end
  end

  @impl true
  def claim_windows(%ClaimBackfillWindows{} = command) do
    with :ok <- validate_claim_windows(command) do
      transaction(fn -> claim_windows!(command) end)
    end
  end

  @impl true
  def transition_window(%TransitionBackfillWindow{} = command) do
    with :ok <- validate_transition(command) do
      transaction(fn -> transition_window!(command) end)
    end
  end

  @impl true
  def get_backfill(%GetBackfill{} = query) do
    with :ok <- validate_get(query) do
      case Repo.get_by(Backfill,
             workspace_id: query.workspace_context.workspace_id,
             backfill_id: query.backfill_id
           ) do
        nil -> {:error, ErrorMapper.map(:not_found)}
        backfill -> {:ok, backfill_result(backfill)}
      end
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def page_windows(%PageBackfillWindows{} = page) do
    with :ok <- validate_window_page(page) do
      query =
        BackfillWindow
        |> where(
          [window],
          window.workspace_id == ^page.workspace_context.workspace_id and
            window.backfill_id == ^page.backfill_id
        )
        |> filter_status(page.status)
        |> after_window(page.after)
        |> order_by([window], asc: window.window_key, asc: window.window_id)
        |> limit(^(page.limit + 1))

      {:ok, window_page(Repo.all(query), page.limit, :forward)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def page_asset_windows(%PageAssetWindows{} = page) do
    with :ok <- validate_asset_window_page(page) do
      workspace_id = page.workspace_context.workspace_id

      query =
        from(window in BackfillWindow,
          join: backfill in Backfill,
          on:
            backfill.workspace_id == window.workspace_id and
              backfill.backfill_id == window.backfill_id,
          where:
            backfill.workspace_id == ^workspace_id and
              backfill.manifest_version_id == ^page.manifest_version_id and
              backfill.target_kind == "asset" and backfill.target_id == ^page.target_id,
          order_by: [desc: window.window_start, desc: window.window_id],
          limit: ^(page.limit + 1),
          select: window
        )
        |> after_asset_window(page.after)

      {:ok, window_page(Repo.all(query), page.limit, :reverse)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  defp start_plan!(command) do
    workspace_id = command.workspace_context.workspace_id
    request_hash = start_request_hash!(command)
    occurred_at = database_datetime(command.occurred_at)

    attrs = %{
      workspace_id: workspace_id,
      backfill_id: command.backfill_id,
      root_run_id: command.root_run_id,
      start_command_id: command.command_id,
      request_hash: request_hash,
      deployment_id: command.deployment_id,
      manifest_version_id: command.manifest_version_id,
      target_kind: Atom.to_string(command.target_kind),
      target_id: command.target_id,
      range_start: database_datetime(command.range_start),
      range_end: database_datetime(command.range_end),
      status: "planning",
      expected_window_count: command.expected_window_count,
      expected_batch_count: command.expected_batch_count,
      appended_window_count: 0,
      appended_batch_count: 0,
      plan_hash: command.plan_hash,
      metadata: command.metadata,
      version: 1,
      inserted_at: occurred_at,
      updated_at: occurred_at
    }

    case Repo.insert_all(Backfill, [attrs], on_conflict: :nothing) do
      {1, _rows} ->
        attrs |> then(&struct!(Backfill, &1)) |> backfill_result()

      {0, _rows} ->
        existing =
          from(backfill in Backfill,
            where:
              backfill.workspace_id == ^workspace_id and
                (backfill.backfill_id == ^command.backfill_id or
                   backfill.start_command_id == ^command.command_id),
            lock: "FOR UPDATE"
          )
          |> Repo.one!()

        if existing.backfill_id == command.backfill_id and
             existing.start_command_id == command.command_id and
             existing.request_hash == request_hash do
          backfill_result(existing)
        else
          Repo.rollback(Error.new(:conflict, "backfill plan identity has different content"))
        end
    end
  end

  defp append_plan_batch!(command) do
    workspace_id = command.workspace_context.workspace_id
    backfill = lock_backfill!(workspace_id, command.backfill_id)
    computed_hash = BackfillPlan.batch_hash(command.windows)

    if computed_hash != command.batch_hash do
      Repo.rollback(Error.new(:invalid, "backfill batch hash does not match its windows"))
    end

    receipt =
      from(batch in BackfillPlanBatch,
        where:
          batch.workspace_id == ^workspace_id and
            (batch.command_id == ^command.command_id or
               (batch.backfill_id == ^command.backfill_id and
                  batch.batch_index == ^command.batch_index)),
        lock: "FOR UPDATE"
      )
      |> Repo.one()

    cond do
      receipt && exact_batch_replay?(receipt, command) ->
        backfill_result(backfill)

      receipt ->
        Repo.rollback(Error.new(:conflict, "backfill batch identity has different content"))

      backfill.status != "planning" ->
        Repo.rollback(Error.new(:conflict, "backfill plan is no longer appendable"))

      command.batch_index >= backfill.expected_batch_count ->
        Repo.rollback(Error.new(:invalid, "backfill batch index exceeds declared plan"))

      backfill.appended_window_count + length(command.windows) > backfill.expected_window_count ->
        Repo.rollback(Error.new(:invalid, "backfill batch exceeds declared window count"))

      true ->
        insert_batch!(backfill, command)
    end
  end

  defp insert_batch!(backfill, command) do
    workspace_id = backfill.workspace_id
    occurred_at = database_datetime(command.occurred_at)

    %BackfillPlanBatch{
      workspace_id: workspace_id,
      backfill_id: command.backfill_id,
      batch_index: command.batch_index,
      command_id: command.command_id,
      batch_hash: command.batch_hash,
      window_count: length(command.windows),
      inserted_at: occurred_at
    }
    |> Repo.insert!()

    rows =
      Enum.map(command.windows, fn window ->
        %{
          workspace_id: workspace_id,
          backfill_id: command.backfill_id,
          window_id: window.window_id,
          batch_index: command.batch_index,
          window_key: window.window_key,
          window_start: database_datetime(window.window_start),
          window_end: database_datetime(window.window_end),
          status: "planned",
          fencing_token: 0,
          attempt_count: 0,
          payload: window.payload,
          version: 1,
          inserted_at: occurred_at,
          updated_at: occurred_at
        }
      end)

    {count, _rows} = Repo.insert_all(BackfillWindow, rows)

    if count != length(rows),
      do: Repo.rollback(Error.new(:conflict, "backfill window identity is duplicated"))

    updated =
      backfill
      |> Ecto.Changeset.change(%{
        appended_window_count: backfill.appended_window_count + length(rows),
        appended_batch_count: backfill.appended_batch_count + 1,
        version: backfill.version + 1,
        updated_at: occurred_at
      })
      |> Repo.update!()

    backfill_result(updated)
  end

  defp activate_plan!(command) do
    workspace_id = command.workspace_context.workspace_id
    backfill = lock_backfill!(workspace_id, command.backfill_id)
    occurred_at = database_datetime(command.occurred_at)

    cond do
      backfill.last_command_id == command.command_id and backfill.status in ["ready", "running"] ->
        backfill_result(backfill)

      backfill.status != "planning" ->
        Repo.rollback(Error.new(:conflict, "backfill plan cannot be activated"))

      backfill.version != command.expected_version ->
        Repo.rollback(Error.new(:conflict, "backfill plan version changed"))

      true ->
        verify_plan!(backfill)

        from(window in BackfillWindow,
          where:
            window.workspace_id == ^workspace_id and
              window.backfill_id == ^command.backfill_id and window.status == "planned"
        )
        |> Repo.update_all(set: [status: "ready", updated_at: occurred_at])

        updated =
          backfill
          |> Ecto.Changeset.change(%{
            last_command_id: command.command_id,
            status: "ready",
            version: backfill.version + 1,
            updated_at: occurred_at
          })
          |> Repo.update!()

        OutboxWriter.insert!(%{
          workspace_id: workspace_id,
          command_id: command.command_id,
          event_kind: "backfill.plan.activated",
          aggregate_kind: "backfill",
          aggregate_id: command.backfill_id,
          aggregate_version: updated.version,
          occurred_at: occurred_at,
          payload: %{
            "backfill_id" => command.backfill_id,
            "window_count" => updated.expected_window_count,
            "status" => "ready"
          }
        })

        backfill_result(updated)
    end
  end

  defp verify_plan!(backfill) do
    receipts =
      from(batch in BackfillPlanBatch,
        where:
          batch.workspace_id == ^backfill.workspace_id and
            batch.backfill_id == ^backfill.backfill_id,
        order_by: [asc: batch.batch_index]
      )
      |> Repo.all()

    indices = Enum.map(receipts, & &1.batch_index)
    expected_indices = Enum.to_list(0..(backfill.expected_batch_count - 1)//1)
    actual_window_count = Enum.sum(Enum.map(receipts, & &1.window_count))
    actual_plan_hash = BackfillPlan.plan_hash(Enum.map(receipts, & &1.batch_hash))

    if indices != expected_indices or length(receipts) != backfill.expected_batch_count or
         actual_window_count != backfill.expected_window_count or
         backfill.appended_window_count != backfill.expected_window_count or
         backfill.appended_batch_count != backfill.expected_batch_count or
         actual_plan_hash != backfill.plan_hash do
      Repo.rollback(Error.new(:conflict, "backfill plan is incomplete or has a hash mismatch"))
    end
  end

  defp claim_windows!(command) do
    workspace_id = command.workspace_context.workspace_id

    replay_query =
      from(window in BackfillWindow,
        where:
          window.workspace_id == ^workspace_id and
            window.claim_command_id == ^command.batch_id,
        order_by: [asc: window.window_start, asc: window.window_id]
      )
      |> maybe_backfill(command.backfill_id)

    case Repo.all(replay_query) do
      [] ->
        claim_new_windows!(command)

      replay ->
        if live_window_replay?(replay, command) do
          Enum.map(replay, &window_result/1)
        else
          Repo.rollback(Error.new(:fenced, "backfill claim batch is no longer live"))
        end
    end
  end

  defp claim_new_windows!(command) do
    %{rows: rows} =
      SQL.query!(
        Repo,
        """
        WITH candidates AS (
          SELECT workspace_id, backfill_id, window_id
          FROM favn_control.backfill_windows
          WHERE workspace_id = $1
            AND ($2::text IS NULL OR backfill_id = $2)
            AND (status = 'ready' OR
                 (status IN ('claimed', 'running') AND
                  claim_expires_at <= clock_timestamp()))
          ORDER BY window_start, window_id
          LIMIT $3
          FOR UPDATE SKIP LOCKED
        )
        UPDATE favn_control.backfill_windows AS target
        SET status = 'claimed',
            claim_owner = $4,
            fencing_token = target.fencing_token + 1,
            claim_command_id = $5,
            claim_expires_at = clock_timestamp() + ($6 * interval '1 millisecond'),
            run_id = NULL,
            attempt_count = target.attempt_count + 1,
            version = target.version + 1,
            updated_at = clock_timestamp()
        FROM candidates
        WHERE target.workspace_id = candidates.workspace_id
          AND target.backfill_id = candidates.backfill_id
          AND target.window_id = candidates.window_id
        RETURNING target.workspace_id, target.backfill_id, target.window_id,
                  target.window_key, target.window_start, target.window_end,
                  target.status, target.claim_owner, target.fencing_token,
                  target.claim_expires_at, target.run_id, target.attempt_count,
                  target.last_error, target.payload, target.version
        """,
        [
          command.workspace_context.workspace_id,
          command.backfill_id,
          command.limit,
          command.owner_id,
          command.batch_id,
          command.lease_duration_ms
        ]
      )

    Enum.map(rows, &window_result/1)
  end

  defp live_window_replay?(windows, command) do
    now = database_now!()

    Enum.all?(windows, fn window ->
      window.claim_owner == command.owner_id and window.status in ["claimed", "running"] and
        match?(%DateTime{}, window.claim_expires_at) and
        DateTime.compare(window.claim_expires_at, now) == :gt
    end)
  end

  defp transition_window!(command) do
    workspace_id = command.workspace_context.workspace_id
    occurred_at = database_datetime(command.occurred_at)
    {window, claim_live?} = lock_transition_window!(command)

    cond do
      window.last_command_id == command.command_id ->
        window_result(window)

      stale_window_claim?(window, claim_live?, command) ->
        Repo.rollback(Error.new(:fenced, "backfill window fencing token is stale"))

      window.version != command.expected_version ->
        Repo.rollback(Error.new(:conflict, "backfill window version changed"))

      not valid_transition?(window.status, command.status) ->
        Repo.rollback(Error.new(:invalid, "invalid backfill window transition"))

      true ->
        persist_window_transition!(window, command, workspace_id, occurred_at)
    end
  end

  defp lock_transition_window!(command) do
    from(window in BackfillWindow,
      where:
        window.workspace_id == ^command.workspace_context.workspace_id and
          window.backfill_id == ^command.backfill_id and
          window.window_id == ^command.window_id and
          (window.last_command_id == ^command.command_id or
             (window.claim_owner == ^command.owner_id and
                window.fencing_token == ^command.fencing_token and
                fragment("? > clock_timestamp()", window.claim_expires_at))),
      select: {window, fragment("? > clock_timestamp()", window.claim_expires_at)},
      lock: "FOR UPDATE"
    )
    |> Repo.one()
    |> case do
      nil -> reject_unavailable_window!(command)
      result -> result
    end
  end

  defp stale_window_claim?(window, claim_live?, command) do
    window.claim_owner != command.owner_id or window.fencing_token != command.fencing_token or
      not claim_live?
  end

  defp persist_window_transition!(window, command, workspace_id, occurred_at) do
    status = Atom.to_string(command.status)

    updated =
      window
      |> Ecto.Changeset.change(%{
        last_command_id: command.command_id,
        status: status,
        run_id: command.run_id || window.run_id,
        last_error: command.error,
        version: window.version + 1,
        updated_at: occurred_at
      })
      |> Repo.update!()

    OutboxWriter.insert!(%{
      workspace_id: workspace_id,
      command_id: command.command_id,
      event_kind: "backfill.window." <> status,
      aggregate_kind: "backfill_window",
      aggregate_id: command.backfill_id <> ":" <> command.window_id,
      aggregate_version: updated.version,
      occurred_at: occurred_at,
      payload: %{
        "backfill_id" => command.backfill_id,
        "window_id" => command.window_id,
        "run_id" => updated.run_id,
        "previous_status" => if(window.status == "claimed", do: "ready", else: window.status),
        "status" => status
      }
    })

    window_result(updated)
  end

  defp reject_unavailable_window!(command) do
    case Repo.get_by(BackfillWindow,
           workspace_id: command.workspace_context.workspace_id,
           backfill_id: command.backfill_id,
           window_id: command.window_id
         ) do
      nil -> Repo.rollback(Error.new(:not_found, "backfill window not found"))
      _window -> Repo.rollback(Error.new(:fenced, "backfill window fencing token is stale"))
    end
  end

  defp lock_backfill!(workspace_id, backfill_id) do
    from(backfill in Backfill,
      where: backfill.workspace_id == ^workspace_id and backfill.backfill_id == ^backfill_id,
      lock: "FOR UPDATE"
    )
    |> Repo.one()
    |> case do
      nil -> Repo.rollback(Error.new(:not_found, "backfill not found"))
      backfill -> backfill
    end
  end

  defp backfill_result(backfill) do
    overview =
      Repo.get_by(BackfillOverview,
        workspace_id: backfill.workspace_id,
        backfill_id: backfill.backfill_id
      )

    %BackfillResult{
      workspace_id: backfill.workspace_id,
      backfill_id: backfill.backfill_id,
      root_run_id: backfill.root_run_id,
      deployment_id: backfill.deployment_id,
      manifest_version_id: backfill.manifest_version_id,
      target_kind: String.to_existing_atom(backfill.target_kind),
      target_id: backfill.target_id,
      range_start: backfill.range_start,
      range_end: backfill.range_end,
      status: String.to_existing_atom(backfill.status),
      expected_window_count: backfill.expected_window_count,
      expected_batch_count: backfill.expected_batch_count,
      appended_window_count: backfill.appended_window_count,
      appended_batch_count: backfill.appended_batch_count,
      version: backfill.version,
      metadata: backfill.metadata,
      progress: overview && overview_map(overview)
    }
  end

  defp encode_idempotent_backfill(%BackfillResult{} = result) do
    response =
      result
      |> Map.from_struct()
      |> Map.update!(:target_kind, &Atom.to_string/1)
      |> Map.update!(:status, &Atom.to_string/1)
      |> Map.update!(:range_start, &DateTime.to_iso8601/1)
      |> Map.update!(:range_end, &DateTime.to_iso8601/1)
      |> Map.new(fn {key, value} -> {Atom.to_string(key), value} end)

    {:ok,
     %{
       response: response,
       response_status: 202,
       resource_kind: "backfill",
       resource_id: result.backfill_id
     }}
  end

  defp decode_idempotent_backfill(%{response: response}) when is_map(response) do
    with {:ok, range_start} <- decode_datetime(Map.get(response, "range_start")),
         {:ok, range_end} <- decode_datetime(Map.get(response, "range_end")),
         {:ok, target_kind} <- known_atom(Map.get(response, "target_kind"), [:asset, :pipeline]),
         {:ok, status} <- known_atom(Map.get(response, "status"), @backfill_statuses),
         {:ok, result} <-
           build_backfill_replay(response, range_start, range_end, target_kind, status) do
      {:ok, result}
    else
      _other -> {:error, Error.new(:internal, "idempotent backfill replay record is invalid")}
    end
  end

  defp decode_idempotent_backfill(_encoded),
    do: {:error, Error.new(:internal, "idempotent backfill replay record is invalid")}

  defp build_backfill_replay(response, range_start, range_end, target_kind, status) do
    required_strings =
      ~w(workspace_id backfill_id root_run_id deployment_id manifest_version_id target_id)

    required_integers =
      ~w(expected_window_count expected_batch_count appended_window_count appended_batch_count version)

    if Enum.all?(
         required_strings,
         &(is_binary(Map.get(response, &1)) and Map.get(response, &1) != "")
       ) and
         Enum.all?(
           required_integers,
           &(is_integer(Map.get(response, &1)) and Map.get(response, &1) >= 0)
         ) and
         is_map(Map.get(response, "metadata")) do
      {:ok,
       %BackfillResult{
         workspace_id: response["workspace_id"],
         backfill_id: response["backfill_id"],
         root_run_id: response["root_run_id"],
         deployment_id: response["deployment_id"],
         manifest_version_id: response["manifest_version_id"],
         target_kind: target_kind,
         target_id: response["target_id"],
         range_start: range_start,
         range_end: range_end,
         status: status,
         expected_window_count: response["expected_window_count"],
         expected_batch_count: response["expected_batch_count"],
         appended_window_count: response["appended_window_count"],
         appended_batch_count: response["appended_batch_count"],
         version: response["version"],
         metadata: response["metadata"],
         progress: response["progress"]
       }}
    else
      {:error, :invalid_backfill_replay}
    end
  end

  defp decode_datetime(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> {:ok, datetime}
      _error -> {:error, :invalid_datetime}
    end
  end

  defp decode_datetime(_value), do: {:error, :invalid_datetime}

  defp database_datetime(%DateTime{} = datetime), do: DateTime.add(datetime, 0, :microsecond)

  defp database_now! do
    %{rows: [[now]]} = SQL.query!(Repo, "SELECT clock_timestamp()", [])
    now
  end

  defp known_atom(value, allowed) when is_binary(value) do
    Enum.find_value(allowed, {:error, :invalid_atom}, fn atom ->
      if Atom.to_string(atom) == value, do: {:ok, atom}
    end)
  end

  defp known_atom(_value, _allowed), do: {:error, :invalid_atom}

  defp window_result(%BackfillWindow{} = window) do
    %BackfillWindowResult{
      workspace_id: window.workspace_id,
      backfill_id: window.backfill_id,
      window_id: window.window_id,
      window_key: window.window_key,
      window_start: window.window_start,
      window_end: window.window_end,
      status: String.to_existing_atom(window.status),
      claim_owner: window.claim_owner,
      fencing_token: window.fencing_token,
      claim_expires_at: window.claim_expires_at,
      run_id: window.run_id,
      attempt_count: window.attempt_count,
      last_error: window.last_error,
      payload: window.payload,
      version: window.version
    }
  end

  defp window_result([
         workspace_id,
         backfill_id,
         window_id,
         window_key,
         window_start,
         window_end,
         status,
         claim_owner,
         fencing_token,
         claim_expires_at,
         run_id,
         attempt_count,
         last_error,
         payload,
         version
       ]) do
    %BackfillWindowResult{
      workspace_id: workspace_id,
      backfill_id: backfill_id,
      window_id: window_id,
      window_key: window_key,
      window_start: window_start,
      window_end: window_end,
      status: String.to_existing_atom(status),
      claim_owner: claim_owner,
      fencing_token: fencing_token,
      claim_expires_at: claim_expires_at,
      run_id: run_id,
      attempt_count: attempt_count,
      last_error: last_error,
      payload: payload,
      version: version
    }
  end

  defp overview_map(overview) do
    %{
      status: String.to_existing_atom(overview.status),
      total_count: overview.total_count,
      planned_count: overview.planned_count,
      ready_count: overview.ready_count,
      active_count: overview.active_count,
      succeeded_count: overview.succeeded_count,
      failed_count: overview.failed_count,
      cancelled_count: overview.cancelled_count,
      source_publication_id: overview.source_publication_id
    }
  end

  defp window_page(rows, limit, direction) do
    items = rows |> Enum.take(limit) |> Enum.map(&window_result/1)
    has_more? = length(rows) > limit
    last = List.last(items)

    next_cursor =
      cond do
        not has_more? or is_nil(last) -> nil
        direction == :forward -> %{window_key: last.window_key, window_id: last.window_id}
        true -> %{window_start: last.window_start, window_id: last.window_id}
      end

    %CursorPage{items: items, limit: limit, has_more?: has_more?, next_cursor: next_cursor}
  end

  defp filter_status(query, nil), do: query

  defp filter_status(query, status),
    do: where(query, [window], window.status == ^Atom.to_string(status))

  defp after_window(query, nil), do: query

  defp after_window(query, %{window_key: key, window_id: id}) do
    where(
      query,
      [window],
      window.window_key > ^key or (window.window_key == ^key and window.window_id > ^id)
    )
  end

  defp after_asset_window(query, nil), do: query

  defp after_asset_window(query, %{window_start: started_at, window_id: id}) do
    where(
      query,
      [window, _backfill],
      window.window_start < ^started_at or
        (window.window_start == ^started_at and window.window_id < ^id)
    )
  end

  defp maybe_backfill(query, nil), do: query

  defp maybe_backfill(query, backfill_id),
    do: where(query, [window], window.backfill_id == ^backfill_id)

  defp exact_batch_replay?(receipt, command) do
    receipt.backfill_id == command.backfill_id and receipt.batch_index == command.batch_index and
      receipt.command_id == command.command_id and receipt.batch_hash == command.batch_hash and
      receipt.window_count == length(command.windows)
  end

  defp valid_transition?("claimed", :running), do: true
  defp valid_transition?("claimed", status), do: status in [:failed, :cancelled]
  defp valid_transition?("running", status), do: status in [:succeeded, :failed, :cancelled]
  defp valid_transition?(_current, _next), do: false

  defp start_request_hash!(command) do
    {:ok, hash} =
      CanonicalJSON.hash(%{
        backfill_id: command.backfill_id,
        root_run_id: command.root_run_id,
        deployment_id: command.deployment_id,
        manifest_version_id: command.manifest_version_id,
        target_kind: command.target_kind,
        target_id: command.target_id,
        range_start: command.range_start,
        range_end: command.range_end,
        expected_window_count: command.expected_window_count,
        expected_batch_count: command.expected_batch_count,
        plan_hash: Base.url_encode64(command.plan_hash, padding: false),
        metadata: command.metadata
      })

    hash
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

  defp validate_start(command) do
    if workspace_context?(command.workspace_context) and
         Enum.all?(
           [
             command.command_id,
             command.backfill_id,
             command.root_run_id,
             command.deployment_id,
             command.manifest_version_id,
             command.target_id
           ],
           &valid_id?/1
         ) and command.target_kind in [:asset, :pipeline] and
         match?(%DateTime{}, command.range_start) and match?(%DateTime{}, command.range_end) and
         DateTime.compare(command.range_start, command.range_end) == :lt and
         valid_plan_size?(command.expected_window_count, command.expected_batch_count) and
         valid_hash?(command.plan_hash) and is_map(command.metadata) and
         Payload.validate(command.metadata, 64 * 1_024) == :ok and
         match?(%DateTime{}, command.occurred_at),
       do: :ok,
       else: {:error, ErrorMapper.map(:invalid)}
  end

  defp validate_append(command) do
    windows = command.windows

    if workspace_context?(command.workspace_context) and
         Enum.all?([command.command_id, command.backfill_id], &valid_id?/1) and
         is_integer(command.batch_index) and command.batch_index >= 0 and
         valid_hash?(command.batch_hash) and is_list(windows) and windows != [] and
         length(windows) <= @max_batch_windows and Enum.all?(windows, &valid_window?/1) and
         unique_windows?(windows) and match?(%DateTime{}, command.occurred_at),
       do: :ok,
       else: {:error, ErrorMapper.map(:invalid)}
  end

  defp validate_activate(command) do
    if workspace_context?(command.workspace_context) and
         Enum.all?([command.command_id, command.backfill_id], &valid_id?/1) and
         is_integer(command.expected_version) and command.expected_version > 0 and
         match?(%DateTime{}, command.occurred_at),
       do: :ok,
       else: {:error, ErrorMapper.map(:invalid)}
  end

  defp validate_claim_windows(command) do
    if workspace_context?(command.workspace_context) and
         Enum.all?([command.batch_id, command.owner_id], &valid_id?/1) and
         (is_nil(command.backfill_id) or valid_id?(command.backfill_id)) and
         valid_duration?(command.lease_duration_ms) and valid_limit?(command.limit),
       do: :ok,
       else: {:error, ErrorMapper.map(:invalid)}
  end

  defp validate_transition(command) do
    if valid_transition_identity?(command) and valid_transition_state?(command) and
         valid_transition_error?(command.error),
       do: :ok,
       else: {:error, ErrorMapper.map(:invalid)}
  end

  defp valid_transition_identity?(command) do
    workspace_context?(command.workspace_context) and
      Enum.all?(
        [command.command_id, command.backfill_id, command.window_id, command.owner_id],
        &valid_id?/1
      ) and (is_nil(command.run_id) or valid_id?(command.run_id))
  end

  defp valid_transition_state?(command) do
    is_integer(command.fencing_token) and command.fencing_token > 0 and
      is_integer(command.expected_version) and command.expected_version > 0 and
      command.status in [:running, :succeeded, :failed, :cancelled] and
      match?(%DateTime{}, command.occurred_at)
  end

  defp valid_transition_error?(nil), do: true

  defp valid_transition_error?(error),
    do: is_map(error) and Payload.validate(error, 64 * 1_024) == :ok

  defp validate_get(query) do
    if workspace_context?(query.workspace_context) and valid_id?(query.backfill_id),
      do: :ok,
      else: {:error, ErrorMapper.map(:invalid)}
  end

  defp validate_window_page(page) do
    cursor? =
      is_nil(page.after) or
        match?(
          %{window_key: key, window_id: id} when is_binary(key) and is_binary(id),
          page.after
        )

    if workspace_context?(page.workspace_context) and valid_id?(page.backfill_id) and
         (is_nil(page.status) or Atom.to_string(page.status) in @window_statuses) and
         cursor? and valid_limit?(page.limit),
       do: :ok,
       else: {:error, ErrorMapper.map(:invalid)}
  end

  defp validate_asset_window_page(page) do
    cursor? =
      is_nil(page.after) or
        match?(
          %{window_start: %DateTime{}, window_id: id} when is_binary(id),
          page.after
        )

    if workspace_context?(page.workspace_context) and valid_id?(page.manifest_version_id) and
         valid_id?(page.target_id) and cursor? and valid_limit?(page.limit),
       do: :ok,
       else: {:error, ErrorMapper.map(:invalid)}
  end

  defp valid_window?(%BackfillPlanWindow{} = window) do
    valid_id?(window.window_id) and valid_id?(window.window_key) and
      match?(%DateTime{}, window.window_start) and match?(%DateTime{}, window.window_end) and
      DateTime.compare(window.window_start, window.window_end) == :lt and is_map(window.payload) and
      Payload.validate(window.payload, 64 * 1_024) == :ok
  end

  defp valid_window?(_other), do: false

  defp unique_windows?(windows) do
    identities = Enum.map(windows, &{&1.window_id, &1.window_key})
    length(identities) == length(Enum.uniq(identities))
  end

  defp workspace_context?(context), do: WorkspaceContext.valid?(context)

  defp valid_hash?(hash), do: is_binary(hash) and byte_size(hash) == 32

  defp valid_plan_size?(0, 0), do: true

  defp valid_plan_size?(window_count, batch_count) do
    is_integer(window_count) and window_count in 1..@max_plan_windows and
      is_integer(batch_count) and batch_count in 1..@max_plan_batches and
      batch_count <= window_count and window_count <= batch_count * @max_batch_windows
  end

  defp valid_duration?(duration), do: is_integer(duration) and duration > 0
  defp valid_limit?(limit), do: is_integer(limit) and limit >= 1 and limit <= 500
  defp valid_id?(value), do: is_binary(value) and value != "" and byte_size(value) <= 255
end
