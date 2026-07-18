defmodule FavnStoragePostgres.Projections.Projector do
  @moduledoc """
  Applies commit-ordered outbox events to disposable compact read models.

  Every node may invoke this module. The database cursor lease and fencing token
  serialize one global projection stream; authoritative writes never wait for it.
  """

  import Ecto.Query

  alias Ecto.Adapters.SQL
  alias FavnOrchestrator.Persistence.Error
  alias Favn.TimePeriod
  alias Favn.Timezone
  alias Favn.Window.Key, as: WindowKey
  alias FavnStoragePostgres.CanonicalJSON
  alias FavnStoragePostgres.ErrorMapper
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.Schemas.Backfill
  alias FavnStoragePostgres.Schemas.Materialization
  alias FavnStoragePostgres.Schemas.OutboxEvent
  alias FavnStoragePostgres.Schemas.Run
  alias FavnStoragePostgres.Schemas.RunEvent
  alias FavnStoragePostgres.Schemas.WorkspaceDeployment

  @projector_name "control_plane_v1"
  @shard_id 0
  @default_limit 250
  @default_lease_ms 15_000

  @type outcome :: %{
          count: non_neg_integer(),
          last_publication_id: non_neg_integer(),
          fencing_token: pos_integer()
        }

  @doc "Projects one bounded publication batch under a database-owned lease."
  @spec project_batch(String.t(), keyword()) :: {:ok, outcome()} | {:error, Error.t()}
  def project_batch(owner_id, opts \\ [])

  def project_batch(owner_id, opts)
      when is_binary(owner_id) and owner_id != "" and is_list(opts) do
    limit = Keyword.get(opts, :limit, @default_limit)
    lease_ms = Keyword.get(opts, :lease_duration_ms, @default_lease_ms)

    if valid_bound?(limit, 1, 250) and valid_bound?(lease_ms, 1, 300_000) do
      project_transaction(owner_id, limit, lease_ms)
    else
      {:error, ErrorMapper.map(:invalid)}
    end
  end

  def project_batch(_owner_id, _opts), do: {:error, ErrorMapper.map(:invalid)}

  defp project_transaction(owner_id, limit, lease_ms) do
    case Repo.transaction(fn -> project_batch!(owner_id, limit, lease_ms) end) do
      {:ok, outcome} ->
        {:ok, outcome}

      {:error, {:projection_failed, event, error_kind}} ->
        record_failure(event, error_kind)

        {:error,
         Error.new(:internal, "projection event failed",
           details: %{publication_id: event.publication_id, event_kind: event.event_kind}
         )}

      {:error, %Error{} = error} ->
        {:error, error}

      {:error, reason} ->
        {:error, ErrorMapper.map(reason)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  defp project_batch!(owner_id, limit, lease_ms) do
    ensure_cursor!()
    {last_publication_id, fencing_token} = claim_cursor!(owner_id, lease_ms)

    events =
      from(event in OutboxEvent,
        where: not is_nil(event.publication_id) and event.publication_id > ^last_publication_id,
        order_by: [asc: event.publication_id],
        limit: ^limit
      )
      |> Repo.all()

    Enum.each(events, fn event ->
      try do
        project_event!(event)
        clear_failure!(event.publication_id)
      rescue
        error ->
          Repo.rollback({:projection_failed, event, error_kind(error)})
      end
    end)

    next_publication_id =
      case List.last(events) do
        nil -> last_publication_id
        event -> event.publication_id
      end

    %{num_rows: 1} =
      SQL.query!(
        Repo,
        """
        UPDATE favn_control.projection_cursors
        SET last_publication_id = $4,
            claim_expires_at = clock_timestamp() + ($5 * interval '1 millisecond'),
            version = version + 1,
            updated_at = clock_timestamp()
        WHERE projector_name = $1 AND shard_id = $2 AND owner_id = $3
          AND fencing_token = $6 AND claim_expires_at > clock_timestamp()
        """,
        [
          @projector_name,
          @shard_id,
          owner_id,
          next_publication_id,
          lease_ms,
          fencing_token
        ]
      )

    %{
      count: length(events),
      last_publication_id: next_publication_id,
      fencing_token: fencing_token
    }
  end

  defp ensure_cursor! do
    SQL.query!(
      Repo,
      """
      INSERT INTO favn_control.projection_cursors
        (projector_name, shard_id, last_publication_id, fencing_token, version, updated_at)
      VALUES ($1, $2, 0, 0, 1, clock_timestamp())
      ON CONFLICT (projector_name, shard_id) DO NOTHING
      """,
      [@projector_name, @shard_id]
    )
  end

  defp claim_cursor!(owner_id, lease_ms) do
    %{rows: rows} =
      SQL.query!(
        Repo,
        """
        UPDATE favn_control.projection_cursors
        SET owner_id = $3,
            fencing_token = CASE WHEN owner_id = $3 THEN fencing_token ELSE fencing_token + 1 END,
            claim_expires_at = clock_timestamp() + ($4 * interval '1 millisecond'),
            updated_at = clock_timestamp()
        WHERE projector_name = $1 AND shard_id = $2
          AND (owner_id IS NULL OR owner_id = $3 OR claim_expires_at <= clock_timestamp())
        RETURNING last_publication_id, fencing_token
        """,
        [@projector_name, @shard_id, owner_id, lease_ms]
      )

    case rows do
      [[last_publication_id, fencing_token]] -> {last_publication_id, fencing_token}
      [] -> Repo.rollback(Error.new(:conflict, "projection stream is owned", retryable?: true))
    end
  end

  @doc false
  @spec rebuild_event!(atom(), OutboxEvent.t()) :: :ok
  def rebuild_event!(:execution_groups, %OutboxEvent{event_kind: "run." <> _} = event),
    do: project_execution_group!(event)

  def rebuild_event!(:target_statuses, %OutboxEvent{event_kind: "run." <> _} = event),
    do: project_target_statuses!(event)

  def rebuild_event!(:backfills, %OutboxEvent{event_kind: "backfill.plan.activated"} = event),
    do: project_backfill_activation!(event)

  def rebuild_event!(:backfills, %OutboxEvent{event_kind: "backfill.window." <> _} = event),
    do: project_backfill_window!(event)

  def rebuild_event!(:freshness, %OutboxEvent{event_kind: "materialization.succeeded"} = event),
    do: project_materialization!(event)

  def rebuild_event!(_projection, %OutboxEvent{}), do: :ok

  defp project_event!(%OutboxEvent{event_kind: "run." <> _event_type} = event),
    do: project_run!(event)

  defp project_event!(%OutboxEvent{event_kind: "backfill.plan.activated"} = event),
    do: project_backfill_activation!(event)

  defp project_event!(%OutboxEvent{event_kind: "backfill.window." <> _status} = event),
    do: project_backfill_window!(event)

  defp project_event!(%OutboxEvent{event_kind: "materialization.succeeded"} = event),
    do: project_materialization!(event)

  defp project_event!(_event), do: :ok

  defp project_run!(event) do
    project_execution_group!(event)
    project_target_statuses!(event)
  end

  defp project_execution_group!(event) do
    run =
      Repo.get_by!(Run,
        workspace_id: event.workspace_id,
        run_id: event.aggregate_id
      )

    run_event = Repo.get_by!(RunEvent, outbox_event_id: event.outbox_event_id)

    previous_status = event.payload["previous_status"]

    new_status = event.payload["status"] || run_event.status || run.status
    old_bucket = status_bucket(previous_status)
    new_bucket = status_bucket(new_status)
    delta = counter_delta(old_bucket, new_bucket)
    run_delta = if run_event.sequence == 1, do: 1, else: 0

    SQL.query!(
      Repo,
      """
      INSERT INTO favn_control.execution_group_overviews
        (workspace_id, root_run_id, status, run_count, pending_count, running_count,
         succeeded_count, failed_count, latest_event_id, source_publication_id,
         inserted_at, updated_at)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $11)
      ON CONFLICT (workspace_id, root_run_id) DO UPDATE
      SET run_count = execution_group_overviews.run_count + EXCLUDED.run_count,
          pending_count = execution_group_overviews.pending_count + EXCLUDED.pending_count,
          running_count = execution_group_overviews.running_count + EXCLUDED.running_count,
          succeeded_count = execution_group_overviews.succeeded_count + EXCLUDED.succeeded_count,
          failed_count = execution_group_overviews.failed_count + EXCLUDED.failed_count,
          status = CASE
            WHEN execution_group_overviews.failed_count + EXCLUDED.failed_count > 0 THEN 'failed'
            WHEN execution_group_overviews.pending_count + EXCLUDED.pending_count > 0 THEN 'pending'
            WHEN execution_group_overviews.running_count + EXCLUDED.running_count > 0 THEN 'running'
            ELSE 'succeeded'
          END,
          latest_event_id = GREATEST(execution_group_overviews.latest_event_id,
                                    EXCLUDED.latest_event_id),
          source_publication_id = EXCLUDED.source_publication_id,
          updated_at = EXCLUDED.updated_at
      WHERE execution_group_overviews.source_publication_id < EXCLUDED.source_publication_id
      """,
      [
        event.workspace_id,
        run.root_execution_group_id,
        group_status(new_bucket),
        run_delta,
        delta.pending,
        delta.running,
        delta.succeeded,
        delta.failed,
        run_event.event_id,
        event.publication_id,
        event.published_at || event.inserted_at
      ]
    )
  end

  defp project_target_statuses!(event) do
    run =
      Repo.get_by!(Run,
        workspace_id: event.workspace_id,
        run_id: event.aggregate_id
      )

    run_event = Repo.get_by!(RunEvent, outbox_event_id: event.outbox_event_id)
    new_status = event.payload["status"] || run_event.status || run.status

    SQL.query!(
      Repo,
      """
      INSERT INTO favn_control.target_statuses
        (workspace_id, deployment_id, target_kind, target_id, status, run_id,
         event_id, source_publication_id, updated_at)
      SELECT target.workspace_id, target.deployment_id, target.target_kind, target.target_id,
             $3, target.run_id, $4, $5, $6
      FROM favn_control.run_targets target
      WHERE target.workspace_id = $1 AND target.run_id = $2
      ON CONFLICT (workspace_id, deployment_id, target_kind, target_id) DO UPDATE
      SET status = EXCLUDED.status, run_id = EXCLUDED.run_id,
          event_id = EXCLUDED.event_id,
          source_publication_id = EXCLUDED.source_publication_id,
          updated_at = EXCLUDED.updated_at
      WHERE target_statuses.source_publication_id < EXCLUDED.source_publication_id
      """,
      [
        event.workspace_id,
        run.run_id,
        new_status,
        run_event.event_id,
        event.publication_id,
        run_event.occurred_at
      ]
    )
  end

  defp project_backfill_activation!(event) do
    backfill =
      Repo.get_by!(Backfill,
        workspace_id: event.workspace_id,
        backfill_id: event.aggregate_id
      )

    SQL.query!(
      Repo,
      """
      INSERT INTO favn_control.backfill_overviews
        (workspace_id, backfill_id, status, total_count, planned_count, ready_count,
         active_count, succeeded_count, failed_count, cancelled_count,
         source_publication_id, updated_at)
      VALUES ($1, $2, 'ready', $3, 0, $3, 0, 0, 0, 0, $4, $5)
      ON CONFLICT (workspace_id, backfill_id) DO UPDATE
      SET status = EXCLUDED.status, total_count = EXCLUDED.total_count,
          planned_count = EXCLUDED.planned_count, ready_count = EXCLUDED.ready_count,
          active_count = EXCLUDED.active_count, succeeded_count = EXCLUDED.succeeded_count,
          failed_count = EXCLUDED.failed_count, cancelled_count = EXCLUDED.cancelled_count,
          source_publication_id = EXCLUDED.source_publication_id,
          updated_at = EXCLUDED.updated_at
      WHERE backfill_overviews.source_publication_id < EXCLUDED.source_publication_id
      """,
      [
        event.workspace_id,
        backfill.backfill_id,
        backfill.expected_window_count,
        event.publication_id,
        event.published_at || event.inserted_at
      ]
    )
  end

  defp project_backfill_window!(event) do
    previous_status = event.payload["previous_status"]
    status = event.payload["status"]
    old_bucket = window_bucket(previous_status)
    new_bucket = window_bucket(status)
    delta = window_delta(old_bucket, new_bucket)

    %{rows: rows} =
      SQL.query!(
        Repo,
        """
        INSERT INTO favn_control.backfill_overviews
          (workspace_id, backfill_id, status, total_count, planned_count, ready_count,
           active_count, succeeded_count, failed_count, cancelled_count,
           source_publication_id, updated_at)
        VALUES ($1, $2, $3, 0, $4, $5, $6, $7, $8, $9, $10, $11)
        ON CONFLICT (workspace_id, backfill_id) DO UPDATE
        SET planned_count = backfill_overviews.planned_count + EXCLUDED.planned_count,
            ready_count = backfill_overviews.ready_count + EXCLUDED.ready_count,
            active_count = backfill_overviews.active_count + EXCLUDED.active_count,
            succeeded_count = backfill_overviews.succeeded_count + EXCLUDED.succeeded_count,
            failed_count = backfill_overviews.failed_count + EXCLUDED.failed_count,
            cancelled_count = backfill_overviews.cancelled_count + EXCLUDED.cancelled_count,
            source_publication_id = EXCLUDED.source_publication_id,
            updated_at = EXCLUDED.updated_at,
            status = CASE
              WHEN backfill_overviews.succeeded_count + EXCLUDED.succeeded_count +
                   backfill_overviews.failed_count + EXCLUDED.failed_count +
                   backfill_overviews.cancelled_count + EXCLUDED.cancelled_count =
                   backfill_overviews.total_count
                THEN CASE WHEN backfill_overviews.failed_count + EXCLUDED.failed_count > 0
                          THEN 'failed' ELSE 'completed' END
              WHEN backfill_overviews.active_count + EXCLUDED.active_count > 0 THEN 'running'
              ELSE 'ready'
            END
        WHERE backfill_overviews.source_publication_id < EXCLUDED.source_publication_id
        RETURNING status, total_count, succeeded_count, failed_count, cancelled_count
        """,
        [
          event.workspace_id,
          event.payload["backfill_id"],
          status,
          delta.planned,
          delta.ready,
          delta.active,
          delta.succeeded,
          delta.failed,
          delta.cancelled,
          event.publication_id,
          event.published_at || event.inserted_at
        ]
      )

    case rows do
      [[overview_status, total, succeeded, failed, cancelled]]
      when total > 0 and succeeded + failed + cancelled == total ->
        from(backfill in Backfill,
          where:
            backfill.workspace_id == ^event.workspace_id and
              backfill.backfill_id == ^event.payload["backfill_id"] and
              backfill.status in ["ready", "running"]
        )
        |> Repo.update_all(
          set: [
            status: overview_status,
            updated_at: event.published_at || event.inserted_at
          ],
          inc: [version: 1]
        )

      _rows ->
        :ok
    end
  end

  defp project_materialization!(event) do
    materialization =
      Repo.get_by!(Materialization,
        workspace_id: event.workspace_id,
        materialization_id: event.aggregate_id
      )

    if materialization.target_kind == "asset" do
      deployment =
        Repo.get_by!(WorkspaceDeployment,
          workspace_id: event.workspace_id,
          deployment_id: materialization.deployment_id
        )

      {window_start, window_end} = materialization_window(materialization)
      node_key_hash = decode_node_key_fingerprint(materialization.payload["node_key_fingerprint"])
      input_fingerprint = optional_hash(materialization.payload["input_fingerprint"])
      projected_at = event.published_at || materialization.inserted_at

      project_asset_window!(
        event,
        deployment,
        materialization,
        window_start,
        window_end,
        projected_at
      )

      SQL.query!(
        Repo,
        """
        INSERT INTO favn_control.asset_freshness_states
          (workspace_id, deployment_id, target_id, freshness_key,
           latest_attempt_materialization_id, latest_success_materialization_id,
           latest_success_node_key_hash, input_fingerprint, status, payload,
           source_publication_id, updated_at)
        VALUES ($1, $2, $3, $4, $5, $5, $6, $7, 'fresh', $8, $9, $10)
        ON CONFLICT (workspace_id, deployment_id, target_id, freshness_key) DO UPDATE
        SET latest_attempt_materialization_id = EXCLUDED.latest_attempt_materialization_id,
            latest_success_materialization_id = EXCLUDED.latest_success_materialization_id,
            latest_success_node_key_hash = EXCLUDED.latest_success_node_key_hash,
            input_fingerprint = EXCLUDED.input_fingerprint, status = EXCLUDED.status,
            payload = EXCLUDED.payload,
            source_publication_id = EXCLUDED.source_publication_id,
            updated_at = EXCLUDED.updated_at
        WHERE asset_freshness_states.source_publication_id < EXCLUDED.source_publication_id
        """,
        [
          event.workspace_id,
          materialization.deployment_id,
          materialization.target_id,
          materialization.partition_key,
          materialization.materialization_id,
          node_key_hash,
          input_fingerprint,
          materialization.payload,
          event.publication_id,
          projected_at
        ]
      )
    end
  end

  defp project_asset_window!(
         event,
         deployment,
         %{partition_key: "window:" <> encoded_key} = materialization,
         window_start,
         window_end,
         projected_at
       ) do
    with {:ok, _key} <- WindowKey.decode(encoded_key) do
      SQL.query!(
        Repo,
        """
        INSERT INTO favn_control.asset_window_states
          (workspace_id, manifest_version_id, target_id, window_key, window_start,
           window_end, status, run_id, materialization_id, payload,
           source_publication_id, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, 'succeeded', $7, $8, $9, $10, $11)
        ON CONFLICT (workspace_id, manifest_version_id, target_id, window_key) DO UPDATE
        SET window_start = EXCLUDED.window_start, window_end = EXCLUDED.window_end,
            status = EXCLUDED.status, run_id = EXCLUDED.run_id,
            materialization_id = EXCLUDED.materialization_id, payload = EXCLUDED.payload,
            source_publication_id = EXCLUDED.source_publication_id,
            updated_at = EXCLUDED.updated_at
        WHERE asset_window_states.source_publication_id < EXCLUDED.source_publication_id
        """,
        [
          event.workspace_id,
          deployment.manifest_version_id,
          materialization.target_id,
          materialization.partition_key,
          window_start,
          window_end,
          materialization.run_id,
          materialization.materialization_id,
          materialization.payload,
          event.publication_id,
          projected_at
        ]
      )

      :ok
    end
  end

  defp project_asset_window!(_event, _deployment, _materialization, _start, _end, _at),
    do: :ok

  defp materialization_window(materialization) do
    with start when is_binary(start) <- materialization.payload["window_start"],
         {:ok, window_start, _offset} <- DateTime.from_iso8601(start),
         finish when is_binary(finish) <- materialization.payload["window_end"],
         {:ok, window_end, _offset} <- DateTime.from_iso8601(finish),
         :lt <- DateTime.compare(window_start, window_end) do
      {window_start, window_end}
    else
      _other -> window_from_partition_key(materialization)
    end
  end

  defp window_from_partition_key(%{partition_key: "window:" <> encoded_key} = materialization) do
    with {:ok, key} <- WindowKey.decode(encoded_key),
         start_at <- DateTime.from_unix!(key.start_at_us, :microsecond),
         {:ok, local_start} <- DateTime.shift_zone(start_at, key.timezone, Timezone.database!()),
         {:ok, local_end} <- TimePeriod.shift(local_start, key.kind, 1),
         {:ok, end_at} <- DateTime.shift_zone(local_end, "Etc/UTC", Timezone.database!()) do
      {start_at, end_at}
    else
      _other -> fallback_materialization_window(materialization)
    end
  end

  defp window_from_partition_key(materialization),
    do: fallback_materialization_window(materialization)

  defp fallback_materialization_window(materialization),
    do: {materialization.inserted_at, DateTime.add(materialization.inserted_at, 1, :microsecond)}

  defp optional_hash(nil), do: nil
  defp optional_hash(value), do: hash!(value)

  defp decode_node_key_fingerprint(nil), do: nil

  defp decode_node_key_fingerprint(fingerprint) when is_binary(fingerprint) do
    case Base.decode16(fingerprint, case: :mixed) do
      {:ok, decoded} when byte_size(decoded) == 32 -> decoded
      _invalid -> nil
    end
  end

  defp decode_node_key_fingerprint(_invalid), do: nil

  defp hash!(value) do
    {:ok, hash} = CanonicalJSON.hash(value)
    hash
  end

  defp status_bucket(nil), do: nil
  defp status_bucket(status) when status in ["pending"], do: :pending
  defp status_bucket(status) when status in ["running"], do: :running
  defp status_bucket(status) when status in ["ok", "partial"], do: :succeeded
  defp status_bucket(_status), do: :failed

  defp counter_delta(old, new) do
    zero = %{pending: 0, running: 0, succeeded: 0, failed: 0}
    zero |> decrement(old) |> increment(new)
  end

  defp decrement(counts, nil), do: counts
  defp decrement(counts, bucket), do: Map.update!(counts, bucket, &(&1 - 1))
  defp increment(counts, bucket), do: Map.update!(counts, bucket, &(&1 + 1))

  defp group_status(:pending), do: "pending"
  defp group_status(:running), do: "running"
  defp group_status(:succeeded), do: "succeeded"
  defp group_status(:failed), do: "failed"

  defp window_bucket("planned"), do: :planned
  defp window_bucket("ready"), do: :ready
  # Claims are not publication events. The first transition out of `claimed`
  # therefore consumes the ready count established by plan activation.
  defp window_bucket("claimed"), do: :ready
  defp window_bucket("running"), do: :active
  defp window_bucket("succeeded"), do: :succeeded
  defp window_bucket("failed"), do: :failed
  defp window_bucket("cancelled"), do: :cancelled

  defp window_delta(old, new) do
    zero = %{planned: 0, ready: 0, active: 0, succeeded: 0, failed: 0, cancelled: 0}
    zero |> decrement(old) |> increment(new)
  end

  defp record_failure(event, error_kind) do
    now = DateTime.utc_now()

    SQL.query(
      Repo,
      """
      INSERT INTO favn_control.projection_failures
        (projector_name, shard_id, publication_id, workspace_id, event_kind,
         error_kind, error_detail, attempt_count, inserted_at, updated_at)
      VALUES ($1, $2, $3, $4, $5, $6, '{}'::jsonb, 1, $7, $7)
      ON CONFLICT (projector_name, shard_id, publication_id) DO UPDATE
      SET error_kind = EXCLUDED.error_kind,
          attempt_count = projection_failures.attempt_count + 1,
          updated_at = EXCLUDED.updated_at
      """,
      [
        @projector_name,
        @shard_id,
        event.publication_id,
        event.workspace_id,
        event.event_kind,
        Atom.to_string(error_kind),
        now
      ]
    )

    :ok
  end

  defp clear_failure!(publication_id) do
    SQL.query!(
      Repo,
      """
      DELETE FROM favn_control.projection_failures
      WHERE projector_name = $1 AND shard_id = $2 AND publication_id = $3
      """,
      [@projector_name, @shard_id, publication_id]
    )

    :ok
  end

  defp error_kind(%Postgrex.Error{postgres: %{code: code}}), do: code
  defp error_kind(%Ecto.NoResultsError{}), do: :missing_authority
  defp error_kind(_error), do: :unexpected

  defp valid_bound?(value, min, max),
    do: is_integer(value) and value >= min and value <= max
end

defmodule FavnStoragePostgres.Projections.Worker do
  @moduledoc false

  use GenServer

  alias FavnStoragePostgres.Projections.Projector

  @doc false
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @impl true
  def init(opts) do
    state = %{
      owner_id: Keyword.get(opts, :owner_id, default_owner_id()),
      batch_size: Keyword.get(opts, :batch_size, 250),
      interval_ms: Keyword.get(opts, :interval_ms, 30_000),
      timer_ref: nil
    }

    {:ok, schedule(state, 0)}
  end

  @impl true
  def handle_cast(:wake, %{timer_ref: :pending} = state), do: {:noreply, state}

  def handle_cast(:wake, %{timer_ref: timer_ref} = state) do
    timer_already_delivered? =
      is_reference(timer_ref) and Process.cancel_timer(timer_ref) == false

    unless timer_already_delivered?, do: send(self(), :project)
    {:noreply, %{state | timer_ref: :pending}}
  end

  @impl true
  def handle_info(:project, state) do
    state = %{state | timer_ref: nil}

    delay =
      case Projector.project_batch(state.owner_id, limit: state.batch_size) do
        {:ok, %{count: count}} ->
          if(count == state.batch_size, do: 0, else: state.interval_ms)

        {:error, _error} ->
          state.interval_ms
      end

    {:noreply, schedule(state, delay)}
  end

  defp schedule(state, delay),
    do: %{state | timer_ref: Process.send_after(self(), :project, delay)}

  defp default_owner_id do
    node = node() |> Atom.to_string() |> String.slice(0, 200)
    node <> ":" <> Integer.to_string(System.unique_integer([:positive]))
  end
end
