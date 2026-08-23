defmodule FavnStoragePostgres.Projections.Projector do
  @moduledoc """
  Applies commit-ordered outbox events to disposable compact read models.

  Every node may invoke this module. The database cursor lease and fencing token
  serialize one global projection stream; authoritative writes never wait for it.
  """

  import Ecto.Query

  alias Ecto.Adapters.SQL
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.AssetStepIdentity
  alias FavnOrchestrator.RunReadModel.AssetAttemptProjection
  alias FavnOrchestrator.Storage.JsonSafe
  alias FavnOrchestrator.Storage.RunEventCodec
  alias FavnOrchestrator.Storage.RunSnapshotCodec
  alias FavnOrchestrator.WindowSummary
  alias Favn.Freshness.Key, as: FreshnessKey
  alias Favn.TimePeriod
  alias Favn.Timezone
  alias FavnStoragePostgres.CanonicalJSON
  alias FavnStoragePostgres.ErrorMapper
  alias FavnStoragePostgres.Projections.Readiness
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.Schemas.Backfill
  alias FavnStoragePostgres.Schemas.AssetAttemptOverview
  alias FavnStoragePostgres.Schemas.Materialization
  alias FavnStoragePostgres.Schemas.ManifestVersion
  alias FavnStoragePostgres.Schemas.OutboxEvent
  alias FavnStoragePostgres.Schemas.Run
  alias FavnStoragePostgres.Schemas.RunEvent
  alias FavnStoragePostgres.Schemas.RunPlan
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
        _ = record_failure_if_unprojected(event, error_kind)

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

    run_contexts = load_run_projection_contexts!(events)

    membership_events =
      Enum.reduce(events, MapSet.new(), fn event, acc ->
        try do
          if project_event!(event, run_contexts) == :membership,
            do: MapSet.put(acc, event.outbox_event_id),
            else: acc
        rescue
          error ->
            Repo.rollback({:projection_failed, event, error_kind(error)})
        end
      end)

    clear_failures!(events)

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
          AND fencing_token = $6
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

    emit_projection_notifications!(events, run_contexts, membership_events, next_publication_id)

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

  defp emit_projection_notifications!(events, contexts, membership_events, publication_id) do
    events
    |> Enum.flat_map(&projection_notification_scopes(&1, contexts, membership_events))
    |> strongest_notification_scopes()
    |> Enum.each(fn scope ->
      payload =
        Jason.encode!(%{
          workspace_id: scope.workspace_id,
          run_id: scope.run_id,
          root_run_id: scope.root_run_id,
          publication_id: publication_id,
          change: scope.change
        })

      if byte_size(payload) > 1_024, do: raise("projection notification exceeds 1 KiB")
      SQL.query!(Repo, "SELECT pg_notify('favn_execution_group_projected', $1)", [payload])
    end)
  end

  @doc false
  @spec strongest_notification_scopes([map()]) :: [map()]
  def strongest_notification_scopes(scopes) when is_list(scopes) do
    scopes
    |> Enum.group_by(fn scope ->
      {scope.workspace_id, scope.run_id, scope.root_run_id}
    end)
    |> Enum.map(fn {_identity, scopes} -> Enum.max_by(scopes, &change_priority(&1.change)) end)
  end

  defp change_priority("membership"), do: 3
  defp change_priority(change) when change in ["steps", "windows"], do: 2
  defp change_priority("header"), do: 1

  defp projection_notification_scopes(
         %OutboxEvent{event_kind: "run." <> kind} = event,
         contexts,
         membership_events
       ) do
    {run, _run_event, previous, _new} = Map.fetch!(contexts, event.outbox_event_id)

    change =
      cond do
        is_nil(previous) or MapSet.member?(membership_events, event.outbox_event_id) ->
          "membership"

        String.starts_with?(kind, "step_") ->
          "steps"

        true ->
          "header"
      end

    [
      %{
        workspace_id: event.workspace_id,
        run_id: run.run_id,
        root_run_id: run.root_execution_group_id,
        change: change
      }
    ]
  end

  defp projection_notification_scopes(
         %OutboxEvent{event_kind: "backfill." <> _kind, aggregate_id: backfill_id} = event,
         _contexts,
         _membership_events
       ) do
    case Repo.get_by(Backfill, workspace_id: event.workspace_id, backfill_id: backfill_id) do
      %Backfill{root_run_id: root_run_id} ->
        [
          %{
            workspace_id: event.workspace_id,
            run_id: root_run_id,
            root_run_id: root_run_id,
            change: "windows"
          }
        ]

      nil ->
        []
    end
  end

  defp projection_notification_scopes(_event, _contexts, _membership_events), do: []

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
    do: event |> run_projection_context!() |> project_execution_group!(event)

  def rebuild_event!(:target_statuses, %OutboxEvent{event_kind: "run." <> _} = event),
    do: event |> run_projection_context!() |> project_target_statuses!(event)

  def rebuild_event!(:asset_attempts, %OutboxEvent{event_kind: "run." <> _} = event) do
    context = run_projection_context!(event)
    maybe_reset_repaired_run!(context, event)
    seed_planned_asset_attempts!(context, event, repair?: true)
    _change = project_asset_attempt!(context, event)
    :ok
  end

  def rebuild_event!(:backfills, %OutboxEvent{event_kind: "backfill.plan.activated"} = event),
    do: project_backfill_activation!(event)

  def rebuild_event!(:backfills, %OutboxEvent{event_kind: "backfill.window." <> _} = event),
    do: project_backfill_window!(event)

  def rebuild_event!(:freshness, %OutboxEvent{event_kind: "materialization.succeeded"} = event),
    do: project_materialization!(event)

  def rebuild_event!(_projection, %OutboxEvent{}), do: :ok

  defp maybe_reset_repaired_run!({run, %RunEvent{sequence: 1}, _previous, _new}, event) do
    {_deleted, _rows} =
      from(attempt in AssetAttemptOverview,
        where: attempt.workspace_id == ^run.workspace_id and attempt.run_id == ^run.run_id
      )
      |> Repo.delete_all()

    payload =
      Jason.encode!(%{
        workspace_id: run.workspace_id,
        run_id: run.run_id,
        root_run_id: run.root_execution_group_id,
        publication_id: event.publication_id,
        repair_generation: repair_generation!(run.workspace_id),
        change: "membership"
      })

    SQL.query!(Repo, "SELECT pg_notify('favn_execution_group_projected', $1)", [payload])

    :ok
  end

  defp maybe_reset_repaired_run!(_context, _event), do: :ok

  defp repair_generation!(workspace_id) do
    %{rows: [[generation]]} =
      SQL.query!(
        Repo,
        "SELECT version FROM favn_control.maintenance_jobs WHERE job_id = $1",
        [Readiness.ready_job_id(workspace_id)]
      )

    generation
  end

  defp project_event!(%OutboxEvent{event_kind: "run." <> _event_type} = event, contexts),
    do: project_run!(event, Map.fetch!(contexts, event.outbox_event_id))

  defp project_event!(%OutboxEvent{event_kind: "backfill.plan.activated"} = event, _contexts) do
    project_backfill_activation!(event)
    :ok
  end

  defp project_event!(%OutboxEvent{event_kind: "backfill.window." <> _status} = event, _contexts) do
    project_backfill_window!(event)
    :ok
  end

  defp project_event!(%OutboxEvent{event_kind: "materialization.succeeded"} = event, _contexts) do
    project_materialization!(event)
    :ok
  end

  defp project_event!(_event, _contexts), do: :ok

  defp project_run!(event, context) do
    project_execution_group!(context, event)
    project_target_statuses!(context, event)
    seed_planned_asset_attempts!(context, event)
    project_asset_attempt!(context, event)
  end

  defp seed_planned_asset_attempts!(context, event, opts \\ [])

  defp seed_planned_asset_attempts!(
         {run, %RunEvent{sequence: 1} = run_event, _, _},
         event,
         opts
       ) do
    with %RunPlan{} = persisted_plan <-
           Repo.get_by(RunPlan, workspace_id: run.workspace_id, run_id: run.run_id),
         %ManifestVersion{} = manifest <- Repo.get(ManifestVersion, run.manifest_version_id),
         {:ok, plan} <-
           RunSnapshotCodec.decode_plan(persisted_plan.plan, manifest.atom_strings || []) do
      rows =
        Enum.map(plan.nodes, fn {node_key, node} ->
          planned_asset_attempt_row(run, run_event, event, node_key, node)
        end)

      on_conflict =
        if Keyword.get(opts, :repair?, false),
          do:
            {:replace,
             [
               :asset_ref,
               :target_id,
               :window_identity,
               :window,
               :window_kind,
               :window_start_at,
               :window_end_at,
               :window_timezone,
               :stage,
               :execution_pool
             ]},
          else: :nothing

      Repo.insert_all(AssetAttemptOverview, rows,
        on_conflict: on_conflict,
        conflict_target: [:workspace_id, :root_run_id, :run_id, :asset_step_id]
      )
    else
      nil -> :ok
      {:error, reason} -> raise "asset attempt plan seed failed: #{inspect(reason)}"
    end
  end

  defp seed_planned_asset_attempts!(_context, _event, _opts), do: :ok

  defp planned_asset_attempt_row(run, run_event, event, node_key, node) do
    asset_ref = plan_ref_text(Map.get(node, :ref))
    asset_step_id = AssetStepIdentity.asset_step_id(run.run_id, node_key, Map.get(node, :ref))
    window = planned_window(Map.get(node, :window))

    %{
      workspace_id: run.workspace_id,
      root_run_id: run.root_execution_group_id,
      run_id: run.run_id,
      asset_step_id: asset_step_id,
      asset_ref: asset_ref,
      target_id: Map.get(node, :target_id),
      window_identity: planned_window_identity(window),
      window: json(window),
      window_kind: window && atom_string(window.kind),
      window_start_at: window && utc_datetime(window.start_at),
      window_end_at: window && utc_datetime(window.end_at),
      window_timezone: window && window.timezone,
      status: "planned",
      stage: Map.get(node, :stage),
      execution_pool: atom_string(Map.get(node, :execution_pool)),
      source_publication_id: event.publication_id,
      updated_at: run_event.occurred_at
    }
  end

  defp project_asset_attempt!({run, run_event, _previous_status, _new_status}, event) do
    with {:ok, decoded} <- RunEventCodec.decode(Jason.encode!(run_event.event)),
         {:ok, attempt} <- AssetAttemptProjection.from_event(decoded) do
      target_id = "asset:" <> attempt.asset_ref

      SQL.query!(
        Repo,
        """
        INSERT INTO favn_control.asset_attempt_overviews
          (workspace_id, root_run_id, run_id, asset_step_id, asset_ref, target_id,
           window_identity, "window", window_kind, window_start_at, window_end_at,
           window_timezone, status, stage, attempt_number, execution_pool,
           queue_reason, started_at, finished_at, duration_ms, error, failure_summary,
           output_metadata, source_publication_id, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, $10, $11, $12,
                $13, $14, $15, $16, $17, $18, $19, $20, $21::jsonb, $22,
                $23::jsonb, $24, $25)
        ON CONFLICT (workspace_id, root_run_id, run_id, asset_step_id) DO UPDATE
        SET target_id = COALESCE(asset_attempt_overviews.target_id, EXCLUDED.target_id),
            window_identity = CASE
              WHEN EXCLUDED."window" IS NULL THEN asset_attempt_overviews.window_identity
              ELSE EXCLUDED.window_identity
            END,
            "window" = COALESCE(EXCLUDED."window", asset_attempt_overviews."window"),
            window_kind = COALESCE(EXCLUDED.window_kind, asset_attempt_overviews.window_kind),
            window_start_at = COALESCE(EXCLUDED.window_start_at,
                                       asset_attempt_overviews.window_start_at),
            window_end_at = COALESCE(EXCLUDED.window_end_at,
                                     asset_attempt_overviews.window_end_at),
            window_timezone = COALESCE(EXCLUDED.window_timezone,
                                       asset_attempt_overviews.window_timezone),
            status = EXCLUDED.status,
            stage = COALESCE(EXCLUDED.stage, asset_attempt_overviews.stage),
            attempt_number = COALESCE(EXCLUDED.attempt_number,
                                      asset_attempt_overviews.attempt_number),
            execution_pool = COALESCE(EXCLUDED.execution_pool,
                                      asset_attempt_overviews.execution_pool),
            queue_reason = COALESCE(EXCLUDED.queue_reason,
                                    asset_attempt_overviews.queue_reason),
            started_at = CASE
              WHEN EXCLUDED.status = 'running' THEN EXCLUDED.started_at
              WHEN EXCLUDED.status IN ('queued', 'retrying') THEN NULL
              ELSE COALESCE(EXCLUDED.started_at, asset_attempt_overviews.started_at)
            END,
            finished_at = CASE
              WHEN EXCLUDED.status IN ('queued', 'running', 'retrying') THEN NULL
              ELSE COALESCE(EXCLUDED.finished_at, asset_attempt_overviews.finished_at)
            END,
            duration_ms = CASE
              WHEN EXCLUDED.status IN ('queued', 'running', 'retrying') THEN NULL
              ELSE COALESCE(EXCLUDED.duration_ms, asset_attempt_overviews.duration_ms)
            END,
            error = CASE
              WHEN EXCLUDED.status IN ('queued', 'running', 'retrying') THEN NULL
              ELSE COALESCE(EXCLUDED.error, asset_attempt_overviews.error)
            END,
            failure_summary = CASE
              WHEN EXCLUDED.status IN ('queued', 'running', 'retrying') THEN NULL
              ELSE COALESCE(EXCLUDED.failure_summary,
                            asset_attempt_overviews.failure_summary)
            END,
            output_metadata = CASE
              WHEN EXCLUDED.status IN ('queued', 'running', 'retrying') THEN NULL
              ELSE COALESCE(EXCLUDED.output_metadata,
                            asset_attempt_overviews.output_metadata)
            END,
            source_publication_id = EXCLUDED.source_publication_id,
            updated_at = EXCLUDED.updated_at
        WHERE asset_attempt_overviews.source_publication_id <= EXCLUDED.source_publication_id
          AND asset_attempt_overviews.asset_ref = EXCLUDED.asset_ref
        RETURNING asset_ref, (xmax = 0) AS inserted
        """,
        [
          event.workspace_id,
          run.root_execution_group_id,
          run.run_id,
          attempt.asset_step_id,
          attempt.asset_ref,
          target_id,
          attempt.window_identity,
          json(attempt.window),
          attempt.window && atom_string(attempt.window.kind),
          attempt.window && utc_datetime(attempt.window.start_at),
          attempt.window && utc_datetime(attempt.window.end_at),
          attempt.window && attempt.window.timezone,
          Atom.to_string(attempt.status),
          attempt.stage,
          attempt.attempt_number,
          attempt.execution_pool,
          attempt.queue_reason,
          attempt.started_at,
          attempt.finished_at,
          attempt.duration_ms,
          json(attempt.error),
          failure_summary(attempt.error),
          json(attempt.output_metadata),
          event.publication_id,
          run_event.occurred_at
        ]
      )
      |> ensure_asset_ref_unchanged!(event, attempt)
    else
      :ignore -> :ok
      {:error, reason} -> raise "asset attempt projection failed: #{inspect(reason)}"
    end
  end

  defp json(nil), do: nil
  defp json(value), do: JsonSafe.data(value)

  defp planned_window(nil), do: nil
  defp planned_window(window), do: WindowSummary.public(window)

  defp planned_window_identity(nil), do: "none"
  defp planned_window_identity(%{key: key}) when is_binary(key), do: key

  defp planned_window_identity(window) do
    window
    |> :erlang.term_to_binary([:deterministic])
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.url_encode64(padding: false)
  end

  defp plan_ref_text({module, name}) when is_atom(module) and is_atom(name),
    do: Atom.to_string(module) <> ":" <> Atom.to_string(name)

  defp plan_ref_text(value) when is_binary(value), do: value
  defp plan_ref_text(value), do: inspect(value)

  defp atom_string(nil), do: nil
  defp atom_string(value) when is_atom(value), do: Atom.to_string(value)
  defp atom_string(value) when is_binary(value), do: value

  defp utc_datetime(%DateTime{} = value) do
    value
    |> DateTime.shift_zone!("Etc/UTC")
    |> DateTime.add(0, :microsecond)
  end

  defp failure_summary(nil), do: nil

  defp failure_summary(%{message: message}) when is_binary(message),
    do: truncate_utf8(message, 1_024)

  defp failure_summary(%{"message" => message}) when is_binary(message),
    do: truncate_utf8(message, 1_024)

  defp failure_summary(error) when is_binary(error), do: truncate_utf8(error, 1_024)
  defp failure_summary(_error), do: "Failure details are available in server logs."

  defp truncate_utf8(value, max_bytes) when byte_size(value) <= max_bytes, do: value

  defp truncate_utf8(value, max_bytes),
    do: value |> String.next_grapheme() |> trim_graphemes(max_bytes, "")

  defp trim_graphemes(nil, _max_bytes, acc), do: acc

  defp trim_graphemes({grapheme, rest}, max_bytes, acc) do
    if byte_size(acc) + byte_size(grapheme) <= max_bytes,
      do: trim_graphemes(String.next_grapheme(rest), max_bytes, acc <> grapheme),
      else: acc
  end

  defp ensure_asset_ref_unchanged!(%{rows: [[_asset_ref, true]]}, _event, _attempt),
    do: :membership

  defp ensure_asset_ref_unchanged!(%{rows: [[_asset_ref, false]]}, _event, _attempt), do: :ok

  defp ensure_asset_ref_unchanged!(%{rows: []}, event, attempt) do
    existing =
      Repo.get_by(AssetAttemptOverview,
        workspace_id: event.workspace_id,
        run_id: event.aggregate_id,
        asset_step_id: attempt.asset_step_id
      )

    if existing && existing.asset_ref != attempt.asset_ref do
      raise "asset attempt identity changed for #{attempt.asset_step_id}"
    end

    :ok
  end

  defp load_run_projection_contexts!(events) do
    run_events = Enum.filter(events, &match?(%OutboxEvent{event_kind: "run." <> _}, &1))

    if run_events == [] do
      %{}
    else
      run_keys =
        run_events
        |> Enum.map(&%{workspace_id: &1.workspace_id, run_id: &1.aggregate_id})
        |> Enum.uniq()

      key_types = %{workspace_id: :string, run_id: :string}

      runs =
        from(run in Run,
          join: key in values(run_keys, key_types),
          on: run.workspace_id == key.workspace_id and run.run_id == key.run_id
        )
        |> Repo.all()
        |> Map.new(&{{&1.workspace_id, &1.run_id}, &1})

      outbox_event_ids = Enum.map(run_events, & &1.outbox_event_id)

      persisted_events =
        from(event in RunEvent, where: event.outbox_event_id in ^outbox_event_ids)
        |> Repo.all()
        |> Map.new(&{&1.outbox_event_id, &1})

      Map.new(run_events, fn event ->
        run = Map.fetch!(runs, {event.workspace_id, event.aggregate_id})

        run_event = Map.fetch!(persisted_events, event.outbox_event_id)
        previous_status = event.payload["previous_status"]
        new_status = event.payload["status"] || run_event.status || run.status

        {event.outbox_event_id, {run, run_event, previous_status, new_status}}
      end)
    end
  end

  defp run_projection_context!(event) do
    run =
      Repo.get_by!(Run,
        workspace_id: event.workspace_id,
        run_id: event.aggregate_id
      )

    run_event = Repo.get_by!(RunEvent, outbox_event_id: event.outbox_event_id)
    previous_status = event.payload["previous_status"]
    new_status = event.payload["status"] || run_event.status || run.status

    {run, run_event, previous_status, new_status}
  end

  defp project_execution_group!({run, run_event, previous_status, new_status}, event) do
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
         trigger_type, started_at, finished_at, inserted_at, updated_at)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $12, $13, $14, $11, $11)
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
          trigger_type = COALESCE(EXCLUDED.trigger_type,
                                  execution_group_overviews.trigger_type),
          started_at = COALESCE(EXCLUDED.started_at, execution_group_overviews.started_at),
          finished_at = CASE
            WHEN execution_group_overviews.pending_count + EXCLUDED.pending_count > 0
              OR execution_group_overviews.running_count + EXCLUDED.running_count > 0
            THEN NULL
            ELSE EXCLUDED.updated_at
          END,
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
        event.published_at || event.inserted_at,
        root_only(run, run.trigger_type),
        root_only(run, run.inserted_at),
        settled_at(delta, event)
      ]
    )
  end

  # The group is named after its root run, and only the root run's own events know
  # when the group started and what triggered it. A child run's event carries
  # neither, so it passes NULL and the upsert coalesces rather than clearing what a
  # root event already established.
  defp root_only(%{run_id: run_id, root_execution_group_id: run_id}, value), do: value
  defp root_only(_run, _value), do: nil

  # When the group stopped, which is not when its root run stopped. A backfill's
  # root run is terminal the instant it is created — it exists to group the window
  # runs that do the work — so taking its `terminal_at` reported every backfill as
  # having finished before it started, and a zero duration with it.
  #
  # A group is finished when nothing in it is pending or running, so this is the
  # instant of the event that settled it, and it goes back to NULL if a later run
  # unsettles the group. On insert the row's own counts are the delta.
  defp settled_at(%{pending: 0, running: 0}, event), do: event.published_at || event.inserted_at
  defp settled_at(_delta, _event), do: nil

  defp project_target_statuses!({_run, _run_event, status, status}, _event), do: :ok

  defp project_target_statuses!({run, run_event, _previous_status, new_status}, event) do
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
      evidence_generation_id = evidence_generation_id(materialization, deployment)

      project_asset_window!(
        event,
        deployment,
        materialization,
        evidence_generation_id,
        window_start,
        window_end,
        projected_at
      )

      SQL.query!(
        Repo,
        """
        INSERT INTO favn_control.asset_freshness_states
          (workspace_id, evidence_generation_id, deployment_id, manifest_version_id,
           target_id, freshness_key,
           latest_attempt_materialization_id, latest_success_materialization_id,
           latest_success_node_key_hash, input_fingerprint, status, payload,
           source_publication_id, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $7, $8, $9, 'fresh', $10, $11, $12)
        ON CONFLICT (workspace_id, evidence_generation_id, target_id, freshness_key) DO UPDATE
        SET deployment_id = EXCLUDED.deployment_id,
            manifest_version_id = EXCLUDED.manifest_version_id,
            latest_attempt_materialization_id = EXCLUDED.latest_attempt_materialization_id,
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
          evidence_generation_id,
          materialization.deployment_id,
          deployment.manifest_version_id,
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
         materialization,
         evidence_generation_id,
         window_start,
         window_end,
         projected_at
       ) do
    with {:ok, data_window_key} <- data_window_key(materialization.partition_key),
         {:ok, storage_window_key} <- FreshnessKey.window(data_window_key) do
      SQL.query!(
        Repo,
        """
        INSERT INTO favn_control.asset_window_states
          (workspace_id, evidence_generation_id, manifest_version_id, target_id,
           window_key, window_start,
           window_end, status, run_id, materialization_id, payload,
           source_publication_id, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, 'succeeded', $8, $9, $10, $11, $12)
        ON CONFLICT (workspace_id, evidence_generation_id, target_id, window_key) DO UPDATE
        SET manifest_version_id = EXCLUDED.manifest_version_id,
            window_start = EXCLUDED.window_start, window_end = EXCLUDED.window_end,
            status = EXCLUDED.status, run_id = EXCLUDED.run_id,
            materialization_id = EXCLUDED.materialization_id, payload = EXCLUDED.payload,
            source_publication_id = EXCLUDED.source_publication_id,
            updated_at = EXCLUDED.updated_at
        WHERE asset_window_states.source_publication_id < EXCLUDED.source_publication_id
        """,
        [
          event.workspace_id,
          evidence_generation_id,
          deployment.manifest_version_id,
          materialization.target_id,
          storage_window_key,
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

    :ok
  end

  defp evidence_generation_id(%{evidence_generation_id: value}, _deployment)
       when is_binary(value),
       do: value

  defp evidence_generation_id(materialization, deployment) do
    digest =
      :crypto.hash(
        :md5,
        Enum.join(
          [
            materialization.workspace_id,
            deployment.manifest_version_id,
            materialization.target_id
          ],
          ":"
        )
      )
      |> Base.encode16(case: :lower)

    "legacy_" <> digest
  end

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

  defp window_from_partition_key(materialization) do
    with {:ok, key} <- data_window_key(materialization.partition_key),
         start_at <- DateTime.from_unix!(key.start_at_us, :microsecond),
         {:ok, local_start} <- DateTime.shift_zone(start_at, key.timezone, Timezone.database!()),
         {:ok, local_end} <- TimePeriod.shift(local_start, key.kind, 1),
         {:ok, end_at} <- DateTime.shift_zone(local_end, "Etc/UTC", Timezone.database!()) do
      {start_at, end_at}
    else
      _other -> fallback_materialization_window(materialization)
    end
  end

  defp data_window_key(freshness_key) do
    case FreshnessKey.parse(freshness_key) do
      {:ok, {:window, window_key}} -> {:ok, window_key}
      {:ok, {:window_refresh, window_key, _kind, _timezone, _period_start}} -> {:ok, window_key}
      _other -> :error
    end
  end

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

  @doc false
  @spec record_failure_if_unprojected(OutboxEvent.t(), atom()) ::
          :recorded | :already_projected | {:error, term()}
  def record_failure_if_unprojected(%OutboxEvent{} = event, error_kind)
      when is_atom(error_kind) do
    now = DateTime.utc_now()

    case SQL.query(
           Repo,
           """
           WITH serialized_cursor AS (
             SELECT last_publication_id
             FROM favn_control.projection_cursors
             WHERE projector_name = $1 AND shard_id = $2
             FOR UPDATE
           )
           INSERT INTO favn_control.projection_failures
             (projector_name, shard_id, publication_id, workspace_id, event_kind,
              error_kind, error_detail, attempt_count, inserted_at, updated_at)
           SELECT $1, $2, $3, $4, $5, $6, '{}'::jsonb, 1, $7, $7
           FROM serialized_cursor
           WHERE last_publication_id < $3
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
         ) do
      {:ok, %{num_rows: 1}} -> :recorded
      {:ok, %{num_rows: 0}} -> :already_projected
      {:error, reason} -> {:error, reason}
    end
  end

  defp clear_failures!([]), do: :ok

  defp clear_failures!(events) do
    publication_ids = Enum.map(events, & &1.publication_id)

    from(failure in "projection_failures",
      where:
        failure.projector_name == ^@projector_name and failure.shard_id == ^@shard_id and
          failure.publication_id in ^publication_ids
    )
    |> Repo.delete_all(prefix: "favn_control")

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
