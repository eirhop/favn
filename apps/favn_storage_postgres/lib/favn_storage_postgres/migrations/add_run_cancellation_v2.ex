defmodule FavnStoragePostgres.Migrations.AddRunCancellationV2 do
  @moduledoc false
  use Ecto.Migration

  alias Ecto.Adapters.SQL
  alias FavnOrchestrator.Persistence.BackfillPlan
  alias FavnOrchestrator.RunSubmission.Intent

  @prefix "favn_control"

  def up do
    for name <- [:runs, :run_submissions] do
      alter table(name, prefix: @prefix) do
        add(:cancellation_owner_run_id, :text)
      end

      create(
        index(name, [:workspace_id, :cancellation_owner_run_id, :run_id],
          prefix: @prefix,
          name: :"#{name}_cancellation_owner_idx"
        )
      )
    end

    alter table(:runs, prefix: @prefix) do
      add(:cancellation_requested_at, :utc_datetime_usec)
      add(:cancellation_status, :text)
    end

    create(
      index(:runs, [:workspace_id, :run_id],
        prefix: @prefix,
        name: :runs_cancelling_idx,
        where: "cancellation_status = 'cancelling'"
      )
    )

    create(
      index(:runs, [:workspace_id, :run_id],
        prefix: @prefix,
        name: :runs_requested_cancellation_idx,
        where: "cancellation_requested_at IS NOT NULL AND status IN ('pending','running')"
      )
    )

    create(
      index(:run_submissions, [:workspace_id, :run_id],
        prefix: @prefix,
        name: :run_submissions_requested_cancellation_idx,
        where:
          "cancellation_requested_at IS NOT NULL AND status IN ('queued','preparing','admitting')"
      )
    )

    create(
      constraint(:runs, :runs_cancellation_status_valid,
        prefix: @prefix,
        check: "cancellation_status IN ('cancelling', 'cancelled', 'needs_attention')"
      )
    )

    alter table(:backfills, prefix: @prefix) do
      add(:cancellation_requested_at, :utc_datetime_usec)
      add(:cancellation_reason, :map)
    end

    create(
      index(:backfills, [:workspace_id, :root_run_id],
        prefix: @prefix,
        name: :backfills_cancelling_idx,
        where: "status = 'cancelling'"
      )
    )

    create(
      index(
        :resource_recovery_candidates,
        [:workspace_id, :source_run_id, :status, :candidate_id],
        prefix: @prefix,
        name: :resource_recovery_candidates_source_idx
      )
    )

    create(
      index(:resource_recovery_candidates, [:workspace_id, :recovery_run_id],
        prefix: @prefix,
        name: :resource_recovery_candidates_run_idx,
        where: "recovery_run_id IS NOT NULL"
      )
    )

    create(
      index(:materializations, [:workspace_id, :run_id],
        prefix: @prefix,
        name: :materializations_run_generation_idx,
        where: "target_generation_id IS NOT NULL"
      )
    )

    flush()

    extend_constraint(
      "backfills",
      "backfills_values_valid",
      "'cancelled'::text",
      "'cancelled'::text, 'cancelling'::text, 'needs_attention'::text"
    )

    extend_constraint(
      "resource_recovery_candidates",
      "resource_recovery_candidates_values_valid",
      "'submitted'::text",
      "'submitted'::text, 'cancelled'::text"
    )

    normalize_owners!()

    execute("""
    UPDATE favn_control.runs SET cancellation_requested_at =
      COALESCE((snapshot->'metadata'->>'cancel_requested_at')::timestamptz, updated_at)
    WHERE snapshot->'metadata'->>'cancel_requested' = 'true'
    """)
  end

  defp extend_constraint(table, name, old, new) do
    %{rows: [[definition]]} =
      SQL.query!(
        repo(),
        "SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid = $1::text::regclass AND conname = $2",
        ["favn_control." <> table, name]
      )

    if not String.contains?(definition, old), do: raise("unexpected #{name} definition")
    execute("ALTER TABLE favn_control.#{table} DROP CONSTRAINT #{name}")

    execute(
      "ALTER TABLE favn_control.#{table} ADD CONSTRAINT #{name} #{String.replace(definition, old, new)}"
    )

    flush()
  end

  defp normalize_owners! do
    SQL.query!(
      repo(),
      "UPDATE favn_control.run_submissions SET cancellation_owner_run_id = run_id WHERE source NOT IN ('backfill', 'recovery')",
      []
    )

    SQL.query!(
      repo(),
      """
      UPDATE favn_control.runs r SET cancellation_owner_run_id = r.run_id
      WHERE r.trigger_type IS DISTINCT FROM 'resource_recovery'
        AND NOT EXISTS (SELECT 1 FROM favn_control.run_submissions s WHERE s.workspace_id=r.workspace_id AND s.run_id=r.run_id AND s.source IN ('backfill','recovery'))
        AND NOT EXISTS (SELECT 1 FROM favn_control.backfill_windows w WHERE w.workspace_id=r.workspace_id AND w.run_id=r.run_id)
      """,
      []
    )

    reject_ambiguous_roots!()
    normalize_pass!()

    for table <- ~w(runs run_submissions) do
      %{rows: [[remaining]]} =
        SQL.query!(
          repo(),
          "SELECT count(*) FROM favn_control.#{table} WHERE cancellation_owner_run_id IS NULL",
          []
        )

      if remaining != 0,
        do:
          raise(
            "cancellation ownership unresolved in #{table}; repair durable membership before upgrading"
          )

      execute(
        "ALTER TABLE favn_control.#{table} ALTER COLUMN cancellation_owner_run_id SET NOT NULL"
      )

      create(
        constraint(String.to_existing_atom(table), :"#{table}_cancellation_owner_valid",
          prefix: @prefix,
          check: "octet_length(cancellation_owner_run_id) BETWEEN 1 AND 255"
        )
      )
    end
  end

  defp reject_ambiguous_roots! do
    %{rows: rows} =
      SQL.query!(
        repo(),
        "SELECT 1 FROM favn_control.backfills GROUP BY workspace_id,root_run_id HAVING count(*) > 1 LIMIT 1",
        []
      )

    if rows != [], do: raise("ambiguous cancellation owner: multiple backfills share one root")
  end

  defp normalize_pass! do
    submissions = normalize_page!("run_submissions", nil, 0)

    %{num_rows: copied} =
      SQL.query!(
        repo(),
        """
        UPDATE favn_control.runs r SET cancellation_owner_run_id=s.cancellation_owner_run_id
        FROM favn_control.run_submissions s
        WHERE r.workspace_id=s.workspace_id AND r.run_id=s.run_id
          AND r.cancellation_owner_run_id IS NULL AND s.cancellation_owner_run_id IS NOT NULL
        """,
        []
      )

    changed = submissions + copied + normalize_page!("runs", nil, 0)
    if changed > 0, do: normalize_pass!()
  end

  defp normalize_page!(table, after_key, changed) do
    {workspace, run} = after_key || {"", ""}

    without_submission =
      if table == "runs",
        do:
          "AND NOT EXISTS (SELECT 1 FROM favn_control.run_submissions s WHERE s.workspace_id=r.workspace_id AND s.run_id=r.run_id)",
        else: ""

    %{rows: rows} =
      SQL.query!(
        repo(),
        """
        SELECT workspace_id,run_id FROM favn_control.#{table} r
        WHERE cancellation_owner_run_id IS NULL AND (workspace_id,run_id) > ($1::text,$2::text) #{without_submission}
        ORDER BY workspace_id,run_id LIMIT 500
        """,
        [workspace, run]
      )

    added =
      Enum.count(rows, fn [workspace, run] ->
        case legacy_owner(table, workspace, run) do
          nil ->
            false

          owner ->
            SQL.query!(
              repo(),
              "UPDATE favn_control.#{table} SET cancellation_owner_run_id=$3 WHERE workspace_id=$1 AND run_id=$2",
              [workspace, run, owner]
            )

            true
        end
      end)

    case List.last(rows) do
      nil -> changed
      [workspace, run] -> normalize_page!(table, {workspace, run}, changed + added)
    end
  end

  defp legacy_owner("run_submissions", workspace, run) do
    %{rows: [[source, intent]]} =
      SQL.query!(
        repo(),
        "SELECT source,intent FROM favn_control.run_submissions WHERE workspace_id=$1 AND run_id=$2",
        [workspace, run]
      )

    with {:ok, {operation, selector, opts}} <- Intent.decode(intent) do
      metadata = Keyword.get(opts, :metadata, %{})

      case source do
        "backfill" ->
          backfill_owner(
            workspace,
            run,
            field(metadata, :backfill_id),
            field(metadata, :backfill_window_id)
          )

        "recovery" when operation == :rerun ->
          ids = field(metadata, :resource_recovery_candidate_ids)

          if field(metadata, :resource_recovery_source_run_id) != selector or not is_list(ids) or
               ids == [],
             do: invalid_membership!()

          recovery_owner(workspace, run, selector, ids)

        _ ->
          invalid_membership!()
      end
    else
      _ -> invalid_membership!()
    end
  end

  defp legacy_owner("runs", workspace, run) do
    case SQL.query!(
           repo(),
           "SELECT backfill_id,window_id FROM favn_control.backfill_windows WHERE workspace_id=$1 AND run_id=$2 LIMIT 501",
           [workspace, run]
         ).rows do
      [] ->
        rows =
          SQL.query!(
            repo(),
            "SELECT source_run_id,candidate_id FROM favn_control.resource_recovery_candidates WHERE workspace_id=$1 AND recovery_run_id=$2 LIMIT 501",
            [workspace, run]
          ).rows

        case Enum.uniq(Enum.map(rows, &hd/1)) do
          [source] when length(rows) <= 500 ->
            recovery_owner(workspace, run, source, Enum.map(rows, &List.last/1))

          _ ->
            invalid_membership!()
        end

      rows when length(rows) <= 500 ->
        case Enum.uniq(
               Enum.map(rows, fn [backfill, window] ->
                 backfill_owner(workspace, run, backfill, window)
               end)
             ) do
          [owner] -> owner
          _ -> invalid_membership!()
        end

      _ ->
        invalid_membership!()
    end
  end

  defp backfill_owner(workspace, run, backfill, window) do
    rows =
      SQL.query!(
        repo(),
        """
        SELECT b.root_run_id,w.payload FROM favn_control.backfill_windows w
        JOIN favn_control.backfills b USING (workspace_id,backfill_id)
        WHERE w.workspace_id=$1 AND w.backfill_id=$2 AND w.window_id=$3
        """,
        [workspace, backfill, window]
      ).rows

    case rows do
      [[owner, payload]] ->
        if BackfillPlan.child_run_id(backfill, window, payload) != run, do: invalid_membership!()
        owner

      _ ->
        invalid_membership!()
    end
  end

  defp recovery_owner(workspace, run, source, ids) do
    rows =
      SQL.query!(
        repo(),
        """
        SELECT resource_kind,resource_name,candidate_id FROM favn_control.resource_recovery_candidates
        WHERE workspace_id=$1 AND source_run_id=$2 AND candidate_id = ANY($3::text[])
        """,
        [workspace, source, ids]
      ).rows

    resource = Enum.uniq(Enum.map(rows, fn [kind, name, _id] -> {kind, name} end))
    if length(rows) != length(Enum.uniq(ids)), do: invalid_membership!()

    case resource do
      [{kind, name}] ->
        kind =
          case kind do
            "connection" -> :connection
            "execution_pool" -> :execution_pool
          end

        identity = {workspace, source, kind, name, Enum.sort(ids)}

        hash =
          identity
          |> :erlang.term_to_binary([:deterministic])
          |> then(&:crypto.hash(:sha256, &1))
          |> Base.url_encode64(padding: false)

        if run != "resource-recovery-" <> hash, do: invalid_membership!()

        case SQL.query!(
               repo(),
               """
               SELECT cancellation_owner_run_id FROM favn_control.run_submissions WHERE workspace_id=$1 AND run_id=$2
               UNION SELECT cancellation_owner_run_id FROM favn_control.runs WHERE workspace_id=$1 AND run_id=$2
               """,
               [workspace, source]
             ).rows
             |> Enum.reject(&(&1 == [nil])) do
          [[owner]] -> owner
          [] -> nil
          _ -> invalid_membership!()
        end

      _ ->
        invalid_membership!()
    end
  end

  defp field(map, key), do: Map.get(map, key) || Map.get(map, Atom.to_string(key))

  defp invalid_membership!,
    do:
      raise(
        "cancellation ownership cannot be verified from durable backfill/recovery records; repair membership before upgrading"
      )
end
