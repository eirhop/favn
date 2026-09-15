defmodule FavnStoragePostgres.RetentionTest do
  use ExUnit.Case, async: false
  alias Ecto.Adapters.SQL
  alias FavnOrchestrator.Persistence.Commands.ConfigureRetention
  alias FavnOrchestrator.Persistence.Commands.RetentionBatch
  alias FavnOrchestrator.Persistence.PlatformContext
  alias FavnOrchestrator.Retention.Policy
  alias FavnStoragePostgres.Config
  alias FavnStoragePostgres.Maintenance.Store
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.StorageV2.Migrations

  setup_all do
    {:ok, options} =
      Config.repo_options(
        url: System.fetch_env!("FAVN_DATABASE_URL"),
        ssl_mode: :disable,
        pool_size: 8
      )

    start_supervised!({Repo, options})
    :ok = Migrations.migrate!(Repo)
    :ok
  end

  setup do
    SQL.query!(
      Repo,
      "DELETE FROM favn_control.maintenance_jobs WHERE job_id = 'retention:scheduler'",
      []
    )

    {:ok, context} = PlatformContext.new("retention-test", "test", [:platform_admin])
    %{context: context}
  end

  test "every authoritative table has an explicit retention classification" do
    doc = File.read!(Path.expand("../../../../docs/storage/postgresql/retention.md", __DIR__))

    classified =
      Regex.scan(~r/^\| `([a-z_]+)` \|/m, doc) |> Enum.map(&Enum.at(&1, 1)) |> Enum.sort()

    %{rows: rows} =
      SQL.query!(
        Repo,
        "SELECT tablename FROM pg_tables WHERE schemaname='favn_control' ORDER BY tablename",
        []
      )

    assert classified == List.flatten(rows)
    assert length(classified) == MapSet.size(MapSet.new(classified))
  end

  test "registry reference guards cover every foreign-key reference, including evidence" do
    %{rows: unguarded} =
      SQL.query!(
        Repo,
        """
        SELECT c.conrelid::regclass::text, a.attname
        FROM pg_constraint c
        CROSS JOIN LATERAL unnest(c.conkey) AS key(attnum)
        JOIN pg_attribute a ON a.attrelid=c.conrelid AND a.attnum=key.attnum
        WHERE c.contype='f'
          AND c.confrelid IN ('favn_control.manifest_versions'::regclass, 'favn_control.workspace_deployments'::regclass)
          AND a.attname <> 'workspace_id'
          AND NOT EXISTS (SELECT 1 FROM pg_trigger t WHERE t.tgrelid=c.conrelid
            AND t.tgname='retention_' || a.attname AND t.tgenabled='O')
        """,
        []
      )

    assert unguarded == []
  end

  test "schema readiness detects a disabled retention reference guard" do
    assert {:ok, %{definition_fingerprint_matches?: true}} = Migrations.diagnostics(Repo)

    assert {:error, :checked} =
             Repo.transaction(fn ->
               SQL.query!(
                 Repo,
                 "ALTER TABLE favn_control.runs DISABLE TRIGGER retention_manifest_version_id",
                 []
               )

               assert {:ok, %{definition_fingerprint_matches?: false}} =
                        Migrations.diagnostics(Repo)

               Repo.rollback(:checked)
             end)

    assert {:ok, %{definition_fingerprint_matches?: true}} = Migrations.diagnostics(Repo)
  end

  test "every enabled family and phase executes against the authoritative schema", %{context: c} do
    policy = %Policy{
      enabled?: true,
      periods: Map.new(Policy.families() -- [:receipts], &{&1, 604_800}),
      row_limit: 5
    }

    assert {:ok, %{version: version}} =
             Store.configure_retention(%ConfigureRetention{
               platform_context: c,
               expected_version: 0,
               policy: policy
             })

    Enum.reduce(1..72, version, fn _, v ->
      assert {:ok, result} =
               Store.retention_batch(%RetentionBatch{
                 platform_context: c,
                 expected_version: v,
                 policy: policy
               })

      assert result.batch_count <= policy.row_limit
      result.version
    end)

    for family <- Policy.families() do
      assert {:ok, _} = Store.retention_preview(c, family), "preview failed for #{family}"
    end
  end

  test "initialization, policy changes and lost acknowledgement use expected versions", %{
    context: c
  } do
    assert {:ok, %{version: 0}} = Store.retention_status(c)
    command = %ConfigureRetention{platform_context: c, expected_version: 0, policy: %Policy{}}
    assert {:ok, %{version: v}} = Store.configure_retention(command)
    assert {:error, %{kind: :conflict}} = Store.configure_retention(command)
    batch = %RetentionBatch{platform_context: c, expected_version: v, policy: %Policy{}}
    assert {:ok, %{version: next, batch_count: 0}} = Store.retention_batch(batch)
    assert next > v
    assert {:error, %{kind: :conflict}} = Store.retention_batch(batch)
  end

  test "policy mismatches fail closed", %{context: c} do
    assert {:ok, %{version: v}} =
             Store.configure_retention(%ConfigureRetention{
               platform_context: c,
               expected_version: 0,
               policy: %Policy{}
             })

    assert {:error, %{kind: :conflict}} =
             Store.retention_batch(%RetentionBatch{
               platform_context: c,
               expected_version: v,
               policy: %Policy{row_limit: 5}
             })
  end

  test "independent transaction holds the batch lock without blocking another worker", %{
    context: c
  } do
    parent = self()

    task =
      Task.async(fn ->
        Repo.transaction(fn ->
          SQL.query!(Repo, "SELECT pg_advisory_xact_lock(704202609)", [])
          send(parent, :locked)

          receive do
            :release -> :ok
          end
        end)
      end)

    assert_receive :locked

    assert {:error, %{kind: :conflict}} =
             Store.retention_batch(%RetentionBatch{
               platform_context: c,
               expected_version: 0,
               policy: %Policy{}
             })

    assert {:error, %{kind: :conflict}} =
             Store.configure_retention(%ConfigureRetention{
               platform_context: c,
               expected_version: 0,
               policy: %Policy{excluded_workspace_ids: ["held"]}
             })

    send(task.pid, :release)
    assert {:ok, :ok} = Task.await(task)
  end

  test "a failed progress write rolls back deleted rows and replay floors", %{context: c} do
    workspace = log_workspace(c)
    append_log(workspace, "atomic", DateTime.utc_now())
    assert {:ok, _} = FavnStoragePostgres.Outbox.Sequencer.sequence_batch()
    age_logs(workspace.workspace_id)
    policy = %Policy{enabled?: true, periods: %{logs: 605_100}}

    assert {:ok, %{version: version}} =
             Store.configure_retention(%ConfigureRetention{
               platform_context: c,
               expected_version: 0,
               policy: policy
             })

    SQL.query!(
      Repo,
      "UPDATE favn_control.maintenance_jobs SET cursor=jsonb_build_object('family_index',1) WHERE job_id='retention:scheduler'",
      []
    )

    SQL.query!(
      Repo,
      "ALTER TABLE favn_control.maintenance_jobs ADD CONSTRAINT test_retention_rollback CHECK (job_id<>'retention:scheduler' OR processed_count=0)",
      []
    )

    try do
      assert {:error, _} =
               Store.retention_batch(%RetentionBatch{
                 platform_context: c,
                 expected_version: version,
                 policy: policy
               })

      assert %{rows: [[1]]} =
               SQL.query!(
                 Repo,
                 "SELECT count(*) FROM favn_control.log_entries WHERE workspace_id=$1",
                 [workspace.workspace_id]
               )

      assert %{rows: [[0]]} =
               SQL.query!(
                 Repo,
                 "SELECT count(*) FROM favn_control.retention_floors WHERE workspace_id=$1",
                 [workspace.workspace_id]
               )

      assert {:ok, %{version: ^version, processed_count: 0}} = Store.retention_status(c)
    after
      SQL.query!(
        Repo,
        "ALTER TABLE favn_control.maintenance_jobs DROP CONSTRAINT test_retention_rollback",
        []
      )
    end

    assert {:ok, %{batch_count: count}} =
             Store.retention_batch(%RetentionBatch{
               platform_context: c,
               expected_version: version,
               policy: policy
             })

    assert count > 0
  end

  test "scheduled worker survives a policy failure and resumes persisted progress after restart",
       %{context: c} do
    policy = %Policy{interval_ms: 1_000, row_limit: 5}

    assert {:ok, %{version: version}} =
             Store.configure_retention(%ConfigureRetention{
               platform_context: c,
               expected_version: 0,
               policy: policy
             })

    observer = self()
    handler = "retention-worker-test"

    :ok =
      :telemetry.attach_many(
        handler,
        [[:favn, :retention, :batch], [:favn, :retention, :failure]],
        fn event, measurements, _, _ ->
          send(observer, {event, measurements})
        end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler) end)
    start_supervised!({Task.Supervisor, name: FavnStoragePostgres.Maintenance.Tasks})

    worker =
      start_supervised!(
        {FavnStoragePostgres.Maintenance.Worker, policy: %Policy{interval_ms: 1_000}}
      )

    send(worker, :tick)
    assert_receive {[:favn, :retention, :failure], _}, 5_000
    assert Process.alive?(worker)
    assert {:ok, %{version: ^version}} = Store.retention_status(c)
    assert %{task: nil} = :sys.get_state(worker)

    # Correct the persisted configuration to match the running replica.
    assert {:ok, %{version: configured}} =
             Store.configure_retention(%ConfigureRetention{
               platform_context: c,
               expected_version: version,
               policy: %Policy{interval_ms: 1_000}
             })

    send(worker, :tick)
    assert_receive {[:favn, :retention, :batch], _}, 5_000
    assert {:ok, %{version: committed}} = Store.retention_status(c)
    assert committed > configured
    Process.exit(worker, :kill)
    assert_receive {[:favn, :retention, :batch], _}, 5_000
    assert {:ok, %{version: resumed}} = Store.retention_status(c)
    assert resumed > committed
    assert Process.alive?(Process.whereis(FavnStoragePostgres.Maintenance.Worker))
  end

  test "preview neither initializes nor changes maintenance state", %{context: c} do
    assert {:ok, %{eligible_count: 0, complete?: true}} = Store.retention_preview(c, :logs)
    assert {:ok, %{version: 0}} = Store.retention_status(c)
  end

  test "log history uses publication order and expires reverse cursors after deletion", %{
    context: c
  } do
    context = log_workspace(c)
    append_log(context, "first", DateTime.add(DateTime.utc_now(), -300, :second))
    append_log(context, "second", DateTime.add(DateTime.utc_now(), -3_000, :second))
    assert {:ok, _} = FavnStoragePostgres.Outbox.Sequencer.sequence_batch()

    query = %FavnOrchestrator.Persistence.Queries.PageLogs{
      workspace_context: context,
      filter: Map.from_struct(%Favn.Log.Filter{}),
      limit: 1
    }

    assert {:ok, %{items: [%{message: "second"}], next_cursor: cursor}} =
             FavnStoragePostgres.Logs.Store.page(query)

    assert %{publication_id: _, batch_offset: _} = cursor

    assert {:ok, %{items: [%{message: "first"}]}} =
             FavnStoragePostgres.Logs.Store.page(%{query | after: cursor})

    age_logs(context.workspace_id)

    assert {:ok, %{deleted_count: count}} =
             Repo.transaction(fn ->
               FavnStoragePostgres.Maintenance.Retention.lock!()

               FavnStoragePostgres.Maintenance.LogRetention.delete!(
                 %Policy{},
                 DateTime.utc_now(),
                 nil,
                 context.workspace_id
               )
             end)

    assert count >= 2

    assert {:error, %{kind: :expired}} =
             FavnStoragePostgres.Logs.Store.page(%{query | after: cursor})
  end

  test "a page whose floor check precedes deletion reads a complete old snapshot", %{context: c} do
    context = log_workspace(c)
    append_log(context, "one", DateTime.utc_now())
    append_log(context, "two", DateTime.utc_now())
    assert {:ok, _} = FavnStoragePostgres.Outbox.Sequencer.sequence_batch()
    age_logs(context.workspace_id)
    parent = self()
    handler = "retention-reader-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler,
      [:favn_storage_postgres, :repo, :query],
      fn _, _, metadata, _ ->
        if String.contains?(
             metadata.query,
             "SELECT publication_id, batch_offset FROM favn_control.retention_floors"
           ) do
          send(parent, {:floor_checked, self()})

          receive do
            :continue_page -> :ok
          end
        end
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    task =
      Task.async(fn ->
        FavnStoragePostgres.Logs.Store.page(%FavnOrchestrator.Persistence.Queries.PageLogs{
          workspace_context: context,
          filter: Map.from_struct(%Favn.Log.Filter{}),
          limit: 10
        })
      end)

    assert_receive {:floor_checked, reader}

    assert {:ok, _} =
             Repo.transaction(fn ->
               FavnStoragePostgres.Maintenance.Retention.lock!()

               FavnStoragePostgres.Maintenance.LogRetention.delete!(
                 %Policy{},
                 DateTime.utc_now(),
                 nil,
                 context.workspace_id
               )
             end)

    send(reader, :continue_page)
    assert {:ok, %{items: [_, _]}} = Task.await(task)
    :telemetry.detach(handler)
  end

  defp log_workspace(platform) do
    id = "retention-#{System.unique_integer([:positive])}"

    :ok =
      FavnStoragePostgres.Registry.Store.provision_workspace(
        %FavnOrchestrator.Persistence.Commands.ProvisionWorkspace{
          platform_context: platform,
          workspace_id: id,
          slug: id,
          display_name: id,
          occurred_at: DateTime.utc_now()
        }
      )

    {:ok, context} =
      FavnOrchestrator.Persistence.WorkspaceContext.new(id, "retention-test", [:workspace_admin])

    context
  end

  defp append_log(context, text, occurred_at) do
    id = "log-#{System.unique_integer([:positive])}"

    assert {:ok, _} =
             FavnStoragePostgres.Logs.Store.append_batch(
               %FavnOrchestrator.Persistence.Commands.AppendLogBatch{
                 workspace_context: context,
                 command_id: id,
                 batch_id: id,
                 occurred_at: DateTime.utc_now(),
                 entries: [
                   %FavnOrchestrator.Persistence.Commands.LogEntry{
                     source: "system",
                     level: :info,
                     message: text,
                     metadata: %{},
                     occurred_at: occurred_at
                   }
                 ]
               }
             )
  end

  defp age_logs(workspace) do
    SQL.query!(
      Repo,
      "UPDATE favn_control.log_entries SET inserted_at=clock_timestamp()-interval '8 days' WHERE workspace_id=$1",
      [workspace]
    )

    SQL.query!(
      Repo,
      "UPDATE favn_control.log_batches SET inserted_at=clock_timestamp()-interval '8 days' WHERE workspace_id=$1",
      [workspace]
    )

    SQL.query!(
      Repo,
      "UPDATE favn_control.outbox_events SET published_at=clock_timestamp()-interval '8 days' WHERE workspace_id=$1 AND event_kind='logs.batch.appended'",
      [workspace]
    )
  end
end
