defmodule FavnStoragePostgres.RetentionLoadTest do
  use ExUnit.Case, async: false
  @moduletag :slow
  alias Ecto.Adapters.SQL
  alias FavnOrchestrator.Persistence.Commands, as: C
  alias FavnOrchestrator.Persistence.Queries.PageLogs
  alias FavnOrchestrator.Persistence.{PlatformContext, WorkspaceContext}
  alias FavnOrchestrator.Retention.Policy
  alias FavnStoragePostgres.{Config, Repo}
  alias FavnStoragePostgres.Maintenance.Store
  alias FavnStoragePostgres.TestSupport.TaskManifest

  setup_all do
    {:ok, opts} =
      Config.repo_options(
        url: System.fetch_env!("FAVN_DATABASE_URL"),
        ssl_mode: :disable,
        pool_size: 8
      )

    start_supervised!({Repo, opts})
    :ok = FavnStoragePostgres.StorageV2.Migrations.migrate!(Repo)
    :ok
  end

  @tag timeout: 120_000
  test "two eligibility waves recover backlog during concurrent enqueue and reads" do
    {:ok, platform} = PlatformContext.new("retention-load", "local-test", [:platform_admin])
    fixtures = for mode <- [:disabled, :enabled], into: %{}, do: {mode, fixture(platform, mode)}

    reports =
      for mode <- [:disabled, :enabled], into: %{} do
        fixture = fixtures[mode]
        held = for {other, f} <- fixtures, other != mode, do: f.workspace_id

        policy = %Policy{
          enabled?: mode == :enabled,
          periods: Map.put(%Policy{}.periods, :logs, 605_100),
          excluded_workspace_ids: held,
          row_limit: 100,
          interval_ms: 1_000
        }

        {:ok, state} = Store.retention_status(platform)

        {:ok, configured} =
          Store.configure_retention(%C.ConfigureRetention{
            platform_context: platform,
            policy: policy,
            expected_version: state.version
          })

        template = task_template(fixture)
        # Seed a backlog using the real log writer. Aging fixtures accelerates the replay window
        # without adding a production clock override or weakening command validation.
        for n <- 1..20, do: append(fixture, "backlog-#{n}", 25)
        publish_and_age(fixture)
        before = statistics()
        started = System.monotonic_time(:microsecond)

        {version, samples, deleted} =
          Enum.reduce(1..40, {configured.version, [], 0}, fn n, {version, samples, deleted} ->
            cleanup =
              if n not in 15..20, do: Task.async(fn -> turns(platform, policy, version, 8) end)

            {write_us, :ok} = :timer.tc(fn -> append(fixture, "wave-#{n}", 25) end)
            publish_and_age(fixture)
            now = DateTime.utc_now()

            task = %{
              template
              | command_id: "load-enqueue-#{n}",
                task_id: "rt_load_#{n}",
                domain_identity: "load-domain-#{n}",
                occurred_at: now,
                issued_at: now,
                deadline_at: DateTime.add(now, 3600, :second)
            }

            {enqueue_us, {:ok, _}} =
              :timer.tc(fn -> FavnStoragePostgres.RunnerTasks.Store.enqueue(task) end)

            {read_us, {:ok, _}} =
              :timer.tc(fn ->
                FavnStoragePostgres.Logs.Store.page(%PageLogs{
                  workspace_context: fixture.workspace_context,
                  filter: Map.from_struct(%Favn.Log.Filter{}),
                  direction: :older,
                  limit: 25
                })
              end)

            # A six-round interruption must accumulate backlog without losing progress.
            {next, removed} =
              if cleanup, do: Task.await(cleanup, 30_000), else: {version, 0}

            {next, [%{write_us: write_us, enqueue_us: enqueue_us, read_us: read_us} | samples],
             deleted + removed}
          end)

        # Bound the recovery tail too; never use an unbounded sweep in the harness.
        {_version, recovered} = turns(platform, policy, version, 80)
        elapsed = System.monotonic_time(:microsecond) - started

        %{rows: [[remaining]]} =
          SQL.query!(
            Repo,
            "SELECT count(*) FROM favn_control.log_entries WHERE workspace_id=$1",
            [fixture.workspace_id]
          )

        %{rows: [[protected_tasks]]} =
          SQL.query!(
            Repo,
            "SELECT count(*) FROM favn_control.runner_tasks WHERE workspace_id=$1 AND status='queued'",
            [fixture.workspace_id]
          )

        assert protected_tasks == 40
        if mode == :enabled, do: assert(remaining == 0), else: assert(remaining == 1500)
        after_stats = statistics()

        %{rows: [[wal_bytes]]} =
          SQL.query!(Repo, "SELECT pg_wal_lsn_diff($1::pg_lsn,$2::pg_lsn)::bigint", [
            after_stats.wal_lsn,
            before.wal_lsn
          ])

        report = %{
          wal_bytes: wal_bytes,
          remaining_logs: remaining,
          protected_tasks: protected_tasks,
          deleted_rows: deleted + recovered,
          elapsed_us: elapsed,
          p95_us: quantiles(samples, 0.95),
          p99_us: quantiles(samples, 0.99),
          before: before,
          after: after_stats
        }

        {mode, report}
      end

    # Broad pre-v1 regression ceiling, declared before measurements. This is not a
    # production SLO; baseline comparison and raw measurements remain in the report.
    assert reports.enabled.p99_us.enqueue_us < 250_000
    assert reports.enabled.p99_us.read_us < 250_000
    assert reports.enabled.p99_us.write_us < 250_000
    json = Jason.encode!(reports, pretty: true)
    if path = System.get_env("FAVN_RETENTION_BENCHMARK_OUTPUT"), do: File.write!(path, json)
    IO.puts("retention_load=" <> json)
  end

  defp turns(platform, policy, version, count) do
    Enum.reduce(1..count, {version, 0}, fn _, {v, total} ->
      assert {:ok, result} =
               Store.retention_batch(%C.RetentionBatch{
                 platform_context: platform,
                 policy: policy,
                 expected_version: v
               })

      assert result.batch_count <= policy.row_limit
      {result.version, total + result.batch_count}
    end)
  end

  defp fixture(platform, mode) do
    id = "retention-load-#{mode}-#{System.unique_integer([:positive])}"
    now = DateTime.utc_now()

    :ok =
      FavnStoragePostgres.Registry.Store.provision_workspace(%C.ProvisionWorkspace{
        platform_context: platform,
        workspace_id: id,
        slug: id,
        display_name: id,
        occurred_at: now
      })

    {:ok, context} = WorkspaceContext.new(id, "retention-load", [:workspace_admin])

    %{
      workspace_id: id,
      workspace_context: context,
      platform_context: platform,
      now: now,
      runner_pool: "retention_load"
    }
  end

  defp append(fixture, id, count) do
    now = DateTime.utc_now()

    entry = %C.LogEntry{
      source: "system",
      level: :info,
      message: String.duplicate("x", 1024),
      metadata: %{},
      occurred_at: now
    }

    assert {:ok, _} =
             FavnStoragePostgres.Logs.Store.append_batch(%C.AppendLogBatch{
               workspace_context: fixture.workspace_context,
               command_id: id,
               batch_id: id,
               occurred_at: now,
               entries: List.duplicate(entry, count)
             })

    :ok
  end

  defp publish_and_age(fixture) do
    assert {:ok, _} = FavnStoragePostgres.Outbox.Sequencer.sequence_batch(5000)
    # Log publications have no projection state; advance only the test's log publication
    # bookkeeping by projecting all queued source, rather than faking consumer progress.
    project()

    for table <- ~w(log_entries log_batches) do
      SQL.query!(
        Repo,
        "UPDATE favn_control.#{table} SET inserted_at=clock_timestamp()-interval '8 days' WHERE workspace_id=$1",
        [fixture.workspace_id]
      )
    end

    SQL.query!(
      Repo,
      "UPDATE favn_control.outbox_events SET published_at=clock_timestamp()-interval '8 days' WHERE workspace_id=$1 AND event_kind='logs.batch.appended'",
      [fixture.workspace_id]
    )
  end

  defp project do
    case FavnStoragePostgres.Projections.Projector.project_batch("retention-load", limit: 250) do
      {:ok, %{count: 250}} -> project()
      {:ok, _} -> :ok
    end
  end

  defp task_template(fixture) do
    release = "rr_" <> String.duplicate("a", 64)
    request = %Favn.Contracts.RelationInspectionRequest{include: [:columns], sample_limit: 0}
    {request, version} = TaskManifest.prepare(fixture, request, fixture.runner_pool, release)

    {:ok, payload, hash} =
      Favn.Contracts.RunnerTask.PersistenceCodec.encode_payload(:relation_inspection, request)

    {:ok, context} = FavnStoragePostgres.RunnerTasks.Codec.encode_orchestration_context(%{})

    %C.EnqueueRunnerTask{
      workspace_context: fixture.workspace_context,
      command_id: "template",
      task_id: "template",
      domain_identity: "template",
      task_kind: :relation_inspection,
      manifest_version_id: version.manifest_version_id,
      manifest_content_hash: version.content_hash,
      runner_pool: fixture.runner_pool,
      required_runner_release_id: release,
      retry_class: :safe_to_retry,
      payload: payload,
      payload_hash: hash,
      orchestration_context: context,
      required_capability: "relation_inspection",
      occurred_at: fixture.now,
      issued_at: fixture.now,
      deadline_at: DateTime.add(fixture.now, 3600, :second)
    }
  end

  defp quantiles(samples, q),
    do:
      Map.new([:enqueue_us, :write_us, :read_us], fn key ->
        {key,
         samples
         |> Enum.map(&Map.fetch!(&1, key))
         |> Enum.sort()
         |> Enum.at(ceil(length(samples) * q) - 1)}
      end)

  defp statistics do
    %{rows: [[wal, bytes, dead]]} =
      SQL.query!(
        Repo,
        "SELECT pg_current_wal_lsn()::text, (SELECT sum(pg_total_relation_size(relid)) FROM pg_stat_user_tables WHERE schemaname='favn_control'), (SELECT sum(n_dead_tup) FROM pg_stat_user_tables WHERE schemaname='favn_control')",
        []
      )

    %{wal_lsn: wal, allocated_bytes: to_string(bytes), estimated_dead_tuples: to_string(dead)}
  end
end
