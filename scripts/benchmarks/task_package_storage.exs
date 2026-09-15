# Run from the umbrella root with MIX_ENV=test mise exec -- mix run --no-start
# scripts/benchmarks/task_package_storage.exs <new-disposable-database-url>.
# Run the identical script at the baseline and changed revision in separate databases.
Code.require_file("../../apps/favn_test_support/fixtures/runner_task_persistence.exs", __DIR__)
Application.ensure_all_started(:postgrex)
Application.ensure_all_started(:favn_test_support)
alias Favn.Contracts.RunnerTask.PersistenceCodec, as: Codec
alias Favn.Manifest.{ExecutionPackage, Serializer, SQLExecution, Version}
alias FavnTestSupport.RunnerTaskPersistence, as: Fixture
[url] = System.argv()
uri = URI.parse(url)
true = String.starts_with?(uri.path, "/favn_705_bench_")
[user, password] = String.split(uri.userinfo, ":", parts: 2)

{:ok, db} =
  Postgrex.start_link(
    hostname: uri.host,
    port: uri.port || 5432,
    username: user,
    password: password,
    database: String.trim_leading(uri.path, "/")
  )

query = fn sql, params -> Postgrex.query!(db, sql, params) end
# CREATE fails if accidentally rerun against a populated benchmark database.
query.(
  "CREATE TABLE packages (hash text PRIMARY KEY, body jsonb NOT NULL) WITH (autovacuum_enabled=false, toast.autovacuum_enabled=false)",
  []
)

query.(
  "CREATE TABLE tasks (id bigint PRIMARY KEY, package_hash text REFERENCES packages(hash), payload jsonb NOT NULL, result jsonb, snapshot jsonb, receipt jsonb, outcome jsonb, inserted_at timestamptz NOT NULL) WITH (autovacuum_enabled=false, toast.autovacuum_enabled=false)",
  []
)

packages =
  for size <- [256, 16_384] do
    version = Fixture.version("Elixir.PackageBenchmark", "asset_#{size}")
    asset = hd(version.manifest.assets)

    comment =
      Enum.map_join(1..div(size, 64), fn n ->
        Base.encode16(:crypto.hash(:sha256, "#{size}:#{n}"))
      end)

    sql = "SELECT 1 AS value /* #{comment} */"

    {:ok, package} =
      ExecutionPackage.new(asset.ref, %SQLExecution{
        sql: sql,
        template: Favn.SQL.Template.compile!(sql, file: "benchmark.sql", line: 1)
      })

    {:ok, version} =
      Version.new(%{
        version.manifest
        | assets: [%{asset | type: :sql, execution_package_hash: package.content_hash}]
      })

    {:asset_attempt, work, result} = hd(Fixture.tasks(version))

    query.("INSERT INTO packages VALUES ($1, $2)", [
      package.content_hash,
      Jason.decode!(Serializer.encode_manifest!(package))
    ])

    {version, package, %{work | execution_package: package}, result}
  end

query.("CHECKPOINT", [])
%{rows: [[start_lsn]]} = query.("SELECT pg_current_wal_insert_lsn()::text", [])

{write_us, _} =
  :timer.tc(fn ->
    for i <- 1..2_000 do
      {_, package, work, result} = Enum.at(packages, rem(i, 2))

      work = %{
        work
        | attempt: rem(i, 3) + 1,
          max_attempts: 3,
          asset_step_id: "window-#{i}",
          metadata: %{
            "window" => i,
            "previous_error" => if(rem(i, 3) == 0, do: "safe failure", else: nil)
          }
      }

      {:ok, payload, _} = Codec.encode_payload(:asset_attempt, work)
      {:ok, result} = Codec.encode_result(:asset_attempt, :succeeded, result)

      query.("INSERT INTO tasks VALUES ($1,$2,$3,$4,$5,$6,$7,$8)", [
        i,
        package.content_hash,
        payload,
        result,
        %{"status" => "succeeded", "window" => i},
        %{"task_id" => i, "assignment_generation" => work.attempt},
        result,
        ~U[2026-01-01 00:00:00Z]
      ])
    end
  end)

%{rows: [[wal]]} =
  query.("SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(), $1::text::pg_lsn)::bigint", [
    start_lsn
  ])

query.("ANALYZE tasks", [])

{read_us, _} =
  :timer.tc(fn ->
    for i <- 1..200 do
      {version, _, _, _} = Enum.at(packages, rem(i, 2))

      %{rows: [[payload, hash]]} =
        query.("SELECT payload, package_hash FROM tasks WHERE id=$1", [i])

      %{rows: [[body]]} = query.("SELECT body FROM packages WHERE hash=$1", [hash])
      {:ok, package} = ExecutionPackage.from_published(body)
      {:ok, _} = Codec.decode_payload(:asset_attempt, payload, version, [package])
    end
  end)

%{rows: [sizes]} =
  query.(
    "SELECT sum(pg_column_size(payload)), sum(pg_column_size(result)), sum(pg_column_size(snapshot)), sum(pg_column_size(receipt)), sum(pg_column_size(outcome)) FROM tasks",
    []
  )

%{rows: [[total]]} =
  query.(
    "SELECT (pg_total_relation_size('tasks') + pg_total_relation_size('packages'))::bigint",
    []
  )

%{rows: settings} =
  query.(
    "SELECT name, setting FROM pg_settings WHERE name IN ('server_version', 'block_size', 'wal_compression', 'full_page_writes', 'default_toast_compression', 'autovacuum') ORDER BY name",
    []
  )

IO.puts(
  Jason.encode!(%{
    postgres_settings: Map.new(settings, fn [key, value] -> {key, value} end),
    rows: 2000,
    payload_result_snapshot_receipt_outcome_bytes: sizes,
    inclusive_table_index_toast_bytes: total,
    workload_wal_bytes: wal,
    encode_insert_us: write_us,
    restore_200_us: read_us,
    queries_per_restore: 2,
    scope:
      "Storage-format microbenchmark; does not measure full enqueue, claim or recovery transactions"
  })
)
