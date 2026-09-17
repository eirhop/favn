Code.require_file(Path.join(:code.priv_dir(:favn_test_support), "fixtures/catalog.exs"))

defmodule FavnDuckdbADBC.CatalogPublicationTest do
  use ExUnit.Case, async: false
  @moduletag :adbc_integration
  alias Favn.Connection.Resolved
  alias Favn.SQL.Adapter.DuckDB.ADBC
  alias Favn.SQL.Adapter.DuckDB.ADBC.Catalog
  alias Favn.SQL.Catalog.Request
  alias Favn.SQL.{Deadline, Session}
  alias Favn.Semantic.Catalog, as: Semantics
  alias FavnTestSupport.CatalogFixture, as: Fixture

  defmodule BarrierClient do
    alias Favn.SQL.Adapter.DuckDB.ADBC.Client.ADBC, as: Native

    for {name, arity} <- [
          open: 2,
          connection: 1,
          query: 3,
          execute: 3,
          fetch_all: 4,
          columns: 1,
          bulk_insert: 3,
          commit: 1,
          rollback: 1,
          release: 1
        ] do
      args = Macro.generate_arguments(arity, __MODULE__)

      def unquote(name)(unquote_splicing(args)),
        do: apply(Native, unquote(name), [unquote_splicing(args)])
    end

    def begin_transaction(conn) do
      result = Native.begin_transaction(conn)

      if parent = :persistent_term.get({__MODULE__, :parent}, nil) do
        send(parent, {:transaction_started, self()})

        receive do
          :continue -> :ok
        after
          5000 -> raise "test barrier expired"
        end
      end

      result
    end
  end

  defmodule SharedAdapter do
    alias Favn.SQL.Adapter.DuckDB.ADBC
    alias FavnDuckdbADBC.CatalogPublicationTest.BarrierClient
    def catalog_publication_backend, do: ADBC.catalog_publication_backend()
    def capabilities(resolved, opts), do: ADBC.capabilities(resolved, opts)

    def connect(resolved, _) do
      conn = resolved.config.shared_conn

      with {:ok, ref} <- conn.client.connection(conn.db_ref),
           do: {:ok, %{conn | conn_ref: ref, client: BarrierClient}}
    end

    def disconnect(conn, _), do: conn.client.release(conn.conn_ref)
    def query(conn, sql, opts), do: ADBC.query(conn, sql, opts)
    def execute(conn, sql, opts), do: ADBC.execute(conn, sql, opts)
    def transaction(conn, fun, opts), do: ADBC.transaction(conn, fun, opts)
  end

  defmodule FaultClient do
    alias Favn.SQL.Adapter.DuckDB.ADBC.Client.ADBC, as: Native

    for {name, arity} <- [
          open: 2,
          connection: 1,
          query: 3,
          fetch_all: 4,
          columns: 1,
          bulk_insert: 3,
          begin_transaction: 1,
          rollback: 1,
          release: 1
        ] do
      args = Macro.generate_arguments(arity, __MODULE__)

      def unquote(name)(unquote_splicing(args)),
        do: apply(Native, unquote(name), [unquote_splicing(args)])
    end

    def execute(conn, sql, params) do
      mode = Agent.get(__MODULE__, & &1)

      if mode == :fail_receipt and String.starts_with?(sql, "INSERT INTO") and
           String.contains?(sql, "\"receipt\"") do
        {:error, :injected_receipt_failure}
      else
        Native.execute(conn, sql, params)
      end
    end

    def commit(conn) do
      mode = Agent.get(__MODULE__, & &1)

      case mode do
        :fail_before_commit ->
          {:error, :injected_before_commit}

        :lost_ack ->
          :ok = Native.commit(conn)
          {:error, :lost_commit_acknowledgement}

        _ ->
          Native.commit(conn)
      end
    end
  end

  test "real precompiled CI command publishes native macros without runtime infrastructure" do
    root = Path.join(System.tmp_dir!(), "catalog-real-cli-#{System.unique_integer([:positive])}")
    File.mkdir_p!(Path.join(root, "config"))
    on_exit(fn -> File.rm_rf!(root) end)
    {:ok, output} = Favn.Semantic.Artifact.write(Fixture.semantic(), root)
    File.write!(Path.join(root, "mix.exs"), "defmodule CatalogCI.Project do
 use Mix.Project
 def project, do: [app: :catalog_ci, version: \"0.1.0\", deps: []]
end")
    File.write!(Path.join(root, "config/runtime.exs"), "raise \"runtime config executed\"")
    provider = Path.join(root, "provider.ex")

    File.write!(provider, """
    defmodule CatalogCI.Warehouse do
      @behaviour Favn.Connection
      def definition, do: %Favn.Connection.Definition{name: :warehouse,
        adapter: Favn.SQL.Adapter.DuckDB.ADBC,
        config_schema: Favn.SQL.Adapter.DuckDB.ADBC.config_schema_fields()}
    end
    """)

    File.write!(Path.join(root, "startup.sql"), "ATTACH '#{root}/mart.duckdb' AS mart;")

    File.write!(Path.join(root, "config/catalog_publish.exs"), """
    import Config
    config :favn,
      catalog_targets: [analytics: [connection: :warehouse, catalog: "mart", schema: "meta"]],
      connection_modules: [warehouse: CatalogCI.Warehouse],
      connections: [warehouse: [open: [database: ":memory:"],
        duckdb: [startup: [file: #{inspect(Path.join(root, "startup.sql"))}], catalogs: [mart: [write_concurrency: 1]]]]],
      duckdb_adbc: [driver: #{inspect(System.fetch_env!("DUCKDB_ADBC_DRIVER"))}, entrypoint: "duckdb_adbc_init"]
    """)

    ebin = Path.join(root, "ebin")
    File.mkdir_p!(ebin)
    build = :code.lib_dir(:favn_core) |> to_string() |> Path.dirname()
    paths = Path.wildcard(Path.join(build, "*/ebin"))

    assert File.exists?(Path.join([build, "favn", "ebin", "Elixir.Favn.Catalog.beam"])),
           "compile the umbrella test build before native qualification"

    erl = Enum.map_join([ebin | paths], " ", &("-pa " <> &1))

    assert {_, 0} =
             System.cmd(System.find_executable("elixirc"), ["-o", ebin, provider],
               env: [{"ELIXIR_ERL_OPTIONS", erl}],
               stderr_to_stdout: true
             )

    args = [
      "favn.catalog.publish",
      "--semantics",
      output.path,
      "--target",
      "analytics",
      "--expect-semantics",
      "none:0"
    ]

    {text, code} =
      System.cmd(System.find_executable("mix"), args,
        cd: root,
        env: [{"ELIXIR_ERL_OPTIONS", erl}, {"MIX_ENV", "test"}],
        stderr_to_stdout: true
      )

    assert code == 0, text
    assert text =~ ~s("outcome":"committed")

    {text, code} =
      System.cmd(System.find_executable("mix"), args ++ ["--reconcile"],
        cd: root,
        env: [{"ELIXIR_ERL_OPTIONS", erl}, {"MIX_ENV", "test"}],
        stderr_to_stdout: true
      )

    assert code == 0, text
    assert text =~ ~s("outcome":"replayed")
  end

  test "publisher reconciles lost commit acknowledgement and explicit replay on fresh sessions" do
    with_publisher(fn registry, opts, _root ->
      request = request([Fixture.semantic()])
      Agent.update(FaultClient, fn _ -> :lost_ack end)

      assert {:ok, receipt} =
               Favn.SQL.Catalog.Publisher.run(request, registry, deadline(), :publish, opts)

      assert receipt["outcome"] == "replayed"
      Agent.update(FaultClient, fn _ -> :normal end)

      assert {:ok, ^receipt} =
               Favn.SQL.Catalog.Publisher.run(request, registry, deadline(), :reconcile, opts)
    end)
  end

  test "publisher rolls back failed receipt, preserves unknown commit and never blindly retries" do
    with_publisher(fn registry, opts, _root ->
      request = request([Fixture.semantic()])

      for mode <- [:fail_receipt, :fail_before_commit] do
        Agent.update(FaultClient, fn _ -> mode end)

        assert {:error, result} =
                 Favn.SQL.Catalog.Publisher.run(request, registry, deadline(), :publish, opts)

        if mode == :fail_before_commit,
          do: assert(result["reason"] == "publication_outcome_unknown")

        assert result["operation_id"] == request.operation_id
        Agent.update(FaultClient, fn _ -> :normal end)

        assert {:error, _} =
                 Favn.SQL.Catalog.Publisher.run(request, registry, deadline(), :reconcile, opts)
      end

      assert {:ok, %{"outcome" => "committed"}} =
               Favn.SQL.Catalog.Publisher.run(request, registry, deadline(), :publish, opts)
    end)
  end

  defp with_publisher(fun) do
    root = Path.join(System.tmp_dir!(), "catalog-publisher-#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)
    script = Path.join(root, "startup.sql")
    File.write!(script, "ATTACH '#{root}/mart.duckdb' AS mart;")

    resolved = %Resolved{
      name: :warehouse,
      adapter: ADBC,
      module: __MODULE__,
      config: %{
        open: [database: ":memory:"],
        duckdb: [startup: [file: script], catalogs: [mart: [write_concurrency: 1]]]
      }
    }

    {:ok, registry} =
      Favn.Connection.Registry.start_link(name: nil, connections: %{warehouse: resolved})

    {:ok, agent} = Agent.start_link(fn -> :normal end, name: FaultClient)

    opts = [
      duckdb_adbc_client: FaultClient,
      duckdb_adbc: [
        driver: System.fetch_env!("DUCKDB_ADBC_DRIVER"),
        entrypoint: "duckdb_adbc_init"
      ]
    ]

    try do
      fun.(registry, opts, root)
    after
      GenServer.stop(registry)
      Agent.stop(agent)
      File.rm_rf!(root)
    end
  end

  for backend <- [:duckdb, :ducklake] do
    @backend backend
    test "#{backend}: independently publishes, retains, replays and rolls back definitions" do
      with_session(@backend, fn session ->
        manifest = Fixture.manifest()
        semantics = Fixture.semantic()
        first = request([manifest, semantics])
        assert :ok = Catalog.qualify(session, first, deadline())
        assert {:ok, receipt} = Catalog.publish(session, first, deadline())
        assert receipt["compatibility"] == "unknown"
        assert receipt["selections"]["manifest"]["revision"] == 1
        assert {:ok, replay} = Catalog.publish(session, first, deadline())
        assert replay["outcome"] == "replayed"
        assert replay["operation_id"] == receipt["operation_id"]

        changed = Fixture.semantic("Revised description")
        next = request([changed], %{"semantic" => receipt["selections"]["semantic"]})
        assert {:ok, updated} = Catalog.publish(session, next, deadline())
        assert updated["selections"]["manifest"] == receipt["selections"]["manifest"]
        assert {:ok, _} = Catalog.publish(session, first, deadline())

        assert {:ok, selected} =
                 ADBC.query(
                   session.conn,
                   "SELECT version FROM mart.meta.selection WHERE context='semantic'",
                   []
                 )

        assert selected.rows == [%{"version" => changed.semantic_version}]

        stale =
          request([Fixture.semantic("Stale")], %{"semantic" => receipt["selections"]["semantic"]})

        assert {:error, %{type: :catalog_conflict}} = Catalog.publish(session, stale, deadline())
        rollback = request([semantics], %{"semantic" => updated["selections"]["semantic"]})
        assert {:ok, rolled_back} = Catalog.publish(session, rollback, deadline())
        assert rolled_back["selections"]["semantic"]["revision"] == 3
        assert {:ok, replay} = Catalog.reconcile(session, first, deadline())
        assert replay["selections"] == receipt["selections"]

        namespace = Semantics.namespace(semantics)
        metric = hd(hd(semantics.models)["metrics"])

        sql =
          "SELECT mart.\"#{namespace}\".\"#{metric["macro_name"]}\"(net, units) AS result FROM (VALUES (90, 3), (120, 2)) source(net, units)"

        assert {:ok, result} = ADBC.query(session.conn, sql, [])
        assert [%{"result" => 42.0}] = result.rows

        assert {:ok, context} =
                 ADBC.query(
                   session.conn,
                   "SELECT context, COUNT(*) AS count FROM mart.meta.\"column\" GROUP BY context ORDER BY context",
                   []
                 )

        assert context.rows == [
                 %{"context" => "manifest", "count" => 3},
                 %{"context" => "semantic", "count" => 6}
               ]
      end)
    end

    test "#{backend}: concurrent initial and existing selections have exactly one winner" do
      with_session(@backend, fn session ->
        for initial <- [true, false] do
          expectations = if initial, do: nil, else: %{"semantic" => selected(session)}

          requests = [
            request([Fixture.semantic("first-#{initial}")], expectations),
            request([Fixture.semantic("second-#{initial}")], expectations)
          ]

          :persistent_term.put({BarrierClient, :parent}, self())

          try do
            resolved = %{
              session.resolved
              | adapter: SharedAdapter,
                config: %{shared_conn: session.conn}
            }

            {:ok, registry} =
              Favn.Connection.Registry.start_link(
                name: nil,
                connections: %{warehouse: resolved}
              )

            tasks =
              Enum.map(requests, fn request ->
                Task.async(fn ->
                  Favn.SQL.Catalog.Publisher.run(request, registry, deadline())
                end)
              end)

            assert_receive {:transaction_started, first}, 5000
            assert_receive {:transaction_started, second}, 5000
            send(first, :continue)
            send(second, :continue)
            results = Enum.map(tasks, &Task.await(&1, 30_000))
            assert Enum.count(results, &match?({:ok, _}, &1)) == 1, inspect(results)
            assert [{:error, conflict}] = Enum.filter(results, &match?({:error, _}, &1))
            assert conflict["reason"] == "catalog_conflict", inspect(results)
            assert is_map(conflict["observed"]), inspect(results)
            assert Map.keys(conflict["observed"]) |> Enum.sort() == ["manifest", "semantic"]
            GenServer.stop(registry)
          after
            :persistent_term.erase({BarrierClient, :parent})
          end
        end
      end)
    end

    test "#{backend}: a fresh read-only connection consumes persisted macros and rejects writes" do
      with_session(@backend, fn session, root ->
        semantic = Fixture.semantic()
        assert {:ok, _} = Catalog.publish(session, request([semantic]), deadline())
        assert {:ok, _} = ADBC.execute(session.conn, "DETACH mart", [])

        location =
          if @backend == :duckdb,
            do: "#{root}/mart.duckdb",
            else: "ducklake:#{root}/catalog.ducklake"

        {:ok, ref} = session.conn.client.connection(session.conn.db_ref)
        readonly = %{session.conn | conn_ref: ref}

        try do
          assert {:ok, _} = ADBC.execute(readonly, "ATTACH '#{location}' AS mart (READ_ONLY)", [])
          namespace = Semantics.namespace(semantic)
          metric = hd(hd(semantic.models)["metrics"])

          assert {:ok, result} =
                   ADBC.query(
                     readonly,
                     "SELECT mart.\"#{namespace}\".\"#{metric["macro_name"]}\"(net, units) AS value FROM (VALUES (90,3),(120,2)) t(net,units)",
                     []
                   )

          assert result.rows == [%{"value" => 42.0}]
          assert {:error, _} = ADBC.execute(readonly, "DELETE FROM mart.meta.selection", [])
        after
          session.conn.client.release(ref)
        end
      end)
    end

    test "#{backend}: failing macro creation rolls back metadata and first bootstrap" do
      with_session(@backend, fn session ->
        original = Fixture.semantic()
        # Trusted-build execution is deliberately made invalid after request validation
        # to inject a native failure after metadata installation.
        request = request([original])

        bad =
          put_in(
            original.models,
            [Access.at(0), "metrics", Access.at(0), "canonical_sql"],
            "missing_function(net)"
          )

        request = %{request | semantic: %{original | models: bad}}
        assert {:error, _} = Catalog.publish(session, request, deadline())

        assert {:ok, result} =
                 ADBC.query(
                   session.conn,
                   "SELECT table_name FROM information_schema.tables WHERE table_catalog='mart' AND table_schema='meta'",
                   []
                 )

        assert result.rows == []
      end)
    end

    test "#{backend}: schemas share verified macros and catalogs install independently" do
      with_session(@backend, fn session, root ->
        semantic = Fixture.semantic()
        assert {:ok, _} = Catalog.publish(session, request([semantic]), deadline())
        other = request([semantic], nil, "meta \"two")
        assert {:ok, _} = Catalog.publish(session, other, deadline())
        namespace = Semantics.namespace(semantic)
        metric = hd(hd(semantic.models)["metrics"])

        assert {:ok, _} =
                 ADBC.execute(
                   session.conn,
                   "DROP MACRO mart.\"#{namespace}\".\"#{metric["macro_name"]}\"",
                   []
                 )

        third = request([semantic], nil, "third")

        assert {:error, %{type: :catalog_integrity_failure}} =
                 Catalog.publish(session, third, deadline())

        location =
          if @backend == :duckdb,
            do: "#{root}/other.duckdb",
            else: "ducklake:#{root}/other.ducklake"

        assert {:ok, _} = ADBC.execute(session.conn, "ATTACH '#{location}' AS other", [])

        {:ok, other_catalog} =
          Request.new(
            "other",
            [connection: :warehouse, catalog: "other", schema: "meta"],
            [semantic],
            %{"semantic" => %{"version" => nil, "revision" => 0}}
          )

        assert {:ok, _} = Catalog.publish(session, other_catalog, deadline())

        assert {:ok, result} =
                 ADBC.query(
                   session.conn,
                   "SELECT other.\"#{namespace}\".\"#{metric["macro_name"]}\"(net, units) AS value FROM (VALUES (90,3),(120,2)) t(net,units)",
                   []
                 )

        assert result.rows == [%{"value" => 42.0}]
      end)
    end
  end

  defp selected(session) do
    {:ok, result} =
      ADBC.query(
        session.conn,
        "SELECT version, revision FROM mart.meta.selection WHERE context='semantic'",
        []
      )

    hd(result.rows)
  end

  defp request(artifacts, expected \\ nil, schema \\ "meta") do
    expected =
      expected ||
        Map.new(artifacts, fn artifact ->
          kind = if is_struct(artifact, Favn.Catalog.Artifact), do: "manifest", else: "semantic"
          {kind, %{"version" => nil, "revision" => 0}}
        end)

    {:ok, request} =
      Request.new(
        "analytics",
        [connection: :warehouse, catalog: "mart", schema: schema],
        artifacts,
        expected
      )

    request
  end

  defp deadline, do: Deadline.new(30_000)

  defp with_session(backend, fun) do
    resolved = %Resolved{
      name: :warehouse,
      adapter: ADBC,
      module: __MODULE__,
      config: %{open: [database: ":memory:"]}
    }

    opts = [
      duckdb_adbc: [
        driver: System.fetch_env!("DUCKDB_ADBC_DRIVER"),
        entrypoint: "duckdb_adbc_init"
      ]
    ]

    assert {:ok, conn} = ADBC.connect(resolved, opts)
    root = Path.join(System.tmp_dir!(), "catalog-native-#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)

    try do
      case backend do
        :duckdb ->
          assert {:ok, _} = ADBC.execute(conn, "ATTACH '#{root}/mart.duckdb' AS mart", [])

        :ducklake ->
          assert {:ok, _} = ADBC.execute(conn, "LOAD ducklake", [])

          assert {:ok, _} =
                   ADBC.execute(
                     conn,
                     "ATTACH 'ducklake:#{root}/catalog.ducklake' AS mart (DATA_PATH '#{root}/data')",
                     []
                   )
      end

      {:ok, capabilities} = ADBC.capabilities(resolved, [])

      session = %Session{
        adapter: ADBC,
        resolved: resolved,
        conn: conn,
        capabilities: capabilities,
        required_catalogs: ["mart"]
      }

      if is_function(fun, 2), do: fun.(session, root), else: fun.(session)
    after
      ADBC.disconnect(conn, [])
      File.rm_rf!(root)
    end
  end
end
