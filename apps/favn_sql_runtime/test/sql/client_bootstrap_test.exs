defmodule FavnSQLRuntime.SQLClientBootstrapTest do
  use ExUnit.Case, async: false

  alias Favn.Connection.Registry
  alias Favn.Connection.Resolved
  alias Favn.SQL.Capabilities
  alias Favn.SQL.Client
  alias Favn.SQL.Error
  alias Favn.SQL.GenerationCapabilities
  alias Favn.SQL.GenerationMarkerInitialization
  alias Favn.SQL.PoolConfig
  alias Favn.SQL.Relation
  alias Favn.SQL.Result
  alias Favn.SQL.SessionPool
  alias Favn.SQL.WritePlan
  alias Favn.SQL.Admission.Limiter

  @events_table :favn_sql_client_bootstrap_events

  defmodule AdapterWithBootstrap do
    @behaviour Favn.SQL.Adapter

    @events_table :favn_sql_client_bootstrap_events

    @impl true
    def connect(%Resolved{name: name}, _opts) do
      conn = {:conn, make_ref()}
      record({:connect, name, conn})
      {:ok, conn}
    end

    @impl true
    def bootstrap(conn, %Resolved{name: name}, _opts) do
      record({:bootstrap, name, conn})

      case Application.get_env(:favn, :sql_bootstrap_mode, :ok) do
        :ok ->
          :ok

        :error ->
          {:error,
           %Error{
             type: :connection_error,
             message: "bootstrap failed",
             operation: :bootstrap,
             connection: name
           }}

        :raise ->
          raise "bootstrap boom"

        :exit ->
          exit(:bootstrap_boom)
      end
    end

    @impl true
    def disconnect(conn, _opts) do
      record({:disconnect, conn})
      :ok
    end

    @impl true
    def capabilities(%Resolved{name: name}, _opts) do
      record({:capabilities, name})
      {:ok, %Capabilities{}}
    end

    @impl true
    def execute(conn, statement, _opts) do
      record({:execute, conn, IO.iodata_to_binary(statement)})
      {:ok, %Result{kind: :execute, command: IO.iodata_to_binary(statement)}}
    end

    @impl true
    def query(conn, statement, _opts) do
      record({:query, conn, IO.iodata_to_binary(statement)})
      {:ok, %Result{kind: :query, command: IO.iodata_to_binary(statement), rows: []}}
    end

    @impl true
    def introspection_query(_kind, _payload, _opts), do: {:ok, "SELECT 1"}

    @impl true
    def materialization_statements(_plan, _capabilities, _opts), do: {:ok, []}

    defp record(event) do
      if :ets.whereis(@events_table) != :undefined do
        :ets.insert(@events_table, {System.unique_integer([:positive, :monotonic]), event})
      end
    end
  end

  defmodule AdapterWithoutBootstrap do
    @behaviour Favn.SQL.Adapter

    @events_table :favn_sql_client_bootstrap_events

    @impl true
    def connect(%Resolved{name: name}, _opts) do
      conn = {:conn, make_ref()}
      record({:connect, name, conn})
      {:ok, conn}
    end

    @impl true
    def disconnect(conn, _opts) do
      record({:disconnect, conn})
      :ok
    end

    @impl true
    def capabilities(%Resolved{name: name}, _opts) do
      record({:capabilities, name})
      {:ok, %Capabilities{}}
    end

    @impl true
    def execute(_conn, statement, _opts),
      do: {:ok, %Result{kind: :execute, command: IO.iodata_to_binary(statement)}}

    @impl true
    def query(_conn, statement, _opts),
      do: {:ok, %Result{kind: :query, command: IO.iodata_to_binary(statement), rows: []}}

    @impl true
    def introspection_query(_kind, _payload, _opts), do: {:ok, "SELECT 1"}

    @impl true
    def materialization_statements(_plan, _capabilities, _opts), do: {:ok, []}

    defp record(event) do
      if :ets.whereis(@events_table) != :undefined do
        :ets.insert(@events_table, {System.unique_integer([:positive, :monotonic]), event})
      end
    end
  end

  defmodule AdapterWithPool do
    @behaviour Favn.SQL.Adapter

    @events_table :favn_sql_client_bootstrap_events

    @impl true
    def connect(%Resolved{name: name}, _opts) do
      conn = {:pooled_conn, make_ref()}
      record({:connect, name, conn})
      {:ok, conn}
    end

    @impl true
    def bootstrap(conn, %Resolved{name: name}, _opts) do
      record({:bootstrap, name, conn})

      case Application.get_env(:favn, :sql_pool_bootstrap_mode, :ok) do
        :ok ->
          :ok

        :fail_once ->
          if event_count({:bootstrap, name}) == 1 do
            {:error,
             %Error{
               type: :connection_error,
               message: "transient bootstrap failure",
               operation: :bootstrap,
               connection: name,
               retryable?: true
             }}
          else
            :ok
          end

        {:block, test_pid} ->
          send(test_pid, {:bootstrap_started, conn, self()})

          receive do
            :continue_bootstrap -> :ok
          after
            1_000 -> :ok
          end
      end
    end

    @impl true
    def disconnect(conn, _opts) do
      record({:disconnect, conn})
      :ok
    end

    @impl true
    def capabilities(%Resolved{name: name}, _opts) do
      record({:capabilities, name})
      {:ok, %Capabilities{}}
    end

    @impl true
    def poolable?(%Resolved{}, _opts), do: true

    @impl true
    def prepare_pool(%Resolved{}, _opts), do: {:ok, :test_pool_adapter, nil}

    @impl true
    def concurrency_policies(%Resolved{} = resolved) do
      limit = Map.get(resolved.config, :raw_write_concurrency, 1)
      {:ok, [Favn.SQL.ConcurrencyPolicy.catalog(resolved, "raw", limit)]}
    end

    @impl true
    def validate_session(conn, _opts) do
      record({:validate, conn})
      :ok
    end

    @impl true
    def reset_session(conn, %Resolved{}, _opts) do
      record({:reset, conn})
      :ok
    end

    @impl true
    def execute(conn, statement, _opts) do
      record({:execute, conn, IO.iodata_to_binary(statement)})
      {:ok, %Result{kind: :execute, command: IO.iodata_to_binary(statement)}}
    end

    @impl true
    def materialize(conn, %WritePlan{}, _opts) do
      record({:materialize, conn})

      case Application.get_env(:favn, :sql_materialize_mode, :ok) do
        :ok ->
          {:ok, %Result{kind: :execute, command: "MATERIALIZE", rows_affected: 1}}

        :unknown_commit ->
          {:error,
           %Error{
             type: :connection_error,
             message: "materialization outcome is unknown",
             operation: :materialize,
             retryable?: true,
             details: %{classification: :unknown_commit_state}
           }}
      end
    end

    @impl true
    def transaction(conn, fun, _opts) do
      record({:transaction, conn})
      fun.(conn)
    end

    @impl true
    def query(conn, statement, _opts) do
      record({:query, conn, IO.iodata_to_binary(statement)})
      {:ok, %Result{kind: :query, command: IO.iodata_to_binary(statement), rows: []}}
    end

    @impl true
    def introspection_query(_kind, _payload, _opts), do: {:ok, "SELECT 1"}

    @impl true
    def materialization_statements(_plan, _capabilities, _opts), do: {:ok, []}

    def generation_capabilities(_resolved, _opts), do: {:ok, %GenerationCapabilities{}}
    def inspect_generation(_conn, _relation, _opts), do: {:ok, :not_found}

    def initialize_generation_marker(conn, _request, _opts) do
      record({:initialize_generation_marker, conn})
      {:ok, :initialized}
    end

    def activate_generation(_conn, _request, _opts), do: {:ok, :activated}
    def reconcile_generation(_conn, _request, _opts), do: {:ok, nil}
    def discard_generation(_conn, _request, _opts), do: :ok

    defp record(event) do
      if :ets.whereis(@events_table) != :undefined do
        :ets.insert(@events_table, {System.unique_integer([:positive, :monotonic]), event})
      end
    end

    defp event_count({kind, name}) do
      if :ets.whereis(@events_table) == :undefined do
        0
      else
        @events_table
        |> :ets.tab2list()
        |> Enum.count(fn {_id, event} -> match?({^kind, ^name, _conn}, event) end)
      end
    end
  end

  setup do
    SessionPool.reset()
    Limiter.reset()

    if :ets.whereis(@events_table) != :undefined do
      :ets.delete(@events_table)
    end

    :ets.new(@events_table, [:named_table, :ordered_set, :public])
    Application.put_env(:favn, :sql_bootstrap_mode, :ok)
    Application.put_env(:favn, :sql_pool_bootstrap_mode, :ok)
    Application.put_env(:favn, :sql_materialize_mode, :ok)

    registry_name = Module.concat(__MODULE__, "Registry#{System.unique_integer([:positive])}")

    on_exit(fn ->
      SessionPool.reset()
      Limiter.reset()

      Application.delete_env(:favn, :sql_bootstrap_mode)
      Application.delete_env(:favn, :sql_pool_bootstrap_mode)
      Application.delete_env(:favn, :sql_materialize_mode)

      if :ets.whereis(@events_table) != :undefined do
        :ets.delete(@events_table)
      end
    end)

    {:ok, registry_name: registry_name}
  end

  test "runs adapter bootstrap before returning a usable session", %{registry_name: registry_name} do
    start_registry(registry_name, AdapterWithBootstrap)

    assert {:ok, session} = Client.connect(:warehouse, registry_name: registry_name)
    assert {:ok, %Result{}} = Client.query(session, "SELECT 1", [])
    Client.disconnect(session)

    assert [
             {:connect, :warehouse, conn},
             {:bootstrap, :warehouse, conn},
             {:capabilities, :warehouse},
             {:query, conn, "SELECT 1"},
             {:disconnect, conn}
           ] = events()
  end

  test "disconnects and releases the session when bootstrap fails", %{
    registry_name: registry_name
  } do
    start_registry(registry_name, AdapterWithBootstrap)
    Application.put_env(:favn, :sql_bootstrap_mode, :error)

    assert {:error, %Error{operation: :bootstrap, connection: :warehouse}} =
             Client.connect(:warehouse, registry_name: registry_name)

    assert [
             {:connect, :warehouse, conn},
             {:bootstrap, :warehouse, conn},
             {:disconnect, conn}
           ] = events()
  end

  test "disconnects when bootstrap raises", %{registry_name: registry_name} do
    start_registry(registry_name, AdapterWithBootstrap)
    Application.put_env(:favn, :sql_bootstrap_mode, :raise)

    assert {:error, %Error{operation: :connect}} =
             Client.connect(:warehouse, registry_name: registry_name)

    assert [
             {:connect, :warehouse, conn},
             {:bootstrap, :warehouse, conn},
             {:disconnect, conn}
           ] = events()
  end

  test "disconnects when bootstrap exits", %{registry_name: registry_name} do
    start_registry(registry_name, AdapterWithBootstrap)
    Application.put_env(:favn, :sql_bootstrap_mode, :exit)

    assert {:error, %Error{operation: :connect}} =
             Client.connect(:warehouse, registry_name: registry_name)

    assert [
             {:connect, :warehouse, conn},
             {:bootstrap, :warehouse, conn},
             {:disconnect, conn}
           ] = events()
  end

  test "adapters without bootstrap callback still connect", %{registry_name: registry_name} do
    start_registry(registry_name, AdapterWithoutBootstrap)

    assert {:ok, session} = Client.connect(:warehouse, registry_name: registry_name)
    Client.disconnect(session)

    assert [
             {:connect, :warehouse, conn},
             {:capabilities, :warehouse},
             {:disconnect, conn}
           ] = events()
  end

  test "reuses pooled sessions for the same connection and catalog set", %{
    registry_name: registry_name
  } do
    pool = %PoolConfig{enabled: true, max_idle_per_key: 1, idle_timeout_ms: 50}
    start_registry(registry_name, AdapterWithPool, %{pool: pool})

    assert {:ok, first} = Client.connect(:warehouse, registry_name: registry_name)
    assert {:ok, %Result{}} = Client.query(first, "SELECT 1", read_only?: true)
    Client.disconnect(first)

    assert {:ok, second} = Client.connect(:warehouse, registry_name: registry_name)
    assert {:ok, %Result{}} = Client.query(second, "SELECT 2", read_only?: true)
    Client.disconnect(second)

    assert [
             {:connect, :warehouse, conn},
             {:bootstrap, :warehouse, conn},
             {:capabilities, :warehouse},
             {:query, conn, "SELECT 1"},
             {:validate, conn},
             {:reset, conn},
             {:query, conn, "SELECT 2"}
           ] = events()

    assert eventually(fn -> {:disconnect, conn} in events() end)
  end

  test "different required catalog sets do not reuse pooled sessions", %{
    registry_name: registry_name
  } do
    pool = %PoolConfig{enabled: true, max_idle_per_key: 2, idle_timeout_ms: 60_000}
    start_registry(registry_name, AdapterWithPool, %{pool: pool})

    assert {:ok, first} =
             Client.connect(:warehouse, registry_name: registry_name, required_catalogs: ["raw"])

    Client.disconnect(first)

    assert {:ok, second} =
             Client.connect(:warehouse, registry_name: registry_name, required_catalogs: ["mart"])

    Client.disconnect(second)

    assert Enum.count(events(), &match?({:connect, :warehouse, _conn}, &1)) == 2
    refute Enum.any?(events(), &match?({:validate, _conn}, &1))
  end

  test "equivalent required catalog sets reuse pooled sessions", %{
    registry_name: registry_name
  } do
    pool = %PoolConfig{enabled: true, max_idle_per_key: 2, idle_timeout_ms: 60_000}
    start_registry(registry_name, AdapterWithPool, %{pool: pool})

    assert {:ok, first} =
             Client.connect(:warehouse,
               registry_name: registry_name,
               required_catalogs: ["raw", :raw, ""]
             )

    Client.disconnect(first)

    assert {:ok, second} =
             Client.connect(:warehouse, registry_name: registry_name, required_catalogs: ["raw"])

    Client.disconnect(second)

    assert Enum.count(events(), &match?({:connect, :warehouse, _conn}, &1)) == 1
    assert Enum.any?(events(), &match?({:validate, _conn}, &1))
  end

  test "two concurrent checkouts never receive the same pooled session", %{
    registry_name: registry_name
  } do
    pool = %PoolConfig{enabled: true, max_idle_per_key: 1, idle_timeout_ms: 60_000}
    start_registry(registry_name, AdapterWithPool, %{pool: pool})

    assert {:ok, warm} = Client.connect(:warehouse, registry_name: registry_name)
    warm_conn = warm.conn
    Client.disconnect(warm)

    first = Task.async(fn -> Client.connect(:warehouse, registry_name: registry_name) end)
    second = Task.async(fn -> Client.connect(:warehouse, registry_name: registry_name) end)

    assert {:ok, first_session} = Task.await(first)
    assert {:ok, second_session} = Task.await(second)
    refute first_session.conn == second_session.conn
    assert warm_conn in [first_session.conn, second_session.conn]

    Client.disconnect(first_session)
    Client.disconnect(second_session)
  end

  test "bootstrap failure retries and then succeeds for pooled creation", %{
    registry_name: registry_name
  } do
    pool = %PoolConfig{enabled: true, max_idle_per_key: 1, idle_timeout_ms: 60_000}
    start_registry(registry_name, AdapterWithPool, %{pool: pool})
    Application.put_env(:favn, :sql_pool_bootstrap_mode, :fail_once)

    assert {:ok, session} = Client.connect(:warehouse, registry_name: registry_name)
    Client.disconnect(session)

    assert Enum.count(events(), &match?({:connect, :warehouse, _conn}, &1)) == 2
    assert Enum.count(events(), &match?({:bootstrap, :warehouse, _conn}, &1)) == 2
    assert Enum.any?(events(), &match?({:disconnect, _conn}, &1))
  end

  test "failed materialization discards the pooled session and does not retry", %{
    registry_name: registry_name
  } do
    pool = %PoolConfig{enabled: true, max_idle_per_key: 1, idle_timeout_ms: 60_000}
    start_registry(registry_name, AdapterWithPool, %{pool: pool})
    Application.put_env(:favn, :sql_materialize_mode, :unknown_commit)

    assert {:ok, session} = Client.connect(:warehouse, registry_name: registry_name)
    failed_conn = session.conn

    assert {:error, %Error{}} = Client.materialize(session, write_plan(), [])
    Client.disconnect(session)

    Application.put_env(:favn, :sql_materialize_mode, :ok)
    assert {:ok, next} = Client.connect(:warehouse, registry_name: registry_name)
    refute next.conn == failed_conn
    Client.disconnect(next)

    assert Enum.count(events(), &match?({:materialize, ^failed_conn}, &1)) == 1
    assert {:disconnect, failed_conn} in events()
  end

  test "successful raw execute discards the pooled session by default", %{
    registry_name: registry_name
  } do
    pool = %PoolConfig{enabled: true, max_idle_per_key: 1, idle_timeout_ms: 60_000}
    start_registry(registry_name, AdapterWithPool, %{pool: pool})

    assert {:ok, session} = Client.connect(:warehouse, registry_name: registry_name)
    executed_conn = session.conn

    assert {:ok, %Result{}} = Client.execute(session, "CREATE TEMP TABLE t AS SELECT 1", [])
    Client.disconnect(session)

    assert {:ok, next} = Client.connect(:warehouse, registry_name: registry_name)
    refute next.conn == executed_conn
    Client.disconnect(next)

    assert {:disconnect, executed_conn} in events()
  end

  test "successful marker initialization discards the mutated pooled session", %{
    registry_name: registry_name
  } do
    pool = %PoolConfig{enabled: true, max_idle_per_key: 1, idle_timeout_ms: 60_000}
    start_registry(registry_name, AdapterWithPool, %{pool: pool})

    assert {:ok, session} = Client.connect(:warehouse, registry_name: registry_name)
    initialized_conn = session.conn

    request = %GenerationMarkerInitialization{
      logical_target_id: "asset:orders",
      stable_relation: %Favn.RelationRef{
        connection: :warehouse,
        schema: "analytics",
        name: "orders"
      },
      active_generation_id: "018f47a0-7b0d-4b1a-8d8b-e18a9a987654",
      expected_physical_fingerprint: String.duplicate("a", 64),
      initialization_operation_id: "initial-materialization",
      initialization_token: "initial-marker-token",
      initialized_at: ~U[2026-07-22 10:00:00Z]
    }

    assert {:ok, :initialized} = Client.initialize_generation_marker(session, request, [])
    Client.disconnect(session)

    assert {:ok, next} = Client.connect(:warehouse, registry_name: registry_name)
    refute next.conn == initialized_conn
    Client.disconnect(next)

    assert {:initialize_generation_marker, initialized_conn} in events()
    assert {:disconnect, initialized_conn} in events()
  end

  test "non-owner process cannot run pooled session operations", %{registry_name: registry_name} do
    pool = %PoolConfig{enabled: true, max_idle_per_key: 1, idle_timeout_ms: 60_000}
    start_registry(registry_name, AdapterWithPool, %{pool: pool})

    assert {:ok, session} = Client.connect(:warehouse, registry_name: registry_name)
    conn = session.conn

    operations = [
      fn -> Client.query(session, "SELECT 1", read_only?: true) end,
      fn -> Client.execute(session, "CREATE TEMP TABLE cross_process AS SELECT 1", []) end,
      fn -> Client.materialize(session, write_plan(), []) end,
      fn -> Client.transaction(session, fn _tx -> {:ok, :ran} end, []) end
    ]

    results =
      operations
      |> Enum.map(fn operation -> Task.async(operation) end)
      |> Task.await_many()

    assert Enum.all?(results, &match?({:error, %Error{type: :invalid_checkout_owner}}, &1))
    refute Enum.any?(events(), &match?({:query, ^conn, _statement}, &1))
    refute Enum.any?(events(), &match?({:execute, ^conn, _statement}, &1))
    refute Enum.any?(events(), &match?({:materialize, ^conn}, &1))
    refute Enum.any?(events(), &match?({:transaction, ^conn}, &1))

    Client.disconnect(session)

    assert {:ok, next} = Client.connect(:warehouse, registry_name: registry_name)
    refute next.conn == conn
    Client.disconnect(next)

    assert {:disconnect, conn} in events()
  end

  test "non-owner disconnect does not return session to idle or release admission", %{
    registry_name: registry_name
  } do
    pool = %PoolConfig{enabled: true, max_idle_per_key: 1, idle_timeout_ms: 60_000}

    start_registry(registry_name, AdapterWithPool, %{
      pool: pool,
      write_concurrency: 1,
      admission_timeout_ms: 10
    })

    connect_opts = [registry_name: registry_name, required_catalogs: ["raw"]]

    assert {:ok, session} = Client.connect(:warehouse, connect_opts)
    conn = session.conn

    non_owner_disconnect = Task.async(fn -> Client.disconnect(session) end)

    assert {:error, %Error{type: :invalid_checkout_owner, operation: :disconnect}} =
             Task.await(non_owner_disconnect)

    blocked = Task.async(fn -> Client.connect(:warehouse, connect_opts) end)
    assert {:ok, {:error, %Error{type: :admission_timeout}}} = Task.yield(blocked, 1_000)
    refute Enum.any?(events(), &match?({:validate, ^conn}, &1))

    assert :ok = Client.disconnect(session)

    assert {:ok, next} = Client.connect(:warehouse, connect_opts)
    refute next.conn == conn
    Client.disconnect(next)
  end

  test "concurrent non-owner calls cannot share a checked-out pooled session", %{
    registry_name: registry_name
  } do
    pool = %PoolConfig{enabled: true, max_idle_per_key: 1, idle_timeout_ms: 60_000}
    start_registry(registry_name, AdapterWithPool, %{pool: pool})

    assert {:ok, session} = Client.connect(:warehouse, registry_name: registry_name)
    conn = session.conn

    first = Task.async(fn -> Client.query(session, "SELECT 1", read_only?: true) end)
    second = Task.async(fn -> Client.query(session, "SELECT 2", read_only?: true) end)

    assert [
             {:error, %Error{type: :invalid_checkout_owner}},
             {:error, %Error{type: :invalid_checkout_owner}}
           ] = Task.await_many([first, second])

    refute Enum.any?(events(), &match?({:query, ^conn, _statement}, &1))

    Client.disconnect(session)
  end

  test "concurrent same-key misses serialize session creation", %{registry_name: registry_name} do
    pool = %PoolConfig{enabled: true, max_idle_per_key: 1, idle_timeout_ms: 60_000}
    start_registry(registry_name, AdapterWithPool, %{pool: pool})
    Application.put_env(:favn, :sql_pool_bootstrap_mode, {:block, self()})

    first = Task.async(fn -> Client.connect(:warehouse, registry_name: registry_name) end)
    assert_receive {:bootstrap_started, first_conn, first_pid}

    second = Task.async(fn -> Client.connect(:warehouse, registry_name: registry_name) end)
    Process.sleep(25)

    assert Enum.count(events(), &match?({:connect, :warehouse, _conn}, &1)) == 1

    Application.put_env(:favn, :sql_pool_bootstrap_mode, :ok)
    send(first_pid, :continue_bootstrap)

    assert {:ok, first_session} = Task.await(first)
    assert first_session.conn == first_conn
    assert {:ok, second_session} = Task.await(second)
    refute second_session.conn == first_session.conn

    Client.disconnect(first_session)
    Client.disconnect(second_session)
  end

  test "concurrent same-key misses create sessions up to catalog capacity", %{
    registry_name: registry_name
  } do
    pool = %PoolConfig{enabled: true, max_idle_per_key: 1, idle_timeout_ms: 60_000}

    start_registry(registry_name, AdapterWithPool, %{
      pool: pool,
      raw_write_concurrency: 2
    })

    Application.put_env(:favn, :sql_pool_bootstrap_mode, {:block, self()})
    connect_opts = [registry_name: registry_name, required_catalogs: ["raw"]]

    first = Task.async(fn -> Client.connect(:warehouse, connect_opts) end)
    assert_receive {:bootstrap_started, first_conn, first_pid}, 500

    second = Task.async(fn -> Client.connect(:warehouse, connect_opts) end)
    assert_receive {:bootstrap_started, second_conn, second_pid}, 500
    refute second_conn == first_conn

    assert Enum.count(events(), &match?({:connect, :warehouse, _conn}, &1)) == 2

    Application.put_env(:favn, :sql_pool_bootstrap_mode, :ok)
    send(first_pid, :continue_bootstrap)
    send(second_pid, :continue_bootstrap)

    assert {:ok, first_session} = Task.await(first)
    assert {:ok, second_session} = Task.await(second)

    Client.disconnect(first_session)
    Client.disconnect(second_session)
  end

  test "concurrent same-key misses do not create beyond catalog capacity", %{
    registry_name: registry_name
  } do
    pool = %PoolConfig{enabled: true, max_idle_per_key: 1, idle_timeout_ms: 60_000}

    start_registry(registry_name, AdapterWithPool, %{
      pool: pool,
      raw_write_concurrency: 2
    })

    Application.put_env(:favn, :sql_pool_bootstrap_mode, {:block, self()})
    connect_opts = [registry_name: registry_name, required_catalogs: ["raw"]]

    first = Task.async(fn -> Client.connect(:warehouse, connect_opts) end)
    assert_receive {:bootstrap_started, first_conn, first_pid}, 500

    second = Task.async(fn -> Client.connect(:warehouse, connect_opts) end)
    assert_receive {:bootstrap_started, second_conn, second_pid}, 500
    refute second_conn == first_conn

    third = Task.async(fn -> Client.connect(:warehouse, connect_opts) end)

    refute_receive {:bootstrap_started, _third_conn, _third_pid}, 50
    assert Enum.count(events(), &match?({:connect, :warehouse, _conn}, &1)) == 2

    send(first_pid, :continue_bootstrap)

    assert {:ok, first_session} = Task.await(first)

    assert_receive {:bootstrap_started, third_conn, third_pid}, 500
    refute third_conn in [first_conn, second_conn]

    Application.put_env(:favn, :sql_pool_bootstrap_mode, :ok)
    send(second_pid, :continue_bootstrap)
    send(third_pid, :continue_bootstrap)

    assert {:ok, second_session} = Task.await(second)
    assert {:ok, third_session} = Task.await(third)

    Client.disconnect(first_session)
    Client.disconnect(second_session)
    Client.disconnect(third_session)
  end

  defp start_registry(registry_name, adapter, config \\ %{}) do
    resolved = %Resolved{
      name: :warehouse,
      adapter: adapter,
      module: __MODULE__,
      config: Map.put(config, :test_nonce, System.unique_integer([:positive])),
      secret_fields: []
    }

    start_supervised!({Registry, name: registry_name, connections: %{warehouse: resolved}})
  end

  defp events do
    @events_table
    |> :ets.tab2list()
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.map(&elem(&1, 1))
  end

  defp write_plan do
    %WritePlan{
      materialization: :table,
      target: %Relation{name: "target", type: :table, catalog: "raw"},
      select_sql: "SELECT 1"
    }
  end

  defp eventually(fun, attempts \\ 20)

  defp eventually(fun, attempts) when attempts > 0 do
    if fun.() do
      true
    else
      Process.sleep(10)
      eventually(fun, attempts - 1)
    end
  end

  defp eventually(_fun, 0), do: false
end
