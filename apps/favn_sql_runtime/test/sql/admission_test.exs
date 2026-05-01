defmodule FavnSQLRuntime.SQLAdmissionTest do
  use ExUnit.Case, async: false

  alias Favn.Connection.Registry
  alias Favn.Connection.Resolved
  alias Favn.SQL.{Admission, Client, ConcurrencyPolicy, Result, Session}
  alias Favn.SQL.Admission.Limiter

  defmodule TrackingAdapter do
    def connect(resolved, _opts) do
      tracker = Map.fetch!(resolved.config, :tracker)
      bump_session(tracker, 1)
      {:ok, %{tracker: tracker}}
    end

    def disconnect(conn, _opts) do
      tracker = Map.fetch!(conn, :tracker)
      bump_session(tracker, -1)
      :ok
    end

    def capabilities(_resolved, _opts), do: {:ok, %Favn.SQL.Capabilities{}}

    def default_concurrency_policy(%Resolved{} = resolved) do
      %ConcurrencyPolicy{limit: 1, scope: {:tracker, resolved.name}, applies_to: :all}
    end

    def query(conn, statement, _opts) do
      tracker = Map.fetch!(conn, :tracker)
      bump_active(tracker, 1)
      Process.sleep(50)
      bump_active(tracker, -1)

      {:ok, %Result{kind: :query, command: IO.iodata_to_binary(statement), rows: [], columns: []}}
    end

    def relation(conn, relation_ref, _opts) do
      tracker = Map.fetch!(conn, :tracker)
      bump_active(tracker, 1)
      Process.sleep(50)
      bump_active(tracker, -1)

      {:ok,
       %Favn.SQL.Relation{schema: relation_ref.schema, name: relation_ref.name, type: :table}}
    end

    def columns(conn, _relation_ref, _opts) do
      tracker = Map.fetch!(conn, :tracker)
      bump_active(tracker, 1)
      Process.sleep(50)
      bump_active(tracker, -1)

      {:ok, [%Favn.SQL.Column{name: "id", position: 1, data_type: "INTEGER"}]}
    end

    defp bump_active(tracker, delta) do
      Agent.update(tracker, fn state ->
        active = state.active + delta
        %{state | active: active, max_active: max(state.max_active, active)}
      end)
    end

    defp bump_session(tracker, delta) do
      Agent.update(tracker, fn state ->
        sessions = state.sessions + delta
        %{state | sessions: sessions, max_sessions: max(state.max_sessions, sessions)}
      end)
    end
  end

  defmodule RaisingConnectAdapter do
    def connect(_resolved, _opts), do: raise("connect failed")
    def capabilities(_resolved, _opts), do: {:ok, %Favn.SQL.Capabilities{}}
    def disconnect(_conn, _opts), do: :ok

    def default_concurrency_policy(%Resolved{} = resolved) do
      %ConcurrencyPolicy{limit: 1, scope: {:tracker, resolved.name}, applies_to: :all}
    end
  end

  defmodule ExitingCapabilitiesAdapter do
    def connect(resolved, _opts) do
      tracker = Map.fetch!(resolved.config, :tracker)

      Agent.update(tracker, fn state ->
        sessions = state.sessions + 1
        %{state | sessions: sessions, max_sessions: max(state.max_sessions, sessions)}
      end)

      {:ok, %{tracker: tracker}}
    end

    def capabilities(_resolved, _opts), do: exit(:capabilities_failed)

    def disconnect(conn, _opts) do
      tracker = Map.fetch!(conn, :tracker)
      Agent.update(tracker, fn state -> %{state | sessions: state.sessions - 1} end)
      :ok
    end

    def default_concurrency_policy(%Resolved{} = resolved) do
      %ConcurrencyPolicy{limit: 1, scope: {:tracker, resolved.name}, applies_to: :all}
    end
  end

  setup do
    Limiter.reset()

    {:ok, tracker} =
      Agent.start_link(fn -> %{active: 0, max_active: 0, sessions: 0, max_sessions: 0} end)

    on_exit(fn ->
      Limiter.reset()
      stop_tracker(tracker)
    end)

    {:ok, tracker: tracker}
  end

  test "serializes admitted SQL operations for limited policies", %{tracker: tracker} do
    session = session(tracker, %ConcurrencyPolicy{limit: 1, scope: {:db, :one}, applies_to: :all})

    tasks =
      for _ <- 1..2 do
        Task.async(fn -> Client.query(session, "create or replace table t as select 1", []) end)
      end

    assert Enum.all?(Task.await_many(tasks, 1_000), &match?({:ok, %Result{}}, &1))
    assert Agent.get(tracker, & &1.max_active) == 1
  end

  test "allows concurrent SQL operations for unlimited policies", %{tracker: tracker} do
    session =
      session(tracker, %ConcurrencyPolicy{
        limit: :unlimited,
        scope: {:db, :many},
        applies_to: :all
      })

    tasks =
      for _ <- 1..2 do
        Task.async(fn -> Client.query(session, "create or replace table t as select 1", []) end)
      end

    assert Enum.all?(Task.await_many(tasks, 1_000), &match?({:ok, %Result{}}, &1))
    assert Agent.get(tracker, & &1.max_active) == 2
  end

  test "does not deadlock when a process already holds the same permit", %{tracker: tracker} do
    policy = %ConcurrencyPolicy{limit: 1, scope: {:db, :nested}, applies_to: :all}
    session = session(tracker, policy)

    assert {:ok, %Result{}} =
             Admission.with_permit(session, :transaction, nil, fn ->
               Client.query(session, "create or replace table t as select 1", [])
             end)
  end

  test "releases an active permit when the holder exits", %{tracker: tracker} do
    parent = self()
    policy = %ConcurrencyPolicy{limit: 1, scope: {:db, :holder_down}, applies_to: :all}
    session = session(tracker, policy)

    pid =
      spawn(fn ->
        Admission.with_permit(session, :query, "create table held as select 1", fn ->
          send(parent, :holder_acquired)

          receive do
            :release -> :ok
          end
        end)
      end)

    assert_receive :holder_acquired, 500
    Process.exit(pid, :kill)

    assert {:ok, %Result{}} = Client.query(session, "create or replace table t as select 1", [])
  end

  test "removes queued waiters when they exit before admission", %{tracker: tracker} do
    parent = self()
    policy = %ConcurrencyPolicy{limit: 1, scope: {:db, :waiter_down}, applies_to: :all}
    session = session(tracker, policy)

    holder =
      spawn(fn ->
        Admission.with_permit(session, :query, "create table held as select 1", fn ->
          send(parent, :holder_acquired)

          receive do
            :release -> :ok
          end
        end)
      end)

    assert_receive :holder_acquired, 500

    waiter =
      spawn(fn ->
        send(parent, :waiter_started)
        _ = Client.query(session, "create or replace table queued as select 1", [])
      end)

    assert_receive :waiter_started, 500
    Process.sleep(20)
    Process.exit(waiter, :kill)
    send(holder, :release)

    assert {:ok, %Result{}} = Client.query(session, "create or replace table t as select 1", [])
  end

  test "removes queued operation waiter when admission times out", %{tracker: tracker} do
    parent = self()

    policy = %ConcurrencyPolicy{
      limit: 1,
      scope: {:db, :operation_timeout},
      applies_to: :all,
      admission_timeout_ms: 10
    }

    session = session(tracker, policy)

    holder =
      spawn(fn ->
        Admission.with_permit(session, :query, "create table held as select 1", fn ->
          send(parent, :holder_acquired)

          receive do
            :release -> :ok
          end
        end)
      end)

    assert_receive :holder_acquired, 500

    assert {:error,
            %Favn.SQL.Error{
              type: :admission_timeout,
              operation: :query,
              retryable?: true,
              details: %{scope: {:db, :operation_timeout}, timeout_ms: 10}
            }} = Client.query(session, "create or replace table queued as select 1", [])

    send(holder, :release)
    assert {:ok, %Result{}} = Client.query(session, "create or replace table t as select 1", [])
  end

  test "applies :all policies to relation and columns metadata calls", %{tracker: tracker} do
    session =
      session(tracker, %ConcurrencyPolicy{limit: 1, scope: {:db, :metadata}, applies_to: :all})

    relation_ref = Favn.RelationRef.new!(schema: "main", name: "events")

    tasks = [
      Task.async(fn -> Client.relation(session, relation_ref) end),
      Task.async(fn -> Client.columns(session, relation_ref) end)
    ]

    assert Enum.all?(Task.await_many(tasks, 1_000), &match?({:ok, _}, &1))
    assert Agent.get(tracker, & &1.max_active) == 1
  end

  test "holds admitted local-file sessions until disconnect", %{tracker: tracker} do
    registry_name = :admission_session_registry

    start_registry(registry_name, TrackingAdapter, tracker)

    tasks =
      for _ <- 1..2 do
        Task.async(fn ->
          {:ok, session} = Client.connect(:warehouse, registry_name: registry_name)
          {:ok, %Result{}} = Client.query(session, "select 1", [])
          Process.sleep(50)
          :ok = Client.disconnect(session)
        end)
      end

    Task.await_many(tasks, 1_000)
    assert Agent.get(tracker, & &1.max_sessions) == 1
  end

  test "returns an admission timeout when connect waits too long", %{tracker: tracker} do
    registry_name = :admission_connect_timeout_registry
    start_registry(registry_name, TrackingAdapter, tracker, %{admission_timeout_ms: 10})

    assert {:ok, session_a} = Client.connect(:warehouse, registry_name: registry_name)

    task = Task.async(fn -> Client.connect(:warehouse, registry_name: registry_name) end)

    assert {:ok,
            {:error,
             %Favn.SQL.Error{
               type: :admission_timeout,
               operation: :connect,
               connection: :warehouse,
               retryable?: true,
               details: %{timeout_ms: 10}
             }}} = Task.yield(task, 500)

    assert :ok = Client.disconnect(session_a)
    assert {:ok, session_b} = Client.connect(:warehouse, registry_name: registry_name)
    assert :ok = Client.disconnect(session_b)
  end

  test "same process can open nested admitted sessions", %{tracker: tracker} do
    registry_name = :admission_nested_session_registry
    start_registry(registry_name, TrackingAdapter, tracker)

    assert {:ok, session_a} = Client.connect(:warehouse, registry_name: registry_name)
    assert {:ok, session_b} = Client.connect(:warehouse, registry_name: registry_name)

    assert :ok = Client.disconnect(session_b)
    assert :ok = Client.disconnect(session_a)
  end

  test "out-of-order nested session disconnect keeps permit held", %{tracker: tracker} do
    registry_name = :admission_out_of_order_session_registry
    start_registry(registry_name, TrackingAdapter, tracker)

    assert {:ok, session_a} = Client.connect(:warehouse, registry_name: registry_name)
    assert {:ok, session_b} = Client.connect(:warehouse, registry_name: registry_name)

    assert :ok = Client.disconnect(session_a)

    blocked = Task.async(fn -> Client.connect(:warehouse, registry_name: registry_name) end)

    refute Task.yield(blocked, 100)
    assert :ok = Client.disconnect(session_b)
    assert {:ok, {:ok, session_c}} = Task.yield(blocked, 500)
    assert :ok = Client.disconnect(session_c)
  end

  test "releases admitted session lease when adapter connect raises", %{tracker: tracker} do
    registry_name = :admission_connect_raise_registry
    start_registry(registry_name, RaisingConnectAdapter, tracker)

    assert {:error, %Favn.SQL.Error{operation: :connect}} =
             Client.connect(:warehouse, registry_name: registry_name)

    :ok = Registry.reload(connections(TrackingAdapter, tracker), registry_name: registry_name)

    task = Task.async(fn -> Client.connect(:warehouse, registry_name: registry_name) end)
    assert {:ok, {:ok, session}} = Task.yield(task, 500)
    assert :ok = Client.disconnect(session)
  end

  test "releases admitted session lease and connection when capabilities exits", %{
    tracker: tracker
  } do
    registry_name = :admission_capabilities_exit_registry
    start_registry(registry_name, ExitingCapabilitiesAdapter, tracker)

    assert {:error, %Favn.SQL.Error{operation: :connect}} =
             Client.connect(:warehouse, registry_name: registry_name)

    assert Agent.get(tracker, & &1.sessions) == 0
    :ok = Registry.reload(connections(TrackingAdapter, tracker), registry_name: registry_name)

    task = Task.async(fn -> Client.connect(:warehouse, registry_name: registry_name) end)
    assert {:ok, {:ok, session}} = Task.yield(task, 500)
    assert :ok = Client.disconnect(session)
  end

  defp session(tracker, policy) do
    %Session{
      adapter: TrackingAdapter,
      resolved: %Resolved{
        name: :warehouse,
        adapter: TrackingAdapter,
        module: __MODULE__,
        config: %{}
      },
      conn: %{tracker: tracker},
      capabilities: %Favn.SQL.Capabilities{},
      concurrency_policy: policy
    }
  end

  defp start_registry(registry_name, adapter, tracker, config \\ %{}) do
    start_supervised!(
      {Registry, name: registry_name, connections: connections(adapter, tracker, config)}
    )
  end

  defp connections(adapter, tracker, config \\ %{}) do
    %{
      warehouse: %Resolved{
        name: :warehouse,
        adapter: adapter,
        module: __MODULE__,
        config: Map.put(config, :tracker, tracker)
      }
    }
  end

  defp stop_tracker(tracker) do
    if Process.alive?(tracker) do
      Agent.stop(tracker)
    end

    :ok
  catch
    :exit, _reason -> :ok
  end
end
