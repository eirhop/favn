defmodule FavnSQLRuntime.SQLAdmissionTest do
  use ExUnit.Case, async: false

  alias Favn.Connection.Resolved
  alias Favn.SQL.{Admission, Client, ConcurrencyPolicy, Result, Session}
  alias Favn.SQL.Admission.Limiter

  defmodule TrackingAdapter do
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
  end

  setup do
    Limiter.reset()
    {:ok, tracker} = Agent.start_link(fn -> %{active: 0, max_active: 0} end)

    on_exit(fn ->
      Limiter.reset()

      if Process.alive?(tracker) do
        Agent.stop(tracker)
      end
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
      session(tracker, %ConcurrencyPolicy{limit: :unlimited, scope: {:db, :many}, applies_to: :all})

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

  test "applies :all policies to relation and columns metadata calls", %{tracker: tracker} do
    session = session(tracker, %ConcurrencyPolicy{limit: 1, scope: {:db, :metadata}, applies_to: :all})
    relation_ref = Favn.RelationRef.new!(schema: "main", name: "events")

    tasks = [
      Task.async(fn -> Client.relation(session, relation_ref) end),
      Task.async(fn -> Client.columns(session, relation_ref) end)
    ]

    assert Enum.all?(Task.await_many(tasks, 1_000), &match?({:ok, _}, &1))
    assert Agent.get(tracker, & &1.max_active) == 1
  end

  defp session(tracker, policy) do
    %Session{
      adapter: TrackingAdapter,
      resolved: %Resolved{name: :warehouse, adapter: TrackingAdapter, module: __MODULE__, config: %{}},
      conn: %{tracker: tracker},
      capabilities: %Favn.SQL.Capabilities{},
      concurrency_policy: policy
    }
  end
end
