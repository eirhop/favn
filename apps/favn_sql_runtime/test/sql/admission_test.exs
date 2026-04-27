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
