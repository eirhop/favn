defmodule FavnOrchestrator.ExecutionAdmissionTest do
  use ExUnit.Case, async: false

  alias FavnOrchestrator.ExecutionAdmission
  alias FavnOrchestrator.ExecutionAdmission.Waiter
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage
  alias FavnOrchestrator.Storage.Adapter.Memory

  defmodule ReleaseDuringWaiterStorageAdapter do
    @moduledoc false
    @behaviour Favn.Storage.Adapter

    callbacks =
      Favn.Storage.Adapter.behaviour_info(:callbacks) --
        Favn.Storage.Adapter.behaviour_info(:optional_callbacks)

    for {name, arity} <- callbacks, name != :upsert_execution_admission_waiter do
      args = Macro.generate_arguments(arity, __MODULE__)

      @impl true
      def unquote(name)(unquote_splicing(args)) do
        apply(FavnOrchestrator.Storage.Adapter.Memory, unquote(name), [unquote_splicing(args)])
      end
    end

    @impl true
    def upsert_execution_admission_waiter(waiter, opts) when is_map(waiter) and is_list(opts) do
      result =
        FavnOrchestrator.Storage.Adapter.Memory.upsert_execution_admission_waiter(waiter, opts)

      if lease_id = Keyword.get(opts, :release_lease_id) do
        :ok = FavnOrchestrator.Storage.Adapter.Memory.release_execution_lease(lease_id, opts)
      end

      result
    end
  end

  defmodule NoGlobalLeaseScanStorageAdapter do
    @moduledoc false
    @behaviour Favn.Storage.Adapter

    callbacks =
      Favn.Storage.Adapter.behaviour_info(:callbacks) --
        Favn.Storage.Adapter.behaviour_info(:optional_callbacks)

    excluded = [:list_execution_leases, :release_execution_leases_for_run]

    for {name, arity} <- callbacks, name not in excluded do
      args = Macro.generate_arguments(arity, __MODULE__)

      @impl true
      def unquote(name)(unquote_splicing(args)) do
        apply(FavnOrchestrator.Storage.Adapter.Memory, unquote(name), [unquote_splicing(args)])
      end
    end

    @impl true
    def list_execution_leases(opts) when is_list(opts) do
      if owner = Keyword.get(opts, :owner), do: send(owner, :global_lease_scan)
      {:error, :global_lease_scan_not_allowed}
    end

    @impl true
    def release_execution_leases_for_run(run_id, opts) when is_binary(run_id) and is_list(opts) do
      if owner = Keyword.get(opts, :owner), do: send(owner, {:release_leases_for_run, run_id})
      FavnOrchestrator.Storage.Adapter.Memory.release_execution_leases_for_run(run_id, opts)
    end
  end

  defmodule WakeCountingStorageAdapter do
    @moduledoc false
    @behaviour Favn.Storage.Adapter

    callbacks =
      Favn.Storage.Adapter.behaviour_info(:callbacks) --
        Favn.Storage.Adapter.behaviour_info(:optional_callbacks)

    excluded = [:expire_execution_admission_waiters]

    for {name, arity} <- callbacks, name not in excluded do
      args = Macro.generate_arguments(arity, __MODULE__)

      @impl true
      def unquote(name)(unquote_splicing(args)) do
        apply(FavnOrchestrator.Storage.Adapter.Memory, unquote(name), [unquote_splicing(args)])
      end
    end

    @impl true
    def expire_execution_admission_waiters(now, opts) when is_list(opts) do
      if owner = Keyword.get(opts, :owner), do: send(owner, :expired_waiters)
      FavnOrchestrator.Storage.Adapter.Memory.expire_execution_admission_waiters(now, opts)
    end
  end

  setup do
    Memory.reset()

    previous_pools = Application.get_env(:favn, :execution_pools)
    previous_ttl = Application.get_env(:favn, :execution_lease_ttl_ms)
    previous_adapter = Application.get_env(:favn_orchestrator, :storage_adapter)
    previous_adapter_opts = Application.get_env(:favn_orchestrator, :storage_adapter_opts)

    on_exit(fn ->
      Memory.reset()
      restore_env(:favn, :execution_pools, previous_pools)
      restore_env(:favn, :execution_lease_ttl_ms, previous_ttl)
      restore_env(:favn_orchestrator, :storage_adapter, previous_adapter)
      restore_env(:favn_orchestrator, :storage_adapter_opts, previous_adapter_opts)
    end)

    :ok
  end

  test "acquires and releases run and pool scoped leases" do
    Application.put_env(:favn, :execution_pools, github_api: [max_concurrency: 1])

    run = run(max_concurrency: 1)
    entry = %{asset_step_id: "step-1", execution_pool: :github_api}

    assert {:ok, lease} = ExecutionAdmission.acquire(run, entry)

    assert lease.scopes == [
             %{kind: :run, key: run.id, limit: 1},
             %{kind: :pool, key: "github_api", limit: 1}
           ]

    assert {:queued, :pipeline_concurrency, %{kind: :run}} =
             ExecutionAdmission.acquire(run, %{entry | asset_step_id: "step-2"})

    assert :ok = ExecutionAdmission.release(lease)
    assert {:ok, _lease} = ExecutionAdmission.acquire(run, %{entry | asset_step_id: "step-2"})
  end

  test "global pool applies to every admitted step" do
    Application.put_env(:favn, :execution_pools, global: [max_concurrency: 1])

    run_a = run(id: "run-a")
    run_b = run(id: "run-b")

    assert {:ok, _lease} = ExecutionAdmission.acquire(run_a, %{asset_step_id: "step-a"})

    assert {:queued, :global_concurrency, %{kind: :global}} =
             ExecutionAdmission.acquire(run_b, %{asset_step_id: "step-b"})
  end

  test "lease ttl covers run timeout even when configured ttl is shorter" do
    Application.put_env(:favn, :execution_lease_ttl_ms, 50)

    run = run(max_concurrency: 1, timeout_ms: 1_000)

    assert {:ok, lease} = ExecutionAdmission.acquire(run, %{asset_step_id: "step-1"})
    assert DateTime.diff(lease.expires_at, lease.acquired_at, :millisecond) > 1_000

    Process.sleep(70)

    assert {:queued, :pipeline_concurrency, %{kind: :run}} =
             ExecutionAdmission.acquire(run, %{asset_step_id: "step-2"})
  end

  test "unknown execution pool fails closed" do
    run = run(max_concurrency: 2)

    assert {:error, {:unknown_execution_pool, :missing_pool}} =
             ExecutionAdmission.acquire(run, %{
               asset_step_id: "step-1",
               execution_pool: :missing_pool
             })
  end

  test "normalizes string-keyed entries and rejects malformed entry identities" do
    run = run(max_concurrency: 1)

    assert {:ok, lease} = ExecutionAdmission.acquire(run, %{"asset_step_id" => "step-string"})
    assert lease.asset_step_id == "step-string"

    assert {:error, {:invalid_execution_admission_entry, :asset_step_id}} =
             ExecutionAdmission.acquire(run, %{})

    assert {:error, {:invalid_execution_admission_entry, :execution_pool}} =
             ExecutionAdmission.acquire(run, %{asset_step_id: "step", execution_pool: %{}})
  end

  test "lease and waiter identities cannot collide across delimiter-bearing ids" do
    run_a = run(id: "run:a", max_concurrency: 1)
    run_b = run(id: "run", max_concurrency: 1)

    assert {:ok, lease_a} = ExecutionAdmission.acquire(run_a, %{asset_step_id: "b"})
    assert {:ok, lease_b} = ExecutionAdmission.acquire(run_b, %{asset_step_id: "a:b"})
    refute lease_a.lease_id == lease_b.lease_id

    refute Waiter.waiter_id("run:a", "b", 0, 1) ==
             Waiter.waiter_id("run", "a:b", 0, 1)
  end

  test "terminal runs cannot acquire leases or register waiters" do
    entry = %{asset_step_id: "step-1"}

    for status <- [:ok, :partial, :error, :cancelled, :timed_out] do
      run = run(id: "run-terminal-#{status}", max_concurrency: 1)
      terminal = terminal_run(run, status)
      run_id = terminal.id

      refute RunState.execution_admissible?(terminal)

      assert {:error, {:run_not_admissible, ^run_id, ^status}} =
               ExecutionAdmission.acquire(terminal, entry)

      assert {:error, {:run_not_admissible, ^run_id, ^status}} =
               ExecutionAdmission.acquire_or_wait(terminal, %{entry | asset_step_id: "step-2"},
                 stage: 0,
                 attempt: 1
               )

      assert {:ok, []} = Storage.list_execution_leases()
      assert {:ok, []} = Storage.list_execution_admission_waiters_for_scope(run_scope(terminal))
    end
  end

  test "intermediate step outcome statuses remain admissible before finalization" do
    for status <- [:error, :timed_out] do
      run = run(id: "run-intermediate-#{status}", max_concurrency: 1)
      intermediate = RunState.transition(run, status: status, error: %{type: status})

      assert RunState.terminal_status?(status)
      assert RunState.execution_admissible?(intermediate)
      assert {:ok, lease} = ExecutionAdmission.acquire(intermediate, %{asset_step_id: "step-1"})
      assert lease.run_id == intermediate.id

      assert :ok = ExecutionAdmission.release(lease)
    end
  end

  test "run lease cleanup is idempotent and does not reopen terminal admission" do
    run = run(max_concurrency: 1)

    assert {:ok, _lease} = ExecutionAdmission.acquire(run, %{asset_step_id: "step-1"})
    assert :ok = ExecutionAdmission.release_run(run.id)
    assert :ok = ExecutionAdmission.release_run(run.id)
    assert {:ok, []} = Storage.list_execution_leases()

    terminal = terminal_run(run, :cancelled)
    run_id = terminal.id

    assert {:error, {:run_not_admissible, ^run_id, :cancelled}} =
             ExecutionAdmission.acquire_or_wait(terminal, %{asset_step_id: "step-2"},
               stage: 0,
               attempt: 1
             )

    assert {:ok, []} = Storage.list_execution_leases()
    assert {:ok, []} = Storage.list_execution_admission_waiters_for_scope(run_scope(terminal))
  end

  test "acquire_or_wait persists waiter and release wakes registered owner" do
    run = run(max_concurrency: 1)
    entry = %{asset_step_id: "step-1"}

    assert {:ok, lease} = ExecutionAdmission.acquire(run, entry)

    assert {:waiting, waiter} =
             ExecutionAdmission.acquire_or_wait(run, %{entry | asset_step_id: "step-2"},
               stage: 0,
               attempt: 1
             )

    assert waiter.queue_reason == :pipeline_concurrency

    assert {:ok, [^waiter]} =
             Storage.list_execution_admission_waiters_for_scope(waiter.blocked_scope)

    assert :ok = ExecutionAdmission.release(lease)
    assert_receive {:execution_admission_wakeup, waiter_id, generation}, 1_000
    assert waiter_id == waiter.waiter_id
    assert generation == waiter.wake_generation

    assert :ok = ExecutionAdmission.cancel_wait(waiter)
    assert {:ok, []} = Storage.list_execution_admission_waiters_for_scope(waiter.blocked_scope)
  end

  test "release_run releases keyed leases without listing every lease" do
    Application.put_env(:favn_orchestrator, :storage_adapter, NoGlobalLeaseScanStorageAdapter)
    Application.put_env(:favn_orchestrator, :storage_adapter_opts, owner: self())

    run_a = run(id: "run-a", max_concurrency: 3)
    run_b = run(id: "run-b", max_concurrency: 3)

    assert {:ok, _lease_a1} = ExecutionAdmission.acquire(run_a, %{asset_step_id: "step-a1"})
    assert {:ok, _lease_a2} = ExecutionAdmission.acquire(run_a, %{asset_step_id: "step-a2"})
    assert {:ok, lease_b} = ExecutionAdmission.acquire(run_b, %{asset_step_id: "step-b"})

    assert :ok = ExecutionAdmission.release_run(run_a.id)
    assert_receive {:release_leases_for_run, "run-a"}, 1_000
    refute_received :global_lease_scan

    assert {:ok, [remaining]} = Memory.list_execution_leases([])
    assert remaining.lease_id == lease_b.lease_id
  end

  test "release_run wakes waiters blocked by scopes freed by the run" do
    Application.put_env(:favn, :execution_pools, github_api: [max_concurrency: 1])

    run_a = run(id: "run-a", max_concurrency: 10)
    run_b = run(id: "run-b", max_concurrency: 10)
    entry = %{asset_step_id: "step-1", execution_pool: :github_api}

    assert {:ok, _lease} = ExecutionAdmission.acquire(run_a, entry)

    assert {:waiting, waiter} =
             ExecutionAdmission.acquire_or_wait(run_b, %{entry | asset_step_id: "step-2"},
               stage: 0,
               attempt: 1
             )

    assert waiter.queue_reason == :execution_pool
    assert :ok = ExecutionAdmission.release_run(run_a.id)

    assert_receive {:execution_admission_wakeup, waiter_id, generation}, 1_000
    assert waiter_id == waiter.waiter_id
    assert generation == waiter.wake_generation

    assert :ok = ExecutionAdmission.cancel_wait(waiter)
  end

  test "coordinator dedupes duplicate notification scopes and expires once per batch" do
    Application.put_env(:favn_orchestrator, :storage_adapter, WakeCountingStorageAdapter)
    Application.put_env(:favn_orchestrator, :storage_adapter_opts, owner: self())

    scope = %{kind: :pool, key: "github_api", limit: 1}
    duplicate_scope = %{kind: "pool", key: "github_api", limit: 1}

    FavnOrchestrator.ExecutionAdmission.Coordinator.notify_scopes([
      scope,
      duplicate_scope,
      scope
    ])

    assert_receive :expired_waiters, 1_000
    refute_received :expired_waiters
  end

  test "acquire_or_wait rechecks admission after registering waiter" do
    run = run(max_concurrency: 1)
    entry = %{asset_step_id: "step-1"}

    assert {:ok, lease} = ExecutionAdmission.acquire(run, entry)

    Application.put_env(:favn_orchestrator, :storage_adapter, ReleaseDuringWaiterStorageAdapter)

    Application.put_env(:favn_orchestrator, :storage_adapter_opts,
      release_lease_id: lease.lease_id
    )

    assert {:ok, next_lease} =
             ExecutionAdmission.acquire_or_wait(run, %{entry | asset_step_id: "step-2"},
               stage: 0,
               attempt: 1
             )

    assert next_lease.asset_step_id == "step-2"
    assert {:ok, []} = Storage.list_execution_admission_waiters_for_scope(hd(next_lease.scopes))
  end

  test "release scans past stale unregistered waiters to wake a live waiter" do
    run = run(max_concurrency: 1)
    scope = %{kind: :run, key: run.id, limit: 1}
    now = DateTime.utc_now()

    assert {:ok, lease} = ExecutionAdmission.acquire(run, %{asset_step_id: "held"})

    for index <- 1..25 do
      assert {:ok, stale_waiter} =
               Waiter.new(
                 run,
                 %{asset_step_id: "stale-#{index}"},
                 [scope],
                 :pipeline_concurrency,
                 scope,
                 stage: 0,
                 attempt: 1,
                 now: DateTime.add(now, index * -1_000, :millisecond)
               )

      assert {:ok, _stored} = Storage.upsert_execution_admission_waiter(stale_waiter)
    end

    assert {:waiting, live_waiter} =
             ExecutionAdmission.acquire_or_wait(run, %{asset_step_id: "live"},
               stage: 0,
               attempt: 1,
               now: DateTime.add(now, 1_000, :millisecond)
             )

    assert :ok = ExecutionAdmission.release(lease)
    assert_receive {:execution_admission_wakeup, waiter_id, generation}, 1_000
    assert waiter_id == live_waiter.waiter_id
    assert generation == live_waiter.wake_generation

    assert :ok = ExecutionAdmission.cancel_wait(live_waiter)
  end

  defp run(opts) do
    RunState.new(
      id: Keyword.get(opts, :id, "run-admission"),
      manifest_version_id: "mv_admission",
      manifest_content_hash: "hash_admission",
      asset_ref: {MyApp.Asset, :asset},
      submit_kind: :pipeline,
      timeout_ms: Keyword.get(opts, :timeout_ms, RunState.default_timeout_ms()),
      metadata: %{
        pipeline_execution_policy: %{
          max_concurrency: Keyword.get(opts, :max_concurrency)
        }
      }
    )
  end

  defp run_scope(%RunState{} = run), do: %{kind: :run, key: run.id, limit: 1}

  defp terminal_run(%RunState{} = run, status) do
    RunState.transition(run,
      status: status,
      result: %{status: status, asset_results: [], metadata: run.metadata},
      metadata: Map.put(run.metadata, :terminal_event_type, terminal_event_type(status))
    )
  end

  defp terminal_event_type(:ok), do: :run_finished
  defp terminal_event_type(:cancelled), do: :run_cancelled
  defp terminal_event_type(:timed_out), do: :run_timed_out
  defp terminal_event_type(_status), do: :run_failed

  defp restore_env(app, key, nil), do: Application.delete_env(app, key)
  defp restore_env(app, key, value), do: Application.put_env(app, key, value)
end
