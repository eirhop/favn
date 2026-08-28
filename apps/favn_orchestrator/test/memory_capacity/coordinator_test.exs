defmodule FavnOrchestrator.MemoryCapacity.CoordinatorTest do
  use ExUnit.Case, async: false

  alias FavnOrchestrator.MemoryCapacity
  alias FavnOrchestrator.MemoryCapacity.Coordinator
  alias FavnOrchestrator.MemoryCapacity.Ledger

  @gib 1_024 * 1_024 * 1_024

  defmodule Provider do
    def snapshot(agent: agent), do: Agent.get(agent, & &1)
  end

  setup do
    {:ok, provider} =
      Agent.start_link(fn ->
        {:ok,
         %{
           source: :cgroup_v2,
           limit_bytes: @gib,
           usage_bytes: 128 * 1_024 * 1_024,
           headroom_bytes: 896 * 1_024 * 1_024
         }}
      end)

    ledger = unique_name(:ledger)
    coordinator = unique_name(:coordinator)
    table = unique_name(:table)

    children = [
      %{
        id: :ledger,
        start: {Ledger, :start_link, [[name: ledger, table: table]]},
        restart: :temporary
      },
      %{
        id: :coordinator,
        start:
          {Coordinator, :start_link,
           [
             [
               name: coordinator,
               ledger: ledger,
               provider: Provider,
               provider_opts: [agent: provider]
             ]
           ]},
        restart: :permanent
      }
    ]

    supervisor =
      start_supervised!(%{
        id: unique_name(:supervisor),
        start: {Supervisor, :start_link, [children, [strategy: :rest_for_one]]}
      })

    %{provider: provider, coordinator: coordinator, ledger: ledger, supervisor: supervisor}
  end

  test "accounts nested resize and retained bytes without stacking", context do
    assert {:ok, token} =
             MemoryCapacity.acquire(256 * 1_024 * 1_024,
               server: context.coordinator,
               kind: :upload
             )

    assert :ok = MemoryCapacity.resize(token, 320 * 1_024 * 1_024, server: context.coordinator)
    assert :ok = MemoryCapacity.retain(token, 32 * 1_024 * 1_024, server: context.coordinator)

    diagnostics = MemoryCapacity.diagnostics(server: context.coordinator)
    assert diagnostics.active_leases == 1
    assert diagnostics.reserved_bytes == 352 * 1_024 * 1_024
    assert diagnostics.by_kind.upload == 352 * 1_024 * 1_024

    assert :ok = MemoryCapacity.resize(token, 0, server: context.coordinator)

    assert MemoryCapacity.diagnostics(server: context.coordinator).reserved_bytes ==
             32 * 1_024 * 1_024
  end

  test "atomically transfers working bytes to retained bytes without double reservation",
       context do
    bytes = 400 * 1_024 * 1_024
    assert {:ok, token} = MemoryCapacity.acquire(bytes, server: context.coordinator)

    assert {:error, %{code: :manifest_capacity_unavailable}} =
             MemoryCapacity.retain(token, bytes, server: context.coordinator)

    assert :ok = MemoryCapacity.transfer(token, bytes, 0, server: context.coordinator)
    assert MemoryCapacity.diagnostics(server: context.coordinator).reserved_bytes == bytes
  end

  test "scoped growth never shrinks an outer working reservation", context do
    outer_bytes = 400 * 1_024 * 1_024
    assert {:ok, token} = MemoryCapacity.acquire(outer_bytes, server: context.coordinator)

    assert :ok = MemoryCapacity.grow(token, 256 * 1_024 * 1_024, server: context.coordinator)

    assert MemoryCapacity.diagnostics(server: context.coordinator).reserved_bytes == outer_bytes
  end

  test "owner death and idempotent release free capacity", context do
    parent = self()

    owner =
      spawn(fn ->
        {:ok, token} =
          MemoryCapacity.acquire(64 * 1_024 * 1_024,
            owner: self(),
            server: context.coordinator
          )

        send(parent, {:token, token})
        receive do: (:stop -> :ok)
      end)

    assert_receive {:token, token}
    assert MemoryCapacity.diagnostics(server: context.coordinator).active_leases == 1
    Process.exit(owner, :kill)

    eventually(fn ->
      MemoryCapacity.diagnostics(server: context.coordinator).active_leases == 0
    end)

    assert :ok = MemoryCapacity.release(token, server: context.coordinator)
  end

  test "handoff installs the new owner and releases on its death", context do
    receiver = spawn(fn -> receive do: (:stop -> :ok) end)
    assert {:ok, token} = MemoryCapacity.acquire(64, server: context.coordinator)
    assert :ok = MemoryCapacity.handoff(token, receiver, server: context.coordinator)

    Process.exit(receiver, :kill)

    eventually(fn ->
      MemoryCapacity.diagnostics(server: context.coordinator).active_leases == 0
    end)
  end

  test "coordinator restart reconstructs live leases and drops dead owners", context do
    owner = spawn(fn -> receive do: (:stop -> :ok) end)

    assert {:ok, _token} =
             MemoryCapacity.acquire(64 * 1_024 * 1_024,
               owner: owner,
               server: context.coordinator,
               kind: :run_plan
             )

    old_pid = Process.whereis(context.coordinator)
    Process.exit(old_pid, :kill)

    eventually(fn ->
      is_pid(Process.whereis(context.coordinator)) and
        Process.whereis(context.coordinator) != old_pid
    end)

    diagnostics = MemoryCapacity.diagnostics(server: context.coordinator)
    assert diagnostics.active_leases == 1
    assert diagnostics.by_kind.run_plan == 64 * 1_024 * 1_024

    Process.exit(owner, :kill)

    eventually(fn ->
      MemoryCapacity.diagnostics(server: context.coordinator).active_leases == 0
    end)
  end

  test "release succeeds while the ledger owns leases during coordinator restart", context do
    assert {:ok, token} =
             MemoryCapacity.acquire(64 * 1_024 * 1_024, server: context.coordinator)

    :ok = Supervisor.terminate_child(context.supervisor, :coordinator)

    assert :ok =
             MemoryCapacity.release(token,
               server: context.coordinator,
               ledger: context.ledger
             )

    {:ok, _pid} = Supervisor.restart_child(context.supervisor, :coordinator)

    eventually(fn ->
      MemoryCapacity.diagnostics(server: context.coordinator).active_leases == 0
    end)
  end

  test "limit reduction blocks growth but still permits shrinking", context do
    assert {:ok, token} = MemoryCapacity.acquire(256 * 1_024 * 1_024, server: context.coordinator)

    Agent.update(context.provider, fn _ ->
      {:ok,
       %{
         source: :cgroup_v2,
         limit_bytes: 512 * 1_024 * 1_024,
         usage_bytes: 400 * 1_024 * 1_024,
         headroom_bytes: 112 * 1_024 * 1_024
       }}
    end)

    assert {:error, %{code: :manifest_capacity_unavailable}} =
             MemoryCapacity.resize(token, 300 * 1_024 * 1_024, server: context.coordinator)

    assert :ok = MemoryCapacity.resize(token, 16 * 1_024 * 1_024, server: context.coordinator)
  end

  test "unknown measurements fail closed before admission", context do
    Agent.update(context.provider, fn _ -> {:error, :unavailable} end)

    assert {:error, %{code: :memory_capacity_unknown}} =
             MemoryCapacity.acquire(1, server: context.coordinator)
  end

  test "missing lease ledger starts closed instead of assuming zero use" do
    coordinator = unique_name(:closed_coordinator)

    start_supervised!(%{
      id: unique_name(:closed_coordinator_child),
      start:
        {Coordinator, :start_link,
         [[name: coordinator, ledger: unique_name(:missing_ledger), provider: Provider]]}
    })

    assert %{status: :closed, active_leases: 0} =
             MemoryCapacity.diagnostics(server: coordinator)

    assert {:error, %{code: :memory_capacity_unknown}} =
             MemoryCapacity.acquire(1, server: coordinator)
  end

  defp unique_name(kind),
    do: String.to_atom("#{__MODULE__}.#{kind}.#{System.unique_integer([:positive])}")

  defp eventually(fun, attempts \\ 100)
  defp eventually(fun, 0), do: assert(fun.())

  defp eventually(fun, attempts) do
    if fun.() do
      :ok
    else
      Process.sleep(10)
      eventually(fun, attempts - 1)
    end
  end
end
