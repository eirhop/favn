defmodule FavnOrchestrator.RunnerRegistryTest do
  use ExUnit.Case, async: false

  alias Favn.Contracts.RunnerTask.ClaimRequest
  alias Favn.Contracts.RunnerTask.NoWork
  alias Favn.Contracts.RunnerTask.Registration
  alias FavnOrchestrator.RunnerQueueCoordinator
  alias FavnOrchestrator.RunnerQueueSupervisor
  alias FavnOrchestrator.RunnerRegistry

  @release "rr_" <> String.duplicate("a", 64)

  setup do
    previous = Application.get_env(:favn_orchestrator, :runner_pools)
    Application.put_env(:favn_orchestrator, :runner_pools, duckdb: [mode: :elastic])
    start_supervised!({RunnerRegistry, []})
    start_supervised!({RunnerQueueSupervisor, []})

    on_exit(fn ->
      if is_nil(previous),
        do: Application.delete_env(:favn_orchestrator, :runner_pools),
        else: Application.put_env(:favn_orchestrator, :runner_pools, previous)
    end)

    :ok
  end

  test "duplicate boot registration is stable and a live identity cannot be replaced" do
    agent = spawn_agent()
    registration = registration("runner-one", "boot-one")

    assert {:ok, first} = RunnerRegistry.register(registration, agent)
    assert first.status == :accepted
    assert first.runner_session_generation > 0

    assert {:ok, ^first} = RunnerRegistry.register(registration, agent)

    replacement = spawn_agent()

    assert {:ok, second} =
             RunnerRegistry.register(%{registration | boot_id: "boot-two"}, replacement)

    assert second.status == :rejected
    assert second.reason == :runner_instance_id_already_registered

    assert {:ok, session} = RunnerRegistry.fetch("runner-one")
    assert session.agent_pid == agent
    assert session.session_generation == first.runner_session_generation

    Process.exit(agent, :kill)

    assert_eventually(fn ->
      RunnerRegistry.fetch("runner-one") == {:error, :runner_session_not_found}
    end)

    assert {:ok, admitted} =
             RunnerRegistry.register(%{registration | boot_id: "boot-two"}, replacement)

    assert admitted.status == :accepted
    refute admitted.runner_session_generation == first.runner_session_generation
  end

  test "an exact active assignment resumes its durable session generation after registry loss" do
    agent = spawn_agent()

    registration =
      registration("runner-resume", "boot-resume")
      |> Map.put(:runner_session_generation, 7)
      |> Map.put(:active_assignment, %{
        workspace_id: "workspace-resume",
        task_id: "rt_resume",
        assignment_generation: 3
      })

    assert {:ok, ack} = RunnerRegistry.register_verified(registration, agent)
    assert ack.runner_session_generation == 7

    assert {:ok, session} = RunnerRegistry.fetch("runner-resume")
    assert session.status == :busy
    assert session.active_assignment.task_id == "rt_resume"
  end

  test "active assignment registration requires durable verification" do
    agent = spawn_agent()

    registration =
      registration("runner-unverified", "boot-unverified")
      |> Map.put(:runner_session_generation, 7)
      |> Map.put(:active_assignment, %{
        workspace_id: "workspace-resume",
        task_id: "rt_resume",
        assignment_generation: 3
      })

    assert {:ok, ack} = RunnerRegistry.register(registration, agent)
    assert ack.status == :rejected
    assert ack.reason == :unverified_active_assignment
  end

  test "registration must match the configured pool lifecycle mode" do
    Application.put_env(:favn_orchestrator, :runner_pools, duckdb: [mode: :resident])
    agent = spawn_agent()

    assert {:ok, ack} =
             RunnerRegistry.register(registration("runner-mode", "boot-mode"), agent)

    assert ack.status == :rejected
    assert ack.reason == :runner_lifecycle_mode_mismatch
  end

  test "one live session cannot claim a second task and duplicate requests replay" do
    agent = spawn_agent()
    assert {:ok, ack} = RunnerRegistry.register(registration("runner-two", "boot-one"), agent)
    request = claim_request("runner-two", ack.runner_session_generation, "claim-one")

    assert {:ok, :start, claiming} = RunnerRegistry.begin_claim(request)
    assert claiming.status == :claiming
    assert {:ok, {:duplicate, :in_flight}, _session} = RunnerRegistry.begin_claim(request)

    assert {:error, :runner_not_idle} =
             RunnerRegistry.begin_claim(%{request | command_id: "claim-two"})

    no_work = %NoWork{
      command_id: request.command_id,
      runner_instance_id: request.runner_instance_id,
      runner_session_generation: request.runner_session_generation,
      action: :wait,
      wait_ms: 100
    }

    assert {:ok, idle} = RunnerRegistry.finish_claim(request, no_work)
    assert idle.status == :idle
    assert {:ok, {:duplicate, ^no_work}, _session} = RunnerRegistry.begin_claim(request)

    assert {:ok, :start, _session} =
             RunnerRegistry.begin_claim(%{request | command_id: "claim-two"})
  end

  test "coordinator retries a missed-wake race and targets only requested waiters" do
    one = registered_session("runner-wait-one")
    two = registered_session("runner-wait-two")
    generation = RunnerQueueCoordinator.generation("duckdb", @release)

    assert :waiting = RunnerQueueCoordinator.wait("duckdb", @release, generation, one)
    assert :waiting = RunnerQueueCoordinator.wait("duckdb", @release, generation, two)

    RunnerQueueCoordinator.notify("duckdb", @release, 1)

    assert_receive {:agent_message, {:favn_runner_task, wake}}
    assert wake.runner_instance_id == "runner-wait-one"
    refute_receive {:agent_message, {:favn_runner_task, _wake}}, 20

    assert :retry = RunnerQueueCoordinator.wait("duckdb", @release, generation, one)
  end

  test "monitors a real distributed BEAM runner process and removes it on node loss" do
    with_distributed_peer(fn peer, peer_node ->
      agent = :peer.call(peer, :erlang, :spawn, [:timer, :sleep, [:infinity]], 10_000)

      registration = %{
        registration("runner-peer", "boot-peer")
        | beam_node: Atom.to_string(peer_node)
      }

      assert {:ok, ack} = RunnerRegistry.register(registration, agent)
      assert ack.status == :accepted
      assert {:ok, %{agent_pid: ^agent}} = RunnerRegistry.fetch("runner-peer")

      :ok = :peer.stop(peer)

      assert_eventually(fn ->
        RunnerRegistry.fetch("runner-peer") == {:error, :runner_session_not_found}
      end)
    end)
  end

  test "distributed runner churn stays bounded and active work re-registers after registry restart" do
    with_distribution(fn ->
      warm_nodes = churn_dynamic_runners(1..20)
      assert MapSet.size(MapSet.new(warm_nodes)) < length(warm_nodes)

      atom_count_before = :erlang.system_info(:atom_count)
      known_nodes_before = MapSet.new(Node.list(:known))

      assigned_nodes = churn_dynamic_runners(21..60)

      assert MapSet.size(MapSet.new(assigned_nodes)) < length(assigned_nodes)

      atom_growth = :erlang.system_info(:atom_count) - atom_count_before
      assert atom_growth <= 8

      known_nodes_after = MapSet.new(Node.list(:known))
      assert MapSet.size(MapSet.difference(known_nodes_after, known_nodes_before)) <= 4

      {peer, peer_node} = start_dynamic_peer()
      active_agent = remote_agent(peer)

      active_registration =
        peer_registration("runner-active", "boot-active", peer_node)
        |> Map.put(:runner_session_generation, 9)
        |> Map.put(:active_assignment, %{
          workspace_id: "workspace-active",
          task_id: "rt_active",
          assignment_generation: 4
        })

      assert {:ok, %{runner_session_generation: 9}} =
               RunnerRegistry.register_verified(active_registration, active_agent)

      assert {:ok, %{status: :busy}} = RunnerRegistry.fetch("runner-active")

      assert :ok = stop_supervised(RunnerRegistry)
      start_supervised!({RunnerRegistry, []})
      assert RunnerRegistry.list() == []

      assert {:ok, %{runner_session_generation: 9}} =
               RunnerRegistry.register_verified(active_registration, active_agent)

      assert {:ok, resumed} = RunnerRegistry.fetch("runner-active")
      assert resumed.status == :busy
      assert resumed.active_assignment.task_id == "rt_active"
      assert resumed.agent_pid == active_agent

      :ok = :peer.stop(peer)
    end)
  end

  defp registered_session(runner_id) do
    parent = self()
    agent = spawn(fn -> agent_loop(parent) end)

    assert {:ok, _ack} =
             RunnerRegistry.register(registration(runner_id, "boot-#{runner_id}"), agent)

    assert {:ok, session} = RunnerRegistry.fetch(runner_id)
    session
  end

  defp registration(runner_id, boot_id) do
    %Registration{
      runner_instance_id: runner_id,
      boot_id: boot_id,
      beam_node: Atom.to_string(node()),
      runner_pool: "duckdb",
      required_runner_release_id: @release,
      lifecycle_mode: :elastic,
      supported_task_kinds: [:asset_attempt],
      capabilities: ["asset_execution"]
    }
  end

  defp claim_request(runner_id, generation, command_id) do
    %ClaimRequest{
      command_id: command_id,
      issued_at: DateTime.utc_now(),
      runner_instance_id: runner_id,
      runner_session_generation: generation,
      runner_pool: "duckdb",
      required_runner_release_id: @release,
      supported_task_kinds: [:asset_attempt],
      capabilities: ["asset_execution"]
    }
  end

  defp spawn_agent do
    parent = self()
    spawn(fn -> agent_loop(parent) end)
  end

  defp remote_agent(peer),
    do: :peer.call(peer, :erlang, :spawn, [:timer, :sleep, [:infinity]], 10_000)

  defp peer_registration(runner_id, boot_id, peer_node) do
    %{registration(runner_id, boot_id) | beam_node: Atom.to_string(peer_node)}
  end

  defp with_distributed_peer(fun) do
    with_distribution(fn ->
      peer_name =
        "favn_elastic_runner_#{System.unique_integer([:positive])}"
        |> String.to_charlist()

      assert {:ok, peer, peer_node} =
               :peer.start_link(%{
                 name: peer_name,
                 host: ~c"127.0.0.1",
                 longnames: true,
                 connection: :standard_io,
                 wait_boot: 10_000
               })

      try do
        assert :ok = :peer.call(peer, :code, :add_paths, [:code.get_path()], 10_000)
        fun.(peer, peer_node)
      after
        if Process.alive?(peer), do: :peer.stop(peer)
      end
    end)
  end

  defp with_distribution(fun) do
    started_distribution? = not Node.alive?()

    if started_distribution? do
      assert {_, 0} = System.cmd("epmd", ["-daemon"], stderr_to_stdout: true)

      client_name =
        String.to_atom("favn_runner_registry_#{System.unique_integer([:positive])}@127.0.0.1")

      assert {:ok, _pid} = Node.start(client_name, name_domain: :longnames)
    end

    try do
      fun.()
    after
      if started_distribution?, do: Node.stop()
    end
  end

  defp start_dynamic_peer do
    assert {:ok, peer, _boot_node} =
             :peer.start_link(%{
               name: ~c"undefined",
               host: ~c"127.0.0.1",
               longnames: true,
               connection: :standard_io,
               wait_boot: 10_000
             })

    assert true = :peer.call(peer, :net_kernel, :connect_node, [node()], 10_000)
    peer_node = :peer.call(peer, :erlang, :node, [], 10_000)
    assert :ok = :peer.call(peer, :code, :add_paths, [:code.get_path()], 10_000)
    {peer, peer_node}
  end

  defp churn_dynamic_runners(range) do
    Enum.map(range, fn index ->
      {peer, peer_node} = start_dynamic_peer()
      agent = remote_agent(peer)
      runner_id = "runner-churn-#{index}"

      assert {:ok, %{status: :accepted}} =
               RunnerRegistry.register(
                 peer_registration(runner_id, "boot-churn-#{index}", peer_node),
                 agent
               )

      assert {:ok, %{agent_pid: ^agent}} = RunnerRegistry.fetch(runner_id)
      :ok = :peer.stop(peer)

      assert_eventually(fn ->
        RunnerRegistry.fetch(runner_id) == {:error, :runner_session_not_found}
      end)

      peer_node
    end)
  end

  defp agent_loop(parent) do
    receive do
      message ->
        send(parent, {:agent_message, message})
        agent_loop(parent)
    end
  end

  defp assert_eventually(fun, attempts \\ 100)
  defp assert_eventually(_fun, 0), do: flunk("condition did not become true")

  defp assert_eventually(fun, attempts) do
    if fun.() do
      :ok
    else
      Process.sleep(10)
      assert_eventually(fun, attempts - 1)
    end
  end
end
