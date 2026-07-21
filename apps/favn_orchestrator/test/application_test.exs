defmodule FavnOrchestrator.ApplicationTest do
  use ExUnit.Case, async: false

  alias FavnOrchestrator.Lifecycle
  alias FavnOrchestrator.RunManager

  defmodule TestRuntimeStarter do
    use GenServer

    def start_link(opts) do
      GenServer.start_link(__MODULE__, opts, name: Keyword.fetch!(opts, :name))
    end

    @impl true
    def init(opts) do
      :ok = Lifecycle.mark_accepting(Keyword.fetch!(opts, :lifecycle))
      {:ok, %{}}
    end
  end

  test "OTP application metadata is stable while test runtime children stay disabled" do
    assert Application.spec(:favn_orchestrator, :mod) == {FavnOrchestrator.Application, []}

    supervisor = Process.whereis(FavnOrchestrator.Supervisor)
    assert is_pid(supervisor)

    children = Supervisor.which_children(supervisor)

    assert {FavnOrchestrator.Lifecycle, lifecycle, :worker, [FavnOrchestrator.Lifecycle]} =
             List.keyfind(children, FavnOrchestrator.Lifecycle, 0)

    assert {FavnOrchestrator.RuntimeStarter, starter, :worker, [FavnOrchestrator.RuntimeStarter]} =
             List.keyfind(children, FavnOrchestrator.RuntimeStarter, 0)

    assert is_pid(lifecycle)
    assert is_pid(starter)
    assert %{status: :accepting, ready?: true} = FavnOrchestrator.Lifecycle.diagnostics()

    refute Process.whereis(FavnOrchestrator.Persistence.Runtime)
  end

  test "a lifecycle crash restarts the dependent runtime and restores admission" do
    lifecycle = Process.whereis(FavnOrchestrator.Lifecycle)
    starter = Process.whereis(FavnOrchestrator.RuntimeStarter)

    Process.exit(lifecycle, :kill)

    assert_eventually(fn ->
      restarted_lifecycle = Process.whereis(FavnOrchestrator.Lifecycle)
      restarted_starter = Process.whereis(FavnOrchestrator.RuntimeStarter)

      is_pid(restarted_lifecycle) and restarted_lifecycle != lifecycle and
        is_pid(restarted_starter) and restarted_starter != starter and
        FavnOrchestrator.Lifecycle.diagnostics().status == :accepting
    end)
  end

  test "an ownership-manager crash cannot orphan an active run server" do
    lifecycle_name = unique_name(:lifecycle)
    run_supervisor_name = unique_name(:run_supervisor)
    manager_name = unique_name(:run_manager)
    starter_name = unique_name(:starter)
    supervisor_name = unique_name(:supervisor)

    children = [
      Supervisor.child_spec(
        {Lifecycle, name: lifecycle_name, shutdown_drain_timeout_ms: 1_000},
        id: :lifecycle
      ),
      Supervisor.child_spec(
        {DynamicSupervisor, strategy: :one_for_one, name: run_supervisor_name},
        id: :run_supervisor
      ),
      Supervisor.child_spec({RunManager, name: manager_name}, id: :run_manager),
      Supervisor.child_spec(
        {TestRuntimeStarter, name: starter_name, lifecycle: lifecycle_name},
        id: :starter
      )
    ]

    start_supervised!(%{
      id: supervisor_name,
      start:
        {Supervisor, :start_link, [children, [strategy: :one_for_all, name: supervisor_name]]}
    })

    run_child = %{
      id: make_ref(),
      start: {Task, :start_link, [fn -> Process.sleep(:infinity) end]},
      restart: :temporary
    }

    assert {:ok, run_server} = DynamicSupervisor.start_child(run_supervisor_name, run_child)

    :sys.replace_state(manager_name, fn state ->
      %{state | run_pids: %{{"workspace", "run"} => run_server}}
    end)

    lifecycle = Process.whereis(lifecycle_name)
    run_supervisor = Process.whereis(run_supervisor_name)
    manager = Process.whereis(manager_name)

    assert :sys.get_state(manager_name).run_pids == %{{"workspace", "run"} => run_server}
    Process.exit(manager, :kill)

    assert_eventually(fn ->
      restarted_lifecycle = Process.whereis(lifecycle_name)
      restarted_run_supervisor = Process.whereis(run_supervisor_name)
      restarted_manager = Process.whereis(manager_name)

      not Process.alive?(run_server) and is_pid(restarted_lifecycle) and
        restarted_lifecycle != lifecycle and is_pid(restarted_run_supervisor) and
        restarted_run_supervisor != run_supervisor and is_pid(restarted_manager) and
        restarted_manager != manager and
        Lifecycle.diagnostics(lifecycle_name).status == :accepting and
        :sys.get_state(manager_name).run_pids == %{}
    end)
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

  defp unique_name(prefix),
    do: :"#{prefix}_#{System.unique_integer([:positive, :monotonic])}"
end
