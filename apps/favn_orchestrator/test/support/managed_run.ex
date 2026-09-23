defmodule FavnOrchestrator.TestSupport.ManagedRun do
  @moduledoc false
  alias FavnOrchestrator.{RunControlSupervisor, RunManager, RunLeaseKeeper}
  alias FavnOrchestrator.Persistence.SystemContext

  def start_link(%{run_state: run} = args) do
    ensure_started()
    context = SystemContext.workspace(run.workspace_id, :run_recovery)

    result =
      case args[:storage_ownership] do
        nil -> RunManager.recover_candidate(context, run.id)
        ownership -> RunManager.recover_claimed_run(context, ownership)
      end

    with {:ok, _} <- result,
         do: await_server({run.workspace_id, run.id}, System.monotonic_time(:millisecond) + 5_000)
  end

  def ensure_started do
    unless Process.whereis(RunControlSupervisor),
      do: ExUnit.Callbacks.start_supervised!({RunControlSupervisor, []})

    unless Process.whereis(FavnOrchestrator.RunManagerTaskSupervisor),
      do:
        ExUnit.Callbacks.start_supervised!(
          {Task.Supervisor, name: FavnOrchestrator.RunManagerTaskSupervisor}
        )
  end

  defp await_server(key, deadline) do
    state = :sys.get_state(RunManager)

    entry =
      Enum.find_value(state.lifecycles, fn {_, e} ->
        if e.key == key and is_pid(e.coordinator), do: e
      end)

    if entry && RunLeaseKeeper.permit(entry.ownership) == :ok do
      Process.link(entry.coordinator)
      {:ok, entry.coordinator}
    else
      if System.monotonic_time(:millisecond) >= deadline do
        {:error, {:managed_run_start_timeout, state.lifecycles}}
      else
        receive do
        after
          5 -> await_server(key, deadline)
        end
      end
    end
  end
end
