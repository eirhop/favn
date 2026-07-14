defmodule FavnOrchestrator.TestSupport.Runtime do
  @moduledoc false

  @idle_attempts 200

  def stop_active_runs do
    case Process.whereis(FavnOrchestrator.RunSupervisor) do
      nil ->
        :ok

      _pid ->
        FavnOrchestrator.RunSupervisor
        |> DynamicSupervisor.which_children()
        |> Enum.each(fn {_id, pid, _type, _modules} ->
          case DynamicSupervisor.terminate_child(FavnOrchestrator.RunSupervisor, pid) do
            :ok -> :ok
            {:error, :not_found} -> :ok
          end
        end)
    end

    await_run_manager_idle(@idle_attempts)
  end

  defp await_run_manager_idle(0), do: :ok

  defp await_run_manager_idle(attempts) do
    case run_manager_state() do
      %{run_pids: run_pids} when map_size(run_pids) > 0 ->
        Process.sleep(5)
        await_run_manager_idle(attempts - 1)

      _idle_or_stopped ->
        :ok
    end
  end

  defp run_manager_state do
    case Process.whereis(FavnOrchestrator.RunManager) do
      nil -> nil
      _pid -> :sys.get_state(FavnOrchestrator.RunManager)
    end
  catch
    :exit, _reason -> nil
  end
end
