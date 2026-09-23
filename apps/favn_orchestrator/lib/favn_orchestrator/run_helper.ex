defmodule FavnOrchestrator.RunHelper do
  @moduledoc false
  alias FavnOrchestrator.RunManager

  # The worker is inert until the manager has registered it. Registration checks
  # that its parent and generation are still members of the active lifecycle.
  def async(authority, fun) do
    parent = self()

    Task.Supervisor.async_nolink(FavnOrchestrator.RunPostStepSupervisor, fn ->
      case GenServer.call(RunManager, {:register_helper, authority, parent, self()}, 1_000) do
        :ok ->
          Process.put(:favn_managed_run, true)
          fun.()

        _ ->
          exit(:run_lifecycle_stopped)
      end
    end)
  end

  # Only work attached to a run needs a run permit. Other domain lifecycles keep
  # their existing ownership checks.
  def permit_new_work(expected_run_id \\ nil, kind \\ :asset_attempt) do
    authority =
      if Process.whereis(RunManager),
        do: GenServer.call(RunManager, {:helper_authority, self()}, 1_000),
        else: nil

    case authority do
      nil ->
        if Process.get(:favn_managed_run) == true or not is_nil(expected_run_id),
          do: {:error, :run_lifecycle_unavailable},
          else: {:ok, nil}

      :stopping ->
        {:error, :run_lifecycle_stopped}

      ownership ->
        if expected_run_id && expected_run_id != ownership.run_id,
          do: {:error, :run_lifecycle_mismatch},
          else: await_permit(ownership, kind, System.monotonic_time(:millisecond) + 30_000)
    end
  catch
    :exit, _ -> {:error, :run_lifecycle_unavailable}
  end

  defp await_permit(ownership, kind, deadline) do
    case FavnOrchestrator.RunLeaseKeeper.permit(ownership, kind) do
      :ok ->
        {:ok, ownership}

      {:error, _} ->
        if System.monotonic_time(:millisecond) < deadline do
          receive do
          after
            250 -> await_permit(ownership, kind, deadline)
          end
        else
          {:error,
           FavnOrchestrator.Persistence.Error.new(
             :timeout,
             "run helper admission remained paused",
             retryable?: true
           )}
        end
    end
  end

  def existing_task_allowed?(%{run_id: run_id, status: status})
      when is_binary(run_id) and status in [:queued, :assigned, :preparing, :running, :cancelling] do
    case GenServer.call(RunManager, {:helper_authority, self()}, 1_000) do
      %{claim_purpose: :cleanup} -> false
      :stopping -> false
      _ -> true
    end
  catch
    :exit, _ -> false
  end

  def existing_task_allowed?(_), do: true

  def start_waiter(fun) do
    parent = self()

    authority =
      if Process.whereis(RunManager),
        do: GenServer.call(RunManager, {:helper_authority, parent}, 1_000),
        else: nil

    case authority do
      nil ->
        if Process.get(:favn_managed_run),
          do: {:error, :run_lifecycle_unavailable},
          else: Task.Supervisor.start_child(FavnOrchestrator.RunnerTaskWaitSupervisor, fun)

      :stopping ->
        {:error, :run_lifecycle_stopped}

      ownership ->
        Task.Supervisor.start_child(FavnOrchestrator.RunPostStepSupervisor, fn ->
          case GenServer.call(RunManager, {:register_helper, ownership, parent, self()}, 1_000) do
            :ok ->
              Process.put(:favn_managed_run, true)
              fun.()

            _ ->
              exit(:run_lifecycle_stopped)
          end
        end)
    end
  end
end
