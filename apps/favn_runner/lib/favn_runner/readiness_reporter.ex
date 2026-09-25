defmodule FavnRunner.ReadinessReporter do
  @moduledoc false
  use GenServer

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @impl true
  def init(opts) do
    state = %{
      path: Keyword.fetch!(opts, :path),
      check: Keyword.get(opts, :check, &FavnRunner.readiness/0),
      interval: Keyword.get(opts, :interval, 2_000),
      timeout: Keyword.get(opts, :timeout, 1_000),
      task: nil,
      timer: nil,
      started_at: nil
    }

    File.rm(state.path)
    send(self(), :check)
    {:ok, state}
  end

  @impl true
  def handle_info(:check, state) do
    check = state.check
    started_at = System.monotonic_time(:millisecond)

    task =
      Task.async(fn ->
        try do
          check.()
        catch
          _kind, _reason -> {:error, :unavailable}
        end
      end)

    timer = Process.send_after(self(), {:timeout, task.ref}, state.timeout)
    {:noreply, %{state | task: task, timer: timer, started_at: started_at}}
  end

  def handle_info({ref, result}, %{task: %Task{ref: ref}} = state) do
    Process.demonitor(ref, [:flush])
    Process.cancel_timer(state.timer)

    if result == :ok and
         System.monotonic_time(:millisecond) - state.started_at < state.timeout do
      temporary = state.path <> ".tmp"

      with :ok <- File.write(temporary, "#{System.system_time(:second)}\n"),
           :ok <- File.rename(temporary, state.path) do
        :ok
      else
        _error -> File.rm(state.path)
      end
    else
      File.rm(state.path)
    end

    {:noreply, schedule(state)}
  end

  def handle_info({:timeout, ref}, %{task: %Task{ref: ref} = task} = state) do
    Task.shutdown(task, :brutal_kill)
    File.rm(state.path)
    {:noreply, schedule(state)}
  end

  def handle_info(_stale_message, state), do: {:noreply, state}

  defp schedule(state) do
    Process.send_after(self(), :check, state.interval)
    %{state | task: nil, timer: nil}
  end
end
