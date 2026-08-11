defmodule FavnView.OrchestratorConnector do
  @moduledoc false

  use GenServer

  @retry_ms 1_000

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    target_node = Keyword.fetch!(opts, :target_node)
    :ok = :net_kernel.monitor_nodes(true)
    send(self(), :connect)
    {:ok, %{target_node: target_node, retry_timer: nil}}
  end

  @impl true
  def handle_info(:connect, state) do
    state = %{state | retry_timer: nil}

    if Node.connect(state.target_node) do
      {:noreply, state}
    else
      {:noreply, schedule_retry(state)}
    end
  end

  def handle_info({:nodedown, target_node}, %{target_node: target_node} = state) do
    {:noreply, schedule_retry(state)}
  end

  def handle_info({:nodeup, target_node}, %{target_node: target_node} = state) do
    {:noreply, cancel_retry(state)}
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp schedule_retry(%{retry_timer: nil} = state) do
    %{state | retry_timer: Process.send_after(self(), :connect, @retry_ms)}
  end

  defp schedule_retry(state), do: state

  defp cancel_retry(%{retry_timer: nil} = state), do: state

  defp cancel_retry(state) do
    _ = Process.cancel_timer(state.retry_timer)
    %{state | retry_timer: nil}
  end
end
