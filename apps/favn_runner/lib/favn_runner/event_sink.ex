defmodule FavnRunner.EventSink do
  @moduledoc false

  alias Favn.Contracts.RunnerEvent

  @spec emit(pid(), String.t(), RunnerEvent.t()) :: :ok
  def emit(server_pid, execution_id, %RunnerEvent{} = event)
      when is_pid(server_pid) and is_binary(execution_id) do
    send(server_pid, {:runner_event, execution_id, event})
    :ok
  end
end
