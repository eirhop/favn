defmodule FavnTestSupport.ExecutionDriver do
  @moduledoc false
  @execution FavnOrchestrator.RunServer.Execution

  def handle_event(state, event), do: drain(apply(@execution, :handle_event, [state, event]))

  def retry_persistence(state, retry),
    do: drain(apply(@execution, :retry_persistence, [state, retry]))

  def resume_persisted_retry(state, retry),
    do: drain(apply(@execution, :resume_persisted_retry, [state, retry]))

  def cancel(state, reason), do: drain(apply(@execution, :cancel, [state, reason]))

  defp drain({:operation, state, operation}) do
    result = apply(@execution, :perform_operation, [operation])
    drain(apply(@execution, :finish_operation, [state, operation, result]))
  end

  defp drain(result), do: result
end
