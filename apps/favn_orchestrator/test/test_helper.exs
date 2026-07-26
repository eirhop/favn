FavnTestSupport.Fixtures.compile_fixtures!([
  :basic_assets,
  :graph_assets,
  :runner_assets,
  :pipeline_assets
])

Code.require_file("support/runtime.ex", __DIR__)

defmodule FavnOrchestrator.TestRunnerTaskStore do
  @behaviour FavnOrchestrator.Persistence.RunnerTaskStore

  alias FavnOrchestrator.Persistence.Error

  def enqueue(_command), do: unavailable()
  def claim(_command), do: unavailable()
  def transition(_command), do: unavailable()
  def persist_runtime_inputs(_command), do: unavailable()
  def append_log_batch(_command), do: unavailable()
  def complete(_command), do: unavailable()
  def request_cancellation(_command), do: unavailable()
  def acknowledge_cancellation(_command), do: unavailable()
  def release(_command), do: unavailable()
  def recover_expired(_command), do: unavailable()
  def reconcile_demand(_command), do: unavailable()
  def get(_query), do: unavailable()
  def page_run(_query), do: unavailable()
  def demand(_query), do: unavailable()

  defp unavailable,
    do: {:error, Error.new(:unavailable, "runner task store is not used by this test")}
end

Logger.configure(level: :warning)
ExUnit.start(capture_log: true)
