defmodule FavnOrchestrator.RunServer.Recovery do
  @moduledoc """
  Decides whether a persisted run has enough durable position to resume safely.

  Recovery is deliberately fail-closed. A retry checkpoint is resumable, as is a
  fresh run that has not created any runner-execution ledger row. Other histories
  may contain an accepted external effect or completed work whose continuation
  position is not durable, so they must be terminalized instead of resubmitted.
  """

  alias FavnOrchestrator.RunExecutionOwnership
  alias FavnOrchestrator.RunServer.RetryCheckpoint
  alias FavnOrchestrator.RunState

  @type disposition :: :resume | {:uncertain, map()}

  @doc "Loads bounded durable runner-execution evidence and assesses recovery safety."
  @spec disposition(RunState.t()) :: {:ok, disposition()} | {:error, term()}
  def disposition(%RunState{} = run) do
    with {:ok, evidence} <- RunExecutionOwnership.recovery_evidence(run) do
      {:ok, assess(run, evidence)}
    end
  end

  @doc "Assesses recovery safety from bounded runner-execution ledger evidence."
  @spec assess(RunState.t(), %{
          active: [RunExecutionOwnership.t()],
          active_truncated?: boolean(),
          any?: boolean()
        }) :: disposition()
  def assess(
        %RunState{} = run,
        %{active: active, active_truncated?: active_truncated?, any?: any?}
      ) do
    if active != [] do
      uncertain(:runner_execution_may_have_been_accepted, active, active_truncated?)
    else
      assess_checkpoint(run, any?)
    end
  end

  defp assess_checkpoint(%RunState{} = run, any?) do
    case RetryCheckpoint.validate(run.metadata, RunState.execution_mode(run)) do
      {:ok, {kind, _state}} when kind in [:sequential, :pipeline] ->
        :resume

      {:ok, :none} when not any? and run.event_seq <= 2 ->
        :resume

      {:ok, :none} ->
        uncertain(:continuation_position_not_durable, [], false)

      {:error, :invalid_retry_checkpoint} ->
        uncertain(:invalid_retry_checkpoint, [], false)
    end
  end

  defp uncertain(reason, active, truncated?) do
    summaries =
      active
      |> Enum.map(fn execution ->
        %{
          execution_id: execution.runner_execution_id || execution.dispatch_id,
          status: execution.status,
          attempt: execution.attempt,
          stage: execution.stage
        }
      end)

    {:uncertain,
     %{
       reason: reason,
       active_execution_count: length(active),
       executions: summaries,
       truncated?: truncated?
     }}
  end
end
