defmodule FavnOrchestrator.RunServer.Cancellation do
  @moduledoc """
  Shared runner cancellation payload and dispatch helpers for run-server paths.

  This module owns the runner cancellation envelope contract only. Callers remain
  responsible for any local run-state cleanup after dispatching cancellation.
  """

  alias Favn.Contracts.RunnerCancellation
  alias FavnOrchestrator.CancellationOutcome
  alias FavnOrchestrator.RunState

  @type execution_id :: String.t()
  @type reason :: term()
  @type envelope :: RunnerCancellation.t()

  @doc """
  Wraps a runner cancellation reason in the control-plane cancellation envelope.
  """
  @spec envelope(RunState.t(), reason()) :: envelope()
  def envelope(%RunState{id: run_id}, reason) do
    RunnerCancellation.request(run_id, reason)
  end

  @doc """
  Cancels unique runner execution ids and returns the ids that were dispatched.

  Use `dispatch_runner_work/5` when callers need per-execution outcomes for
  persisted cleanup state.
  """
  @spec cancel_runner_work(RunState.t(), [term()], reason(), module(), keyword()) :: [
          execution_id()
        ]
  def cancel_runner_work(
        %RunState{} = run_state,
        execution_ids,
        reason,
        runner_client,
        runner_opts
      )
      when is_list(execution_ids) do
    unique_ids = execution_ids |> Enum.filter(&is_binary/1) |> Enum.uniq()
    _results = dispatch_runner_work(run_state, unique_ids, reason, runner_client, runner_opts)

    unique_ids
  end

  @doc "Cancels runner execution ids and returns one normalized outcome per id."
  @spec dispatch_runner_work(RunState.t(), [term()], reason(), module(), keyword()) :: [
          CancellationOutcome.t()
        ]
  def dispatch_runner_work(
        %RunState{} = run_state,
        execution_ids,
        reason,
        runner_client,
        runner_opts
      )
      when is_list(execution_ids) do
    envelope = envelope(run_state, reason)

    execution_ids
    |> Enum.filter(&is_binary/1)
    |> Enum.uniq()
    |> Enum.map(fn execution_id ->
      dispatch_one(runner_client, execution_id, envelope, runner_opts)
    end)
  end

  defp dispatch_one(runner_client, execution_id, envelope, runner_opts) do
    execution_id
    |> CancellationOutcome.from_runner_result(
      runner_client.cancel_work(execution_id, envelope, runner_opts)
    )
  rescue
    exception ->
      CancellationOutcome.from_runner_result(execution_id, {:error, exception})
  catch
    kind, reason ->
      CancellationOutcome.from_runner_result(execution_id, {:error, {kind, reason}})
  end
end
