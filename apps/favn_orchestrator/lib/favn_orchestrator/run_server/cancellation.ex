defmodule FavnOrchestrator.RunServer.Cancellation do
  @moduledoc """
  Shared runner cancellation payload and dispatch helpers for run-server paths.

  This module owns the runner cancellation envelope contract only. Callers remain
  responsible for any local run-state cleanup after dispatching cancellation.
  """

  alias FavnOrchestrator.RunState

  @type execution_id :: String.t()
  @type reason :: term()
  @type envelope :: %{
          required(:run_id) => String.t(),
          required(:reason) => reason(),
          required(:requested_at) => DateTime.t()
        }

  @doc """
  Wraps a runner cancellation reason in the control-plane cancellation envelope.
  """
  @spec envelope(RunState.t(), reason()) :: envelope()
  def envelope(%RunState{id: run_id}, reason) do
    %{run_id: run_id, reason: reason, requested_at: DateTime.utc_now()}
  end

  @doc """
  Cancels unique runner execution ids and returns the ids that were dispatched.

  Cancellation dispatch is best-effort; individual runner-client errors are
  intentionally ignored to preserve the existing run-server cleanup semantics.
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
    envelope = envelope(run_state, reason)

    Enum.each(unique_ids, fn execution_id ->
      _ = runner_client.cancel_work(execution_id, envelope, runner_opts)
    end)

    unique_ids
  end
end
