defmodule FavnOrchestrator.ExecutionStatus do
  @moduledoc """
  Normalizes and classifies persisted run, step, and window statuses.

  Unknown strings remain strings. Persisted input never creates atoms.
  """

  @statuses [
    :pending,
    :queued,
    :running,
    :retrying,
    :ok,
    :partial,
    :error,
    :blocked,
    :cancelled,
    :timed_out,
    :skipped,
    :skipped_fresh
  ]
  @statuses_by_name Map.new(@statuses, &{Atom.to_string(&1), &1})
  @terminal_statuses [
    :ok,
    :partial,
    :error,
    :blocked,
    :cancelled,
    :timed_out,
    :skipped,
    :skipped_fresh
  ]
  @failed_statuses [:error, :timed_out, :cancelled, :blocked]
  @running_statuses [:running, :retrying]
  @queued_statuses [:pending, :queued, nil]

  @type t :: atom() | String.t() | nil

  @doc "Returns the allowlisted atom for a known persisted status name."
  @spec normalize(t()) :: t()
  def normalize(status) when is_atom(status), do: status
  def normalize(status) when is_binary(status), do: Map.get(@statuses_by_name, status, status)
  def normalize(status), do: status

  @doc "Returns whether a status is terminal."
  @spec terminal?(t()) :: boolean()
  def terminal?(status), do: normalize(status) in @terminal_statuses

  @doc "Returns whether a status represents failure."
  @spec failed?(t()) :: boolean()
  def failed?(status), do: normalize(status) in @failed_statuses

  @doc "Returns whether a status represents active execution."
  @spec running?(t()) :: boolean()
  def running?(status), do: normalize(status) in @running_statuses

  @doc "Returns whether a status is waiting to execute."
  @spec queued?(t()) :: boolean()
  def queued?(status), do: normalize(status) in @queued_statuses

  @doc "Returns whether a run or window has not reached a terminal state."
  @spec active?(t()) :: boolean()
  def active?(status), do: normalize(status) in [:pending, :running]
end
