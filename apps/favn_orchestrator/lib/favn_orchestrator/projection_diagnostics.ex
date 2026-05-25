defmodule FavnOrchestrator.ProjectionDiagnostics do
  @moduledoc """
  Bounded in-memory diagnostics for degraded derived projections.

  Projection state is derived and repairable, but operators need to know when a
  projector failed and repair may be needed. This module exposes the latest
  degraded projection facts without making projection state authoritative.
  """

  alias FavnOrchestrator.Redaction
  alias FavnOrchestrator.RunState

  @key {__MODULE__, :latest_failures}
  @max_failures 10

  @type failure :: %{
          required(:projector) => String.t(),
          required(:run_id) => String.t(),
          required(:event_type) => atom(),
          required(:reason) => term(),
          required(:occurred_at) => DateTime.t()
        }

  @doc "Records one projection failure for operator diagnostics."
  @spec record_failure(module(), RunState.t(), atom(), term()) :: :ok
  def record_failure(projector, %RunState{id: run_id}, event_type, reason)
      when is_atom(projector) and is_atom(event_type) do
    failure = %{
      projector: inspect(projector),
      run_id: run_id,
      event_type: event_type,
      reason: Redaction.redact_untrusted(reason),
      occurred_at: DateTime.utc_now()
    }

    failures = [failure | failures()] |> Enum.take(@max_failures)
    :persistent_term.put(@key, failures)
    :ok
  end

  @doc "Returns projection degradation diagnostics for operator readiness reports."
  @spec diagnostics() :: %{
          status: :ok | :degraded,
          repair_needed?: boolean(),
          failures: [failure()]
        }
  def diagnostics do
    failures = failures()

    %{
      status: if(failures == [], do: :ok, else: :degraded),
      repair_needed?: failures != [],
      failures: failures
    }
  end

  @doc false
  @spec reset() :: :ok
  def reset do
    :persistent_term.erase(@key)
    :ok
  rescue
    ArgumentError -> :ok
  end

  defp failures, do: :persistent_term.get(@key, [])
end
