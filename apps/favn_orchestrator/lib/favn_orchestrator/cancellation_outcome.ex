defmodule FavnOrchestrator.CancellationOutcome do
  @moduledoc """
  Orchestrator-facing cancellation outcome for one runner execution.

  The runner reports execution facts; the orchestrator normalizes those facts into
  this DTO before persisting cleanup state or deciding whether a run can be safely
  terminalized as cancelled.
  """

  alias FavnOrchestrator.Redaction

  @type status ::
          :acknowledged
          | :already_completed
          | :not_found
          | :best_effort_failed
          | :unknown_runner_outcome

  @type t :: %__MODULE__{
          execution_id: String.t(),
          status: status(),
          runner_status: atom() | nil,
          native_status: atom() | nil,
          reason_class: atom() | nil,
          correlation_id: String.t() | nil,
          error: term()
        }

  @enforce_keys [:execution_id, :status]
  defstruct [
    :execution_id,
    :status,
    :runner_status,
    :native_status,
    :reason_class,
    :correlation_id,
    :error
  ]

  @doc "Builds a cancellation outcome from a runner cancel response."
  @spec from_runner_result(String.t(), term()) :: t()
  def from_runner_result(execution_id, {:ok, %{status: status} = outcome})
      when is_binary(execution_id) and is_atom(status) do
    %__MODULE__{
      execution_id: execution_id,
      status: normalize_status(status),
      runner_status: status,
      native_status: optional_atom(Map.get(outcome, :native_status)),
      reason_class: optional_atom(Map.get(outcome, :reason_class)),
      correlation_id: optional_string(Map.get(outcome, :correlation_id)),
      error: safe_error(Map.get(outcome, :error))
    }
  end

  def from_runner_result(execution_id, :ok) when is_binary(execution_id) do
    %__MODULE__{execution_id: execution_id, status: :acknowledged, runner_status: :acknowledged}
  end

  def from_runner_result(execution_id, {:error, error}) when is_binary(execution_id) do
    %__MODULE__{
      execution_id: execution_id,
      status: :best_effort_failed,
      runner_status: :error,
      reason_class: classify_reason(error),
      error: safe_error(error)
    }
  end

  def from_runner_result(execution_id, other) when is_binary(execution_id) do
    %__MODULE__{
      execution_id: execution_id,
      status: :unknown_runner_outcome,
      runner_status: :unknown,
      reason_class: classify_reason(other),
      error: safe_error(other)
    }
  end

  @doc "Returns true when cancellation reached a safe terminal runner outcome."
  @spec confirmed?(t() | map()) :: boolean()
  def confirmed?(%__MODULE__{status: status}), do: status in [:acknowledged, :already_completed]
  def confirmed?(%{status: status}), do: status in [:acknowledged, :already_completed]
  def confirmed?(%{"status" => status}), do: status in ["acknowledged", "already_completed"]
  def confirmed?(_outcome), do: false

  @doc "Returns a bounded map for events, metadata, and error DTOs."
  @spec to_map(t()) :: map()
  def to_map(%__MODULE__{} = outcome) do
    %{
      execution_id: outcome.execution_id,
      status: outcome.status,
      runner_status: outcome.runner_status,
      native_status: outcome.native_status,
      reason_class: outcome.reason_class,
      correlation_id: outcome.correlation_id,
      error: outcome.error
    }
    |> Enum.reject(fn {_key, value} -> is_nil(value) end)
    |> Map.new()
  end

  defp normalize_status(status)
       when status in [:acknowledged, :already_completed, :not_found],
       do: status

  defp normalize_status(:best_effort_failed), do: :best_effort_failed
  defp normalize_status(:unknown_runner_outcome), do: :unknown_runner_outcome
  defp normalize_status(_status), do: :unknown_runner_outcome

  defp classify_reason(reason) when is_atom(reason), do: reason
  defp classify_reason(%{type: type}) when is_atom(type), do: type
  defp classify_reason(%{"type" => type}) when is_atom(type), do: type
  defp classify_reason(_reason), do: :unknown

  defp optional_atom(value) when is_atom(value), do: value
  defp optional_atom(_value), do: nil

  defp optional_string(value) when is_binary(value) do
    case Redaction.redact_operational_bounded(value) do
      safe when is_binary(safe) -> safe
      _other -> nil
    end
  end

  defp optional_string(_value), do: nil

  defp safe_error(nil), do: nil

  defp safe_error(error) do
    case Redaction.redact_operational_bounded(%{error: error}) do
      %{error: safe} -> safe
      _other -> "[REDACTED]"
    end
  end
end
