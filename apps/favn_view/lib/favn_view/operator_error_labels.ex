defmodule FavnView.OperatorErrorLabels do
  @moduledoc """
  Safe browser-facing labels for operator errors.

  This module is owned by `favn_view` and intentionally does not stringify or
  inspect arbitrary backend reasons. Keep raw reasons in server logs only.

  `failure/2` is the one place a DTO is unwrapped, because a failed command needs
  two facts rather than one: what to tell the operator, and whether the
  orchestrator considers the outcome worth retrying. Retryability decides whether
  a command keeps its idempotency key — see `FavnView.CommandAttempt`.
  """

  @type reason :: term()
  @type context ::
          :load | :schedule_occurrences | :schedule_activation | :run_cancel | :run_failure_detail
  @type failure :: %{label: String.t(), retryable?: boolean()}

  @doc "Returns a safe label for catalogue and detail load failures."
  @spec load(reason()) :: String.t() | atom()
  def load(reason), do: :load |> FavnOrchestrator.operator_error(reason) |> load_label()

  @doc "Returns a safe label for schedule occurrence preview failures."
  @spec schedule_occurrences(reason()) :: String.t()
  def schedule_occurrences(reason), do: failure(:schedule_occurrences, reason).label

  @doc "Returns a safe label for schedule activation changes."
  @spec schedule_activation(reason()) :: String.t()
  def schedule_activation(reason), do: failure(:schedule_activation, reason).label

  @doc "Returns a safe label for run cancellation failures."
  @spec run_cancel(reason()) :: String.t()
  def run_cancel(reason), do: failure(:run_cancel, reason).label

  @doc "Returns a safe label for arbitrary run failure details."
  @spec run_failure_detail(reason()) :: String.t()
  def run_failure_detail(reason), do: failure(:run_failure_detail, reason).label

  @doc """
  Returns the safe label and the retry advice for one failed operator command.

  A retryable outcome may already have taken effect, so the caller must replay
  the same command rather than issue a new one. A non-retryable outcome — a
  refusal, a missing resource — mutated nothing.
  """
  @spec failure(context(), reason()) :: failure()
  def failure(context, reason) do
    dto = FavnOrchestrator.operator_error(context, reason)
    %{label: dto.detail, retryable?: dto.retryable? == true}
  end

  defp load_label(%FavnOrchestrator.OperatorErrorDTO{code: :not_found}), do: :not_found
  defp load_label(%FavnOrchestrator.OperatorErrorDTO{} = dto), do: dto.detail
end
