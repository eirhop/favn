defmodule FavnView.OperatorErrorLabels do
  @moduledoc """
  Safe browser-facing labels for operator errors.

  This module is owned by `favn_view` and intentionally does not stringify or
  inspect arbitrary backend reasons. Keep raw reasons in server logs only.
  """

  @type reason :: term()

  @doc "Returns a safe label for catalogue and detail load failures."
  @spec load(reason()) :: String.t() | atom()
  def load(reason), do: :load |> FavnOrchestrator.operator_error(reason) |> load_label()

  @doc "Returns a safe label for schedule occurrence preview failures."
  @spec schedule_occurrences(reason()) :: String.t()
  def schedule_occurrences(reason),
    do: :schedule_occurrences |> FavnOrchestrator.operator_error(reason) |> detail()

  @doc "Returns a safe label for schedule activation changes."
  @spec schedule_activation(reason()) :: String.t()
  def schedule_activation(reason),
    do: :schedule_activation |> FavnOrchestrator.operator_error(reason) |> detail()

  @doc "Returns a safe label for run cancellation failures."
  @spec run_cancel(reason()) :: String.t()
  def run_cancel(reason), do: :run_cancel |> FavnOrchestrator.operator_error(reason) |> detail()

  @doc "Returns a safe label for arbitrary run failure details."
  @spec run_failure_detail(reason()) :: String.t()
  def run_failure_detail(reason),
    do: :run_failure_detail |> FavnOrchestrator.operator_error(reason) |> detail()

  defp load_label(%FavnOrchestrator.OperatorErrorDTO{code: :not_found}), do: :not_found
  defp load_label(%FavnOrchestrator.OperatorErrorDTO{} = dto), do: dto.detail

  defp detail(%FavnOrchestrator.OperatorErrorDTO{} = dto), do: dto.detail
end
