defmodule FavnOrchestrator.RunServer.Execution.PreSubmitFailure do
  @moduledoc false

  alias Favn.Contracts.RunnerError

  @spec normalize(term()) :: RunnerError.t()
  def normalize(%RunnerError{} = error), do: error

  def normalize(reason) do
    RunnerError.normalize(reason,
      phase: :pre_submit,
      retryable?: false,
      outcome: :safe_failure
    )
  end
end
