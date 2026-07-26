defmodule FavnReferenceWorkload.Lifecycle.RetryProbe do
  @moduledoc """
  Pure lifecycle probe that safely fails twice and succeeds on its third attempt.

  The probe performs no write or external side effect before returning its
  explicit known-safe failure, so it is suitable for exercising node retries.
  """

  use Favn.Asset

  alias Favn.Contracts.RunnerError

  meta(category: :lifecycle_probe, tags: [:cli_qa, :retry])
  freshness(:always)
  retry(max_attempts: 3, backoff: 250)

  @doc "Return a known-safe failure until the third orchestrated attempt."
  def asset(%{attempt: attempt, max_attempts: max_attempts}) do
    if attempt < 3 do
      {:error,
       RunnerError.new(
         type: :simulated_source_unavailable,
         message: "Lifecycle retry probe failed safely on attempt #{attempt}",
         retryable?: true,
         outcome: :safe_failure,
         retry_after_ms: 50,
         details: %{attempt: attempt, max_attempts: max_attempts}
       )}
    else
      {:ok, %{attempt: attempt, max_attempts: max_attempts, result: "recovered"}}
    end
  end
end
