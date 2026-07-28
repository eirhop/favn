defmodule CrmDemo.Lifecycle.RetryProbe do
  @moduledoc """
  Fails safely twice, then succeeds, to show what `retry/1` actually controls.

  Retry policy decides how many attempts a node gets. It never makes an unsafe
  write retryable. This probe returns `outcome: :safe_failure` because it
  performed no side effect before failing - that is the claim Favn checks before
  it is willing to try again.
  """

  use Favn.Asset

  alias Favn.Contracts.RunnerError

  freshness(:always)
  retry(max_attempts: 3, backoff: 250)
  meta(tags: [:lifecycle])

  @doc "Returns a known-safe failure until the third attempt."
  def asset(%{attempt: attempt}) do
    if attempt < 3 do
      {:error,
       RunnerError.new(
         type: :simulated_source_unavailable,
         message: "Retry probe failed safely on attempt #{attempt}",
         retryable?: true,
         outcome: :safe_failure,
         retry_after_ms: 50
       )}
    else
      {:ok, %{attempt: attempt, result: "recovered"}}
    end
  end
end
