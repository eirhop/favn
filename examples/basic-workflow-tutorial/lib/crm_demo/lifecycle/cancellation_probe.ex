defmodule CrmDemo.Lifecycle.CancellationProbe do
  @moduledoc """
  Stays busy long enough to cancel with `mix favn.runs cancel`.

  Submit it with `--no-wait`, then cancel the persisted run.
  """

  use Favn.Asset

  settings(duration_ms: 30_000)
  freshness(:always)
  meta(tags: [:lifecycle])

  @doc "Sleeps without performing any side effect."
  def asset(ctx) do
    Process.sleep(ctx.asset.settings.duration_ms)
    {:ok, %{result: "completed_without_cancellation"}}
  end
end
