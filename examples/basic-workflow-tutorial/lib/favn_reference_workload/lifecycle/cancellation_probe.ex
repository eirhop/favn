defmodule FavnReferenceWorkload.Lifecycle.CancellationProbe do
  @moduledoc """
  Slow, side-effect-free lifecycle probe for CLI cancellation testing.

  It deliberately remains active long enough for an operator to submit it with
  `--no-wait` and cancel the persisted run through `mix favn.runs`.
  """

  use Favn.Asset

  settings(duration_ms: 30_000)
  meta(category: :lifecycle_probe, tags: [:cli_qa, :cancellation])
  freshness(:always)

  @doc "Sleep for the configured duration without performing external writes."
  def asset(ctx) do
    duration_ms = ctx.asset.settings.duration_ms
    Process.sleep(duration_ms)
    {:ok, %{duration_ms: duration_ms, result: "completed_without_cancellation"}}
  end
end
