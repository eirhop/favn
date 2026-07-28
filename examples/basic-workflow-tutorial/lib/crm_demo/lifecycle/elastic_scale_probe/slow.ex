defmodule CrmDemo.Lifecycle.ElasticScaleProbe.Slow do
  @moduledoc """
  Long branch of the elastic-runner deployment probe.

  It performs no side effect and remains active after the other two branches
  have completed so the final one-runner state is observable.
  """

  use Favn.Asset

  runner_pool(:default)
  settings(duration_ms: 60_000)
  freshness(:always)
  meta(tags: [:lifecycle, :elastic_scale_probe])

  @doc "Waits for the configured duration and returns deterministic probe metadata."
  def asset(ctx) do
    Process.sleep(ctx.asset.settings.duration_ms)
    {:ok, %{probe: "slow", duration_ms: ctx.asset.settings.duration_ms}}
  end
end
