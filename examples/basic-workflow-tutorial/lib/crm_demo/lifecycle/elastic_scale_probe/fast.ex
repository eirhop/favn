defmodule CrmDemo.Lifecycle.ElasticScaleProbe.Fast do
  @moduledoc """
  Short branch of the elastic-runner deployment probe.

  It performs no side effect. The staggered wait makes runner scale-down visible
  without introducing an external dependency.
  """

  use Favn.Asset

  runner_pool(:default)
  settings(duration_ms: 20_000)
  freshness(:always)
  meta(tags: [:lifecycle, :elastic_scale_probe])

  @doc "Waits for the configured duration and returns deterministic probe metadata."
  def asset(ctx) do
    Process.sleep(ctx.asset.settings.duration_ms)
    {:ok, %{probe: "fast", duration_ms: ctx.asset.settings.duration_ms}}
  end
end
