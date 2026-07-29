defmodule CrmDemo.Lifecycle.ElasticScaleProbe.Medium do
  @moduledoc """
  Medium branch of the elastic-runner deployment probe.

  It performs no side effect and stays active long enough for the fast runner
  to finish and self-exit first.
  """

  use Favn.Asset

  runner_pool(:default)
  settings(duration_ms: 40_000)
  freshness(:always)
  meta(tags: [:lifecycle, :elastic_scale_probe])

  @doc "Waits for the configured duration and returns deterministic probe metadata."
  def asset(ctx) do
    Process.sleep(ctx.asset.settings.duration_ms)
    {:ok, %{probe: "medium", duration_ms: ctx.asset.settings.duration_ms}}
  end
end
