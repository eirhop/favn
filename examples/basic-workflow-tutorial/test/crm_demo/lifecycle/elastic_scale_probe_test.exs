defmodule CrmDemo.Lifecycle.ElasticScaleProbeTest do
  use ExUnit.Case, async: true

  alias CrmDemo.Lifecycle.ElasticScaleProbe.Fast
  alias CrmDemo.Lifecycle.ElasticScaleProbe.Medium
  alias CrmDemo.Lifecycle.ElasticScaleProbe.Slow

  test "declares three independent default-pool probes with staggered durations" do
    assert [20_000, 40_000, 60_000] ==
             [Fast, Medium, Slow]
             |> Enum.map(fn module ->
               [asset] = module.__favn_assets__()
               assert asset.runner_pool == :default
               asset.settings.duration_ms
             end)
  end
end
