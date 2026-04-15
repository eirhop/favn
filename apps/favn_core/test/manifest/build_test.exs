defmodule Favn.Manifest.BuildTest do
  use ExUnit.Case, async: true

  alias Favn.Manifest.Build

  test "build wrapper keeps canonical manifest and build metadata" do
    manifest = %{schema_version: 1, runner_contract_version: 1, assets: []}
    now = ~U[2026-01-01 00:00:00Z]

    build =
      Build.new(manifest,
        diagnostics: [%{message: "warn"}],
        generated_at: now,
        compiler_version: "0.5.0-dev",
        build_metadata: %{source: :test}
      )

    assert build.manifest == manifest
    assert build.generated_at == now
    assert build.compiler_version == "0.5.0-dev"
    assert build.build_metadata == %{source: :test}
    assert build.diagnostics == [%{message: "warn"}]
  end
end
