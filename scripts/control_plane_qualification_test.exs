Code.require_file("control_plane_qualification.ex", __DIR__)

ExUnit.start()

defmodule Favn.ControlPlaneQualificationTest do
  use ExUnit.Case, async: true

  alias Favn.ControlPlaneQualification

  @repo_root Path.expand("..", __DIR__)
  @build_id String.duplicate("a", 64)
  @other_build_id String.duplicate("b", 64)

  test "computes deterministic runtime and scan identities" do
    assert {:ok, first} = ControlPlaneQualification.identities(@repo_root, @build_id)
    assert {:ok, second} = ControlPlaneQualification.identities(@repo_root, @build_id)

    assert first == second
    assert first.runtime_qualification_id =~ ~r/\Acpr_[0-9a-f]{64}\z/
    assert first.security_scan_id =~ ~r/\Acps_[0-9a-f]{64}\z/

    assert {:ok, changed} = ControlPlaneQualification.identities(@repo_root, @other_build_id)
    refute first.runtime_qualification_id == changed.runtime_qualification_id
    refute first.security_scan_id == changed.security_scan_id
  end

  test "routes image, runtime, scan, and unaffected paths independently" do
    assert ControlPlaneQualification.path_categories("apps/favn_view/lib/page.ex") == [:image]

    assert ControlPlaneQualification.path_categories("apps/favn_runner/lib/worker.ex") == [
             :runtime
           ]

    assert ControlPlaneQualification.path_categories("apps/favn_duckdb/lib/adapter.ex") == [
             :runtime
           ]

    assert ControlPlaneQualification.path_categories(
             "apps/favn_local/lib/favn/dev/compose_lifecycle.ex"
           ) == [:runtime]

    assert ControlPlaneQualification.path_categories("security/control-plane-grype.yaml") == [
             :scan
           ]

    assert ControlPlaneQualification.path_categories(".github/workflows/control-plane-image.yml") ==
             [:runtime, :scan]

    for path <- [
          ".github/workflows/ci.yml",
          "apps/favn_duckdb_adbc/lib/adapter.ex",
          "apps/favn_azure/lib/plugin.ex",
          "apps/favn_local/lib/favn/dev/backfill.ex",
          "apps/favn_runner/test/worker_test.exs",
          "docs/production/control_plane_image.md"
        ] do
      assert ControlPlaneQualification.path_categories(path) == [:unaffected]
    end
  end

  test "unknown paths conservatively request runtime qualification" do
    assert %{unknown_runtime_paths: ["apps/new_adapter/lib/new_adapter.ex"]} =
             ControlPlaneQualification.classify_paths([
               "docs/README.md",
               "apps/new_adapter/lib/new_adapter.ex"
             ])
  end

  test "runtime input collection excludes unrelated local commands" do
    assert {:ok, paths} = ControlPlaneQualification.input_paths(@repo_root, :runtime)
    assert "apps/favn_local/lib/favn/dev/compose_lifecycle.ex" in paths
    assert "apps/favn_duckdb/lib/favn_duckdb.ex" in paths
    refute "apps/favn_local/lib/favn/dev/backfill.ex" in paths
    refute Enum.any?(paths, &String.starts_with?(&1, "apps/favn_duckdb_adbc/"))
  end
end
