Code.require_file("../test_support/canonical_sample_project.exs", __DIR__)
Code.require_file("../test_support/single_node_artifact_harness.exs", __DIR__)

ExUnit.start(capture_log: true)

ExUnit.after_suite(fn _result ->
  Favn.Local.SingleNodeArtifactHarness.cleanup_shared_artifacts!()
end)
