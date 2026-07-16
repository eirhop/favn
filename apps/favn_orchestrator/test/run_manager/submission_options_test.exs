defmodule FavnOrchestrator.RunManager.SubmissionOptionsTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.RunManager.SubmissionOptions

  test "normalizes validated submission options with explicit defaults" do
    assert {:ok, options} =
             SubmissionOptions.new(
               [
                 run_id: "run_options",
                 dependencies: :none,
                 parent_run_id: "parent",
                 root_run_id: "root",
                 lineage_depth: 2
               ],
               trigger: %{kind: :manual},
               metadata: %{owner: "test"}
             )

    assert options.run_id == "run_options"
    assert options.trigger == %{kind: :manual}
    assert options.metadata == %{owner: "test"}
    assert options.dependencies == :none
    assert options.parent_run_id == "parent"
    assert options.root_run_id == "root"
    assert options.lineage_depth == 2
  end

  test "rejects malformed boundary fields without coercion" do
    invalid = [
      {[:not_keyword], :invalid_options},
      {[run_id: ""], :invalid_run_id},
      {[params: []], :invalid_run_params},
      {[trigger: []], :invalid_pipeline_trigger},
      {[metadata: []], :invalid_run_metadata},
      {[dependencies: :invalid], :invalid_dependencies},
      {[exact_windows: []], :invalid_exact_windows},
      {[parent_run_id: 1], :invalid_parent_run_id},
      {[root_run_id: ""], :invalid_root_run_id},
      {[lineage_depth: -1], :invalid_lineage_depth},
      {[retry_policy: %{max_attempts: 0}], {:invalid_retry_max_attempts, 0}},
      {[timeout_ms: 0], :invalid_timeout_ms}
    ]

    Enum.each(invalid, fn {opts, reason} ->
      assert {:error, ^reason} = SubmissionOptions.new(opts)
    end)
  end

  test "normalizes a typed retry override and ignores no competing legacy form" do
    assert {:ok, options} =
             SubmissionOptions.new(retry_policy: %{max_attempts: 3, backoff: 25})

    assert options.retry_policy_override.max_attempts == 3
    assert options.retry_policy_override.backoff.initial_ms == 25
  end
end
