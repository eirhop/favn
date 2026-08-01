defmodule FavnOrchestrator.OperatorRunActivityTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.OperatorRunActivity
  alias FavnOrchestrator.Persistence.Results.RunSubmission

  test "maps physical inspection preparation failures to actionable diagnostics" do
    now = DateTime.utc_now()

    submission = %RunSubmission{
      workspace_id: "workspace",
      submission_id: "submission",
      source: :operator,
      idempotency_key: "key",
      request_hash: <<0>>,
      authority: nil,
      deployment_id: "deployment",
      manifest_version_id: "manifest",
      target_kind: "pipeline",
      target_id: "crm_Reference",
      run_id: "run",
      intent: %{},
      status: :failed,
      attempt: 1,
      claim_generation: 1,
      retry_root_id: "submission",
      error: %{
        "reason" => %{
          "code" => "operator_decision_required",
          "details" => [%{"reason_code" => "physical_inspection_unavailable"}]
        }
      },
      failure_kind: :permanent,
      enqueued_at: now,
      available_at: now,
      terminal_at: now,
      inserted_at: now,
      updated_at: now
    }

    projected = OperatorRunActivity.project_submission(submission)

    assert projected.status == :failed
    assert projected.failure.code == "physical_inspection_unavailable"
    assert projected.failure.title == "Run preparation failed"
    assert projected.failure.remediation =~ "runner diagnostics"
  end
end
