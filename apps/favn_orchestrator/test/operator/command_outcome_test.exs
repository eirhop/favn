defmodule FavnOrchestrator.Operator.CommandOutcomeTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.Persistence.Error

  test "invalid window values are proven rejections regardless of window kind" do
    for kind <- [:hour, :day, :month, :year] do
      refute FavnOrchestrator.operator_command_retryable?(
               {:invalid_window_value, kind, "invalid"}
             )
    end
  end

  test "uncertain results and unfamiliar tuples retain the exact command key" do
    for reason <- [
          :timeout,
          :orchestrator_outcome_unknown,
          {:operator_audit_incomplete, :unavailable},
          {:unexpected_failure, :month, "2021-31"},
          Error.new(:conflict, "unresolved", retryable?: true),
          Error.new(:unavailable, "unavailable")
        ] do
      assert FavnOrchestrator.operator_command_retryable?(reason)
    end
  end
end
