defmodule FavnOrchestrator.Rebuild.ReconciliationTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.Rebuild.Reconciliation

  test "persisted cancellation wins after either unknown-outcome reconciliation path" do
    assert Reconciliation.next(true, :activation) == :cancel
    assert Reconciliation.next(true, :items) == :cancel
  end

  test "uncancelled reconciliation resumes only its owning lifecycle" do
    assert Reconciliation.next(false, :activation) == :activate
    assert Reconciliation.next(false, :items) == :build
  end
end
