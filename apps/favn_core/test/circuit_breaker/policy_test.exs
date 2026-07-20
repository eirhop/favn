defmodule Favn.CircuitBreaker.PolicyTest do
  use ExUnit.Case, async: true

  alias Favn.CircuitBreaker.Policy

  test "normalizes keyword and JSON-shaped policies" do
    assert {:ok, %Policy{failure_threshold: 3, probe_after_ms: 60_000}} =
             Policy.new(failure_threshold: 3, probe_after_ms: 60_000)

    assert {:ok, %Policy{failure_threshold: 5, probe_after_ms: 120_000}} =
             Policy.new(%{"failure_threshold" => 5, "probe_after_ms" => 120_000})
  end

  test "rejects ambiguous or unbounded options" do
    assert {:error, {:unknown_circuit_breaker_options, [:reset_after]}} =
             Policy.new(failure_threshold: 3, probe_after_ms: 10, reset_after: 10)

    assert {:error, {:invalid_circuit_breaker_failure_threshold, 0}} =
             Policy.new(failure_threshold: 0, probe_after_ms: 10)

    assert {:error, {:invalid_circuit_breaker_probe_after_ms, 0}} =
             Policy.new(failure_threshold: 1, probe_after_ms: 0)
  end
end
