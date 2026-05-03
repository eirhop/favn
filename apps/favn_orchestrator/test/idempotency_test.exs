defmodule FavnOrchestrator.IdempotencyTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.Idempotency

  test "request fingerprints preserve JSON value types" do
    refute Idempotency.request_fingerprint(%{value: true}) ==
             Idempotency.request_fingerprint(%{value: "true"})

    refute Idempotency.request_fingerprint(%{value: false}) ==
             Idempotency.request_fingerprint(%{value: "false"})

    refute Idempotency.request_fingerprint(%{value: nil}) ==
             Idempotency.request_fingerprint(%{value: "nil"})
  end

  test "request fingerprints are stable across map key order and key type" do
    assert Idempotency.request_fingerprint(%{
             operation: "run.submit",
             request: %{:a => 1, "b" => true}
           }) ==
             Idempotency.request_fingerprint(%{
               "request" => %{"b" => true, "a" => 1},
               "operation" => "run.submit"
             })
  end
end
