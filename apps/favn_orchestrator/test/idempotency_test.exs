defmodule FavnOrchestrator.IdempotencyTest do
  use ExUnit.Case, async: true

  alias Favn.Backfill.RangeRequest
  alias FavnOrchestrator.Idempotency
  alias FavnOrchestrator.OperatorCommands.AssetBackfillRequest
  alias FavnOrchestrator.OperatorCommands.PipelineBackfillRequest

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

  test "request HMACs support nested operator command structs" do
    key = String.duplicate("k", 32)

    range = %RangeRequest{
      from: "2026-07",
      to: "2026-07",
      kind: :month,
      mode: :explicit,
      timezone: "Europe/Oslo"
    }

    pipeline_request = %PipelineBackfillRequest{refresh_mode: :missing, range: range}

    requests = [
      pipeline_request,
      %AssetBackfillRequest{
        dependency_mode: :all,
        refresh_mode: :missing,
        range: range
      }
    ]

    fingerprint = fn request ->
      Idempotency.request_hmac(
        %{operation: "backfill.submit", request: request},
        key
      )
    end

    fingerprints = Enum.map(requests, fingerprint)
    changed_request = %{pipeline_request | range: %{range | to: "2026-08"}}

    assert Enum.all?(fingerprints, &(byte_size(&1) == 64))
    assert Enum.uniq(fingerprints) == fingerprints
    assert Enum.map(requests, fingerprint) == fingerprints
    refute fingerprint.(changed_request) == fingerprint.(pipeline_request)
  end
end
