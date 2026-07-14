defmodule FavnOrchestrator.Storage.LogEntryCodecTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.Storage.LogEntryCodec

  test "round-trips normalized entries through the explicit DTO" do
    now = ~U[2026-07-14 08:30:00.123456Z]

    assert {:ok, entry} =
             LogEntryCodec.normalize(%{
               run_id: "run_log_codec",
               node_key: {{__MODULE__.Asset, :orders}, nil},
               asset_ref: {__MODULE__.Asset, :orders},
               occurred_at: now,
               level: :warning,
               source: :runner,
               stream: :stderr,
               message: "retrying",
               metadata: %{attempt: 2},
               producer_id: "runner-1",
               producer_sequence: 0,
               attempt: 2
             })

    entry = LogEntryCodec.assign_global_sequence(entry, 1)
    assert {:ok, payload} = LogEntryCodec.encode(entry)
    assert {:ok, restored} = LogEntryCodec.decode(payload)

    assert restored.id == entry.id
    assert restored.global_sequence == 1
    assert restored.node_key == entry.node_key
    assert restored.asset_ref == entry.asset_ref
    assert restored.occurred_at == now
    assert restored.metadata == %{"attempt" => 2}
  end

  test "rejects invalid ingress fields instead of replacing them with defaults" do
    assert {:error, {:invalid_log_entry_field, :id, 123}} =
             LogEntryCodec.normalize(%{id: 123, message: "bad"})

    assert {:error, {:invalid_log_entry_field, :attempt, 0}} =
             LogEntryCodec.normalize(%{attempt: 0, message: "bad"})

    assert {:error, {:invalid_log_entry_field, :metadata, []}} =
             LogEntryCodec.normalize(%{metadata: [], message: "bad"})

    assert {:error, {:invalid_log_entry, %ArgumentError{}}} =
             LogEntryCodec.normalize(%{level: :verbose, message: "bad"})
  end

  test "rejects malformed persisted fields and payloads" do
    {:ok, entry} = LogEntryCodec.normalize(%{message: "hello"})
    {:ok, payload} = entry |> LogEntryCodec.assign_global_sequence(1) |> LogEntryCodec.encode()
    dto = Jason.decode!(payload)

    for {field, value, expected_field} <- [
          {"id", nil, :id},
          {"global_sequence", 0, :global_sequence},
          {"occurred_at", nil, :occurred_at},
          {"message", 123, :message},
          {"metadata", [], :metadata},
          {"truncated", "false", :truncated}
        ] do
      assert {:error, {:invalid_log_entry_field, ^expected_field, _}} =
               dto |> Map.put(field, value) |> Jason.encode!() |> LogEntryCodec.decode()
    end

    assert {:error, {:invalid_log_entry_payload_field, _}} =
             dto
             |> Map.put("node_key_payload", "not-json")
             |> Jason.encode!()
             |> LogEntryCodec.decode()

    assert {:error, {:unknown_log_entry_fields, ["unexpected"]}} =
             dto
             |> Map.put("unexpected", true)
             |> Jason.encode!()
             |> LogEntryCodec.decode()
  end
end
