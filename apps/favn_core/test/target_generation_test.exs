defmodule Favn.TargetGenerationTest do
  use ExUnit.Case, async: true

  alias Favn.TargetGeneration

  test "validates stable generation identity and lifecycle state" do
    now = ~U[2026-07-22 10:00:00Z]

    assert {:ok, generation} =
             TargetGeneration.new(%{
               workspace_id: "workspace",
               target_id: "asset:orders",
               target_generation_id: "018f47a0-7b0d-4b1a-8d8b-e18a9a987654",
               creating_manifest_id: "manifest",
               creating_descriptor_hash: String.duplicate("a", 64),
               logical_relation: %{schema: "analytics", table: "orders"},
               physical_relation: %{schema: "analytics", table: "orders"},
               status: :building,
               version: 1,
               created_at: now,
               updated_at: now
             })

    assert generation.status == :building
    assert :active in TargetGeneration.statuses()
  end

  test "rejects malformed UUIDs and descriptor hashes" do
    attrs = %{
      workspace_id: "workspace",
      target_id: "asset:orders",
      target_generation_id: "not-a-uuid",
      creating_manifest_id: "manifest",
      creating_descriptor_hash: String.duplicate("a", 64),
      logical_relation: %{table: "orders"},
      physical_relation: %{table: "orders"},
      status: :building,
      version: 1,
      created_at: ~U[2026-07-22 10:00:00Z],
      updated_at: ~U[2026-07-22 10:00:00Z]
    }

    assert {:error, {:invalid_target_generation_id, "not-a-uuid"}} =
             TargetGeneration.new(attrs)

    assert {:error, {:invalid_target_generation_hash, "short"}} =
             attrs
             |> Map.put(:target_generation_id, "018f47a0-7b0d-4b1a-8d8b-e18a9a987654")
             |> Map.put(:creating_descriptor_hash, "short")
             |> TargetGeneration.new()
  end

  test "validates optional physical and lifecycle fields" do
    attrs = %{
      workspace_id: "workspace",
      target_id: "asset:orders",
      target_generation_id: "018f47a0-7b0d-4b1a-8d8b-e18a9a987654",
      creating_manifest_id: "manifest",
      creating_descriptor_hash: String.duplicate("a", 64),
      logical_relation: %{table: "orders"},
      physical_relation: %{table: "orders"},
      status: :active,
      version: 1,
      created_at: ~U[2026-07-22 10:00:00Z],
      updated_at: ~U[2026-07-22 10:00:00Z]
    }

    assert {:error, {:invalid_target_generation_hash, "short"}} =
             attrs
             |> Map.put(:physical_schema_fingerprint, "short")
             |> TargetGeneration.new()

    assert {:error, :invalid_target_generation_timestamps} =
             attrs
             |> Map.put(:activated_at, "yesterday")
             |> TargetGeneration.new()
  end
end
