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

  test "validates the exact persisted data-plane marker identity" do
    generation_id = "018f47a0-7b0d-4b1a-8d8b-e18a9a987654"

    attrs = %{
      workspace_id: "workspace",
      target_id: "asset:orders",
      target_generation_id: generation_id,
      creating_manifest_id: "manifest",
      creating_descriptor_hash: String.duplicate("a", 64),
      logical_relation: %{table: "orders"},
      physical_relation: %{table: "orders"},
      data_plane_marker: %{
        "target_id" => "asset:orders",
        "active_relation" => %{
          "connection" => "warehouse",
          "catalog" => nil,
          "schema" => "analytics",
          "name" => "orders"
        },
        "active_generation_id" => generation_id,
        "activation_operation_id" => "initial-materialization",
        "activation_token" => "initial-token",
        "activated_at" => "2026-07-22T10:00:00Z"
      },
      status: :active,
      version: 1,
      created_at: ~U[2026-07-22 10:00:00Z],
      updated_at: ~U[2026-07-22 10:00:00Z]
    }

    assert {:ok, _generation} = TargetGeneration.new(attrs)

    assert {:error,
            {:generation_data_plane_marker_identity_mismatch, :active_generation_id, _, _}} =
             attrs
             |> put_in(
               [:data_plane_marker, "active_generation_id"],
               "118f47a0-7b0d-4b1a-8d8b-e18a9a987654"
             )
             |> TargetGeneration.new()

    assert {:error, {:incomplete_generation_data_plane_marker, :marker, _}} =
             attrs
             |> update_in([:data_plane_marker], &Map.delete(&1, "activation_operation_id"))
             |> TargetGeneration.new()
  end
end
