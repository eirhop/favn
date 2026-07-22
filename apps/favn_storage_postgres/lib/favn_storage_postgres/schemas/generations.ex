defmodule FavnStoragePostgres.Schemas.AssetTargetGeneration do
  @moduledoc false
  use Ecto.Schema

  @primary_key false
  @schema_prefix "favn_control"
  schema "asset_target_generations" do
    field(:workspace_id, :string, primary_key: true)
    field(:target_id, :string, primary_key: true)
    field(:target_generation_id, Ecto.UUID, primary_key: true)
    field(:creating_manifest_id, :string)
    field(:creation_command_id, :string)
    field(:creating_descriptor_hash, :string)
    field(:active_descriptor_hash, :string)
    field(:logical_relation, :map)
    field(:physical_relation, :map)
    field(:physical_schema_fingerprint, :string)
    field(:data_plane_marker, :map)
    field(:activation_token, :string)
    field(:status, :string)
    field(:creating_rebuild_operation_id, :string)
    field(:version, :integer)
    field(:created_at, :utc_datetime_usec)
    field(:activated_at, :utc_datetime_usec)
    field(:retired_at, :utc_datetime_usec)
    field(:updated_at, :utc_datetime_usec)
  end
end

defmodule FavnStoragePostgres.Schemas.AssetTargetBinding do
  @moduledoc false
  use Ecto.Schema

  @primary_key false
  @schema_prefix "favn_control"
  schema "asset_target_bindings" do
    field(:workspace_id, :string, primary_key: true)
    field(:target_id, :string, primary_key: true)
    field(:active_generation_id, Ecto.UUID)
    field(:desired_manifest_id, :string)
    field(:desired_descriptor_hash, :string)
    field(:compatibility_status, :string)
    field(:reason_code, :string)
    field(:compatibility_diff, :map)
    field(:active_physical_fingerprint, :string)
    field(:version, :integer)
    field(:updated_at, :utc_datetime_usec)
  end
end

defmodule FavnStoragePostgres.Schemas.RebuildOperation do
  @moduledoc false
  use Ecto.Schema

  @primary_key false
  @schema_prefix "favn_control"
  schema "rebuild_operations" do
    field(:workspace_id, :string, primary_key: true)
    field(:operation_id, :string, primary_key: true)
    field(:root_target_id, :string)
    field(:manifest_version_id, :string)
    field(:active_generation_id, Ecto.UUID)
    field(:candidate_generation_id, Ecto.UUID)
    field(:plan_hash, :string)
    field(:plan_version, :integer)
    field(:trigger, :string)
    field(:actor_id, :string)
    field(:session_id, :string)
    field(:reason, :string)
    field(:idempotency_key, :string)
    field(:evaluated_at, :utc_datetime_usec)
    field(:coverage_start, :utc_datetime_usec)
    field(:coverage_end, :utc_datetime_usec)
    field(:action_count, :integer)
    field(:window_count, :integer)
    field(:state, :string)
    field(:phase, :string)
    field(:activation_token, :string)
    field(:dispatched_at, :utc_datetime_usec)
    field(:result_marker, :map)
    field(:unknown_outcome, :map)
    field(:validation_result, :map)
    field(:terminal_error, :map)
    field(:cleanup_state, :string)
    field(:version, :integer)
    field(:started_at, :utc_datetime_usec)
    field(:completed_at, :utc_datetime_usec)
    field(:cancelled_at, :utc_datetime_usec)
    timestamps(type: :utc_datetime_usec)
  end
end

defmodule FavnStoragePostgres.Schemas.RebuildPlanAction do
  @moduledoc false
  use Ecto.Schema

  @primary_key false
  @schema_prefix "favn_control"
  schema "rebuild_plan_actions" do
    field(:workspace_id, :string, primary_key: true)
    field(:operation_id, :string, primary_key: true)
    field(:target_id, :string, primary_key: true)
    field(:ordinal, :integer)
    field(:action, :string)
    field(:reason, :map)
    field(:upstream_impact, :map)
    field(:mapping_proof, :map)
    field(:pinned_input_generation_ids, :map)
    field(:candidate_generation_id, Ecto.UUID)
    field(:status, :string)
    field(:child_operation_id, :string)
    field(:child_run_id, :string)
    field(:version, :integer)
    timestamps(type: :utc_datetime_usec)
  end
end

defmodule FavnStoragePostgres.Schemas.RebuildWindow do
  @moduledoc false
  use Ecto.Schema

  @primary_key false
  @schema_prefix "favn_control"
  schema "rebuild_windows" do
    field(:workspace_id, :string, primary_key: true)
    field(:operation_id, :string, primary_key: true)
    field(:target_id, :string, primary_key: true)
    field(:item_id, :string, primary_key: true)
    field(:ordinal, :integer)
    field(:work_kind, :string)
    field(:window_key, :string)
    field(:window_start, :utc_datetime_usec)
    field(:window_end, :utc_datetime_usec)
    field(:status, :string)
    field(:claim_owner, :string)
    field(:fencing_token, :integer)
    field(:claim_command_id, :string)
    field(:last_command_id, :string)
    field(:claim_expires_at, :utc_datetime_usec)
    field(:child_run_id, :string)
    field(:materialization_id, :string)
    field(:attempt_count, :integer)
    field(:row_count, :integer)
    field(:last_error, :map)
    field(:candidate_generation_id, Ecto.UUID)
    field(:version, :integer)
    timestamps(type: :utc_datetime_usec)
  end
end

defmodule FavnStoragePostgres.Schemas.TargetOperationLock do
  @moduledoc false
  use Ecto.Schema

  @primary_key false
  @schema_prefix "favn_control"
  schema "target_operation_locks" do
    field(:workspace_id, :string, primary_key: true)
    field(:target_id, :string, primary_key: true)
    field(:operation_id, :string)
    field(:operation_type, :string)
    field(:fencing_token, :integer)
    field(:lease_owner, :string)
    field(:lease_expires_at, :utc_datetime_usec)
    field(:version, :integer)
    timestamps(type: :utc_datetime_usec)
  end
end
