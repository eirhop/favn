defmodule FavnStoragePostgres.Schemas.ProjectionCursor do
  @moduledoc false
  use Ecto.Schema

  @primary_key false
  @schema_prefix "favn_control"
  schema "projection_cursors" do
    field(:projector_name, :string, primary_key: true)
    field(:shard_id, :integer, primary_key: true)
    field(:last_publication_id, :integer)
    field(:owner_id, :string)
    field(:fencing_token, :integer)
    field(:claim_expires_at, :utc_datetime_usec)
    field(:version, :integer)
    field(:updated_at, :utc_datetime_usec)
  end
end

defmodule FavnStoragePostgres.Schemas.ExecutionGroupOverview do
  @moduledoc false
  use Ecto.Schema

  @primary_key false
  @schema_prefix "favn_control"
  schema "execution_group_overviews" do
    field(:workspace_id, :string, primary_key: true)
    field(:root_run_id, :string, primary_key: true)
    field(:status, :string)
    field(:run_count, :integer)
    field(:pending_count, :integer)
    field(:running_count, :integer)
    field(:succeeded_count, :integer)
    field(:failed_count, :integer)
    field(:latest_event_id, :integer)
    field(:source_publication_id, :integer)
    field(:inserted_at, :utc_datetime_usec)
    field(:updated_at, :utc_datetime_usec)
  end
end

defmodule FavnStoragePostgres.Schemas.TargetStatus do
  @moduledoc false
  use Ecto.Schema

  @primary_key false
  @schema_prefix "favn_control"
  schema "target_statuses" do
    field(:workspace_id, :string, primary_key: true)
    field(:deployment_id, :string, primary_key: true)
    field(:target_kind, :string, primary_key: true)
    field(:target_id, :string, primary_key: true)
    field(:status, :string)
    field(:run_id, :string)
    field(:event_id, :integer)
    field(:source_publication_id, :integer)
    field(:updated_at, :utc_datetime_usec)
  end
end

defmodule FavnStoragePostgres.Schemas.AssetWindowState do
  @moduledoc false
  use Ecto.Schema

  @primary_key false
  @schema_prefix "favn_control"
  schema "asset_window_states" do
    field(:workspace_id, :string, primary_key: true)
    field(:evidence_generation_id, :string, primary_key: true)
    field(:manifest_version_id, :string)
    field(:target_id, :string, primary_key: true)
    field(:window_key, :string, primary_key: true)
    field(:window_start, :utc_datetime_usec)
    field(:window_end, :utc_datetime_usec)
    field(:status, :string)
    field(:run_id, :string)
    field(:materialization_id, :string)
    field(:payload, :map)
    field(:source_publication_id, :integer)
    field(:updated_at, :utc_datetime_usec)
  end
end

defmodule FavnStoragePostgres.Schemas.AssetFreshnessState do
  @moduledoc false
  use Ecto.Schema

  @primary_key false
  @schema_prefix "favn_control"
  schema "asset_freshness_states" do
    field(:workspace_id, :string, primary_key: true)
    field(:evidence_generation_id, :string, primary_key: true)
    field(:deployment_id, :string)
    field(:manifest_version_id, :string)
    field(:target_id, :string, primary_key: true)
    field(:freshness_key, :string, primary_key: true)
    field(:latest_attempt_materialization_id, :string)
    field(:latest_success_materialization_id, :string)
    field(:latest_success_node_key_hash, :binary)
    field(:input_fingerprint, :binary)
    field(:status, :string)
    field(:payload, :map)
    field(:source_publication_id, :integer)
    field(:updated_at, :utc_datetime_usec)
  end
end

defmodule FavnStoragePostgres.Schemas.AssetAttemptOverview do
  @moduledoc false
  use Ecto.Schema

  @primary_key false
  @schema_prefix "favn_control"
  schema "asset_attempt_overviews" do
    field(:workspace_id, :string, primary_key: true)
    field(:root_run_id, :string, primary_key: true)
    field(:run_id, :string, primary_key: true)
    field(:asset_step_id, :string, primary_key: true)
    field(:asset_ref, :string)
    field(:window_identity, :string)
    field(:window, :map)
    field(:status, :string)
    field(:stage, :integer)
    field(:attempt_number, :integer)
    field(:execution_pool, :string)
    field(:queue_reason, :string)
    field(:started_at, :utc_datetime_usec)
    field(:finished_at, :utc_datetime_usec)
    field(:duration_ms, :integer)
    field(:error, :map)
    field(:output_metadata, :map)
    field(:source_publication_id, :integer)
    field(:updated_at, :utc_datetime_usec)
  end
end
