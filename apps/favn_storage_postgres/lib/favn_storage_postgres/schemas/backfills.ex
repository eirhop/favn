defmodule FavnStoragePostgres.Schemas.Backfill do
  @moduledoc false
  use Ecto.Schema

  @primary_key false
  @schema_prefix "favn_control"
  schema "backfills" do
    field(:workspace_id, :string, primary_key: true)
    field(:backfill_id, :string, primary_key: true)
    field(:root_run_id, :string)
    field(:start_command_id, :string)
    field(:last_command_id, :string)
    field(:request_hash, :binary)
    field(:deployment_id, :string)
    field(:manifest_version_id, :string)
    field(:target_kind, :string)
    field(:target_id, :string)
    field(:range_start, :utc_datetime_usec)
    field(:range_end, :utc_datetime_usec)
    field(:status, :string)
    field(:expected_window_count, :integer)
    field(:expected_batch_count, :integer)
    field(:appended_window_count, :integer)
    field(:appended_batch_count, :integer)
    field(:plan_hash, :binary)
    field(:metadata, :map)
    field(:version, :integer)
    timestamps(type: :utc_datetime_usec)
  end
end

defmodule FavnStoragePostgres.Schemas.BackfillPlanBatch do
  @moduledoc false
  use Ecto.Schema

  @primary_key false
  @schema_prefix "favn_control"
  schema "backfill_plan_batches" do
    field(:workspace_id, :string, primary_key: true)
    field(:backfill_id, :string, primary_key: true)
    field(:batch_index, :integer, primary_key: true)
    field(:command_id, :string)
    field(:batch_hash, :binary)
    field(:window_count, :integer)
    field(:inserted_at, :utc_datetime_usec)
  end
end

defmodule FavnStoragePostgres.Schemas.BackfillWindow do
  @moduledoc false
  use Ecto.Schema

  @primary_key false
  @schema_prefix "favn_control"
  schema "backfill_windows" do
    field(:workspace_id, :string, primary_key: true)
    field(:backfill_id, :string, primary_key: true)
    field(:window_id, :string, primary_key: true)
    field(:batch_index, :integer)
    field(:window_key, :string)
    field(:window_start, :utc_datetime_usec)
    field(:window_end, :utc_datetime_usec)
    field(:status, :string)
    field(:claim_owner, :string)
    field(:fencing_token, :integer)
    field(:claim_command_id, :string)
    field(:last_command_id, :string)
    field(:claim_expires_at, :utc_datetime_usec)
    field(:run_id, :string)
    field(:attempt_count, :integer)
    field(:last_error, :map)
    field(:payload, :map)
    field(:version, :integer)
    timestamps(type: :utc_datetime_usec)
  end
end

defmodule FavnStoragePostgres.Schemas.BackfillOverview do
  @moduledoc false
  use Ecto.Schema

  @primary_key false
  @schema_prefix "favn_control"
  schema "backfill_overviews" do
    field(:workspace_id, :string, primary_key: true)
    field(:backfill_id, :string, primary_key: true)
    field(:status, :string)
    field(:total_count, :integer)
    field(:planned_count, :integer)
    field(:ready_count, :integer)
    field(:active_count, :integer)
    field(:succeeded_count, :integer)
    field(:failed_count, :integer)
    field(:cancelled_count, :integer)
    field(:source_publication_id, :integer)
    field(:updated_at, :utc_datetime_usec)
  end
end
