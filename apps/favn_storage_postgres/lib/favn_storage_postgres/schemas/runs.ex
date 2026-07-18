defmodule FavnStoragePostgres.Schemas.OutboxEvent do
  @moduledoc false
  use Ecto.Schema

  @primary_key {:outbox_event_id, :id, autogenerate: true}
  @schema_prefix "favn_control"
  schema "outbox_events" do
    field(:workspace_id, :string)
    field(:command_id, :string)
    field(:event_kind, :string)
    field(:aggregate_kind, :string)
    field(:aggregate_id, :string)
    field(:aggregate_version, :integer)
    field(:payload_version, :integer)
    field(:payload, :map)
    field(:payload_hash, :binary)
    field(:occurred_at, :utc_datetime_usec)
    field(:publication_id, :integer)
    field(:published_at, :utc_datetime_usec)
    field(:inserted_at, :utc_datetime_usec)
  end

  @type t :: %__MODULE__{}
end

defmodule FavnStoragePostgres.Schemas.Run do
  @moduledoc false
  use Ecto.Schema

  @primary_key false
  @schema_prefix "favn_control"
  schema "runs" do
    field(:workspace_id, :string, primary_key: true)
    field(:run_id, :string, primary_key: true)
    field(:deployment_id, :string)
    field(:manifest_version_id, :string)
    field(:root_execution_group_id, :string)
    field(:parent_run_id, :string)
    field(:rerun_of_run_id, :string)
    field(:submit_kind, :string)
    field(:trigger_type, :string)
    field(:status, :string)
    field(:event_sequence, :integer)
    field(:submitted_event_id, :integer)
    field(:latest_event_id, :integer)
    field(:snapshot_version, :integer)
    field(:creation_hash, :binary)
    field(:snapshot_hash, :binary)
    field(:snapshot, :map)
    field(:inserted_at, :utc_datetime_usec)
    field(:updated_at, :utc_datetime_usec)
    field(:terminal_at, :utc_datetime_usec)
  end
end

defmodule FavnStoragePostgres.Schemas.RunEvent do
  @moduledoc false
  use Ecto.Schema

  @primary_key {:event_id, :id, autogenerate: true}
  @schema_prefix "favn_control"
  schema "run_events" do
    field(:workspace_id, :string)
    field(:run_id, :string)
    field(:sequence, :integer)
    field(:event_type, :string)
    field(:entity_type, :string)
    field(:asset_step_id, :string)
    field(:status, :string)
    field(:stage, :integer)
    field(:occurred_at, :utc_datetime_usec)
    field(:payload_version, :integer)
    field(:event, :map)
    field(:event_hash, :binary)
    field(:outbox_event_id, :integer)
    field(:inserted_at, :utc_datetime_usec)
  end
end

defmodule FavnStoragePostgres.Schemas.RunPlan do
  @moduledoc false
  use Ecto.Schema

  @primary_key false
  @schema_prefix "favn_control"
  schema "run_plans" do
    field(:workspace_id, :string, primary_key: true)
    field(:run_id, :string, primary_key: true)
    field(:manifest_version_id, :string)
    field(:plan_version, :integer)
    field(:plan_hash, :binary)
    field(:plan, :map)
    field(:inserted_at, :utc_datetime_usec)
  end
end

defmodule FavnStoragePostgres.Schemas.RunTarget do
  @moduledoc false
  use Ecto.Schema

  @primary_key false
  @schema_prefix "favn_control"
  schema "run_targets" do
    field(:workspace_id, :string, primary_key: true)
    field(:run_id, :string, primary_key: true)
    field(:deployment_id, :string)
    field(:manifest_version_id, :string)
    field(:target_kind, :string, primary_key: true)
    field(:target_id, :string, primary_key: true)
    field(:target_module, :string)
    field(:target_name, :string)
    field(:is_primary, :boolean)
    field(:submitted_event_id, :integer)
    field(:inserted_at, :utc_datetime_usec)
  end
end

defmodule FavnStoragePostgres.Schemas.RunOwnership do
  @moduledoc false
  use Ecto.Schema

  @primary_key false
  @schema_prefix "favn_control"
  schema "run_ownerships" do
    field(:workspace_id, :string, primary_key: true)
    field(:run_id, :string, primary_key: true)
    field(:owner_id, :string)
    field(:fencing_token, :integer)
    field(:claim_command_id, :string)
    field(:last_renewal_id, :string)
    field(:expires_at, :utc_datetime_usec)
    field(:released_at, :utc_datetime_usec)
    field(:updated_at, :utc_datetime_usec)
  end
end

defmodule FavnStoragePostgres.Schemas.RuntimeInputPin do
  @moduledoc false
  use Ecto.Schema

  @primary_key false
  @schema_prefix "favn_control"
  schema "runtime_input_pins" do
    field(:workspace_id, :string, primary_key: true)
    field(:run_id, :string, primary_key: true)
    field(:node_key_hash, :binary, primary_key: true)
    field(:payload_fingerprint, :binary)
    field(:execution_package_hash, :binary)
    field(:resolver_module, :string)
    field(:encryption_key_version, :integer)
    field(:payload, :binary)
    field(:inserted_at, :utc_datetime_usec)
  end
end
