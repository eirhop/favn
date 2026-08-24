defmodule FavnStoragePostgres.Schemas.ManifestDeploymentOperation do
  @moduledoc false
  use Ecto.Schema

  @primary_key false
  @schema_prefix "favn_control"
  schema "manifest_deployment_operations" do
    field(:workspace_id, :string, primary_key: true)
    field(:operation_id, :string, primary_key: true)
    field(:archive_sha256, :binary)
    field(:request_fingerprint, :binary)
    field(:service_identity, :string)
    field(:manifest_version_id, :string)
    field(:manifest_content_hash, :binary)
    field(:runner_releases, :map)
    field(:state, :string)
    field(:deployment_id, :string)
    field(:failure_class, :string)
    field(:activation_diagnostics, :map)
    field(:claim_owner, :string)
    field(:claim_fence, :integer)
    field(:claim_expires_at, :utc_datetime_usec)
    field(:inspection_total, :integer)
    field(:inspection_completed, :integer)
    field(:accepted_at, :utc_datetime_usec)
    field(:activating_at, :utc_datetime_usec)
    field(:terminal_at, :utc_datetime_usec)
    timestamps(type: :utc_datetime_usec)
  end
end

defmodule FavnStoragePostgres.Schemas.ManifestDeploymentUploadLease do
  @moduledoc false
  use Ecto.Schema

  @primary_key {:lease_id, :string, autogenerate: false}
  @schema_prefix "favn_control"
  schema "manifest_deployment_upload_leases" do
    field(:workspace_id, :string)
    field(:service_identity, :string)
    field(:expires_at, :utc_datetime_usec)
    timestamps(type: :utc_datetime_usec)
  end
end

defmodule FavnStoragePostgres.Schemas.ManifestActivationLease do
  @moduledoc false
  use Ecto.Schema

  @primary_key {:workspace_id, :string, autogenerate: false}
  @schema_prefix "favn_control"
  schema "manifest_activation_leases" do
    field(:operation_id, :string)
    field(:owner, :string)
    field(:fencing_token, :integer)
    field(:expires_at, :utc_datetime_usec)
    timestamps(type: :utc_datetime_usec)
  end
end
