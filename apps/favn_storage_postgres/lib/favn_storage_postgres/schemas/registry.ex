defmodule FavnStoragePostgres.Schemas.Workspace do
  @moduledoc false
  use Ecto.Schema
  import Ecto.Changeset

  @primary_key {:workspace_id, :string, autogenerate: false}
  @schema_prefix "favn_control"
  schema "workspaces" do
    field(:slug, :string)
    field(:display_name, :string)
    field(:status, :string, default: "active")
    field(:version, :integer, default: 1)
    timestamps(type: :utc_datetime_usec)
  end

  def changeset(workspace, attrs) do
    workspace
    |> cast(attrs, [:workspace_id, :slug, :display_name, :status, :version])
    |> validate_required([:workspace_id, :slug, :display_name, :status, :version])
    |> validate_format(:slug, ~r/^[a-z0-9][a-z0-9-]{0,62}$/)
    |> validate_inclusion(:status, ~w(active suspended retired))
    |> unique_constraint(:slug)
  end
end

defmodule FavnStoragePostgres.Schemas.ManifestVersion do
  @moduledoc false
  use Ecto.Schema

  @primary_key {:manifest_version_id, :string, autogenerate: false}
  @schema_prefix "favn_control"
  schema "manifest_versions" do
    field(:content_hash, :binary)
    field(:schema_version, :integer)
    field(:runner_contract_version, :integer)
    field(:payload_version, :integer)
    field(:manifest, :map)
    field(:inserted_at, :utc_datetime_usec)
  end
end

defmodule FavnStoragePostgres.Schemas.ExecutionPackage do
  @moduledoc false
  use Ecto.Schema

  @primary_key {:content_hash, :binary, autogenerate: false}
  @schema_prefix "favn_control"
  schema "execution_packages" do
    field(:asset_module, :string)
    field(:asset_name, :string)
    field(:payload, :map)
    field(:inserted_at, :utc_datetime_usec)
  end
end

defmodule FavnStoragePostgres.Schemas.ManifestExecutionPackage do
  @moduledoc false
  use Ecto.Schema

  @primary_key false
  @schema_prefix "favn_control"
  schema "manifest_execution_packages" do
    field(:manifest_version_id, :string, primary_key: true)
    field(:package_hash, :binary, primary_key: true)
  end
end

defmodule FavnStoragePostgres.Schemas.WorkspaceDeployment do
  @moduledoc false
  use Ecto.Schema

  @primary_key false
  @schema_prefix "favn_control"
  schema "workspace_deployments" do
    field(:workspace_id, :string, primary_key: true)
    field(:deployment_id, :string, primary_key: true)
    field(:manifest_version_id, :string)
    field(:configuration, :map)
    field(:configuration_fingerprint, :binary)
    field(:target_catalog_fingerprint, :binary)
    field(:configuration_version, :integer)
    field(:deployed_by_actor_id, :string)
    field(:inserted_at, :utc_datetime_usec)
  end
end

defmodule FavnStoragePostgres.Schemas.WorkspaceDeploymentTarget do
  @moduledoc false
  use Ecto.Schema

  @primary_key false
  @schema_prefix "favn_control"
  schema "workspace_deployment_targets" do
    field(:workspace_id, :string, primary_key: true)
    field(:deployment_id, :string, primary_key: true)
    field(:target_kind, :string, primary_key: true)
    field(:target_id, :string, primary_key: true)
    field(:selection_source, :string)
    field(:customer_visible, :boolean)
    field(:inserted_at, :utc_datetime_usec)
  end
end

defmodule FavnStoragePostgres.Schemas.WorkspaceRuntimeState do
  @moduledoc false
  use Ecto.Schema

  @primary_key {:workspace_id, :string, autogenerate: false}
  @schema_prefix "favn_control"
  schema "workspace_runtime_state" do
    field(:active_deployment_id, :string)
    field(:revision, :integer)
    field(:activated_by_actor_id, :string)
    field(:activated_at, :utc_datetime_usec)
    field(:updated_at, :utc_datetime_usec)
  end
end
