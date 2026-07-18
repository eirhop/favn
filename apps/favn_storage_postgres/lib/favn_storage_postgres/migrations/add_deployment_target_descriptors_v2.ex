defmodule FavnStoragePostgres.Migrations.AddDeploymentTargetDescriptorsV2 do
  @moduledoc false

  use Ecto.Migration

  @prefix "favn_control"

  def up do
    alter table(:workspace_deployment_targets, prefix: @prefix) do
      add(:descriptor, :map, null: false, default: fragment("'{}'::jsonb"))
    end

    execute(
      "ALTER TABLE #{@prefix}.workspace_deployment_targets ALTER COLUMN descriptor DROP DEFAULT"
    )

    create(
      constraint(
        :workspace_deployment_targets,
        :workspace_deployment_targets_descriptor_valid,
        prefix: @prefix,
        check: "jsonb_typeof(descriptor) = 'object' AND octet_length(descriptor::text) <= 262144"
      )
    )
  end

  def down do
    drop(
      constraint(
        :workspace_deployment_targets,
        :workspace_deployment_targets_descriptor_valid,
        prefix: @prefix
      )
    )

    alter table(:workspace_deployment_targets, prefix: @prefix) do
      remove(:descriptor)
    end
  end
end
