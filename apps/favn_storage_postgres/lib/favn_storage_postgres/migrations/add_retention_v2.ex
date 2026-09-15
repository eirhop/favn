defmodule FavnStoragePostgres.Migrations.AddRetentionV2 do
  @moduledoc false
  use Ecto.Migration
  @prefix "favn_control"

  def up do
    alter table(:runs, prefix: @prefix) do
      add(:retiring, :boolean, null: false, default: false)
    end

    alter table(:runner_tasks, prefix: @prefix) do
      add(:retiring, :boolean, null: false, default: false)
    end

    alter table(:rebuild_operations, prefix: @prefix) do
      add(:retiring, :boolean, null: false, default: false)
    end

    for owner <- [:manifest_versions, :workspace_deployments] do
      alter table(owner, prefix: @prefix) do
        add(:retiring, :boolean, null: false, default: false)
      end
    end

    create table(:retention_floors, primary_key: false, prefix: @prefix) do
      add(:workspace_id, :text, null: false, primary_key: true)
      add(:stream, :text, null: false, primary_key: true)
      add(:publication_id, :bigint, null: false)
      add(:batch_offset, :integer, null: false, default: 0)
    end

    create(
      constraint(:retention_floors, :retention_floors_valid,
        prefix: @prefix,
        check: "stream IN ('logs', 'events') AND publication_id >= 0 AND batch_offset >= 0"
      )
    )

    create(index(:log_entries, [:workspace_id, :inserted_at, :log_id], prefix: @prefix))

    create(
      index(:run_submission_commands, [:inserted_at, :workspace_id, :command_id],
        prefix: @prefix,
        name: :run_submission_commands_global_retention_idx
      )
    )

    execute("""
    CREATE FUNCTION favn_control.guard_retained_registry_reference() RETURNS trigger
    LANGUAGE plpgsql AS $$
    DECLARE identity text; workspace text; retired boolean;
    BEGIN
      identity := to_jsonb(NEW)->>TG_ARGV[1];
      IF identity IS NULL THEN RETURN NEW; END IF;
      IF TG_ARGV[0] = 'manifest' THEN
        SELECT retiring INTO retired FROM favn_control.manifest_versions
          WHERE manifest_version_id=identity FOR SHARE;
      ELSE
        workspace := to_jsonb(NEW)->>'workspace_id';
        SELECT retiring INTO retired FROM favn_control.workspace_deployments
          WHERE workspace_id=workspace AND deployment_id=identity FOR SHARE;
      END IF;
      IF retired THEN
        RAISE EXCEPTION 'registry history is retiring' USING ERRCODE='23514', CONSTRAINT='registry_history_retiring';
      END IF;
      RETURN NEW;
    END $$
    """)

    execute(
      "CREATE TRIGGER retention_manifest_version_id BEFORE INSERT OR UPDATE OF manifest_version_id ON favn_control.asset_freshness_states FOR EACH ROW EXECUTE FUNCTION favn_control.guard_retained_registry_reference('manifest', 'manifest_version_id')"
    )

    execute(
      "CREATE TRIGGER retention_desired_manifest_id BEFORE INSERT OR UPDATE OF desired_manifest_id ON favn_control.asset_target_bindings FOR EACH ROW EXECUTE FUNCTION favn_control.guard_retained_registry_reference('manifest', 'desired_manifest_id')"
    )

    execute(
      "CREATE TRIGGER retention_creating_manifest_id BEFORE INSERT OR UPDATE OF creating_manifest_id ON favn_control.asset_target_generations FOR EACH ROW EXECUTE FUNCTION favn_control.guard_retained_registry_reference('manifest', 'creating_manifest_id')"
    )

    execute(
      "CREATE TRIGGER retention_manifest_version_id BEFORE INSERT OR UPDATE OF manifest_version_id ON favn_control.asset_window_states FOR EACH ROW EXECUTE FUNCTION favn_control.guard_retained_registry_reference('manifest', 'manifest_version_id')"
    )

    execute(
      "CREATE TRIGGER retention_manifest_version_id BEFORE INSERT OR UPDATE OF manifest_version_id ON favn_control.backfills FOR EACH ROW EXECUTE FUNCTION favn_control.guard_retained_registry_reference('manifest', 'manifest_version_id')"
    )

    execute(
      "CREATE TRIGGER retention_manifest_version_id BEFORE INSERT OR UPDATE OF manifest_version_id ON favn_control.coverage_baselines FOR EACH ROW EXECUTE FUNCTION favn_control.guard_retained_registry_reference('manifest', 'manifest_version_id')"
    )

    execute(
      "CREATE TRIGGER retention_manifest_version_id BEFORE INSERT OR UPDATE OF manifest_version_id ON favn_control.manifest_deployment_operations FOR EACH ROW EXECUTE FUNCTION favn_control.guard_retained_registry_reference('manifest', 'manifest_version_id')"
    )

    execute(
      "CREATE TRIGGER retention_manifest_version_id BEFORE INSERT OR UPDATE OF manifest_version_id ON favn_control.manifest_execution_packages FOR EACH ROW EXECUTE FUNCTION favn_control.guard_retained_registry_reference('manifest', 'manifest_version_id')"
    )

    execute(
      "CREATE TRIGGER retention_manifest_version_id BEFORE INSERT OR UPDATE OF manifest_version_id ON favn_control.rebuild_operations FOR EACH ROW EXECUTE FUNCTION favn_control.guard_retained_registry_reference('manifest', 'manifest_version_id')"
    )

    execute(
      "CREATE TRIGGER retention_manifest_version_id BEFORE INSERT OR UPDATE OF manifest_version_id ON favn_control.run_plans FOR EACH ROW EXECUTE FUNCTION favn_control.guard_retained_registry_reference('manifest', 'manifest_version_id')"
    )

    execute(
      "CREATE TRIGGER retention_manifest_version_id BEFORE INSERT OR UPDATE OF manifest_version_id ON favn_control.run_submissions FOR EACH ROW EXECUTE FUNCTION favn_control.guard_retained_registry_reference('manifest', 'manifest_version_id')"
    )

    execute(
      "CREATE TRIGGER retention_manifest_version_id BEFORE INSERT OR UPDATE OF manifest_version_id ON favn_control.run_targets FOR EACH ROW EXECUTE FUNCTION favn_control.guard_retained_registry_reference('manifest', 'manifest_version_id')"
    )

    execute(
      "CREATE TRIGGER retention_manifest_version_id BEFORE INSERT OR UPDATE OF manifest_version_id ON favn_control.runner_tasks FOR EACH ROW EXECUTE FUNCTION favn_control.guard_retained_registry_reference('manifest', 'manifest_version_id')"
    )

    execute(
      "CREATE TRIGGER retention_manifest_version_id BEFORE INSERT OR UPDATE OF manifest_version_id ON favn_control.runs FOR EACH ROW EXECUTE FUNCTION favn_control.guard_retained_registry_reference('manifest', 'manifest_version_id')"
    )

    execute(
      "CREATE TRIGGER retention_desired_manifest_id BEFORE INSERT OR UPDATE OF desired_manifest_id ON favn_control.target_recovery_operations FOR EACH ROW EXECUTE FUNCTION favn_control.guard_retained_registry_reference('manifest', 'desired_manifest_id')"
    )

    execute(
      "CREATE TRIGGER retention_source_manifest_id BEFORE INSERT OR UPDATE OF source_manifest_id ON favn_control.target_recovery_operations FOR EACH ROW EXECUTE FUNCTION favn_control.guard_retained_registry_reference('manifest', 'source_manifest_id')"
    )

    execute(
      "CREATE TRIGGER retention_manifest_version_id BEFORE INSERT OR UPDATE OF manifest_version_id ON favn_control.workspace_deployments FOR EACH ROW EXECUTE FUNCTION favn_control.guard_retained_registry_reference('manifest', 'manifest_version_id')"
    )

    execute(
      "CREATE TRIGGER retention_deployment_id BEFORE INSERT OR UPDATE OF deployment_id ON favn_control.asset_freshness_states FOR EACH ROW EXECUTE FUNCTION favn_control.guard_retained_registry_reference('deployment', 'deployment_id')"
    )

    execute(
      "CREATE TRIGGER retention_deployment_id BEFORE INSERT OR UPDATE OF deployment_id ON favn_control.backfills FOR EACH ROW EXECUTE FUNCTION favn_control.guard_retained_registry_reference('deployment', 'deployment_id')"
    )

    execute(
      "CREATE TRIGGER retention_deployment_id BEFORE INSERT OR UPDATE OF deployment_id ON favn_control.coverage_baselines FOR EACH ROW EXECUTE FUNCTION favn_control.guard_retained_registry_reference('deployment', 'deployment_id')"
    )

    execute(
      "CREATE TRIGGER retention_deployment_id BEFORE INSERT OR UPDATE OF deployment_id ON favn_control.manifest_deployment_operations FOR EACH ROW EXECUTE FUNCTION favn_control.guard_retained_registry_reference('deployment', 'deployment_id')"
    )

    execute(
      "CREATE TRIGGER retention_deployment_id BEFORE INSERT OR UPDATE OF deployment_id ON favn_control.materialization_claims FOR EACH ROW EXECUTE FUNCTION favn_control.guard_retained_registry_reference('deployment', 'deployment_id')"
    )

    execute(
      "CREATE TRIGGER retention_deployment_id BEFORE INSERT OR UPDATE OF deployment_id ON favn_control.materializations FOR EACH ROW EXECUTE FUNCTION favn_control.guard_retained_registry_reference('deployment', 'deployment_id')"
    )

    execute(
      "CREATE TRIGGER retention_deployment_id BEFORE INSERT OR UPDATE OF deployment_id ON favn_control.run_submissions FOR EACH ROW EXECUTE FUNCTION favn_control.guard_retained_registry_reference('deployment', 'deployment_id')"
    )

    execute(
      "CREATE TRIGGER retention_deployment_id BEFORE INSERT OR UPDATE OF deployment_id ON favn_control.run_targets FOR EACH ROW EXECUTE FUNCTION favn_control.guard_retained_registry_reference('deployment', 'deployment_id')"
    )

    execute(
      "CREATE TRIGGER retention_deployment_id BEFORE INSERT OR UPDATE OF deployment_id ON favn_control.runs FOR EACH ROW EXECUTE FUNCTION favn_control.guard_retained_registry_reference('deployment', 'deployment_id')"
    )

    execute(
      "CREATE TRIGGER retention_deployment_id BEFORE INSERT OR UPDATE OF deployment_id ON favn_control.schedule_cursors FOR EACH ROW EXECUTE FUNCTION favn_control.guard_retained_registry_reference('deployment', 'deployment_id')"
    )

    execute(
      "CREATE TRIGGER retention_deployment_id BEFORE INSERT OR UPDATE OF deployment_id ON favn_control.schedule_occurrences FOR EACH ROW EXECUTE FUNCTION favn_control.guard_retained_registry_reference('deployment', 'deployment_id')"
    )

    execute(
      "CREATE TRIGGER retention_deployment_id BEFORE INSERT OR UPDATE OF deployment_id ON favn_control.target_statuses FOR EACH ROW EXECUTE FUNCTION favn_control.guard_retained_registry_reference('deployment', 'deployment_id')"
    )

    execute(
      "CREATE TRIGGER retention_deployment_id BEFORE INSERT OR UPDATE OF deployment_id ON favn_control.workspace_deployment_targets FOR EACH ROW EXECUTE FUNCTION favn_control.guard_retained_registry_reference('deployment', 'deployment_id')"
    )

    execute(
      "CREATE TRIGGER retention_active_deployment_id BEFORE INSERT OR UPDATE OF active_deployment_id ON favn_control.workspace_runtime_state FOR EACH ROW EXECUTE FUNCTION favn_control.guard_retained_registry_reference('deployment', 'active_deployment_id')"
    )
  end

  def down do
    execute("DROP FUNCTION favn_control.guard_retained_registry_reference() CASCADE")

    for name <- [
          :runs,
          :runner_tasks,
          :rebuild_operations,
          :manifest_versions,
          :workspace_deployments
        ] do
      alter table(name, prefix: @prefix) do
        remove(:retiring)
      end
    end

    drop(table(:retention_floors, prefix: @prefix))
    drop(index(:log_entries, [:workspace_id, :inserted_at, :log_id], prefix: @prefix))

    drop(
      index(:run_submission_commands, [:inserted_at, :workspace_id, :command_id],
        prefix: @prefix,
        name: :run_submission_commands_global_retention_idx
      )
    )
  end
end
