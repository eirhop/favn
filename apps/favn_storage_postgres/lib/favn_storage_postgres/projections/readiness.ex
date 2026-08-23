defmodule FavnStoragePostgres.Projections.Readiness do
  @moduledoc false

  alias Ecto.Adapters.SQL
  alias FavnStoragePostgres.Repo

  @asset_attempts_version 2

  @spec mark_new_workspace_ready!(String.t(), DateTime.t()) :: :ok
  def mark_new_workspace_ready!(workspace_id, %DateTime{} = occurred_at) do
    mark_ready!(workspace_id, occurred_at)
  end

  @spec mark_ready!(String.t(), DateTime.t()) :: :ok
  def mark_ready!(workspace_id, %DateTime{} = occurred_at) do
    job_id = ready_job_id(workspace_id)

    SQL.query!(
      Repo,
      """
      INSERT INTO favn_control.maintenance_jobs
        (job_id, job_kind, scope_kind, workspace_id, status, configuration,
         fencing_token, processed_count, version, inserted_at, updated_at)
      VALUES ($1, 'projection_missing_row_backfill', 'workspace', $2, 'completed',
              $3::jsonb, 0, 0, 1, $4, $4)
      ON CONFLICT (job_id) DO UPDATE
      SET status = 'completed', configuration = EXCLUDED.configuration,
          updated_at = EXCLUDED.updated_at, version = maintenance_jobs.version + 1
      """,
      [
        job_id,
        workspace_id,
        %{"projection" => "asset_attempts", "version" => @asset_attempts_version},
        occurred_at
      ]
    )

    :ok
  end

  @spec mark_running!(String.t(), DateTime.t()) :: :ok
  def mark_running!(workspace_id, %DateTime{} = occurred_at) do
    SQL.query!(
      Repo,
      """
      INSERT INTO favn_control.maintenance_jobs
        (job_id, job_kind, scope_kind, workspace_id, status, configuration,
         fencing_token, processed_count, version, inserted_at, updated_at)
      VALUES ($1, 'projection_missing_row_backfill', 'workspace', $2, 'running',
              $3::jsonb, 0, 0, 1, $4, $4)
      ON CONFLICT (job_id) DO UPDATE
      SET status = 'running', configuration = EXCLUDED.configuration,
          updated_at = EXCLUDED.updated_at, version = maintenance_jobs.version + 1
      """,
      [
        ready_job_id(workspace_id),
        workspace_id,
        %{"projection" => "asset_attempts", "version" => @asset_attempts_version},
        occurred_at
      ]
    )

    :ok
  end

  @spec ready_job_id(String.t()) :: String.t()
  def ready_job_id(workspace_id) when is_binary(workspace_id) do
    "asset-attempts-v2-" <> Base.encode16(:crypto.hash(:sha256, workspace_id), case: :lower)
  end
end
