defmodule FavnStoragePostgres.Migrations.HardenPayloadBoundsV2 do
  @moduledoc false

  use Ecto.Migration

  @prefix "favn_control"
  @payload_columns %{
    manifest_versions: [manifest: 256 * 1_024 * 1_024],
    execution_packages: [payload: 4 * 1_024 * 1_024],
    workspace_deployments: [configuration: 262_144],
    outbox_events: [payload: 262_144],
    runs: [snapshot: 4 * 1_024 * 1_024],
    run_events: [event: 512 * 1_024],
    runtime_input_pins: [payload: 262_256],
    runner_executions: [dispatch_payload: 262_144, result: 262_144, error: 65_536],
    schedule_cursors: [cursor: 65_536],
    schedule_occurrences: [payload: 65_536, last_error: 65_536],
    admission_waiters: [requested_scopes: 16_384],
    materialization_claims: [result: 262_144, error: 65_536],
    materializations: [payload: 262_144],
    coverage_baselines: [evidence: 65_536],
    backfills: [metadata: 65_536],
    backfill_windows: [payload: 65_536, last_error: 65_536],
    projection_failures: [error_detail: 65_536],
    asset_window_states: [payload: 262_144],
    asset_freshness_states: [payload: 262_144],
    log_entries: [metadata: 32_768],
    auth_audit_entries: [detail: 65_536],
    auth_platform_audit_entries: [detail: 65_536],
    maintenance_jobs: [cursor: 65_536, configuration: 65_536, last_error: 65_536]
  }

  def up do
    Enum.each(@payload_columns, fn {table, columns} ->
      check =
        Enum.map_join(columns, " AND ", fn {column, maximum} ->
          "(#{column} IS NULL OR pg_column_size(#{column}) <= #{maximum})"
        end)

      create(
        constraint(table, constraint_name(table),
          prefix: @prefix,
          check: check
        )
      )
    end)
  end

  def down do
    @payload_columns
    |> Map.keys()
    |> Enum.reverse()
    |> Enum.each(fn table ->
      drop(constraint(table, constraint_name(table), prefix: @prefix))
    end)
  end

  defp constraint_name(table), do: String.to_atom("#{table}_payload_bounds_v2")
end
