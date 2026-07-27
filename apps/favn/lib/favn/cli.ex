defmodule Favn.CLI do
  @moduledoc false

  alias Favn.CLI.Activate
  alias Favn.CLI.Backfill
  alias Favn.CLI.Context
  alias Favn.CLI.DataInspection
  alias Favn.CLI.OrchestratorClient
  alias Favn.CLI.Publish
  alias Favn.CLI.Rebuild
  alias Favn.CLI.Run
  alias Favn.CLI.Runs
  alias Favn.CLI.Schedules

  def activate(opts), do: Activate.run(opts)
  def publish(opts), do: Publish.run(opts)
  def inspect_relation(relation, opts), do: DataInspection.inspect_relation(relation, opts)
  def inspect_partitions(relation, opts), do: DataInspection.inspect_partitions(relation, opts)
  def run(target, opts), do: Run.submit(target, opts)
  def list_runs(opts), do: Runs.list(opts)
  def get_run(run_id, opts), do: Runs.get(run_id, opts)
  def run_events(run_id, opts), do: Runs.events(run_id, opts)
  def cancel_run(run_id, opts), do: Runs.cancel(run_id, opts)
  def list_schedules(opts), do: Schedules.list(opts)
  def get_schedule(schedule_id, opts), do: Schedules.get(schedule_id, opts)
  def preview_schedule(schedule_id, opts), do: Schedules.preview(schedule_id, opts)

  def activate_schedule(schedule_id, reason, opts),
    do: Schedules.activate(schedule_id, reason, opts)

  def deactivate_schedule(schedule_id, reason, opts),
    do: Schedules.deactivate(schedule_id, reason, opts)

  def submit_backfill(pipeline, opts), do: Backfill.submit_pipeline(pipeline, opts)
  def plan_missing_asset_backfill(asset, opts), do: Backfill.plan_missing_asset(asset, opts)

  def submit_missing_asset_backfill(asset, plan, opts),
    do: Backfill.submit_missing_asset(asset, plan, opts)

  def get_backfill(backfill_id, opts), do: Backfill.get(backfill_id, opts)
  def list_backfill_windows(run_id, opts), do: Backfill.list_windows(run_id, opts)

  def plan_rebuild(asset, reason, opts), do: Rebuild.plan(asset, reason, opts)
  def start_rebuild(plan_id, plan_hash, opts), do: Rebuild.start(plan_id, plan_hash, opts)
  def get_rebuild(operation_id, opts), do: Rebuild.status(operation_id, opts)
  def cancel_rebuild(operation_id, reason, opts), do: Rebuild.cancel(operation_id, reason, opts)
  def retry_rebuild(operation_id, opts), do: Rebuild.retry(operation_id, opts)
  def reconcile_rebuild(operation_id, opts), do: Rebuild.reconcile(operation_id, opts)

  def diagnostics(opts) do
    with {:ok, url, %{service_token: token}, context} <- Context.resolve(opts) do
      OrchestratorClient.diagnostics(url, token, context)
    end
  end
end
