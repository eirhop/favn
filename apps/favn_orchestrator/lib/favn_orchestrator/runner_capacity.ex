defmodule FavnOrchestrator.RunnerCapacity do
  @moduledoc """
  Provider-neutral reads over durable pool/release demand and process-local runners.
  """

  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.Queries, as: Q
  alias FavnOrchestrator.RunnerPools
  alias FavnOrchestrator.RunnerRegistry
  alias FavnOrchestrator.RuntimeConfig

  @diagnostic_partition_limit 256

  @spec demand(FavnOrchestrator.Persistence.PlatformContext.t(), String.t(), String.t()) ::
          {:ok, FavnOrchestrator.Persistence.Results.RunnerCapacityDemand.t()}
          | {:error, term()}
  def demand(context, pool, release_id) do
    with :ok <- Favn.RunnerPool.validate_runtime(pool),
         :ok <- Favn.RunnerRelease.validate_id(release_id),
         {:ok, _policy} <- RunnerPools.fetch(RuntimeConfig.runner_pools(), pool) do
      Persistence.stores().runner_tasks.demand(%Q.GetRunnerCapacityDemand{
        platform_context: context,
        runner_pool: pool,
        required_runner_release_id: release_id
      })
    end
  end

  @spec diagnostics(FavnOrchestrator.Persistence.PlatformContext.t()) ::
          {:ok, map()} | {:error, term()}
  def diagnostics(context) do
    with {:ok, drains} <-
           Persistence.stores().runner_tasks.list_release_drains(%Q.ListRunnerReleaseDrains{
             platform_context: context,
             limit: @diagnostic_partition_limit + 1
           }) do
      registry = RunnerRegistry.snapshot()
      {visible, overflow} = Enum.split(drains, @diagnostic_partition_limit)

      partitions =
        Enum.map(visible, &partition_diagnostics(&1, registry))

      {:ok,
       %{
         registered_runners: registry.registered,
         partition_limit: @diagnostic_partition_limit,
         truncated: overflow != [],
         partitions: partitions
       }}
    end
  end

  @spec drain(FavnOrchestrator.Persistence.PlatformContext.t(), String.t(), String.t()) ::
          {:ok, map()} | {:error, term()}
  def drain(context, pool, release_id) do
    with :ok <- Favn.RunnerPool.validate_runtime(pool),
         :ok <- Favn.RunnerRelease.validate_id(release_id),
         {:ok, drain} <-
           Persistence.stores().runner_tasks.release_drain(%Q.GetRunnerReleaseDrain{
             platform_context: context,
             runner_pool: pool,
             required_runner_release_id: release_id
           }) do
      {:ok, partition_diagnostics(drain, RunnerRegistry.snapshot())}
    end
  end

  defp partition_diagnostics(drain, registry) do
    live =
      Map.get(
        registry.partitions,
        {drain.runner_pool, drain.required_runner_release_id},
        %{registered: 0, statuses: %{}, lifecycle_modes: %{}}
      )

    %{
      runner_pool: drain.runner_pool,
      required_runner_release_id: drain.required_runner_release_id,
      outstanding: drain.outstanding_task_count,
      active_runs: drain.active_run_count,
      pending_operations: drain.pending_operation_count,
      durable_blockers: drain.blocker_count,
      healthy: drain.healthy?,
      registered: live.registered,
      runner_statuses: live.statuses,
      lifecycle_modes: live.lifecycle_modes,
      drained: drain.durable_drained? and live.registered == 0
    }
  end
end
