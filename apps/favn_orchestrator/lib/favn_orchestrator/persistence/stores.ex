defmodule FavnOrchestrator.Persistence.Stores do
  @moduledoc """
  Complete set of persistence capabilities required by the orchestrator.

  Startup validates this struct once. Request paths do not discover optional
  callbacks dynamically.
  """

  @capabilities [
    registry: FavnOrchestrator.Persistence.RegistryStore,
    runs: FavnOrchestrator.Persistence.RunStore,
    run_ownership: FavnOrchestrator.Persistence.RunOwnershipStore,
    scheduler: FavnOrchestrator.Persistence.SchedulerStore,
    admission: FavnOrchestrator.Persistence.AdmissionStore,
    resource_circuits: FavnOrchestrator.Persistence.ResourceCircuitStore,
    target_generations: FavnOrchestrator.Persistence.TargetGenerationStore,
    rebuilds: FavnOrchestrator.Persistence.RebuildStore,
    target_operation_locks: FavnOrchestrator.Persistence.TargetOperationLockStore,
    materialization: FavnOrchestrator.Persistence.MaterializationStore,
    backfills: FavnOrchestrator.Persistence.BackfillStore,
    operator_reads: FavnOrchestrator.Persistence.OperatorReadStore,
    logs: FavnOrchestrator.Persistence.LogStore,
    identity: FavnOrchestrator.Persistence.IdentityStore,
    maintenance: FavnOrchestrator.Persistence.MaintenanceStore
  ]
  @fields Keyword.keys(@capabilities)

  @enforce_keys @fields
  defstruct @fields

  @type t :: %__MODULE__{
          registry: module(),
          runs: module(),
          run_ownership: module(),
          scheduler: module(),
          admission: module(),
          resource_circuits: module(),
          target_generations: module(),
          rebuilds: module(),
          target_operation_locks: module(),
          materialization: module(),
          backfills: module(),
          operator_reads: module(),
          logs: module(),
          identity: module(),
          maintenance: module()
        }

  @doc "Validates that every capability module is present and loaded."
  @spec validate(t()) ::
          :ok
          | {:error,
             {:invalid_stores, atom()} | {:missing_store_callback, atom(), atom(), arity()}}
  def validate(%__MODULE__{} = stores) do
    Enum.reduce_while(@capabilities, :ok, fn {field, behaviour}, :ok ->
      module = Map.fetch!(stores, field)

      if is_atom(module) and Code.ensure_loaded?(module) do
        case missing_callback(module, behaviour) do
          nil ->
            {:cont, :ok}

          {operation, arity} ->
            {:halt, {:error, {:missing_store_callback, field, operation, arity}}}
        end
      else
        {:halt, {:error, {:invalid_stores, field}}}
      end
    end)
  end

  defp missing_callback(module, behaviour) do
    behaviour.behaviour_info(:callbacks)
    |> Enum.find(fn {operation, arity} -> not function_exported?(module, operation, arity) end)
  end
end
