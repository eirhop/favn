defmodule FavnStoragePostgres.Operation do
  @moduledoc false

  alias FavnOrchestrator.Persistence.Error

  @event_prefix [:favn, :persistence, :operation]

  @spec run(atom(), atom(), [term()], (-> term())) :: term()
  def run(store, operation, arguments, function) when is_function(function, 0) do
    started_at = System.monotonic_time()
    metadata = %{store: store, operation: operation, batch_size: batch_size(arguments)}
    :telemetry.execute(@event_prefix ++ [:start], %{system_time: System.system_time()}, metadata)

    try do
      result = function.()

      :telemetry.execute(
        @event_prefix ++ [:stop],
        %{duration: System.monotonic_time() - started_at},
        Map.put(metadata, :result, result_class(result))
      )

      result
    rescue
      exception ->
        :telemetry.execute(
          @event_prefix ++ [:exception],
          %{duration: System.monotonic_time() - started_at},
          metadata
          |> Map.put(:kind, :error)
          |> Map.put(:reason, exception_class(exception))
        )

        reraise exception, __STACKTRACE__
    catch
      kind, reason ->
        :telemetry.execute(
          @event_prefix ++ [:exception],
          %{duration: System.monotonic_time() - started_at},
          metadata
          |> Map.put(:kind, kind)
          |> Map.put(:reason, exception_class(reason))
        )

        :erlang.raise(kind, reason, __STACKTRACE__)
    end
  end

  defp result_class(:ok), do: :ok
  defp result_class({:ok, _result}), do: :ok
  defp result_class({:error, %Error{kind: kind}}), do: kind
  defp result_class({:error, _reason}), do: :error
  defp result_class(_result), do: :other

  defp exception_class(%Postgrex.Error{postgres: %{code: code}}), do: code
  defp exception_class(%DBConnection.ConnectionError{}), do: :connection_error
  defp exception_class(%module{}), do: module
  defp exception_class(_reason), do: :unknown

  defp batch_size([%{pins: pins} | _arguments]) when is_list(pins), do: length(pins)
  defp batch_size([%{entries: entries} | _arguments]) when is_list(entries), do: length(entries)
  defp batch_size([%{windows: windows} | _arguments]) when is_list(windows), do: length(windows)

  defp batch_size([%{requests: requests} | _arguments]) when is_list(requests),
    do: length(requests)

  defp batch_size([%{limit: limit} | _arguments]) when is_integer(limit), do: limit
  defp batch_size(_arguments), do: 1
end

defmodule FavnStoragePostgres.InstrumentedStore do
  @moduledoc false

  defmacro __using__(options) do
    behaviour = Macro.expand_once(Keyword.fetch!(options, :behaviour), __CALLER__)
    implementation = Macro.expand_once(Keyword.fetch!(options, :implementation), __CALLER__)
    store = Keyword.fetch!(options, :store)

    delegates =
      for {operation, arity} <- behaviour.behaviour_info(:callbacks) do
        arguments = for index <- 1..arity//1, do: Macro.var(:"argument#{index}", __MODULE__)

        quote do
          @impl true
          def unquote(operation)(unquote_splicing(arguments)) do
            FavnStoragePostgres.Operation.run(
              unquote(store),
              unquote(operation),
              [unquote_splicing(arguments)],
              fn ->
                apply(unquote(implementation), unquote(operation), [unquote_splicing(arguments)])
              end
            )
          end
        end
      end

    quote do
      @behaviour unquote(behaviour)
      unquote_splicing(delegates)
    end
  end
end

defmodule FavnStoragePostgres.Instrumented.Registry do
  @moduledoc false
  use FavnStoragePostgres.InstrumentedStore,
    behaviour: FavnOrchestrator.Persistence.RegistryStore,
    implementation: FavnStoragePostgres.Registry.Store,
    store: :registry
end

defmodule FavnStoragePostgres.Instrumented.Runs do
  @moduledoc false
  use FavnStoragePostgres.InstrumentedStore,
    behaviour: FavnOrchestrator.Persistence.RunStore,
    implementation: FavnStoragePostgres.Runs.Store,
    store: :runs
end

defmodule FavnStoragePostgres.Instrumented.RunOwnership do
  @moduledoc false
  use FavnStoragePostgres.InstrumentedStore,
    behaviour: FavnOrchestrator.Persistence.RunOwnershipStore,
    implementation: FavnStoragePostgres.RunOwnership.Store,
    store: :run_ownership
end

defmodule FavnStoragePostgres.Instrumented.Scheduler do
  @moduledoc false
  use FavnStoragePostgres.InstrumentedStore,
    behaviour: FavnOrchestrator.Persistence.SchedulerStore,
    implementation: FavnStoragePostgres.Scheduler.Store,
    store: :scheduler
end

defmodule FavnStoragePostgres.Instrumented.Admission do
  @moduledoc false
  use FavnStoragePostgres.InstrumentedStore,
    behaviour: FavnOrchestrator.Persistence.AdmissionStore,
    implementation: FavnStoragePostgres.Admission.Store,
    store: :admission
end

defmodule FavnStoragePostgres.Instrumented.Materialization do
  @moduledoc false
  use FavnStoragePostgres.InstrumentedStore,
    behaviour: FavnOrchestrator.Persistence.MaterializationStore,
    implementation: FavnStoragePostgres.Materialization.Store,
    store: :materialization
end

defmodule FavnStoragePostgres.Instrumented.TargetGenerations do
  @moduledoc false
  use FavnStoragePostgres.InstrumentedStore,
    behaviour: FavnOrchestrator.Persistence.TargetGenerationStore,
    implementation: FavnStoragePostgres.TargetGenerations.Store,
    store: :target_generations
end

defmodule FavnStoragePostgres.Instrumented.ResourceCircuits do
  @moduledoc false
  use FavnStoragePostgres.InstrumentedStore,
    behaviour: FavnOrchestrator.Persistence.ResourceCircuitStore,
    implementation: FavnStoragePostgres.ResourceCircuits.Store,
    store: :resource_circuits
end

defmodule FavnStoragePostgres.Instrumented.Backfills do
  @moduledoc false
  use FavnStoragePostgres.InstrumentedStore,
    behaviour: FavnOrchestrator.Persistence.BackfillStore,
    implementation: FavnStoragePostgres.Backfills.Store,
    store: :backfills
end

defmodule FavnStoragePostgres.Instrumented.OperatorReads do
  @moduledoc false
  use FavnStoragePostgres.InstrumentedStore,
    behaviour: FavnOrchestrator.Persistence.OperatorReadStore,
    implementation: FavnStoragePostgres.OperatorReads.Store,
    store: :operator_reads
end

defmodule FavnStoragePostgres.Instrumented.Logs do
  @moduledoc false
  use FavnStoragePostgres.InstrumentedStore,
    behaviour: FavnOrchestrator.Persistence.LogStore,
    implementation: FavnStoragePostgres.Logs.Store,
    store: :logs
end

defmodule FavnStoragePostgres.Instrumented.Identity do
  @moduledoc false
  use FavnStoragePostgres.InstrumentedStore,
    behaviour: FavnOrchestrator.Persistence.IdentityStore,
    implementation: FavnStoragePostgres.Identity.Store,
    store: :identity
end

defmodule FavnStoragePostgres.Instrumented.Maintenance do
  @moduledoc false
  use FavnStoragePostgres.InstrumentedStore,
    behaviour: FavnOrchestrator.Persistence.MaintenanceStore,
    implementation: FavnStoragePostgres.Maintenance.Store,
    store: :maintenance
end
