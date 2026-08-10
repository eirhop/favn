defmodule FavnStoragePostgres.Bootstrap.WorkflowRunner do
  @moduledoc false

  require Logger

  alias FavnStoragePostgres.Bootstrap.Result

  @context_key :favn_postgres_workflow_context
  @relayed_failure_tag :favn_postgres_relayed_failure
  @max_completed_stages 32

  @type operation :: :status | :bootstrap | :upgrade
  @type result :: {:ok, map()} | {:error, map()}

  @spec run(operation(), (-> result())) :: result()
  def run(operation, function)
      when operation in [:status, :bootstrap, :upgrade] and is_function(function, 0) do
    caller = self()
    reference = make_ref()
    diagnostic_id = diagnostic_id()

    {worker, monitor} =
      spawn_monitor(fn ->
        initialize_context(caller, reference, operation, diagnostic_id)

        try do
          send(caller, {reference, :result, function.()})
        catch
          kind, reason ->
            failure = failure(kind, reason, __STACKTRACE__)
            send(caller, {reference, :failure, failure})
        end
      end)

    await_worker(operation, reference, worker, monitor, diagnostic_id, [], [])
  end

  @spec track_stage(atom(), (-> term())) :: term()
  def track_stage(stage, function) when is_atom(stage) and is_function(function, 0) do
    if Process.get(@context_key) do
      do_track_stage(stage, function)
    else
      function.()
    end
  end

  @doc false
  @spec current_context() :: map() | nil
  def current_context, do: Process.get(@context_key)

  @doc false
  @spec context_reference(map() | nil) :: reference() | nil
  def context_reference(%{reference: reference}), do: reference
  def context_reference(nil), do: nil

  @doc false
  @spec with_context(map() | nil, (-> result)) :: result when result: term()
  def with_context(nil, function) when is_function(function, 0), do: function.()

  def with_context(workflow_context, function)
      when is_map(workflow_context) and is_function(function, 0) do
    previous_context = Process.get(@context_key)
    Process.put(@context_key, workflow_context)

    try do
      function.()
    after
      restore_context(previous_context)
    end
  end

  @doc false
  @spec absorb_context_event(reference() | nil, term()) :: :ok
  def absorb_context_event(reference, event) do
    case Process.get(@context_key) do
      %{reference: ^reference} -> update_context(&apply_context_event(&1, event))
      _context -> :ok
    end
  end

  @doc false
  @spec guarded_result(map() | nil, (-> result)) :: {:ok, result} | {:error, map()}
        when result: term()
  def guarded_result(workflow_context, function) when is_function(function, 0) do
    with_context(workflow_context, fn ->
      try do
        {:ok, function.()}
      catch
        kind, reason -> {:error, failure(kind, reason, __STACKTRACE__)}
      end
    end)
  end

  @doc false
  @spec propagate_failure(map()) :: no_return()
  def propagate_failure(failure) when is_map(failure) do
    throw({@relayed_failure_tag, failure})
  end

  defp do_track_stage(stage, function) do
    start_stage(stage)
    result = function.()
    finish_stage(stage, stage_outcome(result))
    result
  end

  @spec record_completed([atom()]) :: :ok
  def record_completed(stages) when is_list(stages) do
    completed_stages = Enum.take(stages, @max_completed_stages)
    update_context(&Map.put(&1, :completed_stages, completed_stages))
    notify({:completed_stages, completed_stages})
    :ok
  end

  defp initialize_context(caller, reference, operation, diagnostic_id) do
    Process.put(@context_key, %{
      caller: caller,
      owner: self(),
      reference: reference,
      operation: operation,
      diagnostic_id: diagnostic_id,
      stage_stack: [],
      completed_stages: []
    })
  end

  defp start_stage(stage) do
    update_context(fn context ->
      Map.update!(context, :stage_stack, &[stage | &1])
    end)

    notify({:stage_started, stage})
    emit_stage(stage, :started)
  end

  defp finish_stage(stage, outcome) do
    update_context(fn context ->
      Map.update!(context, :stage_stack, &pop_stage(&1, stage))
    end)

    notify({:stage_finished, stage})
    emit_stage(stage, outcome)
  end

  defp pop_stage([stage | rest], stage), do: rest
  defp pop_stage(stages, stage), do: List.delete(stages, stage)

  defp stage_outcome({:error, _reason}), do: :failed
  defp stage_outcome({:error, _code, _stage, _completed}), do: :failed
  defp stage_outcome(_result), do: :complete

  defp emit_stage(stage, outcome) do
    operation = context().operation

    Logger.info(
      "favn.database_workflow.stage operation=#{operation} stage=#{stage} outcome=#{outcome}"
    )

    :telemetry.execute(
      [:favn, :storage_postgres, :database_workflow, :stage],
      %{system_time: System.system_time()},
      %{operation: operation, stage: stage, outcome: outcome}
    )
  end

  defp await_worker(
         operation,
         reference,
         worker,
         monitor,
         diagnostic_id,
         stage_stack,
         completed_stages
       ) do
    receive do
      {^reference, :result, result} ->
        Process.demonitor(monitor, [:flush])
        result

      {^reference, :failure, failure} ->
        Process.demonitor(monitor, [:flush])
        failure_result(operation, failure)

      {^reference, {:stage_started, stage}} ->
        await_worker(
          operation,
          reference,
          worker,
          monitor,
          diagnostic_id,
          [stage | stage_stack],
          completed_stages
        )

      {^reference, {:stage_finished, stage}} ->
        await_worker(
          operation,
          reference,
          worker,
          monitor,
          diagnostic_id,
          pop_stage(stage_stack, stage),
          completed_stages
        )

      {^reference, {:completed_stages, stages}} ->
        await_worker(
          operation,
          reference,
          worker,
          monitor,
          diagnostic_id,
          stage_stack,
          stages
        )

      {:DOWN, ^monitor, :process, ^worker, reason} ->
        failure = %{
          diagnostic_id: diagnostic_id,
          stage: List.first(stage_stack) || :internal,
          completed_stages: completed_stages,
          failure_kind: :exit,
          failure_class: safe_exit_class(reason),
          failure_location: "worker"
        }

        failure_result(operation, failure)
    end
  end

  defp failure(:throw, {@relayed_failure_tag, relayed_failure}, _stacktrace) do
    sanitize_relayed_failure(relayed_failure)
  end

  defp failure(kind, reason, stacktrace) do
    workflow_context = context()

    %{
      diagnostic_id: workflow_context.diagnostic_id,
      stage: List.first(workflow_context.stage_stack) || :internal,
      completed_stages: workflow_context.completed_stages,
      failure_kind: safe_failure_kind(kind),
      failure_class: safe_failure_class(kind, reason),
      failure_location: safe_failure_location(stacktrace)
    }
  end

  defp sanitize_relayed_failure(relayed_failure) do
    workflow_context = context()

    %{
      diagnostic_id: workflow_context.diagnostic_id,
      stage: safe_relayed_stage(relayed_failure, workflow_context),
      completed_stages: safe_relayed_completed_stages(relayed_failure, workflow_context),
      failure_kind: safe_failure_kind(Map.get(relayed_failure, :failure_kind)),
      failure_class: safe_relayed_label(Map.get(relayed_failure, :failure_class), "error", 120),
      failure_location:
        safe_relayed_label(Map.get(relayed_failure, :failure_location), "application", 160)
    }
  end

  defp safe_relayed_stage(%{stage: stage}, _context) when is_atom(stage), do: stage
  defp safe_relayed_stage(_failure, context), do: List.first(context.stage_stack) || :internal

  defp safe_relayed_completed_stages(%{completed_stages: stages}, _context)
       when is_list(stages) do
    stages
    |> Enum.filter(&is_atom/1)
    |> Enum.take(@max_completed_stages)
  end

  defp safe_relayed_completed_stages(_failure, context), do: context.completed_stages

  defp safe_relayed_label(value, fallback, max_length) when is_binary(value) do
    if String.match?(value, ~r/\A[A-Za-z0-9_.\/:\-]+\z/) do
      String.slice(value, 0, max_length)
    else
      fallback
    end
  end

  defp safe_relayed_label(_value, fallback, _max_length), do: fallback

  defp failure_result(operation, failure) do
    log_failure(operation, failure)

    :telemetry.execute(
      [:favn, :storage_postgres, :database_workflow, :worker_failure],
      %{system_time: System.system_time()},
      Map.take(failure, [
        :diagnostic_id,
        :stage,
        :failure_kind,
        :failure_class,
        :failure_location
      ])
      |> Map.put(:operation, operation)
    )

    details =
      Map.take(failure, [
        :diagnostic_id,
        :failure_kind,
        :failure_class,
        :failure_location
      ])

    case operation do
      :status ->
        Result.error(
          :status,
          :operation_failed,
          :unexpected_worker_exit,
          failure.stage,
          failure.completed_stages,
          details
        )

      operation when operation in [:bootstrap, :upgrade] ->
        Result.error(
          operation,
          :unknown_outcome,
          :unexpected_worker_exit,
          failure.stage,
          failure.completed_stages,
          details
        )
    end
  end

  defp log_failure(operation, failure) do
    completed_stages =
      case failure.completed_stages do
        [] -> "none"
        stages -> Enum.join(stages, ",")
      end

    Logger.error(
      "favn.database_workflow.worker_failed operation=#{operation} " <>
        "stage=#{failure.stage} failure_kind=#{failure.failure_kind} " <>
        "failure_class=#{failure.failure_class} " <>
        "failure_location=#{failure.failure_location} " <>
        "completed_stages=#{completed_stages} " <>
        "diagnostic_id=#{failure.diagnostic_id}"
    )
  end

  defp safe_failure_kind(kind) when kind in [:error, :exit, :throw], do: kind
  defp safe_failure_kind(_kind), do: :error

  defp safe_failure_class(:error, %{__struct__: module}) when is_atom(module),
    do: safe_module_name(module)

  defp safe_failure_class(:exit, reason), do: safe_exit_class(reason)
  defp safe_failure_class(:throw, _reason), do: "throw"
  defp safe_failure_class(_kind, _reason), do: "error"

  defp safe_exit_class(reason) when reason in [:normal, :shutdown, :killed, :kill, :noproc],
    do: Atom.to_string(reason)

  defp safe_exit_class({class, _details}) when class in [:shutdown, :timeout, :noproc],
    do: Atom.to_string(class)

  defp safe_exit_class(_reason), do: "exit"

  defp safe_failure_location(stacktrace) when is_list(stacktrace) do
    Enum.find_value(stacktrace, "application", fn
      {module, function, arguments_or_arity, _location}
      when is_atom(module) and is_atom(function) ->
        module_name = Atom.to_string(module)

        if application_module?(module_name) do
          arity =
            if is_integer(arguments_or_arity),
              do: arguments_or_arity,
              else: length(arguments_or_arity)

          "#{safe_module_name(module)}.#{function}/#{arity}"
        end

      _frame ->
        nil
    end)
  end

  defp safe_failure_location(_stacktrace), do: "application"

  defp application_module?(module_name) do
    String.starts_with?(module_name, [
      "Elixir.Favn",
      "Elixir.FavnStoragePostgres"
    ])
  end

  defp safe_module_name(module) do
    module
    |> Atom.to_string()
    |> String.trim_leading("Elixir.")
    |> String.slice(0, 120)
  end

  defp diagnostic_id do
    "diag_" <> Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)
  end

  defp notify(message) do
    workflow_context = context()
    send(workflow_context.caller, {workflow_context.reference, message})

    if workflow_context.owner != self() do
      send(
        workflow_context.owner,
        {:favn_workflow_context, workflow_context.reference, message}
      )
    end
  end

  defp apply_context_event(context, {:stage_started, stage}) do
    Map.update!(context, :stage_stack, &[stage | &1])
  end

  defp apply_context_event(context, {:stage_finished, stage}) do
    Map.update!(context, :stage_stack, &pop_stage(&1, stage))
  end

  defp apply_context_event(context, {:completed_stages, stages}) do
    Map.put(context, :completed_stages, Enum.take(stages, @max_completed_stages))
  end

  defp apply_context_event(context, _event), do: context

  defp restore_context(nil), do: Process.delete(@context_key)
  defp restore_context(context), do: Process.put(@context_key, context)

  defp update_context(function) do
    Process.put(@context_key, function.(context()))
  end

  defp context do
    Process.get(@context_key) ||
      %{
        caller: self(),
        owner: self(),
        reference: make_ref(),
        operation: :unknown,
        diagnostic_id: "diag_unavailable",
        stage_stack: [],
        completed_stages: []
      }
  end
end
