defmodule FavnOrchestrator.RuntimeInputPins do
  @moduledoc false

  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest.ExecutionPackage
  alias Favn.Replay.InputMode
  alias Favn.RuntimeInput.Pin
  alias Favn.RuntimeInput.Resolution
  alias FavnOrchestrator.Persistence.SystemContext
  alias FavnOrchestrator.RunnerDispatch
  alias FavnOrchestrator.Runs
  alias FavnOrchestrator.RunState

  @spec prepare(RunState.t(), RunnerWork.t(), module(), keyword()) ::
          {:ok, RunnerWork.t()} | {:error, term()}
  def prepare(
        %RunState{workspace_id: workspace_id} = run,
        %RunnerWork{} = work,
        runner_client,
        runner_opts
      )
      when is_binary(workspace_id) and is_atom(runner_client) and is_list(runner_opts) do
    context = SystemContext.workspace(workspace_id, :runtime_input_pins)

    if runtime_inputs?(work.execution_package) do
      do_prepare(context, run, work, runner_client, runner_opts)
    else
      {:ok, work}
    end
  end

  defp do_prepare(context, run, work, runner_client, runner_opts) do
    node_key = RunnerWork.node_key(work)

    case Runs.get_runtime_inputs(context, run.id, [node_key]) do
      {:ok, [%Pin{} = pin]} ->
        with :ok <- validate_expectation(work, pin) do
          {:ok, attach(work, pin, :runtime_inputs_pin_reused)}
        end

      {:ok, []} ->
        create_pin(context, work, node_key, runner_client, runner_opts)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp create_pin(context, work, node_key, runner_client, runner_opts) do
    with {:ok, mode} <- input_mode(work.metadata) do
      case mode do
        :fresh ->
          resolve_and_persist(context, work, node_key, runner_client, runner_opts)

        mode when mode in [:pinned, :inherit] ->
          inherit_and_persist(context, work, node_key, mode, runner_client, runner_opts)
      end
    end
  end

  defp resolve_and_persist(context, work, node_key, runner_client, runner_opts) do
    if function_exported?(runner_client, :resolve_runtime_inputs, 2) do
      case RunnerDispatch.resolve_runtime_inputs(runner_client, work, runner_opts) do
        {:ok, %Resolution{} = resolution} ->
          pin = Pin.new(work.run_id, node_key, resolution)

          with :ok <- validate_expectation(work, pin) do
            persist_and_attach(context, work, pin, :runtime_inputs_resolved)
          end

        {:ok, nil} ->
          if expectation_present?(work),
            do: {:error, :rebuild_runtime_input_pin_changed},
            else: {:ok, work}

        {:error, reason} ->
          {:error, reason}
      end
    else
      {:error, :runner_runtime_input_resolution_not_supported}
    end
  end

  defp inherit_and_persist(context, work, node_key, mode, runner_client, runner_opts) do
    with {:ok, source_run_id} <- source_run_id(work.metadata),
         {:ok, [%Pin{} = source]} <- Runs.get_runtime_inputs(context, source_run_id, [node_key]) do
      persist_and_attach(
        context,
        work,
        Pin.inherit(work.run_id, node_key, source),
        :runtime_inputs_pin_inherited
      )
    else
      {:ok, []} when mode == :inherit ->
        resolve_and_persist(context, work, node_key, runner_client, runner_opts)

      {:ok, []} ->
        {:error, {mode, :runtime_input_pin_not_found}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp persist_and_attach(context, work, pin, action) do
    with :ok <- validate_expectation(work, pin),
         {:ok, [persisted]} <- Runs.pin_runtime_inputs(context, work.run_id, [pin]) do
      {:ok, attach(work, persisted, action)}
    end
  end

  defp attach(work, pin, action) do
    metadata =
      work.metadata
      |> Map.put(:runtime_input_event, action)
      |> Map.put(:runtime_input_lineage, Pin.lineage(pin))

    %{work | runtime_input_pin: pin, metadata: metadata}
  end

  defp runtime_inputs?(%ExecutionPackage{sql_execution: %{runtime_inputs: resolver}}),
    do: not is_nil(resolver)

  defp runtime_inputs?(_package), do: false

  defp validate_expectation(work, pin) do
    case expectation(work) do
      :absent ->
        :ok

      expected when is_map(expected) ->
        if field(expected, :resolver) == Atom.to_string(pin.resolver) and
             field(expected, :input_identity) == pin.input_identity and
             field(expected, :payload_fingerprint) == pin.payload_fingerprint,
           do: :ok,
           else: {:error, :rebuild_runtime_input_pin_changed}

      _invalid ->
        {:error, :rebuild_runtime_input_pin_changed}
    end
  end

  defp expectation_present?(work), do: expectation(work) != :absent

  defp expectation(%{metadata: metadata}) do
    cond do
      Map.has_key?(metadata, :runtime_input_expectation) ->
        Map.get(metadata, :runtime_input_expectation)

      Map.has_key?(metadata, "runtime_input_expectation") ->
        Map.get(metadata, "runtime_input_expectation")

      true ->
        :absent
    end
  end

  defp field(map, key), do: Map.get(map, key, Map.get(map, Atom.to_string(key)))

  defp input_mode(metadata) do
    value =
      Map.get(metadata, :runtime_input_mode, Map.get(metadata, "runtime_input_mode", :fresh))

    InputMode.normalize(value)
  end

  defp source_run_id(metadata) do
    case Map.get(metadata, :source_run_id, Map.get(metadata, "source_run_id")) do
      run_id when is_binary(run_id) and run_id != "" -> {:ok, run_id}
      _missing -> {:error, :runtime_input_source_run_id_required}
    end
  end
end
