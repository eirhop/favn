defmodule FavnOrchestrator.RuntimeInputPins do
  @moduledoc false

  alias Favn.Contracts.RunnerWork
  alias Favn.RuntimeInput.Pin
  alias Favn.RuntimeInput.Resolution
  alias FavnOrchestrator.Persistence.SystemContext
  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.Queries, as: Q

  @doc false
  @spec pin_for_resolution(String.t(), String.t(), Resolution.t()) ::
          {:ok, Pin.t()} | {:error, term()}
  def pin_for_resolution(workspace_id, task_id, %Resolution{} = resolution) do
    context = SystemContext.workspace(workspace_id, :runner_task_runtime_input_pin)

    with {:ok, %{task_kind: :asset_attempt, payload: %RunnerWork{} = work}} <-
           Persistence.stores().runner_tasks.get(%Q.GetRunnerTask{
             workspace_context: context,
             task_id: task_id
           }),
         pin <- Pin.new(work.run_id, RunnerWork.node_key(work), resolution),
         :ok <- validate_expectation(work, pin) do
      {:ok, pin}
    else
      {:ok, _other_task} -> {:error, :runtime_inputs_not_supported_for_task}
      {:error, reason} -> {:error, reason}
    end
  end

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
end
