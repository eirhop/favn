defmodule FavnLocal.ActivationObserver do
  @moduledoc false

  alias FavnOrchestrator.ManifestDeployments
  alias FavnOrchestrator.Manifests
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [
    :workspace,
    :operation_id,
    :deadline,
    :read_operation,
    :read_runtime,
    :cancel,
    :now,
    :wait
  ]
  defstruct @enforce_keys

  @doc false
  @spec await(WorkspaceContext.t(), String.t(), integer(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def await(workspace, operation_id, deadline, opts \\ []) do
    observe(%__MODULE__{
      workspace: workspace,
      operation_id: operation_id,
      deadline: deadline,
      read_operation: Keyword.get(opts, :read_operation, &ManifestDeployments.get_local/2),
      read_runtime: Keyword.get(opts, :read_runtime, &Manifests.active_runtime/1),
      cancel: Keyword.get(opts, :cancel, &ManifestDeployments.cancel/3),
      now: Keyword.get(opts, :now, fn -> System.monotonic_time(:millisecond) end),
      wait: Keyword.get(opts, :wait, &Process.sleep/1)
    })
  end

  defp observe(state) do
    case read(state, fn -> state.read_operation.(state.workspace, state.operation_id) end) do
      {:ok, %{state: status, activation_receipt: receipt}}
      when status in [:succeeded, :needs_attention] and not is_nil(receipt) ->
        reconcile(state, receipt)

      {:ok, %{state: status, failure_class: reason}}
      when status in [:failed, :cancelled, :unknown] ->
        {:error, {:deployment_operation, state.operation_id, status, reason}}

      {:ok, _pending} ->
        if remaining(state) <= 0 do
          cancel(state)
        else
          pause(state)
          observe(state)
        end

      {:error, reason} ->
        unknown(state, reason)
    end
  end

  defp reconcile(state, receipt) do
    case read(state, fn -> state.read_runtime.(state.workspace) end) do
      {:ok, runtime}
      when runtime.deployment_id == :erlang.map_get("deployment_id", receipt) and
             runtime.revision == :erlang.map_get("runtime_revision", receipt) ->
        {:ok, runtime}

      {:ok, _runtime} ->
        {:error, {:deployment_superseded, state.operation_id}}

      {:error, reason} ->
        unknown(state, reason)
    end
  end

  defp cancel(state) do
    case state.cancel.(state.workspace, state.operation_id, :startup_timeout) do
      {:ok, %{activation_receipt: receipt}} when not is_nil(receipt) ->
        reconcile(state, receipt)

      {:ok, %{state: status, cleanup_state: cleanup}} when status != :unknown ->
        {:error,
         {:deployment_interrupted,
          %{operation_id: state.operation_id, activation: :not_committed, cleanup: cleanup}}}

      _unknown ->
        unknown(state, :operation_wait_timeout)
    end
  end

  defp read(state, fun, first_error \\ nil) do
    case fun.() do
      {:error, %Error{retryable?: true} = reason} ->
        if remaining(state) > 0 do
          pause(state)
          read(state, fun, first_error || reason)
        else
          {:error, first_error || reason}
        end

      result ->
        result
    end
  end

  defp remaining(state), do: state.deadline - state.now.()

  # Publication runs this bounded observer in a supervised task, not the runtime GenServer.
  defp pause(state), do: state.wait.(max(min(remaining(state), 250), 0))

  defp unknown(state, reason),
    do: {:error, {:reload_outcome_unknown, %{operation_id: state.operation_id, reason: reason}}}
end
