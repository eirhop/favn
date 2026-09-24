defmodule FavnOrchestrator.RunPreparation do
  @moduledoc false
  use GenServer
  alias FavnOrchestrator.{ManifestStore, RunManager, RunOwnership, RunnerIdentityVerifier, Runs}
  alias FavnOrchestrator.RunState

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)
  @impl true
  def init(opts), do: {:ok, Map.new(opts)}

  @impl true
  def handle_info(:claim, state) do
    started = System.monotonic_time(:millisecond)

    result =
      RunOwnership.claim(state.context, state.run_id, RunOwnership.owner_id(state.run_id),
        purpose: state.purpose
      )

    send(RunManager, {:preparation_claim, state.id, self(), started, result})
    {:noreply, state}
  end

  def handle_info({:load, keeper, ownership, diagnostic_reason}, state) do
    send(self(), :load_run)

    {:noreply,
     Map.merge(state, %{
       keeper: keeper,
       ownership: ownership,
       diagnostic_reason: diagnostic_reason
     })}
  end

  def handle_info(:load_run, state) do
    case Runs.get(state.context, state.run_id) do
      {:ok, run} ->
        run =
          RunState.with_storage_fence(
            run,
            state.ownership.owner_id,
            state.ownership.fencing_token
          )

        send(
          self(),
          case {state.diagnostic_reason, state.ownership.claim_purpose} do
            {_, :diagnosis} -> :diagnose
            {nil, :cleanup} -> :load_manifest
            {nil, :execution} -> :load_manifest
            _ -> :diagnose
          end
        )

        {:noreply, Map.put(state, :run, run)}

      error ->
        failed(state, error)
    end
  end

  def handle_info(:diagnose, state) do
    result =
      FavnOrchestrator.RunServer.FailureCleanup.fail(
        state.run,
        state.diagnostic_reason || state.ownership.diagnosis_reason ||
          :automatic_recovery_exhausted
      )

    send(RunManager, {:preparation_failed, state.id, {:failure_cleanup, result}})
    {:noreply, state}
  end

  def handle_info(:load_manifest, state) do
    result =
      with {:ok, version} <-
             ManifestStore.get_deployment_manifest(
               state.context,
               state.run.deployment_id,
               state.run.manifest_version_id
             ),
           :ok <- RunnerIdentityVerifier.verify_run_manifest(state.run, version),
           do: {:ok, version}

    case result do
      {:ok, version} ->
        send(RunManager, {:preparation_ready, state.id, state.run, version})
        {:noreply, state}

      error ->
        failed(state, error)
    end
  end

  def handle_info({:lease_challenge, keeper, generation, challenge}, state) do
    send(keeper, {:lease_response, self(), generation, challenge})
    {:noreply, state}
  end

  def handle_info(:stop_preparer, state), do: {:stop, :normal, state}
  def handle_info(_, state), do: {:noreply, state}

  defp failed(state, error) do
    send(RunManager, {:preparation_failed, state.id, error})
    {:noreply, state}
  end
end
