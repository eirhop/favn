defmodule FavnOrchestrator.TestSupport.UnitRunAuthority do
  @moduledoc false
  use GenServer
  alias FavnOrchestrator.Persistence.Results.RunOwnership

  # A boundary double for pure execution tests whose test process plays the
  # coordinator. ManagedRun and PostgreSQL tests exercise the real lifecycle.
  def start(run) do
    ExUnit.Callbacks.start_supervised!(
      {Registry, keys: :unique, name: FavnOrchestrator.RunLeaseRegistry}
    )

    unless Process.whereis(FavnOrchestrator.RunPostStepSupervisor),
      do:
        ExUnit.Callbacks.start_supervised!(
          {Task.Supervisor, name: FavnOrchestrator.RunPostStepSupervisor}
        )

    ownership = %RunOwnership{
      workspace_id: run.workspace_id,
      run_id: run.id,
      owner_id: run.storage_owner_id,
      fencing_token: run.storage_fencing_token,
      expires_at: DateTime.add(DateTime.utc_now(), 120)
    }

    ExUnit.Callbacks.start_supervised!(%{
      id: :unit_run_manager,
      start:
        {GenServer, :start_link,
         [__MODULE__, {:manager, ownership}, [name: FavnOrchestrator.RunManager]]}
    })

    name = FavnOrchestrator.RunLeaseKeeper.name(ownership)

    ExUnit.Callbacks.start_supervised!(%{
      id: :unit_run_keeper,
      start: {GenServer, :start_link, [__MODULE__, :keeper, [name: name]]}
    })

    ExUnit.Callbacks.start_supervised!(%{
      id: :unit_run_targets,
      start:
        {GenServer, :start_link,
         [
           __MODULE__,
           :targets,
           [name: {:via, Registry, {FavnOrchestrator.RunLeaseRegistry, {:targets, name}}}]
         ]}
    })

    :ok
  end

  @impl true
  def init(state), do: {:ok, state}
  @impl true
  def handle_call({:helper_authority, _}, _, {:manager, ownership} = state),
    do: {:reply, ownership, state}

  def handle_call({:register_helper, _, _, _}, _, state), do: {:reply, :ok, state}
  def handle_call({:permit, _}, _, :keeper), do: {:reply, {:ok, :execution}, :keeper}
  def handle_call(:permit, _, :targets), do: {:reply, :ok, :targets}

  def handle_call({:register, _}, _, :targets),
    do: {:reply, {:ok, {self(), make_ref()}}, :targets}

  @impl true
  def handle_cast(_, state), do: {:noreply, state}
  @impl true
  def handle_info(_, state), do: {:noreply, state}
end
