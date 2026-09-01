defmodule FavnOrchestrator.RunnerRegistry do
  @moduledoc """
  Process-local live runner session registry for the single-control-plane topology.

  PostgreSQL remains authoritative for work. This registry contains only
  monitored BEAM process presence and one-slot claim state.
  """

  use GenServer

  alias Favn.Contracts.RunnerTask.ClaimRequest
  alias Favn.Contracts.RunnerTask.Registration
  alias Favn.Contracts.RunnerTask.RegistrationAck

  @statuses [:idle, :claiming, :reserved, :busy, :draining]

  defmodule Session do
    @moduledoc false
    @enforce_keys [
      :runner_instance_id,
      :boot_id,
      :agent_pid,
      :beam_node,
      :runner_pool,
      :required_runner_release_id,
      :protocol_version,
      :supported_task_kinds,
      :capabilities,
      :lifecycle_mode,
      :session_generation,
      :monitor_ref,
      :status,
      :registered_at
    ]
    defstruct @enforce_keys ++
                [:session_row_id, :claim_command_id, :claim_outcome, :active_assignment]

    @type t :: %__MODULE__{}
  end

  def start_link(opts),
    do: GenServer.start_link(__MODULE__, %{}, name: Keyword.get(opts, :name, __MODULE__))

  def register(registration, agent_pid),
    do: GenServer.call(__MODULE__, {:register, registration, agent_pid, false})

  @doc false
  def register_verified(registration, agent_pid),
    do: GenServer.call(__MODULE__, {:register, registration, agent_pid, true})

  def fetch(runner_instance_id), do: GenServer.call(__MODULE__, {:fetch, runner_instance_id})
  def list, do: GenServer.call(__MODULE__, :list)
  def snapshot, do: GenServer.call(__MODULE__, :snapshot)

  def count(runner_pool, required_runner_release_id),
    do: GenServer.call(__MODULE__, {:count, runner_pool, required_runner_release_id})

  def begin_claim(request), do: GenServer.call(__MODULE__, {:begin_claim, request})

  def finish_claim(request, outcome),
    do: GenServer.call(__MODULE__, {:finish_claim, request, outcome})

  def mark_busy(runner_instance_id, generation, assignment),
    do: GenServer.call(__MODULE__, {:mark, runner_instance_id, generation, :busy, assignment})

  def mark_idle(runner_instance_id, generation),
    do: GenServer.call(__MODULE__, {:mark, runner_instance_id, generation, :idle, nil})

  def drain(runner_instance_id, generation),
    do: GenServer.call(__MODULE__, {:mark, runner_instance_id, generation, :draining, nil})

  @impl true
  def init(state), do: {:ok, Map.merge(%{sessions: %{}, monitors: %{}}, state)}

  @impl true
  def handle_call(
        {:register, %Registration{} = registration, agent_pid, resume_verified?},
        _from,
        state
      ) do
    case validate_registration(registration, agent_pid, resume_verified?) do
      :ok ->
        {reply, state} = put_registration(state, registration, agent_pid)
        {:reply, reply, state}

      {:error, reason} ->
        {:reply,
         {:ok,
          %RegistrationAck{
            runner_instance_id: registration.runner_instance_id,
            status: :rejected,
            reason: reason
          }}, state}
    end
  end

  def handle_call({:fetch, runner_instance_id}, _from, state) do
    case Map.fetch(state.sessions, runner_instance_id) do
      {:ok, session} -> {:reply, {:ok, session}, state}
      :error -> {:reply, {:error, :runner_session_not_found}, state}
    end
  end

  def handle_call(:list, _from, state), do: {:reply, Map.values(state.sessions), state}

  def handle_call(:snapshot, _from, state) do
    sessions = Map.values(state.sessions)

    by_partition =
      sessions
      |> Enum.group_by(&{&1.runner_pool, &1.required_runner_release_id})
      |> Map.new(fn {{pool, release}, partition_sessions} ->
        statuses = Enum.frequencies_by(partition_sessions, & &1.status)

        {{pool, release},
         %{
           registered: length(partition_sessions),
           statuses: statuses,
           lifecycle_modes: Enum.frequencies_by(partition_sessions, & &1.lifecycle_mode)
         }}
      end)

    {:reply, %{available?: true, registered: length(sessions), partitions: by_partition}, state}
  end

  def handle_call({:count, pool, release}, _from, state) do
    count =
      Enum.count(state.sessions, fn {_id, session} ->
        session.runner_pool == pool and session.required_runner_release_id == release
      end)

    {:reply, count, state}
  end

  def handle_call({:begin_claim, %ClaimRequest{} = request}, _from, state) do
    with :ok <- ClaimRequest.validate(request),
         {:ok, session} <- matching_session(state, request),
         :ok <- claimable(session, request) do
      updated = %{
        session
        | status: :claiming,
          claim_command_id: request.command_id,
          claim_outcome: :in_flight
      }

      {:reply, {:ok, :start, updated}, put_session(state, updated)}
    else
      {:duplicate, outcome, session} -> {:reply, {:ok, {:duplicate, outcome}, session}, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:finish_claim, %ClaimRequest{} = request, outcome}, _from, state) do
    with {:ok, session} <- matching_session(state, request),
         true <- session.claim_command_id == request.command_id,
         true <- session.status == :claiming do
      status =
        if match?(%Favn.Contracts.RunnerTask.Assignment{}, outcome), do: :reserved, else: :idle

      updated = %{session | status: status, claim_outcome: outcome}
      {:reply, {:ok, updated}, put_session(state, updated)}
    else
      false -> {:reply, {:error, :stale_claim_request}, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:mark, runner_id, generation, status, assignment}, _from, state)
      when status in @statuses do
    with {:ok, session} <- Map.fetch(state.sessions, runner_id),
         true <- session.session_generation == generation do
      updated = %{
        session
        | status: status,
          active_assignment: assignment,
          claim_command_id: if(status == :idle, do: nil, else: session.claim_command_id),
          claim_outcome: if(status == :idle, do: nil, else: session.claim_outcome)
      }

      {:reply, :ok, put_session(state, updated)}
    else
      _other -> {:reply, {:error, :stale_runner_session}, state}
    end
  end

  @impl true
  def handle_info({:DOWN, monitor_ref, :process, _pid, reason}, state) do
    case Map.pop(state.monitors, monitor_ref) do
      {nil, _monitors} ->
        {:noreply, state}

      {{runner_id, generation}, monitors} ->
        state = %{state | monitors: monitors}

        case Map.get(state.sessions, runner_id) do
          %Session{session_generation: ^generation} = session ->
            if recovery = Process.whereis(FavnOrchestrator.RunnerTaskRecovery) do
              send(recovery, {:runner_down, runner_id, generation, reason})
            end

            spawn_session_write(fn ->
              FavnOrchestrator.RunnerSessions.close(session, reason, DateTime.utc_now())
            end)

            {:noreply, %{state | sessions: Map.delete(state.sessions, runner_id)}}

          _other ->
            {:noreply, state}
        end
    end
  end

  defp validate_registration(registration, agent_pid, resume_verified?) do
    with :ok <- Registration.validate(registration),
         {:ok, policy} <-
           FavnOrchestrator.RunnerPools.fetch(
             FavnOrchestrator.RuntimeConfig.runner_pools(),
             registration.runner_pool
           ),
         true <- policy.mode == registration.lifecycle_mode,
         :ok <- validate_agent_pid(registration, agent_pid),
         true <- is_nil(registration.active_assignment) or resume_verified? do
      :ok
    else
      false ->
        cond do
          policy_mode_mismatch?(registration) ->
            {:error, :runner_lifecycle_mode_mismatch}

          registration.active_assignment && not resume_verified? ->
            {:error, :unverified_active_assignment}

          true ->
            {:error, :invalid_runner_agent}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp validate_agent_pid(_registration, agent_pid) when not is_pid(agent_pid),
    do: {:error, :invalid_runner_agent}

  defp validate_agent_pid(registration, agent_pid) do
    actual_node = Atom.to_string(node(agent_pid))

    if registration.beam_node == actual_node,
      do: :ok,
      else: {:error, {:runner_agent_node_mismatch, registration.beam_node, actual_node}}
  end

  defp policy_mode_mismatch?(registration) do
    case FavnOrchestrator.RunnerPools.fetch(
           FavnOrchestrator.RuntimeConfig.runner_pools(),
           registration.runner_pool
         ) do
      {:ok, policy} -> policy.mode != registration.lifecycle_mode
      {:error, _reason} -> false
    end
  end

  defp put_registration(state, registration, agent_pid) do
    case Map.get(state.sessions, registration.runner_instance_id) do
      %Session{boot_id: boot_id, agent_pid: ^agent_pid} = session
      when boot_id == registration.boot_id ->
        {{:ok, acknowledgement(session)}, state}

      %Session{} ->
        acknowledgement = %RegistrationAck{
          runner_instance_id: registration.runner_instance_id,
          status: :rejected,
          reason: :runner_instance_id_already_registered
        }

        {{:ok, acknowledgement}, state}

      nil ->
        generation = registration_generation(registration)
        monitor_ref = Process.monitor(agent_pid)

        session = %Session{
          session_row_id: mint_session_row_id(),
          runner_instance_id: registration.runner_instance_id,
          boot_id: registration.boot_id,
          agent_pid: agent_pid,
          beam_node: registration.beam_node,
          runner_pool: registration.runner_pool,
          required_runner_release_id: registration.required_runner_release_id,
          protocol_version: registration.protocol_version,
          supported_task_kinds: registration.supported_task_kinds,
          capabilities: registration.capabilities,
          lifecycle_mode: registration.lifecycle_mode,
          session_generation: generation,
          monitor_ref: monitor_ref,
          status: if(registration.active_assignment, do: :busy, else: :idle),
          active_assignment: registration.active_assignment,
          registered_at: DateTime.utc_now()
        }

        state =
          state
          |> put_session(session)
          |> Map.update!(
            :monitors,
            &Map.put(&1, monitor_ref, {session.runner_instance_id, generation})
          )

        spawn_session_write(fn -> FavnOrchestrator.RunnerSessions.open(session) end)

        {{:ok, acknowledgement(session)}, state}
    end
  end

  defp mint_session_row_id,
    do: "rs_" <> Base.encode16(:crypto.strong_rand_bytes(16), case: :lower)

  defp spawn_session_write(fun) do
    case Process.whereis(FavnOrchestrator.RunnerSessionTaskSupervisor) do
      nil ->
        spawn(fun)

      _supervisor ->
        Task.Supervisor.start_child(FavnOrchestrator.RunnerSessionTaskSupervisor, fun)
    end

    :ok
  end

  defp acknowledgement(session) do
    %RegistrationAck{
      runner_instance_id: session.runner_instance_id,
      runner_session_generation: session.session_generation,
      status: :accepted
    }
  end

  defp registration_generation(registration) do
    case {registration.active_assignment, registration.runner_session_generation} do
      {%{assignment_generation: assignment_generation}, resumed}
      when is_integer(assignment_generation) and assignment_generation > 0 and
             is_integer(resumed) and resumed > 0 ->
        resumed

      _other ->
        random_session_generation()
    end
  end

  defp random_session_generation do
    <<generation::unsigned-63, _::1>> = :crypto.strong_rand_bytes(8)
    max(generation, 1)
  end

  defp matching_session(state, request) do
    case Map.get(state.sessions, request.runner_instance_id) do
      %Session{
        session_generation: generation,
        runner_pool: pool,
        required_runner_release_id: release,
        supported_task_kinds: kinds,
        capabilities: capabilities
      } = session
      when generation == request.runner_session_generation and
             pool == request.runner_pool and release == request.required_runner_release_id and
             kinds == request.supported_task_kinds and capabilities == request.capabilities ->
        {:ok, session}

      _other ->
        {:error, :stale_or_incompatible_runner_session}
    end
  end

  defp claimable(
         %Session{claim_command_id: command_id, claim_outcome: outcome} = session,
         request
       )
       when command_id == request.command_id and not is_nil(outcome),
       do: {:duplicate, outcome, session}

  defp claimable(%Session{status: :idle}, _request), do: :ok
  defp claimable(_session, _request), do: {:error, :runner_not_idle}

  defp put_session(state, session),
    do: %{state | sessions: Map.put(state.sessions, session.runner_instance_id, session)}
end
