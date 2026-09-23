defmodule FavnOrchestrator.RunLeaseKeeper do
  @moduledoc """
  Independent authority and responsiveness watchdog for one run generation.

  Receipts use database time; local deadlines use monotonic request start. A
  delayed reply never buys extra time. Revocation is permanent for this process.
  """
  use GenServer
  alias FavnOrchestrator.{RunManager, RunOwnership, RunHelper}
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Results.RunOwnership, as: Ownership

  defstruct [
    :context,
    :ownership,
    :owner,
    :monitor,
    :manager,
    :deadline,
    :challenge,
    :worker,
    :renewal_id,
    :renewal_started,
    :renewal_timer,
    :watchdog_timer,
    :last_response,
    phase: :preparing,
    healthy: 0,
    open?: false,
    revoked?: false,
    confirmed?: false
  ]

  @interval 10_000
  @watchdog 45_000
  @operation 2_000
  @margin 10_000

  @doc false
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: name(opts[:ownership]))

  @doc "Returns a fresh admission permit, bounded to one second."
  @spec permit(map(), atom()) :: :ok | {:error, term()}
  def permit(run, kind \\ :asset_attempt) do
    deadline = now() + 1_000

    with {:ok, purpose} <- GenServer.call(name(run), {:permit, kind}, 1_000) do
      if purpose == :execution do
        remaining = deadline - now()

        if remaining > 0,
          do: FavnOrchestrator.RunTargetMaintenance.permit(run, remaining),
          else: {:error, :run_lease_unavailable}
      else
        :ok
      end
    end
  rescue
    ArgumentError -> {:error, :run_lease_unavailable}
  catch
    :exit, _ -> {:error, :run_lease_unavailable}
  end

  @doc false
  def ready(run), do: GenServer.call(name(run), :ready, 1_000)

  @doc false
  def transfer(keeper, owner), do: GenServer.call(keeper, {:transfer, owner}, 1_000)

  @doc false
  def receipt(keeper), do: GenServer.call(keeper, :receipt, 1_000)

  @doc false
  def name(%Ownership{} = ownership),
    do: via({ownership.workspace_id, ownership.run_id, ownership.fencing_token})

  def name(run), do: via({run.workspace_id, run.id, run.storage_fencing_token})
  defp via(key), do: {:via, Registry, {FavnOrchestrator.RunLeaseRegistry, key}}

  @doc false
  def deadline(%Ownership{expires_at: expires, database_observed_at: observed}, started)
      when not is_nil(observed),
      do: started + max(DateTime.diff(expires, observed, :millisecond), 0)

  def deadline(_, started), do: started

  @impl true
  def init(opts) do
    ownership = Keyword.fetch!(opts, :ownership)
    owner = Keyword.fetch!(opts, :owner)
    now = now()

    state = %__MODULE__{
      context: opts[:context],
      ownership: ownership,
      owner: owner,
      monitor: Process.monitor(owner),
      manager: opts[:manager],
      last_response: now,
      deadline: deadline(ownership, opts[:started_at]),
      challenge: make_ref()
    }

    send(owner, {:lease_challenge, self(), ownership.fencing_token, state.challenge})
    Process.send_after(self(), :check, 1_000)
    Process.send_after(self(), :renew, :erlang.phash2(ownership.run_id, 1_001))
    {:ok, watchdog(state)}
  end

  @impl true
  def handle_call(:ready, _from, state) do
    allowed = state.open? and healthy?(state) and not state.revoked?
    {:reply, if(allowed, do: :ok, else: {:error, :run_lease_degraded}), state}
  end

  def handle_call({:permit, kind}, _from, state) do
    purpose = state.ownership.claim_purpose

    allowed =
      purpose == :execution or
        (purpose == :cleanup and
           kind in [:relation_inspection, :generation_capabilities, :generation_marker_read])

    reply =
      if allowed and state.open? and healthy?(state) and not state.revoked?,
        do: {:ok, purpose},
        else: {:error, :run_lease_degraded}

    {:reply, reply, state}
  end

  def handle_call(:receipt, _from, state), do: {:reply, state.ownership, state}

  def handle_call({:transfer, owner}, _from, %{revoked?: false} = state) do
    Process.demonitor(state.monitor, [:flush])
    challenge = make_ref()
    send(owner, {:lease_challenge, self(), state.ownership.fencing_token, challenge})

    {:reply, :ok,
     %{
       state
       | owner: owner,
         monitor: Process.monitor(owner),
         challenge: challenge,
         phase: :running,
         open?: false,
         healthy: 0
     }}
  end

  def handle_call({:transfer, _}, _from, state), do: {:reply, {:error, :revoked}, state}

  @impl true
  def handle_info({:lease_response, owner, generation, challenge}, state)
      when owner == state.owner and generation == state.ownership.fencing_token and
             challenge == state.challenge do
    if state.revoked? or now() - state.last_response >= @watchdog do
      {:noreply, revoke(state, :unresponsive)}
    else
      {:noreply, watchdog(%{state | last_response: now(), challenge: nil})}
    end
  end

  def handle_info(:check, state) do
    Process.send_after(self(), :check, 1_000)

    cond do
      state.revoked? ->
        {:noreply, state}

      now() >= state.deadline - @margin ->
        {:noreply, revoke(state, :lease_deadline)}

      now() - state.last_response >= @watchdog ->
        {:noreply, revoke(state, :unresponsive)}

      true ->
        count = if healthy?(state), do: min(state.healthy + 1, 2), else: 0
        open = count >= 2

        if open != state.open? do
          FavnOrchestrator.OperationalEvents.emit(
            :run_lease_admission_changed,
            %{
              headroom_ms: state.deadline - now(),
              responsiveness_age_ms: now() - state.last_response
            },
            %{
              workspace_id: state.ownership.workspace_id,
              run_id: state.ownership.run_id,
              generation: state.ownership.fencing_token,
              open?: open
            },
            level: :debug
          )
        end

        state = %{state | healthy: count, open?: open}

        if is_nil(state.challenge) and now() - state.last_response >= 5_000 do
          challenge = make_ref()
          send(state.owner, {:lease_challenge, self(), state.ownership.fencing_token, challenge})
          {:noreply, %{state | challenge: challenge}}
        else
          {:noreply, state}
        end
    end
  end

  def handle_info({:watchdog, last_response}, %{last_response: last_response} = state),
    do: {:noreply, revoke(state, :unresponsive)}

  def handle_info(:renew, %{worker: nil, revoked?: false} = state) do
    renewal_id =
      state.renewal_id ||
        "renew:" <> Base.url_encode64(:crypto.strong_rand_bytes(18), padding: false)

    started = now()

    task =
      RunHelper.async(state.ownership, fn ->
        RunOwnership.renew(state.context, state.ownership, renewal_id: renewal_id)
      end)

    timer = Process.send_after(self(), {:renew_timeout, task.ref}, @operation)

    {:noreply,
     %{
       state
       | worker: task,
         renewal_id: renewal_id,
         renewal_started: started,
         renewal_timer: timer
     }}
  end

  def handle_info({ref, result}, %{worker: %{ref: ref}} = state) do
    Process.demonitor(ref, [:flush])
    Process.cancel_timer(state.renewal_timer)
    state = %{state | worker: nil}

    FavnOrchestrator.Telemetry.emit(
      :run_lease_renewal,
      %{total_ms: now() - state.renewal_started, headroom_ms: state.deadline - now()},
      %{result: if(match?({:ok, _}, result), do: :ok, else: :error)}
    )

    case result do
      {:ok, ownership} ->
        deadline = deadline(ownership, state.renewal_started)

        if state.revoked? or now() >= state.deadline - @margin or now() >= deadline - @margin do
          {:noreply, revoke(state, :late_renewal)}
        else
          Process.send_after(self(), :renew, @interval)

          {:noreply,
           %{state | ownership: ownership, deadline: deadline, renewal_id: nil, confirmed?: true}}
        end

      {:error, %Error{kind: :fenced}} ->
        {:noreply, revoke(state, :fenced)}

      _ ->
        {:noreply, retry(state)}
    end
  end

  def handle_info({:renew_timeout, ref}, %{worker: %{ref: ref, pid: pid}} = state) do
    Process.exit(pid, :kill)
    # Wait for DOWN before another request. A timeout does not prove rollback.
    {:noreply, state}
  end

  def handle_info({:DOWN, ref, :process, _, _}, %{worker: %{ref: ref}} = state),
    do: {:noreply, retry(%{state | worker: nil})}

  def handle_info({:DOWN, ref, :process, _, _}, %{monitor: ref} = state),
    do: {:noreply, revoke(state, :owner_down)}

  def handle_info(_, state), do: {:noreply, state}

  defp retry(%{revoked?: true} = state), do: state

  defp retry(state) do
    Process.send_after(self(), :renew, 249 + :rand.uniform(751))
    state
  end

  defp healthy?(state),
    do:
      state.confirmed? and state.deadline - now() >= 30_000 and
        now() - state.last_response <= 20_000

  defp watchdog(state) do
    if state.watchdog_timer, do: Process.cancel_timer(state.watchdog_timer)

    timer =
      Process.send_after(
        self(),
        {:watchdog, state.last_response},
        max(@watchdog - (now() - state.last_response), 0)
      )

    %{state | watchdog_timer: timer}
  end

  defp revoke(%{revoked?: true} = state, _), do: state

  defp revoke(state, reason) do
    FavnOrchestrator.OperationalEvents.emit(
      :run_lease_revoked,
      %{headroom_ms: state.deadline - now(), responsiveness_age_ms: now() - state.last_response},
      %{
        workspace_id: state.ownership.workspace_id,
        run_id: state.ownership.run_id,
        generation: state.ownership.fencing_token,
        reason: reason
      },
      level: :warning
    )

    send(state.manager || RunManager, {:lease_revoked, self(), state.ownership, reason})
    %{state | revoked?: true, open?: false}
  end

  defp now, do: System.monotonic_time(:millisecond)
end
