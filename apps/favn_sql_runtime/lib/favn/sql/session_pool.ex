defmodule Favn.SQL.SessionPool do
  @moduledoc """
  Local GenServer-backed pool for idle SQL sessions.

  The pool is intentionally per-BEAM only. Checked-out sessions are exclusive:
  an idle session is removed from the pool when checked out and can only be
  operated on or checked back in by the process recorded in its checkout
  metadata. The shared SQL client rejects non-owner use.

  Pooling is enabled by default for poolable adapters, but this process does not
  coordinate across runner nodes and does not raise catalog/write concurrency.
  Unsafe write/materialization/raw execution paths mark sessions for discard so
  mutated state is not returned to the idle pool unless the caller has explicitly
  proven the operation pool-safe.
  """

  use GenServer

  alias Favn.SQL.{Admission, Observability, PoolConfig, PoolKey, Session}
  alias Favn.SQL.SessionPool.Checkout

  @type status :: :ok | {:discard, term()}
  @type diagnostics :: %{
          active: non_neg_integer(),
          idle: non_neg_integer(),
          creating: non_neg_integer(),
          waiters: non_neg_integer(),
          keys: [%{hash: binary(), idle: non_neg_integer(), active: non_neg_integer()}]
        }

  defstruct idle: %{},
            active: %{},
            monitors: %{},
            creating: %{},
            creator_monitors: %{},
            waiters: %{},
            waiter_monitors: %{},
            discard_reasons: %{}

  @doc false
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, %__MODULE__{}, name: name)
  end

  @doc """
  Attaches checkout metadata to a freshly created session.
  """
  @spec attach_checkout(Session.t(), PoolKey.t(), PoolConfig.t(), pid()) :: Session.t()
  def attach_checkout(
        %Session{} = session,
        %PoolKey{} = key,
        %PoolConfig{} = config,
        owner \\ self()
      )
      when is_pid(owner) do
    %Session{
      session
      | pool_checkout: %Checkout{key: key, config: config, token: make_ref(), owner: owner}
    }
  end

  @doc """
  Records an already checked-out session so it can later be checked in.

  This is intended for callers that create a fresh adapter session after a pool
  miss but still want the pool to own checkin, discard, and owner-death cleanup.
  """
  @spec track_checkout(Session.t(), keyword()) :: :ok
  def track_checkout(%Session{pool_checkout: %Checkout{}} = session, opts \\ []) do
    GenServer.call(pool_name(opts), {:track_checkout, session})
  end

  @doc """
  Checks out an idle session for `key`, if one is available.
  """
  @spec checkout(PoolKey.t(), keyword()) :: {:ok, Session.t()} | :miss
  def checkout(%PoolKey{} = key, opts \\ []) do
    GenServer.call(pool_name(opts), {:checkout, key, self()})
  end

  @doc """
  Checks out an idle session or reserves the caller as the single creator for `key`.

  Concurrent misses for the same key are serialized. Waiters block until the
  current creator finishes, then either receive an idle session or become the next
  creator.
  """
  @spec checkout_or_create(PoolKey.t(), keyword()) :: {:ok, Session.t()} | :create
  def checkout_or_create(%PoolKey{} = key, opts \\ []) do
    GenServer.call(pool_name(opts), {:checkout_or_create, key, self()}, :infinity)
  end

  @doc """
  Releases the per-key creation gate after a create attempt succeeds or fails.
  """
  @spec creation_finished(PoolKey.t(), keyword()) :: :ok
  def creation_finished(%PoolKey{} = key, opts \\ []) do
    GenServer.cast(pool_name(opts), {:creation_finished, key})
  end

  @doc """
  Marks a checked-out pooled session to be discarded on checkin.

  The mark is stored in the pool rather than the caller process, so accidental
  cross-process use cannot return a mutated session to idle storage.
  """
  @spec mark_discard(Session.t() | reference(), term(), keyword()) :: :ok
  def mark_discard(session_or_token, reason, opts \\ [])

  def mark_discard(%Session{pool_checkout: %Checkout{token: token}}, reason, opts) do
    mark_discard(token, reason, opts)
  end

  def mark_discard(token, reason, opts) when is_reference(token) do
    GenServer.call(pool_name(opts), {:mark_discard, token, reason})
  end

  @doc """
  Checks a session back into the pool or discards it.
  """
  @spec checkin(Session.t(), status(), keyword()) :: :ok
  def checkin(%Session{} = session, status, opts \\ []) do
    GenServer.call(pool_name(opts), {:checkin, session, status})
  end

  @doc """
  Discards a checked-out session without returning it to idle storage.
  """
  @spec discard(Session.t(), term(), keyword()) :: :ok
  def discard(%Session{} = session, reason, opts \\ []) do
    checkin(session, {:discard, reason}, opts)
  end

  @doc """
  Returns local pool diagnostics without exposing raw connection data.
  """
  @spec diagnostics(keyword()) :: diagnostics()
  def diagnostics(opts \\ []) do
    GenServer.call(pool_name(opts), :diagnostics)
  end

  @impl true
  def init(%__MODULE__{} = state) do
    Process.flag(:trap_exit, true)
    {:ok, state}
  end

  @impl true
  def handle_call({:checkout, %PoolKey{} = key, owner}, _from, %__MODULE__{} = state) do
    {entry, idle} = pop_idle(state.idle, key)

    case entry do
      nil ->
        {:reply, :miss, %__MODULE__{state | idle: idle}}

      %{session: %Session{} = session, config: config} ->
        token = make_ref()
        monitor = Process.monitor(owner)
        checkout = %Checkout{key: key, config: config, token: token, owner: owner}
        session = %Session{session | pool_checkout: checkout}

        active_entry = %{
          session: session,
          key: key,
          config: config,
          owner: owner,
          monitor: monitor
        }

        {:reply, {:ok, session},
         %__MODULE__{
           state
           | idle: idle,
             active: Map.put(state.active, token, active_entry),
             monitors: Map.put(state.monitors, monitor, token)
          }}
    end
  end

  def handle_call({:checkout_or_create, %PoolKey{} = key, owner}, from, %__MODULE__{} = state) do
    state
    |> checkout_or_reserve(key, owner, from)
    |> case do
      {:reply, reply, state} -> {:reply, reply, state}
      {:noreply, state} -> {:noreply, state}
    end
  end

  def handle_call(
        {:track_checkout, %Session{pool_checkout: %Checkout{} = checkout} = session},
        _from,
        %__MODULE__{} = state
      ) do
    monitor = Process.monitor(checkout.owner)

    active_entry = %{
      session: session,
      key: checkout.key,
      config: checkout.config,
      owner: checkout.owner,
      monitor: monitor
    }

    {:reply, :ok,
     %__MODULE__{
       state
       | active: Map.put(state.active, checkout.token, active_entry),
         monitors: Map.put(state.monitors, monitor, checkout.token)
     }}
  end

  def handle_call(
        {:checkin, %Session{pool_checkout: %Checkout{} = checkout} = session, :ok},
        {caller, _ref},
        %__MODULE__{} = state
      ) do
    {discard_reason, discard_reasons} = Map.pop(state.discard_reasons, checkout.token)

    case Map.pop(state.active, checkout.token) do
      {nil, active} ->
        close_session(session)
        {:reply, :ok, %__MODULE__{state | active: active, discard_reasons: discard_reasons}}

      {%{owner: owner} = entry, active} ->
        {monitors, session} = release_active_monitor(state.monitors, entry, session)
        state = %__MODULE__{state | active: active, monitors: monitors, discard_reasons: discard_reasons}

        cond do
          not is_nil(discard_reason) ->
            close_session(session, discard_reason)
            {:reply, :ok, state}

          owner == caller ->
          {:reply, :ok,
           return_to_idle(
              state,
              session,
              checkout
           )}

          true ->
          close_session(session)
          {:reply, :ok, state}
        end
    end
  end

  def handle_call(
        {:checkin, %Session{pool_checkout: %Checkout{} = checkout} = session,
         {:discard, reason}},
        _from,
        %__MODULE__{} = state
      ) do
    {entry, active} = Map.pop(state.active, checkout.token)
    monitors = if entry, do: Map.delete(state.monitors, entry.monitor), else: state.monitors
    if entry, do: Process.demonitor(entry.monitor, [:flush])
    close_session(session, reason)
    {:reply, :ok,
     %__MODULE__{
       state
       | active: active,
         monitors: monitors,
         discard_reasons: Map.delete(state.discard_reasons, checkout.token)
     }}
  end

  def handle_call({:checkin, %Session{} = session, _status}, _from, %__MODULE__{} = state) do
    close_session(session)
    {:reply, :ok, state}
  end

  def handle_call(:diagnostics, _from, %__MODULE__{} = state) do
    {:reply, build_diagnostics(state), state}
  end

  def handle_call({:mark_discard, token, reason}, _from, %__MODULE__{} = state)
      when is_reference(token) do
    discard_reasons =
      if Map.has_key?(state.active, token) do
        Map.put(state.discard_reasons, token, reason)
      else
        state.discard_reasons
      end

    {:reply, :ok, %__MODULE__{state | discard_reasons: discard_reasons}}
  end

  @impl true
  def handle_cast({:creation_finished, %PoolKey{} = key}, %__MODULE__{} = state) do
    {:noreply, state |> finish_creation(key) |> drain_waiters(key.hash)}
  end

  @impl true
  def handle_info({:DOWN, monitor, :process, _pid, reason}, %__MODULE__{} = state) do
    case Map.pop(state.monitors, monitor) do
      {nil, monitors} ->
        case Map.pop(state.waiter_monitors, monitor) do
          {nil, waiter_monitors} ->
            case Map.pop(state.creator_monitors, monitor) do
              {nil, creator_monitors} ->
                {:noreply,
                 %__MODULE__{
                   state
                   | monitors: monitors,
                     waiter_monitors: waiter_monitors,
                     creator_monitors: creator_monitors
                 }}

              {hash, creator_monitors} ->
                {:noreply,
                 %__MODULE__{
                   state
                   | monitors: monitors,
                     waiter_monitors: waiter_monitors,
                     creator_monitors: creator_monitors,
                     creating: Map.delete(state.creating, hash)
                 }
                 |> drain_waiters(hash)}
            end

          {hash, waiter_monitors} ->
            {:noreply,
             %__MODULE__{
               state
               | monitors: monitors,
                 waiter_monitors: waiter_monitors,
                 waiters: remove_waiter(state.waiters, hash, monitor)
             }}
        end

      {token, monitors} ->
        {entry, active} = Map.pop(state.active, token)
        if entry, do: close_session(entry.session, reason)

        {:noreply,
         %__MODULE__{
           state
           | monitors: monitors,
             active: active,
             discard_reasons: Map.delete(state.discard_reasons, token)
         }}
    end
  end

  def handle_info({:evict_idle, hash, token}, %__MODULE__{} = state) do
    {expired, idle} = pop_idle_token(state.idle, hash, token)
    if expired, do: close_session(expired.session, :idle_timeout)
    {:noreply, %__MODULE__{state | idle: idle}}
  end

  @impl true
  def terminate(reason, %__MODULE__{} = state) do
    state.idle
    |> Map.values()
    |> List.flatten()
    |> Enum.each(&close_session(&1.session, {:pool_shutdown, reason}))

    state.active
    |> Map.values()
    |> Enum.each(&close_session(&1.session, {:pool_shutdown, reason}))

    :ok
  end

  defp checkout_or_reserve(%__MODULE__{} = state, %PoolKey{} = key, owner, from) do
    {entry, idle} = pop_idle(state.idle, key)
    state = %__MODULE__{state | idle: idle}

    case entry do
      %{session: %Session{} = session, config: config} ->
        {session, state} = activate_idle_session(state, session, key, config, owner)
        emit_pool_checkout(:hit, key.hash, 0)
        {:reply, {:ok, session}, state}

      nil ->
        reserve_or_wait(state, key, owner, from)
    end
  end

  defp activate_idle_session(%__MODULE__{} = state, %Session{} = session, key, config, owner) do
    token = make_ref()
    monitor = Process.monitor(owner)
    checkout = %Checkout{key: key, config: config, token: token, owner: owner}
    session = %Session{session | pool_checkout: checkout}

    active_entry = %{
      session: session,
      key: key,
      config: config,
      owner: owner,
      monitor: monitor
    }

    {session,
     %__MODULE__{
       state
       | active: Map.put(state.active, token, active_entry),
         monitors: Map.put(state.monitors, monitor, token)
     }}
  end

  defp reserve_or_wait(%__MODULE__{} = state, %PoolKey{hash: hash} = key, owner, from) do
    if Map.has_key?(state.creating, hash) do
      monitor = Process.monitor(owner)

      waiter = %{
        from: from,
        key: key,
        owner: owner,
        monitor: monitor,
        wait_started_at: monotonic_ms()
      }

      {:noreply,
       %__MODULE__{
         state
         | waiters: Map.update(state.waiters, hash, :queue.from_list([waiter]), &:queue.in(waiter, &1)),
           waiter_monitors: Map.put(state.waiter_monitors, monitor, hash)
       }}
    else
      emit_pool_checkout(:miss, hash, 0)
      {:reply, :create, reserve_creator(state, key, owner)}
    end
  end

  defp reserve_creator(%__MODULE__{} = state, %PoolKey{hash: hash} = key, owner) do
    monitor = Process.monitor(owner)

    %__MODULE__{
      state
      | creating: Map.put(state.creating, hash, %{key: key, owner: owner, monitor: monitor}),
        creator_monitors: Map.put(state.creator_monitors, monitor, hash)
    }
  end

  defp finish_creation(%__MODULE__{} = state, %PoolKey{hash: hash}) do
    case Map.pop(state.creating, hash) do
      {nil, creating} ->
        %__MODULE__{state | creating: creating}

      {%{monitor: monitor}, creating} ->
        Process.demonitor(monitor, [:flush])

        %__MODULE__{
          state
          | creating: creating,
            creator_monitors: Map.delete(state.creator_monitors, monitor)
        }
    end
  end

  defp drain_waiters(%__MODULE__{} = state, hash) do
    queue = Map.get(state.waiters, hash, :queue.new())

    cond do
      :queue.is_empty(queue) ->
        state

      true ->
        case pop_idle_by_hash(state.idle, hash) do
          {%{session: %Session{} = session, key: key, config: config}, idle} ->
            {{:value, waiter}, queue} = :queue.out(queue)
            {waiter_monitors, wait_ms} = release_waiter_monitor(state.waiter_monitors, waiter)
            {session, state} = activate_idle_session(%__MODULE__{state | idle: idle}, session, key, config, waiter.owner)
            GenServer.reply(waiter.from, {:ok, session})
            emit_pool_checkout(:hit, hash, wait_ms)

            %__MODULE__{
              state
              | waiters: put_waiters(state.waiters, hash, queue),
                waiter_monitors: waiter_monitors
            }
            |> drain_waiters(hash)

          {nil, _idle} ->
            if Map.has_key?(state.creating, hash) do
              state
            else
              {{:value, waiter}, queue} = :queue.out(queue)
              {waiter_monitors, wait_ms} = release_waiter_monitor(state.waiter_monitors, waiter)
              GenServer.reply(waiter.from, :create)
              emit_pool_checkout(:miss, hash, wait_ms)

              %__MODULE__{
                reserve_creator(state, waiter.key, waiter.owner)
                | waiters: put_waiters(state.waiters, hash, queue),
                  waiter_monitors: waiter_monitors
              }
            end
        end
    end
  end

  defp return_to_idle(%__MODULE__{} = state, %Session{} = session, %Checkout{
         key: key,
         config: config
       }) do
    session = %Session{session | pool_checkout: nil}

    if config.enabled and config.max_idle_per_key > 0 do
      entries = Map.get(state.idle, key.hash, [])

      if length(entries) < config.max_idle_per_key do
        token = make_ref()
        Process.send_after(self(), {:evict_idle, key.hash, token}, config.idle_timeout_ms)

        entry = %{
          session: session,
          key: key,
          config: config,
          token: token,
          idle_since: monotonic_ms()
        }

        %__MODULE__{state | idle: Map.put(state.idle, key.hash, [entry | entries])}
      else
        close_session(session)
        state
      end
    else
      close_session(session)
      state
    end
  end

  defp release_active_monitor(monitors, entry, %Session{} = session) do
    Process.demonitor(entry.monitor, [:flush])
    {Map.delete(monitors, entry.monitor), %Session{session | pool_checkout: nil}}
  end

  defp pop_idle(idle, %PoolKey{hash: hash}) do
    case Map.get(idle, hash, []) do
      [] -> {nil, idle}
      [entry | rest] -> {entry, put_or_delete(idle, hash, rest)}
    end
  end

  defp pop_idle_by_hash(idle, hash) do
    case Map.get(idle, hash, []) do
      [] -> {nil, idle}
      [entry | rest] -> {entry, put_or_delete(idle, hash, rest)}
    end
  end

  defp pop_idle_token(idle, hash, token) do
    {matched, remaining} =
      idle
      |> Map.get(hash, [])
      |> Enum.split_with(&(&1.token == token))

    {List.first(matched), put_or_delete(idle, hash, remaining)}
  end

  defp put_or_delete(map, key, []), do: Map.delete(map, key)
  defp put_or_delete(map, key, value), do: Map.put(map, key, value)

  defp put_waiters(waiters, hash, queue) do
    if :queue.is_empty(queue), do: Map.delete(waiters, hash), else: Map.put(waiters, hash, queue)
  end

  defp release_waiter_monitor(waiter_monitors, waiter) do
    Process.demonitor(waiter.monitor, [:flush])
    {Map.delete(waiter_monitors, waiter.monitor), monotonic_ms() - waiter.wait_started_at}
  end

  defp remove_waiter(waiters, hash, monitor) do
    waiters
    |> Map.get(hash, :queue.new())
    |> :queue.to_list()
    |> Enum.reject(&(&1.monitor == monitor))
    |> :queue.from_list()
    |> then(&put_waiters(waiters, hash, &1))
  end

  defp close_session(
         %Session{adapter: adapter, conn: conn, admission_lease: lease},
         reason \\ :discard
       ) do
    try do
      Observability.emit([:pool, :session, :discarded], %{}, %{reason: inspect(reason)})
      _ = adapter.disconnect(conn, [])
      Admission.release_session(lease)
      :ok
    rescue
      _error ->
        Admission.release_session(lease)
        :ok
    catch
      :exit, _reason ->
        Admission.release_session(lease)
        :ok
    end
  end

  defp build_diagnostics(state) do
    idle_counts = Map.new(state.idle, fn {hash, entries} -> {hash, length(entries)} end)

    active_counts =
      state.active
      |> Map.values()
      |> Enum.frequencies_by(& &1.key.hash)

    keys =
      (Map.keys(idle_counts) ++ Map.keys(active_counts))
      |> Enum.uniq()
      |> Enum.sort()
      |> Enum.map(fn hash ->
        %{
          hash: hash,
          idle: Map.get(idle_counts, hash, 0),
          active: Map.get(active_counts, hash, 0)
        }
      end)

    %{
      active: map_size(state.active),
      idle: Enum.sum(Map.values(idle_counts)),
      creating: map_size(state.creating),
      waiters:
        state.waiters
        |> Map.values()
        |> Enum.map(&:queue.len/1)
        |> Enum.sum(),
      keys: keys
    }
  end

  defp emit_pool_checkout(result, hash, wait_time_ms) do
    Observability.emit(
      [:pool, :checkout],
      %{wait_time_ms: wait_time_ms},
      %{result: result, key_hash: hash}
    )
  end

  defp monotonic_ms, do: System.monotonic_time(:millisecond)
  defp pool_name(opts), do: Keyword.get(opts, :name, __MODULE__)
end
