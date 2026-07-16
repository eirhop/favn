defmodule Favn.Azure.Credentials.Cache do
  @moduledoc false

  use GenServer

  alias Favn.Azure.Credentials.{Request, Source}
  alias Favn.Azure.{Token, TokenError}

  @default_refresh_before_seconds 300
  @default_fetch_timeout 10_000
  @default_max_entries 128
  @default_max_inflight 32
  @default_max_waiters_per_key 256
  @max_refresh_before_seconds 3_600
  @max_fetch_timeout 60_000
  @max_entries 4_096
  @max_inflight 256
  @max_waiters_per_key 1_024

  defstruct [
    :task_supervisor,
    :refresh_before_seconds,
    :fetch_timeout,
    :max_entries,
    :max_inflight,
    :max_waiters_per_key,
    :clock,
    entries: %{},
    inflight: %{},
    refs: %{}
  ]

  @type server :: GenServer.server()

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    {name, opts} = Keyword.pop(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @spec fetch(server(), Request.t(), keyword(), timeout()) ::
          {:ok, Token.t()} | {:error, TokenError.t()}
  def fetch(server, %Request{} = request, provider_opts, timeout) do
    GenServer.call(server, {:fetch, request, provider_opts}, timeout)
  catch
    :exit, {:timeout, _call} -> {:error, cache_error(:call_timeout)}
    :exit, _reason -> {:error, cache_error(:cache_unavailable)}
  end

  @impl true
  def init(opts) do
    state = %__MODULE__{
      task_supervisor: Keyword.fetch!(opts, :task_supervisor),
      refresh_before_seconds:
        bounded_integer(
          opts,
          :refresh_before_seconds,
          @default_refresh_before_seconds,
          0,
          @max_refresh_before_seconds
        ),
      fetch_timeout:
        bounded_integer(opts, :fetch_timeout, @default_fetch_timeout, 1, @max_fetch_timeout),
      max_entries: bounded_integer(opts, :max_entries, @default_max_entries, 1, @max_entries),
      max_inflight:
        bounded_integer(opts, :max_inflight, @default_max_inflight, 1, @max_inflight),
      max_waiters_per_key:
        bounded_integer(
          opts,
          :max_waiters_per_key,
          @default_max_waiters_per_key,
          1,
          @max_waiters_per_key
        ),
      clock: clock(opts)
    }

    {:ok, state}
  end

  @impl true
  def handle_call({:fetch, %Request{} = request, provider_opts}, from, state) do
    key = {request, provider_options_fingerprint(provider_opts)}
    now = state.clock.()

    case Map.get(state.entries, key) do
      %Token{} = token ->
        if Token.valid_for?(token, state.refresh_before_seconds, now) do
          {:reply, {:ok, token}, state}
        else
          queue_or_start(key, request, provider_opts, token, from, state)
        end

      nil ->
        queue_or_start(key, request, provider_opts, nil, from, state)
    end
  end

  @impl true
  def handle_info({ref, result}, state) when is_reference(ref) do
    case Map.fetch(state.refs, ref) do
      {:ok, key} ->
        Process.demonitor(ref, [:flush])
        {:noreply, finish_fetch(key, result, state)}

      :error ->
        {:noreply, state}
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    case Map.fetch(state.refs, ref) do
      {:ok, key} -> {:noreply, finish_fetch(key, {:task_failure, :exited}, state)}
      :error -> {:noreply, state}
    end
  end

  def handle_info({:fetch_timeout, ref}, state) do
    case Map.fetch(state.refs, ref) do
      {:ok, key} ->
        inflight = Map.fetch!(state.inflight, key)
        Task.shutdown(inflight.task, :brutal_kill)
        {:noreply, finish_fetch(key, {:task_failure, :timeout}, state)}

      :error ->
        {:noreply, state}
    end
  end

  defp queue_or_start(key, request, provider_opts, stale, from, state) do
    case Map.get(state.inflight, key) do
      nil ->
        if map_size(state.inflight) < state.max_inflight do
          task =
            Task.Supervisor.async_nolink(state.task_supervisor, fn ->
              Source.fetch_token(request, provider_opts)
            end)

          timer = Process.send_after(self(), {:fetch_timeout, task.ref}, state.fetch_timeout)

          inflight = %{
            task: task,
            timer: timer,
            waiters: [from],
            stale: stale
          }

          {:noreply,
           %{
             state
             | inflight: Map.put(state.inflight, key, inflight),
               refs: Map.put(state.refs, task.ref, key)
           }}
        else
          reply_with_stale(stale, cache_error(:too_many_inflight_fetches), state)
        end

      %{waiters: waiters} = inflight when length(waiters) < state.max_waiters_per_key ->
        updated = %{inflight | waiters: [from | waiters]}
        {:noreply, %{state | inflight: Map.put(state.inflight, key, updated)}}

      _inflight ->
        reply_with_stale(stale, cache_error(:too_many_waiters), state)
    end
  end

  defp reply_with_stale(stale, error, state) do
    {reply, _entries} = fallback(stale, state.entries, error)
    {:reply, reply, state}
  end

  defp finish_fetch(key, result, state) do
    {inflight, inflight_by_key} = Map.pop(state.inflight, key)

    if inflight do
      Process.cancel_timer(inflight.timer)
      refs = Map.delete(state.refs, inflight.task.ref)
      {reply, entries} = resolve_result(result, inflight.stale, state.entries, key, state)
      Enum.each(inflight.waiters, &GenServer.reply(&1, reply))
      %{state | inflight: inflight_by_key, refs: refs, entries: entries}
    else
      state
    end
  end

  defp resolve_result({:ok, %Token{} = token}, stale, entries, key, state) do
    if Token.valid_for?(token, 0) do
      entries = put_bounded(entries, key, token, state.max_entries)
      {{:ok, token}, entries}
    else
      fallback(stale, entries, cache_error(:provider_returned_expired_token))
    end
  end

  defp resolve_result({:error, %TokenError{} = error}, stale, entries, _key, _state),
    do: fallback(stale, entries, error)

  defp resolve_result({:task_failure, reason}, stale, entries, _key, _state),
    do: fallback(stale, entries, cache_error(reason))

  defp resolve_result(_invalid, stale, entries, _key, _state),
    do: fallback(stale, entries, cache_error(:invalid_provider_result))

  defp fallback(%Token{} = token, entries, error) do
    if Token.valid_for?(token, 0), do: {{:ok, token}, entries}, else: {{:error, error}, entries}
  end

  defp fallback(_stale, entries, error), do: {{:error, error}, entries}

  defp put_bounded(entries, key, token, max_entries) do
    entries =
      Map.reject(entries, fn {_entry_key, cached} -> not Token.valid_for?(cached, 0) end)

    entries =
      if map_size(entries) >= max_entries and not Map.has_key?(entries, key) do
        {oldest_key, _token} = Enum.min_by(entries, fn {_entry_key, cached} -> cached.expires_at end)
        Map.delete(entries, oldest_key)
      else
        entries
      end

    Map.put(entries, key, token)
  end

  defp bounded_integer(opts, key, default, minimum, maximum) do
    case Keyword.get(opts, key, default) do
      value when is_integer(value) and value >= minimum and value <= maximum ->
        value

      _invalid ->
        raise ArgumentError, "#{key} must be an integer in #{minimum}..#{maximum}"
    end
  end

  defp clock(opts) do
    case Keyword.get(opts, :clock, &DateTime.utc_now/0) do
      clock when is_function(clock, 0) -> clock
      _invalid -> raise ArgumentError, "clock must be a zero-arity function"
    end
  end

  defp provider_options_fingerprint(provider_opts) do
    provider_opts
    |> :erlang.term_to_binary([:deterministic])
    |> then(&:crypto.hash(:sha256, &1))
  end

  defp cache_error(reason) do
    %TokenError{
      type: :connection_error,
      message: "Azure credential cache failed",
      retryable?: true,
      details: %{reason: reason}
    }
  end
end
