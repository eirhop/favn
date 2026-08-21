defmodule FavnRunner.ControlPlaneConnection do
  @moduledoc """
  Owns the bounded outbound distributed-BEAM connection to one control plane.

  The process coalesces reconnect requests, applies capped backoff, publishes
  state changes to its subscriber, and exposes only redacted failure classes.
  Runner registration remains owned by `FavnRunner.RunnerAgent`.
  """

  use GenServer

  alias FavnRunner.OperationalEvents

  @max_node_bytes 255
  @diagnostics_timeout_ms 250
  @epmd_timeout_ms 1_000
  @max_retry_ms 30_000
  @subscribe_timeout_ms 250

  @type failure_class ::
          :connection_probe_failed
          | :distribution_handshake_failed
          | :distribution_monitor_unavailable
          | :distribution_not_started
          | :dns_resolution_failed
          | :epmd_probe_failed
          | :node_connection_closed
          | :node_down
          | :node_tick_timeout

  @type diagnostics :: %{
          status: :connected | :connecting | :unavailable,
          connected?: boolean(),
          target_node: String.t() | :unknown,
          retry_count: non_neg_integer() | :unknown,
          last_failure_class: failure_class() | nil | :unknown,
          last_failure_at: DateTime.t() | nil,
          connected_at: DateTime.t() | nil,
          next_retry_ms: non_neg_integer() | nil | :unknown,
          next_retry_at: DateTime.t() | nil
        }

  def start_link(opts) do
    case Keyword.get(opts, :name, __MODULE__) do
      nil -> GenServer.start_link(__MODULE__, opts)
      name -> GenServer.start_link(__MODULE__, opts, name: name)
    end
  end

  @doc "Returns the remote runner gateway only while the target node is connected."
  @spec gateway(GenServer.server()) :: {:ok, {module(), node()}} | {:error, atom()}
  def gateway(server \\ __MODULE__), do: GenServer.call(server, :gateway)

  @doc "Requests a coalesced connection attempt."
  @spec reconnect(GenServer.server()) :: :ok
  def reconnect(server \\ __MODULE__), do: GenServer.cast(server, :connect)

  @doc "Subscribes one process to bounded connection-state messages."
  @spec subscribe(GenServer.server(), pid()) :: :ok | {:error, :control_plane_unavailable}
  def subscribe(server \\ __MODULE__, subscriber \\ self()) when is_pid(subscriber) do
    GenServer.call(server, {:subscribe, subscriber}, @subscribe_timeout_ms)
  catch
    :exit, _reason -> {:error, :control_plane_unavailable}
  end

  @doc "Returns bounded, secret-free control-plane connection diagnostics."
  @spec diagnostics(GenServer.server()) :: diagnostics()
  def diagnostics(server \\ __MODULE__) do
    GenServer.call(server, :diagnostics, @diagnostics_timeout_ms)
  catch
    :exit, _reason -> unavailable_diagnostics()
  end

  def register(gateway, registration, agent_pid),
    do: GenServer.call(gateway, {:register, registration, agent_pid}, 15_000)

  def request(gateway, message),
    do: GenServer.call(gateway, {:request, message}, 60_000)

  def fetch_manifest(gateway, assignment),
    do: GenServer.call(gateway, {:fetch_manifest, assignment}, 60_000)

  @impl true
  def init(opts) do
    with {:ok, node_name} <- parse_node(Keyword.fetch!(opts, :node)) do
      state = %{
        node: node_name,
        status: :connecting,
        connected?: false,
        retry_count: 0,
        retry_timer: nil,
        retry_token: nil,
        next_retry_ms: nil,
        next_retry_at: nil,
        last_failure_class: nil,
        last_failure_at: nil,
        last_logged_retry_count: 0,
        connected_at: nil,
        monitor_enabled?: false,
        connect_fun: Keyword.get(opts, :connect_fun, &Node.connect/1),
        probe_fun: Keyword.get(opts, :probe_fun, &probe/1),
        monitor_fun:
          Keyword.get(opts, :monitor_fun, fn ->
            :net_kernel.monitor_nodes(true, %{
              node_type: :all,
              nodedown_reason: true
            })
          end),
        retry_delay_fun: Keyword.get(opts, :retry_delay_fun, &reconnect_delay/1),
        now_fun: Keyword.get(opts, :now_fun, &DateTime.utc_now/0),
        subscribers: %{},
        subscriber_monitors: %{}
      }

      {:ok, schedule_connect(state, 0)}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call(:gateway, _from, %{connected?: true, node: node_name} = state),
    do: {:reply, {:ok, {:"Elixir.FavnOrchestrator.RunnerGateway", node_name}}, state}

  def handle_call(:gateway, _from, state),
    do: {:reply, {:error, :control_plane_unavailable}, state}

  def handle_call(:diagnostics, _from, state),
    do: {:reply, connection_diagnostics(state), state}

  def handle_call({:subscribe, subscriber}, _from, state) do
    state = put_subscriber(state, subscriber)
    send(subscriber, {:favn_control_plane_connection, connection_diagnostics(state)})
    {:reply, :ok, state}
  end

  @impl true
  def handle_cast(:connect, %{connected?: true} = state) do
    notify_subscribers(state)
    {:noreply, state}
  end

  def handle_cast(:connect, state), do: {:noreply, schedule_connect(state, 0)}

  @impl true
  def handle_info({:connect, token}, %{retry_token: token} = state) do
    started_at = System.monotonic_time()
    state = clear_retry(state)

    case connect(state) do
      {:ok, state} ->
        duration_ms =
          System.convert_time_unit(System.monotonic_time() - started_at, :native, :millisecond)

        {:noreply, connection_succeeded(state, duration_ms)}

      {:error, failure_class, state} ->
        {:noreply, connection_failed(state, failure_class)}
    end
  end

  def handle_info({:connect, _stale_token}, state), do: {:noreply, state}

  def handle_info({:nodedown, node_name, _info}, %{node: node_name, connected?: false} = state),
    do: {:noreply, state}

  def handle_info({:nodedown, node_name, info}, %{node: node_name} = state) do
    failure_class = nodedown_failure_class(info)
    {:noreply, connection_failed(cancel_retry(state), failure_class, 250)}
  end

  def handle_info({:nodeup, node_name, _info}, %{node: node_name} = state),
    do: {:noreply, connection_succeeded(cancel_retry(state), 0)}

  def handle_info({:DOWN, monitor, :process, subscriber, _reason}, state) do
    case Map.pop(state.subscriber_monitors, monitor) do
      {^subscriber, subscriber_monitors} ->
        {:noreply,
         %{
           state
           | subscribers: Map.delete(state.subscribers, subscriber),
             subscriber_monitors: subscriber_monitors
         }}

      {nil, _subscriber_monitors} ->
        {:noreply, state}
    end
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp connect(state) do
    case ensure_monitor(state) do
      {:ok, state} ->
        case connect_transport(state) do
          :ok -> {:ok, state}
          {:error, failure_class} -> {:error, failure_class, state}
        end

      {:error, failure_class} ->
        {:error, failure_class, state}
    end
  end

  defp connect_transport(%{node: node_name}) when node_name == node(), do: :ok

  defp connect_transport(state) do
    with :ok <- invoke_probe(state.probe_fun, state.node),
         true <- invoke_connect(state.connect_fun, state.node) do
      :ok
    else
      false -> {:error, :distribution_handshake_failed}
      :ignored -> {:error, :distribution_not_started}
      {:error, failure_class} -> {:error, failure_class}
      _other -> {:error, :distribution_handshake_failed}
    end
  end

  defp invoke_probe(probe_fun, node_name) do
    probe_fun.(node_name)
  rescue
    _exception -> {:error, :connection_probe_failed}
  catch
    _kind, _reason -> {:error, :connection_probe_failed}
  end

  defp invoke_connect(connect_fun, node_name) do
    connect_fun.(node_name)
  rescue
    _exception -> false
  catch
    _kind, _reason -> false
  end

  defp ensure_monitor(%{monitor_enabled?: true} = state), do: {:ok, state}

  defp ensure_monitor(state) do
    case state.monitor_fun.() do
      :ok -> {:ok, %{state | monitor_enabled?: true}}
      _other -> {:error, :distribution_monitor_unavailable}
    end
  rescue
    _exception -> {:error, :distribution_monitor_unavailable}
  catch
    _kind, _reason -> {:error, :distribution_monitor_unavailable}
  end

  defp connection_succeeded(%{connected?: true} = state, _duration_ms), do: state

  defp connection_succeeded(state, duration_ms) do
    now = state.now_fun.()
    retry_count = state.retry_count

    OperationalEvents.emit(
      :control_plane_connected,
      %{duration_ms: max(duration_ms, 0), retry_count: retry_count},
      %{target_node: Atom.to_string(state.node)},
      level: :info
    )

    state = %{
      state
      | status: :connected,
        connected?: true,
        retry_count: 0,
        next_retry_ms: nil,
        next_retry_at: nil,
        last_logged_retry_count: 0,
        connected_at: now
    }

    notify_subscribers(state)
    state
  end

  defp connection_failed(state, failure_class, delay_override \\ nil) do
    retry_count = state.retry_count + 1
    delay = retry_delay(state, retry_count, delay_override)
    now = state.now_fun.()

    should_log? =
      retry_count in [1, 2, 4, 8] or rem(retry_count, 20) == 0 or
        failure_class != state.last_failure_class

    suppressed_count = max(retry_count - state.last_logged_retry_count - 1, 0)

    OperationalEvents.emit(
      :control_plane_connection_attempt,
      %{retry_count: retry_count, next_retry_ms: delay},
      %{
        target_node: Atom.to_string(state.node),
        failure_class: failure_class,
        result: :failed
      },
      level: :debug
    )

    OperationalEvents.emit(
      :control_plane_connection_failed,
      %{
        retry_count: retry_count,
        next_retry_ms: delay,
        suppressed_count: suppressed_count
      },
      %{
        target_node: Atom.to_string(state.node),
        failure_class: failure_class
      },
      level: :warning,
      log?: should_log?
    )

    state = %{
      state
      | status: :connecting,
        connected?: false,
        retry_count: retry_count,
        last_failure_class: failure_class,
        last_failure_at: now,
        last_logged_retry_count:
          if(should_log?, do: retry_count, else: state.last_logged_retry_count)
    }

    notify_subscribers(state)
    schedule_connect(state, delay)
  end

  defp retry_delay(state, retry_count, nil) do
    state.retry_delay_fun.(retry_count - 1)
    |> normalize_retry_delay()
  rescue
    _exception -> @max_retry_ms
  catch
    _kind, _reason -> @max_retry_ms
  end

  defp retry_delay(_state, _retry_count, delay), do: normalize_retry_delay(delay)

  defp normalize_retry_delay(delay) when is_integer(delay),
    do: min(max(delay, 1), @max_retry_ms)

  defp normalize_retry_delay(_delay), do: @max_retry_ms

  defp schedule_connect(%{retry_timer: timer} = state, _delay) when is_reference(timer),
    do: state

  defp schedule_connect(state, delay) do
    token = make_ref()
    timer = Process.send_after(self(), {:connect, token}, delay)
    now = state.now_fun.()

    %{
      state
      | retry_timer: timer,
        retry_token: token,
        next_retry_ms: delay,
        next_retry_at: DateTime.add(now, delay, :millisecond)
    }
  end

  defp clear_retry(state) do
    %{
      state
      | retry_timer: nil,
        retry_token: nil,
        next_retry_ms: nil,
        next_retry_at: nil
    }
  end

  defp cancel_retry(%{retry_timer: timer} = state) when is_reference(timer) do
    Process.cancel_timer(timer)
    clear_retry(state)
  end

  defp cancel_retry(state), do: clear_retry(state)

  defp put_subscriber(state, subscriber) do
    if Map.has_key?(state.subscribers, subscriber) do
      state
    else
      monitor = Process.monitor(subscriber)

      %{
        state
        | subscribers: Map.put(state.subscribers, subscriber, monitor),
          subscriber_monitors: Map.put(state.subscriber_monitors, monitor, subscriber)
      }
    end
  end

  defp notify_subscribers(state) do
    message = {:favn_control_plane_connection, connection_diagnostics(state)}
    Enum.each(Map.keys(state.subscribers), &send(&1, message))
    :ok
  end

  defp connection_diagnostics(state) do
    %{
      status: state.status,
      connected?: state.connected?,
      target_node: Atom.to_string(state.node),
      retry_count: state.retry_count,
      last_failure_class: state.last_failure_class,
      last_failure_at: state.last_failure_at,
      connected_at: state.connected_at,
      next_retry_ms: state.next_retry_ms,
      next_retry_at: state.next_retry_at
    }
  end

  defp unavailable_diagnostics do
    %{
      status: :unavailable,
      connected?: false,
      target_node: :unknown,
      retry_count: :unknown,
      last_failure_class: :unknown,
      last_failure_at: nil,
      connected_at: nil,
      next_retry_ms: :unknown,
      next_retry_at: nil
    }
  end

  defp probe(node_name) do
    [name, host] = node_name |> Atom.to_string() |> String.split("@", parts: 2)
    name = String.to_charlist(name)
    host = String.to_charlist(host)

    case :erl_epmd.address_please(name, host, :inet) do
      {:ok, _address} -> probe_epmd(name, host)
      {:error, _reason} -> {:error, :dns_resolution_failed}
    end
  end

  defp probe_epmd(name, host) do
    case :erl_epmd.port_please(name, host, @epmd_timeout_ms) do
      {:port, _port, _version} -> :ok
      _unavailable_or_unregistered -> {:error, :epmd_probe_failed}
    end
  end

  defp nodedown_failure_class(info) when is_map(info) do
    info |> Map.get(:nodedown_reason) |> map_nodedown_reason()
  end

  defp nodedown_failure_class(info) when is_list(info) do
    info |> Keyword.get(:nodedown_reason) |> map_nodedown_reason()
  end

  defp nodedown_failure_class(_info), do: :node_down

  defp map_nodedown_reason(:connection_closed), do: :node_connection_closed
  defp map_nodedown_reason(:net_tick_timeout), do: :node_tick_timeout
  defp map_nodedown_reason(_reason), do: :node_down

  defp reconnect_delay(attempt) do
    ceiling = min(250 * round(:math.pow(2, attempt)), @max_retry_ms)
    max(1, div(ceiling, 2) + :rand.uniform(max(div(ceiling, 2), 1)))
  end

  defp parse_node(value) when is_atom(value), do: parse_node(Atom.to_string(value))

  defp parse_node(value)
       when is_binary(value) and byte_size(value) in 3..@max_node_bytes do
    with [local_name, host] <- String.split(value, "@", parts: 2),
         true <- Regex.match?(~r/^[A-Za-z0-9_.-]+$/, local_name),
         true <- fully_qualified_dns_host?(host) do
      {:ok, String.to_atom(value)}
    else
      _invalid -> {:error, :invalid_control_plane_node}
    end
  end

  defp parse_node(_value), do: {:error, :invalid_control_plane_node}

  defp fully_qualified_dns_host?(host) do
    labels = String.split(host, ".")

    length(labels) >= 2 and
      Enum.all?(labels, fn label ->
        byte_size(label) in 1..63 and
          Regex.match?(~r/^[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?$/, label)
      end) and
      not match?({:ok, _address}, :inet.parse_address(String.to_charlist(host)))
  end
end
