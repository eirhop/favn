defmodule Favn.Azure.ControlPlanePostgresAuth.Server do
  @moduledoc false

  use GenServer

  alias Favn.Azure.Credentials.Cache

  @enforce_keys [
    :auth,
    :cache_name,
    :minimum_validity_seconds,
    :fetch_timeout,
    :provider_options,
    :clock,
    :user_assigned?
  ]
  defstruct @enforce_keys ++
              [
                successful_password_deliveries: 0,
                last_password_delivery_monotonic_ms: nil,
                last_failure_class: nil
              ]

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(options) do
    {name, options} = Keyword.pop(options, :name)
    GenServer.start_link(__MODULE__, options, name: name)
  end

  @impl true
  def init(options) do
    client_id = Keyword.get(options, :client_id)

    auth =
      [
        provider: Keyword.fetch!(options, :credential_provider),
        endpoint: Keyword.fetch!(options, :endpoint)
      ]
      |> maybe_put(:client_id, client_id)

    {:ok,
     %__MODULE__{
       auth: auth,
       cache_name: Keyword.fetch!(options, :cache_name),
       minimum_validity_seconds: Keyword.fetch!(options, :minimum_validity_seconds),
       fetch_timeout: Keyword.fetch!(options, :fetch_timeout),
       provider_options: Keyword.fetch!(options, :provider_options),
       clock: Keyword.fetch!(options, :clock),
       user_assigned?: not is_nil(client_id)
     }}
  end

  @impl true
  def handle_call(:credential_config, _from, state) do
    config = %{
      auth: state.auth,
      cache_name: state.cache_name,
      minimum_validity_seconds: state.minimum_validity_seconds,
      fetch_timeout: state.fetch_timeout,
      provider_options: state.provider_options,
      clock: state.clock,
      user_assigned?: state.user_assigned?
    }

    {:reply, {:ok, config}, state}
  end

  def handle_call(:status, _from, state) do
    cache_status = Cache.status(state.cache_name)

    status =
      state
      |> local_status()
      |> Map.put(:lifecycle_ready?, Map.get(cache_status, :available?, false))
      |> Map.merge(Map.delete(cache_status, :available?))

    {:reply, status, state}
  end

  def handle_call({:password_delivery, delivery}, _from, state) do
    {:reply, :ok, record_delivery(state, delivery)}
  end

  @impl true
  def handle_cast({:password_delivery, :ok}, state) do
    {:noreply, record_delivery(state, :ok)}
  end

  def handle_cast({:password_delivery, {:error, class}}, state) when is_atom(class) do
    {:noreply, record_delivery(state, {:error, class})}
  end

  @impl true
  def format_status(_reason, [_process_dictionary, state]), do: local_status(state)

  defp local_status(state) do
    %{
      lifecycle_ready?: alive?(state.cache_name),
      user_assigned?: state.user_assigned?,
      successful_password_deliveries: state.successful_password_deliveries,
      last_password_delivery_monotonic_ms: state.last_password_delivery_monotonic_ms,
      last_failure_class: state.last_failure_class
    }
  end

  defp record_delivery(state, :ok) do
    %{
      state
      | successful_password_deliveries: state.successful_password_deliveries + 1,
        last_password_delivery_monotonic_ms: System.monotonic_time(:millisecond),
        last_failure_class: nil
    }
  end

  defp record_delivery(state, {:error, class}) when is_atom(class),
    do: %{state | last_failure_class: class}

  defp alive?(pid) when is_pid(pid), do: Process.alive?(pid)
  defp alive?(name), do: not is_nil(Process.whereis(name))

  defp maybe_put(options, _key, nil), do: options
  defp maybe_put(options, key, value), do: Keyword.put(options, key, value)
end
