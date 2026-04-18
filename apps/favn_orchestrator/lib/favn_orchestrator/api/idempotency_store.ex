defmodule FavnOrchestrator.API.IdempotencyStore do
  @moduledoc false

  use GenServer

  @type response_entry :: %{
          required(:status) => pos_integer(),
          required(:body) => String.t(),
          required(:content_type) => String.t()
        }

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, %{}, name: name)
  end

  @spec fetch(String.t()) :: {:ok, response_entry()} | :not_found
  def fetch(scope_key) when is_binary(scope_key) do
    GenServer.call(__MODULE__, {:fetch, scope_key})
  end

  @spec put(String.t(), response_entry()) :: :ok
  def put(scope_key, response_entry) when is_binary(scope_key) and is_map(response_entry) do
    GenServer.call(__MODULE__, {:put, scope_key, response_entry})
  end

  @spec reset() :: :ok
  def reset do
    GenServer.call(__MODULE__, :reset)
  end

  @impl true
  def init(_state) do
    {:ok, %{entries: %{}}}
  end

  @impl true
  def handle_call({:fetch, scope_key}, _from, state) do
    now = DateTime.utc_now()
    entries = prune_expired(state.entries, now)

    reply =
      case Map.get(entries, scope_key) do
        nil -> :not_found
        %{response: response_entry} -> {:ok, response_entry}
      end

    {:reply, reply, %{state | entries: entries}}
  end

  def handle_call({:put, scope_key, response_entry}, _from, state) do
    now = DateTime.utc_now()
    ttl_seconds = idempotency_ttl_seconds()
    expires_at = DateTime.add(now, ttl_seconds, :second)

    entries =
      state.entries
      |> prune_expired(now)
      |> Map.put(scope_key, %{expires_at: expires_at, response: response_entry})

    {:reply, :ok, %{state | entries: entries}}
  end

  def handle_call(:reset, _from, _state) do
    {:reply, :ok, %{entries: %{}}}
  end

  defp prune_expired(entries, now) do
    Enum.reduce(entries, %{}, fn {key, value}, acc ->
      case DateTime.compare(value.expires_at, now) do
        :gt -> Map.put(acc, key, value)
        _ -> acc
      end
    end)
  end

  defp idempotency_ttl_seconds do
    Application.get_env(:favn_orchestrator, :api_idempotency_ttl_seconds, 86_400)
  end
end
