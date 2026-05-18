defmodule FavnView.Auth.BrowserSessionStore do
  @moduledoc """
  Volatile server-side browser session mapping for operator LiveView auth.

  The Phoenix cookie session is signed but client-readable, so it stores only a
  random browser session id. This process keeps the raw orchestrator bearer token
  server-side for the current single-BEAM production target.
  """

  use GenServer

  @type browser_session_id :: String.t()
  @type entry :: %{
          required(:id) => browser_session_id(),
          required(:operator_session_token) => String.t(),
          required(:operator_session_id) => String.t(),
          required(:live_socket_id) => String.t(),
          required(:expires_at) => DateTime.t()
        }

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, %{}, name: name)
  end

  @doc """
  Stores one raw orchestrator token server-side and returns a browser session id.
  """
  @spec put(map()) :: {:ok, entry()}
  def put(session) when is_map(session) do
    GenServer.call(__MODULE__, {:put, session})
  end

  @doc """
  Fetches one browser session mapping when present and not expired.
  """
  @spec fetch(browser_session_id()) :: {:ok, entry()} | {:error, :not_found | :expired}
  def fetch(browser_session_id) when is_binary(browser_session_id) do
    GenServer.call(__MODULE__, {:fetch, browser_session_id})
  end

  @doc """
  Deletes one browser session mapping.
  """
  @spec delete(browser_session_id()) :: :ok
  def delete(browser_session_id) when is_binary(browser_session_id) do
    GenServer.call(__MODULE__, {:delete, browser_session_id})
  end

  @doc false
  @spec reset() :: :ok
  def reset, do: GenServer.call(__MODULE__, :reset)

  @impl true
  def init(_state), do: {:ok, %{}}

  @impl true
  def handle_call({:put, session}, _from, state) do
    id = random_id()
    operator_session_id = Map.fetch!(session, :id)

    entry = %{
      id: id,
      operator_session_token: Map.fetch!(session, :token),
      operator_session_id: operator_session_id,
      live_socket_id: live_socket_id(id),
      expires_at: Map.fetch!(session, :expires_at)
    }

    {:reply, {:ok, entry}, Map.put(state, id, entry)}
  end

  def handle_call({:fetch, browser_session_id}, _from, state) do
    case Map.fetch(state, browser_session_id) do
      {:ok, %{expires_at: %DateTime{} = expires_at} = entry} ->
        if DateTime.compare(expires_at, DateTime.utc_now()) == :lt do
          {:reply, {:error, :expired}, Map.delete(state, browser_session_id)}
        else
          {:reply, {:ok, entry}, state}
        end

      {:ok, _entry} ->
        {:reply, {:error, :expired}, Map.delete(state, browser_session_id)}

      :error ->
        {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call({:delete, browser_session_id}, _from, state) do
    {:reply, :ok, Map.delete(state, browser_session_id)}
  end

  def handle_call(:reset, _from, _state), do: {:reply, :ok, %{}}

  @spec live_socket_id(browser_session_id()) :: String.t()
  def live_socket_id(browser_session_id), do: "operator_browser_sessions:#{browser_session_id}"

  defp random_id do
    32
    |> :crypto.strong_rand_bytes()
    |> Base.url_encode64(padding: false)
  end
end
