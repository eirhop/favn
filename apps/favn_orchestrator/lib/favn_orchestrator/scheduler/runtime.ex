defmodule FavnOrchestrator.Scheduler.Runtime do
  @moduledoc """
  Client API for the PostgreSQL-backed multi-node scheduler runtime.

  The supervised server is `FavnOrchestrator.Scheduler.PersistenceRuntime` and
  registers under this module name. Keeping the client separate prevents the
  old node-local scheduler implementation from becoming an accidental fallback.
  """

  @default_call_timeout_ms 5_000

  @doc "Reloads deployed schedule state. Manifest activation already persists definitions."
  @spec reload(GenServer.server()) :: :ok | {:error, term()}
  def reload(server \\ __MODULE__), do: call(server, :reload)

  @doc "Forces one scheduler evaluation tick."
  @spec tick(GenServer.server()) :: :ok | {:error, term()}
  def tick(server \\ __MODULE__), do: call(server, :tick)

  @doc "Returns the runtime's compact scheduled-entry view."
  @spec scheduled(GenServer.server()) :: [map()] | {:error, term()}
  def scheduled(server \\ __MODULE__), do: call(server, :scheduled)

  @doc "Returns bounded scheduler diagnostics."
  @spec diagnostics(GenServer.server()) :: {:ok, map()} | {:error, term()}
  def diagnostics(server \\ __MODULE__), do: call(server, :diagnostics)

  @doc "Returns the runtime's compact schedule inspection view."
  @spec inspect_entries(GenServer.server()) :: [map()] | {:error, term()}
  def inspect_entries(server \\ __MODULE__), do: call(server, :inspect_entries)

  defp call(server, message) do
    GenServer.call(server, message, call_timeout_ms())
  catch
    :exit, {:timeout, _call} -> {:error, {:scheduler_call_timeout, message}}
    :exit, {:noproc, _call} -> {:error, :scheduler_not_running}
  end

  defp call_timeout_ms do
    case Application.get_env(
           :favn_orchestrator,
           :scheduler_call_timeout_ms,
           @default_call_timeout_ms
         ) do
      timeout when is_integer(timeout) and timeout > 0 -> timeout
      _invalid -> @default_call_timeout_ms
    end
  end
end
