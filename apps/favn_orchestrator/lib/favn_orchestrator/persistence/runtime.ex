defmodule FavnOrchestrator.Persistence.Runtime do
  @moduledoc """
  Immutable, supervised persistence composition for one orchestrator node.

  The concrete backend is selected once at boot by the deployment composition
  root. Capability modules are validated before any request handler or worker is
  started and are then published through `:persistent_term` for read-only access.
  """

  use GenServer

  alias FavnOrchestrator.Persistence.Backend
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Stores

  @backend_callbacks Backend.behaviour_info(:callbacks)

  @enforce_keys [:backend, :options, :stores]
  defstruct [:backend, :options, :stores]

  @type t :: %__MODULE__{
          backend: module(),
          options: keyword(),
          stores: Stores.t()
        }

  @doc "Builds and validates the persistence composition from application config."
  @spec from_app_env() :: {:ok, t()} | {:error, Error.t()}
  def from_app_env do
    backend = Application.get_env(:favn_orchestrator, :persistence_backend)
    options = Application.get_env(:favn_orchestrator, :persistence_options, [])
    new(backend, options)
  end

  @doc "Builds and validates an explicit persistence composition."
  @spec new(module(), keyword()) :: {:ok, t()} | {:error, Error.t()}
  def new(backend, options) do
    with :ok <- validate_backend(backend),
         :ok <- validate_options(options),
         %Stores{} = stores <- backend.stores(),
         :ok <- validate_stores(stores) do
      {:ok, %__MODULE__{backend: backend, options: options, stores: stores}}
    else
      {:error, %Error{} = error} -> {:error, error}
      _invalid -> {:error, invalid_configuration("backend returned an invalid store registry")}
    end
  rescue
    error ->
      {:error,
       Error.new(:invalid, "persistence backend configuration failed",
         details: %{exception: error.__struct__}
       )}
  end

  @doc "Builds a composition or raises during boot with a redacted error."
  @spec from_app_env!() :: t()
  def from_app_env! do
    case from_app_env() do
      {:ok, runtime} -> runtime
      {:error, error} -> raise ArgumentError, error.message
    end
  end

  @doc "Starts the immutable runtime registry."
  @spec start_link(t() | keyword()) :: GenServer.on_start()
  def start_link(runtime_or_options)

  def start_link(%__MODULE__{} = runtime) do
    GenServer.start_link(__MODULE__, {runtime, __MODULE__}, name: __MODULE__)
  end

  def start_link(options) when is_list(options) do
    runtime = Keyword.get_lazy(options, :runtime, &from_app_env!/0)
    name = Keyword.get(options, :name, __MODULE__)
    GenServer.start_link(__MODULE__, {runtime, name}, name: name)
  end

  @doc "Returns the boot-frozen persistence composition."
  @spec current(atom()) :: t()
  def current(name \\ __MODULE__) do
    case :persistent_term.get(persistent_key(name), :missing) do
      %__MODULE__{} = runtime -> runtime
      :missing -> from_app_env!()
    end
  end

  @doc "Returns the validated capability registry."
  @spec stores(atom()) :: Stores.t()
  def stores(name \\ __MODULE__), do: current(name).stores

  @impl true
  def init({%__MODULE__{} = runtime, name}) do
    :persistent_term.put(persistent_key(name), runtime)
    {:ok, %{name: name, runtime: runtime}}
  end

  @impl true
  def terminate(_reason, state) do
    :persistent_term.erase(persistent_key(state.name))
    :ok
  end

  defp validate_backend(backend) when is_atom(backend) do
    cond do
      is_nil(backend) ->
        {:error, invalid_configuration("persistence backend is required")}

      not Code.ensure_loaded?(backend) ->
        {:error, invalid_configuration("persistence backend is unavailable")}

      missing =
          Enum.find(@backend_callbacks, fn {operation, arity} ->
            not function_exported?(backend, operation, arity)
          end) ->
        {operation, arity} = missing

        {:error,
         invalid_configuration("persistence backend is incomplete",
           missing_callback: %{operation: operation, arity: arity}
         )}

      true ->
        :ok
    end
  end

  defp validate_backend(_backend),
    do: {:error, invalid_configuration("persistence backend is required")}

  defp validate_options(options) when is_list(options) do
    if Keyword.keyword?(options),
      do: :ok,
      else: {:error, invalid_configuration("persistence options must be a keyword list")}
  end

  defp validate_options(_options),
    do: {:error, invalid_configuration("persistence options must be a keyword list")}

  defp validate_stores(stores) do
    case Stores.validate(stores) do
      :ok ->
        :ok

      {:error, reason} ->
        {:error, invalid_configuration("persistence stores are incomplete", reason: reason)}
    end
  end

  defp invalid_configuration(message, details \\ []) do
    Error.new(:invalid, message, details: Map.new(details))
  end

  defp persistent_key(name), do: {__MODULE__, name}
end
