defmodule FavnOrchestrator.Persistence do
  @moduledoc """
  Internal lifecycle and capability entrypoint for control-plane persistence.

  Domain code receives typed commands and invokes one capability store. Database
  rows, Ecto, and concrete backend modules never cross this boundary.
  """

  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Runtime
  alias FavnOrchestrator.Persistence.Stores

  @doc "Returns backend children after validating the boot-frozen composition."
  @spec child_specs(Runtime.t()) ::
          {:ok, [Supervisor.child_spec()]} | {:error, Error.t()}
  def child_specs(%Runtime{} = runtime) do
    case runtime.backend.child_specs(runtime.options) do
      {:ok, children} when is_list(children) -> normalize_child_specs(children)
      {:error, %Error{} = error} -> {:error, error}
      _invalid -> {:error, Error.new(:invalid, "persistence backend returned invalid children")}
    end
  rescue
    error ->
      {:error,
       Error.new(:invalid, "persistence backend startup failed",
         details: %{exception: error.__struct__}
       )}
  end

  @doc "Returns the validated capability store registry."
  @spec stores() :: Stores.t()
  def stores, do: Runtime.stores()

  @doc "Runs the backend readiness probe."
  @spec readiness() :: {:ok, FavnOrchestrator.Persistence.Readiness.t()} | {:error, Error.t()}
  def readiness do
    runtime = Runtime.current()
    runtime.backend.readiness(runtime.options)
  end

  @doc "Returns redacted backend diagnostics."
  @spec diagnostics() ::
          {:ok, FavnOrchestrator.Persistence.Diagnostics.t()} | {:error, Error.t()}
  def diagnostics do
    runtime = Runtime.current()
    runtime.backend.diagnostics(runtime.options)
  end

  defp normalize_child_specs(children) do
    {:ok, Enum.map(children, &Supervisor.child_spec(&1, []))}
  rescue
    _error -> {:error, Error.new(:invalid, "persistence backend returned invalid children")}
  end
end
