defmodule FavnOrchestrator.ControlPlaneRuntimeConfig do
  @moduledoc """
  Loads the production control plane's environment exactly once before either
  the orchestrator or View supervision tree starts.

  Validation is side-effect free. The validated Orchestrator and View configs
  are then applied together, and only their redacted diagnostic summary is
  retained by this composition boundary.
  """

  alias FavnOrchestrator.ProductionRuntimeConfig

  @view_runtime_config Module.concat(["FavnView.ProductionRuntimeConfig"])
  @persistent_key {__MODULE__, :diagnostics}

  @type config :: %{orchestrator: ProductionRuntimeConfig.config(), view: map()}

  @doc "Loads and applies the unified process environment once when configured."
  @spec apply_from_env_if_configured() :: :ok | {:error, map()}
  def apply_from_env_if_configured do
    cond do
      Application.get_env(:favn_orchestrator, :local_dev_mode, false) ->
        :ok

      Application.get_env(:favn_orchestrator, :control_plane_runtime_config, false) ->
        load_process_environment_once()

      true ->
        :ok
    end
  end

  @doc "Loads an explicit environment map for tests and non-release composition roots."
  @spec apply_from_env_if_configured(map()) :: :ok | {:error, map()}
  def apply_from_env_if_configured(env) when is_map(env) do
    cond do
      Application.get_env(:favn_orchestrator, :local_dev_mode, false) ->
        :ok

      Application.get_env(:favn_orchestrator, :control_plane_runtime_config, false) ->
        load_once(env)

      true ->
        ProductionRuntimeConfig.apply_from_env_if_configured(env)
    end
  end

  defp load_process_environment_once do
    case :persistent_term.get(@persistent_key, :missing) do
      :missing -> load_once(System.get_env())
      _already_applied -> :ok
    end
  end

  defp load_once(env) do
    case :persistent_term.get(@persistent_key, :missing) do
      :missing ->
        with {:ok, config} <- validate(env) do
          apply(config)
        end

      _already_applied ->
        :ok
    end
  end

  @doc "Validates both halves of the control-plane environment without mutation."
  @spec validate(map()) :: {:ok, config()} | {:error, map()}
  def validate(env) when is_map(env) do
    orchestrator = ProductionRuntimeConfig.validate(env)
    view = validate_view(env)

    case {orchestrator, view} do
      {{:ok, orchestrator_config}, {:ok, view_config}} ->
        {:ok, %{orchestrator: orchestrator_config, view: view_config}}

      _invalid ->
        {:error,
         %{
           status: :invalid,
           errors:
             %{}
             |> maybe_put_error(:orchestrator, orchestrator)
             |> maybe_put_error(:view, view)
         }}
    end
  end

  @doc "Applies an already validated unified control-plane configuration."
  @spec apply(config()) :: :ok
  def apply(%{orchestrator: orchestrator, view: view}) do
    :ok = ProductionRuntimeConfig.apply(orchestrator)
    :ok = Kernel.apply(@view_runtime_config, :apply, [view])

    :persistent_term.put(@persistent_key, %{
      status: :ok,
      orchestrator: ProductionRuntimeConfig.diagnostics(orchestrator),
      view: Kernel.apply(@view_runtime_config, :diagnostics, [view])
    })

    :ok
  end

  @doc "Confirms that the unified loader ran before Favn View starts."
  @spec ensure_applied() :: :ok | {:error, :control_plane_runtime_config_not_applied}
  def ensure_applied do
    if Application.get_env(:favn_orchestrator, :control_plane_runtime_config, false) do
      case :persistent_term.get(@persistent_key, :missing) do
        :missing -> {:error, :control_plane_runtime_config_not_applied}
        _diagnostics -> :ok
      end
    else
      :ok
    end
  end

  @doc "Returns the immutable redacted boot diagnostic summary."
  @spec diagnostics() :: map() | nil
  def diagnostics do
    case :persistent_term.get(@persistent_key, :missing) do
      :missing -> nil
      diagnostics -> diagnostics
    end
  end

  defp validate_view(env) do
    if Code.ensure_loaded?(@view_runtime_config) and
         function_exported?(@view_runtime_config, :validate, 1) do
      Kernel.apply(@view_runtime_config, :validate, [env])
    else
      {:error, %{status: :invalid, error: :view_runtime_config_unavailable}}
    end
  end

  defp maybe_put_error(errors, _component, {:ok, _config}), do: errors

  defp maybe_put_error(errors, component, {:error, %{status: :invalid} = error}),
    do: Map.put(errors, component, Map.delete(error, :status))

  defp maybe_put_error(errors, component, {:error, _reason}),
    do: Map.put(errors, component, %{error: :invalid_runtime_config})
end
