defmodule Favn.ModuleDiscovery do
  @moduledoc """
  Discovers Favn-authored modules from compiled OTP application metadata.

  Discovery is intentionally app-scoped. It reads the module list from each
  configured OTP application instead of scanning source files, which keeps the
  behavior compatible with releases and deterministic for manifest generation.

  ## Config

      config :favn,
        discovery: [
          apps: [:my_app],
          assets: :all,
          pipelines: :all,
          schedules: :all,
          connections: :all
        ]

  Explicit `asset_modules`, `pipeline_modules`, `schedule_modules`, or
  `connection_modules` lists can still be used when a project needs tighter
  control than app-wide discovery.
  """

  alias Favn.Assets.Compiler

  @type kind :: :assets | :pipelines | :schedules | :connections
  @type error ::
          {:invalid_discovery_config, term()}
          | {:app_modules_unavailable, atom(), term()}
          | {:asset_discovery_failed, module(), term()}

  @doc """
  Discovers modules of `kind` from configured application modules.
  """
  @spec discover(kind(), keyword()) :: {:ok, [module()]} | {:error, error()}
  def discover(kind, config) when kind in [:assets, :pipelines, :schedules, :connections] do
    with {:ok, apps} <- configured_apps(config),
         {:ok, modules} <- app_modules(apps) do
      discover_modules(modules, kind)
    end
  end

  def discover(_kind, config), do: {:error, {:invalid_discovery_config, config}}

  @doc """
  Returns all modules declared by the configured OTP applications.
  """
  @spec app_modules([atom()]) :: {:ok, [module()]} | {:error, error()}
  def app_modules(apps) when is_list(apps) do
    Enum.reduce_while(apps, {:ok, []}, fn app, {:ok, acc} ->
      case fetch_app_modules(app) do
        {:ok, modules} -> {:cont, {:ok, modules ++ acc}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, modules} -> {:ok, modules |> Enum.uniq() |> sort_modules()}
      {:error, _reason} = error -> error
    end
  end

  def app_modules(_apps), do: {:error, {:invalid_discovery_config, :apps}}

  defp configured_apps(config) when is_list(config) do
    case Keyword.get(config, :apps, []) do
      apps when is_list(apps) ->
        if apps != [] and Enum.all?(apps, &is_atom/1) do
          {:ok, apps}
        else
          {:error, {:invalid_discovery_config, {:apps, apps}}}
        end

      other ->
        {:error, {:invalid_discovery_config, {:apps, other}}}
    end
  end

  defp configured_apps(config), do: {:error, {:invalid_discovery_config, config}}

  defp fetch_app_modules(app) when is_atom(app) do
    with :ok <- :application.load(app),
         {:ok, modules} <- :application.get_key(app, :modules) do
      {:ok, modules}
    else
      {:error, {:already_loaded, ^app}} ->
        case :application.get_key(app, :modules) do
          {:ok, modules} -> {:ok, modules}
          error -> {:error, {:app_modules_unavailable, app, error}}
        end

      error ->
        {:error, {:app_modules_unavailable, app, error}}
    end
  end

  defp discover_modules(modules, kind) do
    Enum.reduce_while(modules, {:ok, []}, fn module, {:ok, acc} ->
      case classify_module(module, kind) do
        {:match, module} -> {:cont, {:ok, [module | acc]}}
        :no_match -> {:cont, {:ok, acc}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, modules} -> {:ok, modules |> Enum.uniq() |> sort_modules()}
      {:error, _reason} = error -> error
    end
  end

  defp classify_module(module, :assets) do
    case Compiler.compile_module_assets(module) do
      {:ok, _assets} -> {:match, module}
      {:error, :not_asset_module} -> :no_match
      {:error, reason} -> {:error, {:asset_discovery_failed, module, reason}}
    end
  end

  defp classify_module(module, :pipelines) do
    if Code.ensure_loaded?(module) and function_exported?(module, :__favn_pipeline__, 0) do
      {:match, module}
    else
      :no_match
    end
  end

  defp classify_module(module, :schedules) do
    if Code.ensure_loaded?(module) and function_exported?(module, :__favn_schedules__, 0) do
      {:match, module}
    else
      :no_match
    end
  end

  defp classify_module(module, :connections) do
    if Code.ensure_loaded?(module) and function_exported?(module, :definition, 0) and
         Favn.Connection in module_behaviours(module) do
      {:match, module}
    else
      :no_match
    end
  end

  defp module_behaviours(module) do
    module.module_info(:attributes)
    |> Keyword.get(:behaviour, [])
    |> List.flatten()
  rescue
    _ -> []
  end

  defp sort_modules(modules) do
    Enum.sort_by(modules, &Atom.to_string/1)
  end
end
