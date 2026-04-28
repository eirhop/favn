defmodule Favn.Dev.Doctor do
  @moduledoc """
  Local project setup validation for Favn development workflows.
  """

  alias Favn.Dev.Paths

  @type check :: %{name: String.t(), status: :ok | :error, message: String.t()}

  @spec run(keyword()) :: {:ok, [check()]} | {:error, [check()]}
  def run(opts) when is_list(opts) do
    root_dir = Paths.root_dir(opts) |> Path.expand()

    checks = [
      mix_project_check(root_dir),
      config_file_check(root_dir),
      config_key_check(:asset_modules),
      config_key_check(:pipeline_modules),
      config_key_check(:connection_modules),
      config_key_check(:connections),
      config_key_check(:runner_plugins),
      modules_check(:asset_modules),
      modules_check(:pipeline_modules),
      modules_check(:connection_modules),
      runner_plugins_check(),
      connection_runtime_check(),
      manifest_check()
    ]

    if Enum.all?(checks, &(&1.status == :ok)) do
      {:ok, checks}
    else
      {:error, checks}
    end
  end

  defp mix_project_check(root_dir) do
    path_check("mix project", Path.join(root_dir, "mix.exs"), "mix.exs found")
  end

  defp config_file_check(root_dir) do
    path_check("config", Path.join([root_dir, "config", "config.exs"]), "config/config.exs found")
  end

  defp path_check(name, path, ok_message) do
    if File.exists?(path) do
      ok(name, ok_message)
    else
      error(name, "missing #{Path.basename(path)}")
    end
  end

  defp config_key_check(key) do
    case Application.get_env(:favn, key) do
      value when is_list(value) and value != [] -> ok(to_string(key), "configured")
      _other -> error(to_string(key), "missing or empty config :favn, #{key}")
    end
  end

  defp modules_check(key) do
    modules = Application.get_env(:favn, key, [])
    missing = Enum.reject(modules, &Code.ensure_loaded?/1)

    case missing do
      [] -> ok(to_string(key) <> " loaded", "#{length(modules)} module(s) available")
      modules -> error(to_string(key) <> " loaded", "not loaded: #{inspect(modules)}")
    end
  end

  defp runner_plugins_check do
    plugins = Application.get_env(:favn, :runner_plugins, [])

    missing =
      plugins
      |> Enum.map(&plugin_module/1)
      |> Enum.reject(&Code.ensure_loaded?/1)

    case missing do
      [] -> ok("runner plugins loaded", "#{length(plugins)} plugin(s) available")
      modules -> error("runner plugins loaded", "not loaded: #{inspect(modules)}")
    end
  end

  defp connection_runtime_check do
    modules = Application.get_env(:favn, :connection_modules, [])
    runtime = Application.get_env(:favn, :connections, [])
    runtime_names = runtime |> Keyword.keys() |> MapSet.new()

    missing =
      modules
      |> Enum.flat_map(fn module ->
        if Code.ensure_loaded?(module) and function_exported?(module, :definition, 0) do
          definition = module.definition()
          if MapSet.member?(runtime_names, definition.name), do: [], else: [definition.name]
        else
          []
        end
      end)

    case missing do
      [] -> ok("connection runtime", "runtime config exists for connection definitions")
      names -> error("connection runtime", "missing runtime config for #{inspect(names)}")
    end
  end

  defp manifest_check do
    case FavnAuthoring.generate_manifest() do
      {:ok, manifest} ->
        ok(
          "manifest",
          "compiled #{length(manifest.assets)} asset(s) and #{length(manifest.pipelines)} pipeline(s)"
        )

      {:error, reason} ->
        error("manifest", "generation failed: #{inspect(reason)}")
    end
  end

  defp plugin_module({module, _opts}) when is_atom(module), do: module
  defp plugin_module(module) when is_atom(module), do: module
  defp plugin_module(other), do: other

  defp ok(name, message), do: %{name: name, status: :ok, message: message}
  defp error(name, message), do: %{name: name, status: :error, message: message}
end
