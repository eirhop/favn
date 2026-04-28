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
      safe_check("mix project", fn -> mix_project_check(root_dir) end),
      safe_check("config", fn -> config_file_check(root_dir) end),
      safe_check("asset_modules", fn -> config_key_check(:asset_modules, :module_list) end),
      safe_check("pipeline_modules", fn -> config_key_check(:pipeline_modules, :module_list) end),
      safe_check("connection_modules", fn ->
        config_key_check(:connection_modules, :module_list)
      end),
      safe_check("connections", fn -> config_key_check(:connections, :keyword) end),
      safe_check("runner_plugins", fn -> config_key_check(:runner_plugins, :plugin_list) end),
      safe_check("asset_modules loaded", fn -> modules_check(:asset_modules) end),
      safe_check("pipeline_modules loaded", fn -> modules_check(:pipeline_modules) end),
      safe_check("connection_modules loaded", fn -> modules_check(:connection_modules) end),
      safe_check("runner plugins loaded", fn -> runner_plugins_check() end),
      safe_check("connection runtime", fn -> connection_runtime_check() end),
      safe_check("manifest", fn -> manifest_check() end)
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

  defp config_key_check(key, shape) do
    case Application.get_env(:favn, key) do
      value when is_list(value) and value != [] ->
        validate_config_shape(key, value, shape)

      _other ->
        error(to_string(key), "missing or empty config :favn, #{key}")
    end
  end

  defp validate_config_shape(key, value, :module_list) do
    if Enum.all?(value, &is_atom/1) do
      ok(to_string(key), "configured")
    else
      error(to_string(key), "expected a non-empty list of modules")
    end
  end

  defp validate_config_shape(key, value, :keyword) do
    if Keyword.keyword?(value) do
      ok(to_string(key), "configured")
    else
      error(to_string(key), "expected a non-empty keyword list")
    end
  end

  defp validate_config_shape(key, value, :plugin_list) do
    if Enum.all?(value, &valid_plugin_entry?/1) do
      ok(to_string(key), "configured")
    else
      error(to_string(key), "expected plugin modules or {module, keyword_opts} entries")
    end
  end

  defp modules_check(key) do
    case fetch_module_list(key) do
      {:ok, modules} ->
        missing = Enum.reject(modules, &Code.ensure_loaded?/1)

        case missing do
          [] -> ok(to_string(key) <> " loaded", "#{length(modules)} module(s) available")
          modules -> error(to_string(key) <> " loaded", "not loaded: #{inspect(modules)}")
        end

      {:error, message} ->
        error(to_string(key) <> " loaded", message)
    end
  end

  defp runner_plugins_check do
    case fetch_plugin_list() do
      {:ok, plugins} ->
        missing =
          plugins
          |> Enum.map(&plugin_module/1)
          |> Enum.reject(&Code.ensure_loaded?/1)

        case missing do
          [] -> ok("runner plugins loaded", "#{length(plugins)} plugin(s) available")
          modules -> error("runner plugins loaded", "not loaded: #{inspect(modules)}")
        end

      {:error, message} ->
        error("runner plugins loaded", message)
    end
  end

  defp connection_runtime_check do
    with {:ok, modules} <- fetch_module_list(:connection_modules),
         {:ok, runtime} <- fetch_keyword(:connections) do
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
    else
      {:error, message} -> error("connection runtime", message)
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

  defp valid_plugin_entry?(module) when is_atom(module), do: true

  defp valid_plugin_entry?({module, opts}) when is_atom(module) and is_list(opts),
    do: Keyword.keyword?(opts)

  defp valid_plugin_entry?(_other), do: false

  defp fetch_module_list(key) do
    case Application.get_env(:favn, key, []) do
      modules when is_list(modules) ->
        if Enum.all?(modules, &is_atom/1), do: {:ok, modules}, else: {:error, "expected modules"}

      _other ->
        {:error, "expected a list"}
    end
  end

  defp fetch_keyword(key) do
    case Application.get_env(:favn, key, []) do
      values when is_list(values) ->
        if Keyword.keyword?(values), do: {:ok, values}, else: {:error, "expected a keyword list"}

      _other ->
        {:error, "expected a keyword list"}
    end
  end

  defp fetch_plugin_list do
    case Application.get_env(:favn, :runner_plugins, []) do
      plugins when is_list(plugins) ->
        if Enum.all?(plugins, &valid_plugin_entry?/1),
          do: {:ok, plugins},
          else: {:error, "expected plugin entries"}

      _other ->
        {:error, "expected a plugin list"}
    end
  end

  defp safe_check(name, fun) when is_function(fun, 0) do
    fun.()
  rescue
    error -> error(name, Exception.message(error))
  catch
    kind, reason -> error(name, "#{kind}: #{inspect(reason)}")
  end

  defp ok(name, message), do: %{name: name, status: :ok, message: message}
  defp error(name, message), do: %{name: name, status: :error, message: message}
end
