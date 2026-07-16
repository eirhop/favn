defmodule FavnRunner.PluginLoader do
  @moduledoc false

  @max_plugins 64
  @max_applications_per_plugin 16
  @max_children 256
  @callback_timeout 5_000
  @application_start_timeout 30_000
  @max_plugin_options_bytes 1_048_576
  @max_child_spec_bytes 1_048_576

  @type reason ::
          :invalid_runner_plugins
          | :too_many_runner_plugins
          | :too_many_plugin_children
          | {:invalid_runner_plugin_entry, non_neg_integer()}
          | {:invalid_runner_plugin, module()}
          | {:plugin_options_too_large, module()}
          | {:plugin_callback_failed, module(), atom()}
          | {:invalid_plugin_result, module()}
          | {:invalid_plugin_applications, module()}
          | {:plugin_application_callback_failed, module(), atom()}
          | {:plugin_application_start_failed, module(), atom()}
          | {:invalid_plugin_child_spec, module(), non_neg_integer()}
          | :duplicate_plugin_child_id

  @spec load(term()) :: {:ok, [Supervisor.child_spec()]} | {:error, reason()}
  def load(entries) when is_list(entries) do
    with :ok <- validate_plugin_count(entries),
         {:ok, plugins} <- normalize_entries(entries),
         {:ok, children} <- load_plugins(plugins),
         :ok <- validate_unique_ids(children) do
      {:ok, children}
    end
  end

  def load(_entries), do: {:error, :invalid_runner_plugins}

  @spec format_error(reason()) :: String.t()
  def format_error(reason), do: "invalid runner plugin configuration: #{inspect(reason)}"

  defp validate_plugin_count(entries) do
    case bounded_list_count(entries, @max_plugins) do
      {:ok, _count} -> :ok
      :too_many -> {:error, :too_many_runner_plugins}
    end
  end

  defp normalize_entries(entries) do
    entries
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, []}, fn {entry, index}, {:ok, acc} ->
      case normalize_entry(entry, index) do
        {:ok, plugin} -> {:cont, {:ok, [plugin | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> reverse_ok()
  end

  defp normalize_entry(module, _index) when is_atom(module), do: validate_plugin(module, [])

  defp normalize_entry({module, opts}, _index)
       when is_atom(module) and is_list(opts),
       do: validate_plugin(module, opts)

  defp normalize_entry(_entry, index), do: {:error, {:invalid_runner_plugin_entry, index}}

  defp validate_plugin(module, opts) do
    with {:module, ^module} <- Code.ensure_loaded(module),
         true <- implements_plugin?(module),
         true <- function_exported?(module, :child_specs, 1),
         true <- Keyword.keyword?(opts) do
      if bounded_term?(opts, @max_plugin_options_bytes) do
        {:ok, {module, opts}}
      else
        {:error, {:plugin_options_too_large, module}}
      end
    else
      _other -> {:error, {:invalid_runner_plugin, module}}
    end
  end

  defp implements_plugin?(module) do
    module.module_info(:attributes)
    |> Keyword.get_values(:behaviour)
    |> List.flatten()
    |> Enum.member?(Favn.Runner.Plugin)
  end

  defp load_plugins(plugins) do
    plugins
    |> Enum.reduce_while({:ok, [], 0}, fn {plugin, opts}, {:ok, acc, count} ->
      with {:ok, applications} <- load_applications(plugin, opts),
           :ok <- start_applications(plugin, applications),
           {:ok, children} <- load_children(plugin, opts),
           {:ok, child_count} <- remaining_child_count(children, count) do
        {:cont, {:ok, Enum.reverse(children, acc), count + child_count}}
      else
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, children, _count} -> {:ok, Enum.reverse(children)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp load_applications(plugin, opts) do
    if function_exported?(plugin, :applications, 1) do
      case invoke(plugin, :applications, opts) do
        {:ok, applications} ->
          {:ok, applications}

        {:returned_error, reason} ->
          {:error, {:plugin_application_callback_failed, plugin, safe_callback_reason(reason)}}

        {:callback_failure, kind} ->
          {:error, {:plugin_application_callback_failed, plugin, kind}}

        :invalid_result ->
          {:error, {:invalid_plugin_applications, plugin}}
      end
    else
      {:ok, []}
    end
  end

  defp start_applications(plugin, applications) do
    Enum.reduce_while(applications, :ok, fn application, :ok ->
      case bounded_call(
             fn -> Application.ensure_all_started(application) end,
             @application_start_timeout
           ) do
        {:ok, {:ok, _started}} -> {:cont, :ok}
        _other -> {:halt, {:error, {:plugin_application_start_failed, plugin, application}}}
      end
    end)
  end

  defp load_children(plugin, opts) do
    case invoke(plugin, :child_specs, opts) do
      {:ok, children} ->
        {:ok, children}

      {:error, reason} ->
        {:error, reason}

      {:returned_error, reason} ->
        {:error, {:plugin_callback_failed, plugin, safe_callback_reason(reason)}}

      {:callback_failure, kind} ->
        {:error, {:plugin_callback_failed, plugin, kind}}

      :invalid_result ->
        {:error, {:invalid_plugin_result, plugin}}
    end
  end

  defp invoke(plugin, callback, opts) do
    bounded_call(
      fn ->
        plugin
        |> invoke_callback(callback, opts)
        |> normalize_callback_result(plugin, callback)
      end,
      @callback_timeout
    )
    |> case do
      {:ok, result} -> result
      {:error, kind} -> {:callback_failure, kind}
    end
  end

  defp invoke_callback(plugin, :applications, opts), do: plugin.applications(opts)
  defp invoke_callback(plugin, :child_specs, opts), do: plugin.child_specs(opts)

  defp normalize_callback_result({:ok, applications}, _plugin, :applications)
       when is_list(applications) do
    with {:ok, _count} <- bounded_list_count(applications, @max_applications_per_plugin),
         true <- Enum.all?(applications, &(is_atom(&1) and not is_nil(&1))) do
      {:ok, Enum.uniq(applications)}
    else
      _other -> :invalid_result
    end
  end

  defp normalize_callback_result({:ok, specs}, plugin, :child_specs) when is_list(specs) do
    with {:ok, _count} <- bounded_list_count(specs, @max_children),
         {:ok, children} <- normalize_child_specs(plugin, specs) do
      {:ok, children}
    else
      :too_many -> {:error, :too_many_plugin_children}
      {:error, reason} -> {:error, reason}
    end
  end

  defp normalize_callback_result({:error, reason}, _plugin, _callback),
    do: {:returned_error, reason}

  defp normalize_callback_result(_result, _plugin, _callback), do: :invalid_result

  defp bounded_call(fun, timeout) do
    parent = self()
    result_ref = make_ref()

    {pid, monitor_ref} =
      spawn_monitor(fn ->
        result =
          try do
            {:ok, fun.()}
          rescue
            _error -> {:error, :raised}
          catch
            :exit, _reason -> {:error, :exited}
            _kind, _reason -> {:error, :threw}
          end

        send(parent, {result_ref, result})
      end)

    receive do
      {^result_ref, result} ->
        Process.demonitor(monitor_ref, [:flush])
        result

      {:DOWN, ^monitor_ref, :process, ^pid, _reason} ->
        {:error, :exited}
    after
      timeout ->
        Process.exit(pid, :kill)

        receive do
          {:DOWN, ^monitor_ref, :process, ^pid, _reason} -> :ok
        end

        {:error, :timeout}
    end
  end

  defp normalize_child_specs(plugin, specs) do
    specs
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, []}, fn {spec, index}, {:ok, acc} ->
      try do
        child = Supervisor.child_spec(spec, [])

        if valid_child_spec?(child) and bounded_term?(child, @max_child_spec_bytes) do
          {:cont, {:ok, [child | acc]}}
        else
          {:halt, {:error, {:invalid_plugin_child_spec, plugin, index}}}
        end
      rescue
        _error -> {:halt, {:error, {:invalid_plugin_child_spec, plugin, index}}}
      catch
        _kind, _reason -> {:halt, {:error, {:invalid_plugin_child_spec, plugin, index}}}
      end
    end)
    |> reverse_ok()
  end

  defp valid_child_spec?(%{id: _id, start: {module, function, args}})
       when is_atom(module) and is_atom(function) and is_list(args),
       do: true

  defp valid_child_spec?(_child), do: false

  defp bounded_term?(term, limit) do
    :erlang.external_size(term) <= limit
  rescue
    _error -> false
  end

  defp remaining_child_count(children, current_count) do
    case bounded_list_count(children, @max_children - current_count) do
      {:ok, count} -> {:ok, count}
      :too_many -> {:error, :too_many_plugin_children}
    end
  end

  defp bounded_list_count(list, limit), do: bounded_list_count(list, limit, 0)

  defp bounded_list_count([], _limit, count), do: {:ok, count}
  defp bounded_list_count([_item | _rest], limit, count) when count == limit, do: :too_many

  defp bounded_list_count([_item | rest], limit, count),
    do: bounded_list_count(rest, limit, count + 1)

  defp validate_unique_ids(children) do
    Enum.reduce_while(children, MapSet.new(), fn child, ids ->
      if MapSet.member?(ids, child.id) do
        {:halt, {:error, :duplicate_plugin_child_id}}
      else
        {:cont, MapSet.put(ids, child.id)}
      end
    end)
    |> case do
      %MapSet{} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp safe_callback_reason(reason) when is_atom(reason), do: reason
  defp safe_callback_reason(_reason), do: :returned_error

  defp reverse_ok({:ok, values}), do: {:ok, Enum.reverse(values)}
  defp reverse_ok({:error, reason}), do: {:error, reason}
end
