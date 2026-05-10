defmodule FavnOrchestrator.RunnerLogBridge do
  @moduledoc false

  require Logger

  alias FavnOrchestrator.LogWriter

  @context_fields [
    :run_id,
    :asset_step_id,
    :node_key,
    :asset_ref,
    :runner_execution_id,
    :attempt
  ]

  @spec start(module(), String.t(), keyword(), map() | keyword()) ::
          {:ok, pid()} | {:error, term()}
  def start(runner_client, execution_id, runner_opts, context \\ %{})
      when is_atom(runner_client) and is_binary(execution_id) and is_list(runner_opts) do
    if function_exported?(runner_client, :subscribe_execution_logs, 3) do
      parent = self()
      context = normalize_context(context, execution_id)

      pid =
        spawn(fn ->
          parent_ref = Process.monitor(parent)
          send(parent, {__MODULE__, self(), :ready})
          loop(execution_id, context, parent_ref)
        end)

      receive do
        {__MODULE__, ^pid, :ready} -> :ok
      end

      case runner_client.subscribe_execution_logs(execution_id, pid, runner_opts) do
        :ok ->
          {:ok, pid}

        {:error, reason} ->
          send(pid, :stop)
          {:error, reason}
      end
    else
      {:error, :runner_log_subscription_not_supported}
    end
  end

  @spec stop(pid(), module(), String.t(), keyword()) :: :ok
  def stop(pid, runner_client, execution_id, runner_opts)
      when is_pid(pid) and is_atom(runner_client) and is_binary(execution_id) and
             is_list(runner_opts) do
    if function_exported?(runner_client, :unsubscribe_execution_logs, 3) do
      _ = runner_client.unsubscribe_execution_logs(execution_id, pid, runner_opts)
    end

    send(pid, :stop)
    :ok
  end

  defp loop(execution_id, context, parent_ref) do
    receive do
      {:runner_log_entry, ^execution_id, entry} ->
        safe_handle_entry(entry, context)

        loop(execution_id, context, parent_ref)

      {:runner_log_entry, _other_execution_id, _entry} ->
        loop(execution_id, context, parent_ref)

      {:DOWN, ^parent_ref, :process, _pid, _reason} ->
        :ok

      :stop ->
        :ok
    end
  end

  defp normalize_context(context, execution_id) when is_list(context) do
    context |> Map.new() |> normalize_context(execution_id)
  end

  defp normalize_context(context, execution_id) when is_map(context) do
    context
    |> atomize_known_keys()
    |> Map.put_new(:runner_execution_id, execution_id)
    |> Map.take(@context_fields)
  end

  defp normalize_context(_context, execution_id), do: %{runner_execution_id: execution_id}

  defp atomize_known_keys(map) do
    Enum.reduce(map, %{}, fn {key, value}, acc ->
      key = if is_binary(key), do: string_context_key(key), else: key
      Map.put(acc, key, value)
    end)
  end

  defp string_context_key(key),
    do: Enum.find(@context_fields, key, &(Atom.to_string(&1) == key)) || key

  defp merge_context(entry, context) do
    entry
    |> entry_to_map()
    |> then(fn attrs ->
      Enum.reduce(context, attrs, fn {key, value}, acc ->
        if missing?(Map.get(acc, key)), do: Map.put(acc, key, value), else: acc
      end)
    end)
  end

  defp entry_to_map(%_{} = entry), do: Map.from_struct(entry)
  defp entry_to_map(entry) when is_list(entry), do: Map.new(entry)
  defp entry_to_map(entry) when is_map(entry), do: entry
  defp entry_to_map(entry), do: %{message: inspect(entry), source: :runner, level: :warning}

  defp missing?(nil), do: true
  defp missing?(""), do: true
  defp missing?(_value), do: false

  defp safe_handle_entry(entry, context) do
    entry
    |> merge_context(context)
    |> safe_write()
  rescue
    error -> log_ignored_entry(error)
  catch
    kind, reason -> log_ignored_entry({kind, reason})
  end

  defp safe_write(entry) do
    case LogWriter.write(entry) do
      {:ok, _entries} -> :ok
      {:error, reason} -> log_ignored_entry(reason)
    end
  rescue
    error -> log_ignored_entry(error)
  catch
    kind, reason -> log_ignored_entry({kind, reason})
  end

  defp log_ignored_entry(reason) do
    Logger.warning("ignored malformed runner log entry: #{inspect(reason)}")
    :ok
  end
end
