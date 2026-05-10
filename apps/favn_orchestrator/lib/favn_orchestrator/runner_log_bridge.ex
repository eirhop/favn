defmodule FavnOrchestrator.RunnerLogBridge do
  @moduledoc false

  alias FavnOrchestrator.LogWriter

  @spec start_link(module(), String.t(), keyword()) :: {:ok, pid()} | {:error, term()}
  def start_link(runner_client, execution_id, runner_opts)
      when is_atom(runner_client) and is_binary(execution_id) and is_list(runner_opts) do
    if function_exported?(runner_client, :subscribe_execution_logs, 3) do
      parent = self()

      pid =
        spawn_link(fn ->
          send(parent, {__MODULE__, self(), :ready})
          loop()
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

  defp loop do
    receive do
      {:runner_log_entry, _execution_id, entry} ->
        _ = LogWriter.write(entry)
        loop()

      :stop ->
        :ok
    end
  end
end
