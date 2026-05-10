defmodule FavnRunner.LogSink do
  @moduledoc false

  @spec emit(pid(), String.t(), map()) :: :ok
  def emit(server_pid, execution_id, attrs)
      when is_pid(server_pid) and is_binary(execution_id) and is_map(attrs) do
    send(server_pid, {:runner_log_entry, execution_id, build_entry(attrs)})
    :ok
  end

  defp build_entry(attrs) do
    case Code.ensure_loaded(Favn.Log.Entry) do
      {:module, Favn.Log.Entry} -> struct(Favn.Log.Entry, attrs)
      _other -> attrs
    end
  end
end
