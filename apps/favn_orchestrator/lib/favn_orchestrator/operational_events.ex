defmodule FavnOrchestrator.OperationalEvents do
  @moduledoc """
  Structured operational logging and the minimal metrics hook for the runtime.

  Set `config :favn_orchestrator, :metrics_hook, MyHook` to receive
  `handle_event(event, measurements, metadata)` calls. Hook failures are ignored
  after a redacted warning so metrics exporters cannot affect runtime behavior.
  """

  require Logger

  alias FavnOrchestrator.Redaction

  @type event :: atom()
  @type measurements :: map()
  @type metadata :: map()

  @doc """
  Emits one structured operational event to logs and the optional metrics hook.
  """
  @spec emit(event(), measurements(), metadata(), keyword()) :: :ok
  def emit(event, measurements \\ %{}, metadata \\ %{}, opts \\ [])
      when is_atom(event) and is_map(measurements) and is_map(metadata) and is_list(opts) do
    level = Keyword.get(opts, :level, :info)
    safe_measurements = Redaction.redact(measurements)
    safe_metadata = Redaction.redact(metadata)

    Logger.log(
      level,
      "favn.operator.#{event} measurements=#{inspect(safe_measurements)} metadata=#{inspect(safe_metadata)}"
    )

    emit_metrics_hook(event, safe_measurements, safe_metadata)
    :ok
  end

  defp emit_metrics_hook(event, measurements, metadata) do
    case Application.get_env(:favn_orchestrator, :metrics_hook) do
      hook when is_atom(hook) -> call_hook(hook, event, measurements, metadata)
      fun when is_function(fun, 3) -> call_hook(fun, event, measurements, metadata)
      _other -> :ok
    end
  end

  defp call_hook(hook, event, measurements, metadata) when is_atom(hook) do
    if function_exported?(hook, :handle_event, 3) do
      hook.handle_event(event, measurements, metadata)
    end
  rescue
    error -> Logger.warning("favn.operator.metrics_hook_failed error=#{inspect(error)}")
  catch
    kind, reason ->
      Logger.warning("favn.operator.metrics_hook_failed error=#{inspect({kind, reason})}")
  end

  defp call_hook(fun, event, measurements, metadata) when is_function(fun, 3) do
    fun.(event, measurements, metadata)
  rescue
    error -> Logger.warning("favn.operator.metrics_hook_failed error=#{inspect(error)}")
  catch
    kind, reason ->
      Logger.warning("favn.operator.metrics_hook_failed error=#{inspect({kind, reason})}")
  end
end
