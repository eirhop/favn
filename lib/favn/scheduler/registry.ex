defmodule Favn.Scheduler.Registry do
  @moduledoc """
  Scheduler pipeline discovery and normalization.
  """

  alias Favn.Pipeline
  alias Favn.Pipeline.Resolver
  alias Favn.Triggers.Schedule

  @type scheduled_pipeline :: %{
          module: module(),
          id: atom(),
          schedule: Schedule.t(),
          window: atom() | nil,
          schedule_fingerprint: String.t()
        }

  @spec discover() :: {:ok, %{optional(module()) => scheduled_pipeline()}} | {:error, term()}
  def discover do
    pipeline_modules = Application.get_env(:favn, :pipeline_modules, [])

    if is_list(pipeline_modules) do
      with {:ok, assets} <- Favn.list_assets() do
        pipeline_modules
        |> Enum.uniq()
        |> Enum.reduce_while({:ok, %{}}, fn pipeline_module, {:ok, acc} ->
          case discover_module(pipeline_module, assets) do
            {:ok, nil} -> {:cont, {:ok, acc}}
            {:ok, scheduled} -> {:cont, {:ok, Map.put(acc, pipeline_module, scheduled)}}
            {:error, reason} -> {:halt, {:error, reason}}
          end
        end)
      end
    else
      {:error, :invalid_pipeline_modules}
    end
  end

  defp discover_module(pipeline_module, assets)
       when is_atom(pipeline_module) and is_list(assets) do
    with {:ok, definition} <- Pipeline.fetch(pipeline_module),
         {:ok, resolution} <- Resolver.resolve(definition, assets: assets),
         {:ok, schedule} <- resolve_schedule(resolution.pipeline_ctx.schedule),
         :ok <- validate_window_for_schedule(definition.window, schedule) do
      case schedule do
        nil ->
          {:ok, nil}

        %Schedule{} = value ->
          {:ok,
           %{
             module: pipeline_module,
             id: definition.name,
             schedule: value,
             window: definition.window,
             schedule_fingerprint: fingerprint(value, definition.window)
           }}
      end
    end
  end

  defp discover_module(_pipeline_module, _assets), do: {:error, :invalid_pipeline_module}

  defp resolve_schedule(nil), do: {:ok, nil}
  defp resolve_schedule(%Schedule{} = schedule), do: {:ok, schedule}
  defp resolve_schedule(_), do: {:error, :invalid_schedule}

  defp validate_window_for_schedule(nil, _schedule), do: :ok

  defp validate_window_for_schedule(window, %Schedule{}) when window in [:hour, :day, :month],
    do: :ok

  defp validate_window_for_schedule(window, %Schedule{}),
    do: {:error, {:invalid_scheduler_window, window}}

  defp fingerprint(%Schedule{} = schedule, window) do
    payload = %{
      id: schedule.id,
      ref: schedule.ref,
      cron: schedule.cron,
      timezone: schedule.timezone,
      overlap: schedule.overlap,
      missed: schedule.missed,
      active: schedule.active,
      window: window
    }

    payload
    |> :erlang.term_to_binary()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end
end
