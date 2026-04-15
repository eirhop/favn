defmodule FavnOrchestrator.Scheduler.ManifestEntries do
  @moduledoc false

  alias Favn.Manifest.Index
  alias Favn.Manifest.Pipeline
  alias Favn.Manifest.Schedule
  alias Favn.Manifest.Version

  @spec discover(Version.t(), Index.t()) ::
          {:ok, %{optional(module()) => map()}} | {:error, term()}
  def discover(%Version{} = version, %Index{} = index) do
    index
    |> Index.list_pipelines()
    |> Enum.reduce_while({:ok, %{}}, fn pipeline, {:ok, acc} ->
      case build_entry(version, index, pipeline) do
        {:ok, nil} -> {:cont, {:ok, acc}}
        {:ok, entry} -> {:cont, {:ok, Map.put(acc, pipeline.module, entry)}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp build_entry(%Version{} = version, %Index{} = index, %Pipeline{} = pipeline) do
    with {:ok, schedule} <- resolve_schedule(index, pipeline.schedule),
         :ok <- validate_window_for_schedule(pipeline.window, schedule) do
      case schedule do
        nil ->
          {:ok, nil}

        %Schedule{} = value ->
          {:ok,
           %{
             module: pipeline.module,
             id: pipeline.name,
             pipeline: pipeline,
             schedule: value,
             window: pipeline.window,
             schedule_fingerprint: fingerprint(value, pipeline.window),
             manifest_version_id: version.manifest_version_id,
             manifest_content_hash: version.content_hash
           }}
      end
    end
  end

  defp resolve_schedule(_index, nil), do: {:ok, nil}

  defp resolve_schedule(index, {:ref, {module, name}}) when is_atom(module) and is_atom(name),
    do: Index.fetch_schedule(index, {module, name})

  defp resolve_schedule(_index, {:inline, %Schedule{} = schedule}), do: {:ok, schedule}
  defp resolve_schedule(_index, %Schedule{} = schedule), do: {:ok, schedule}
  defp resolve_schedule(_index, _other), do: {:error, :invalid_schedule}

  defp validate_window_for_schedule(nil, _schedule), do: :ok

  defp validate_window_for_schedule(window, %Schedule{}) when window in [:hour, :day, :month],
    do: :ok

  defp validate_window_for_schedule(window, %Schedule{}),
    do: {:error, {:invalid_scheduler_window, window}}

  defp fingerprint(%Schedule{} = schedule, window) do
    %{
      id: schedule.name,
      ref: schedule.ref,
      cron: schedule.cron,
      timezone: schedule.timezone,
      overlap: schedule.overlap,
      missed: schedule.missed,
      active: schedule.active,
      window: window
    }
    |> :erlang.term_to_binary()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end
end
