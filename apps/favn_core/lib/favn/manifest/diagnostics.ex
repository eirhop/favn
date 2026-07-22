defmodule Favn.Manifest.Diagnostics do
  @moduledoc false

  alias Favn.Coverage.Effective
  alias Favn.Diagnostic
  alias Favn.Manifest
  alias Favn.Manifest.Index
  alias Favn.Manifest.PipelineResolver
  alias Favn.Window.Policy
  alias Favn.Window.Spec

  @spec for_manifest(Manifest.t()) :: [Diagnostic.t()]
  def for_manifest(%Manifest{} = manifest) do
    {:ok, index} = Index.build(manifest)

    index
    |> Index.list_pipelines()
    |> Enum.flat_map(&pipeline_diagnostics(index, &1))
  end

  defp pipeline_diagnostics(index, pipeline) do
    case PipelineResolver.resolve(index, pipeline, trigger: %{kind: :compile}) do
      {:ok, resolution} ->
        Enum.flat_map(resolution.target_refs, fn ref ->
          {:ok, asset} = Index.fetch_asset(index, ref)

          timezone_diagnostics(resolution.pipeline, asset) ++
            availability_diagnostics(resolution.pipeline_ctx.schedule, resolution.pipeline, asset)
        end)

      {:error, _reason} ->
        []
    end
  end

  defp timezone_diagnostics(
         %Favn.Manifest.Pipeline{window: %Policy{} = policy} = pipeline,
         %{window: %Spec{} = asset_window} = asset
       ) do
    if policy.timezone == asset_window.timezone do
      []
    else
      [
        %Diagnostic{
          severity: :warning,
          stage: :compile,
          code: :pipeline_asset_timezone_mismatch,
          message:
            "pipeline #{pipeline_name(pipeline)} uses #{policy.timezone}, while selected asset #{ref_name(asset.ref)} uses #{asset_window.timezone}",
          asset_ref: asset.ref,
          details: %{
            pipeline_ref: {pipeline.module, pipeline.name},
            pipeline_timezone: policy.timezone,
            pipeline_timezone_source: policy.timezone_source,
            asset_timezone: asset_window.timezone,
            asset_timezone_source: asset_window.timezone_source
          }
        }
      ]
    end
  end

  defp timezone_diagnostics(_pipeline, _asset), do: []

  defp availability_diagnostics(
         %{cron: cron, timezone: timezone} = schedule,
         %Favn.Manifest.Pipeline{window: %Policy{} = policy} = pipeline,
         %{
           window: %Spec{} = asset_window,
           coverage: %Effective{
             through: :latest_closed,
             availability_delay_seconds: delay
           }
         } = asset
       )
       when is_integer(delay) and delay > 0 do
    with true <- policy.kind == asset_window.kind,
         true <- policy.timezone == timezone and asset_window.timezone == timezone,
         {:ok, occurrence_offset} <- recurring_offset_seconds(cron, asset_window.kind),
         true <- occurrence_offset < delay do
      [
        %Diagnostic{
          severity: :warning,
          stage: :compile,
          code: :cron_before_coverage_availability,
          message:
            "schedule #{schedule_name(schedule)} for pipeline #{pipeline_name(pipeline)} occurs before selected asset #{ref_name(asset.ref)} reaches its latest-closed availability delay",
          asset_ref: asset.ref,
          details: %{
            pipeline_ref: {pipeline.module, pipeline.name},
            cron: cron,
            timezone: timezone,
            occurrence_offset_seconds: occurrence_offset,
            availability_delay_seconds: delay
          }
        }
      ]
    else
      _other -> []
    end
  end

  defp availability_diagnostics(_schedule, _pipeline, _asset), do: []

  defp recurring_offset_seconds(cron, kind) do
    with {:ok, fields} <- cron_fields(cron),
         {:ok, second} <- fixed(fields.second, 0, 59),
         {:ok, minute} <- fixed(fields.minute, 0, 59),
         {:ok, hour} <- hour_for_kind(fields.hour, kind),
         true <- recurring_shape?(fields, kind) do
      {:ok, hour * 3_600 + minute * 60 + second}
    else
      _other -> :not_recurring_at_one_period_offset
    end
  end

  defp cron_fields(cron) when is_binary(cron) do
    case String.split(cron, ~r/\s+/, trim: true) do
      [minute, hour, day, month, weekday] ->
        {:ok,
         %{second: "0", minute: minute, hour: hour, day: day, month: month, weekday: weekday}}

      [second, minute, hour, day, month, weekday] ->
        {:ok,
         %{
           second: second,
           minute: minute,
           hour: hour,
           day: day,
           month: month,
           weekday: weekday
         }}

      _other ->
        :invalid_cron
    end
  end

  defp hour_for_kind("*", :hour), do: {:ok, 0}
  defp hour_for_kind(value, kind) when kind in [:day, :month, :year], do: fixed(value, 0, 23)
  defp hour_for_kind(_value, _kind), do: :invalid_hour

  defp recurring_shape?(%{hour: "*"}, :hour), do: true

  defp recurring_shape?(%{day: "*", month: "*"}, :day), do: true
  defp recurring_shape?(%{day: "1", month: "*", weekday: "*"}, :month), do: true
  defp recurring_shape?(%{day: "1", month: "1", weekday: "*"}, :year), do: true
  defp recurring_shape?(_fields, _kind), do: false

  defp fixed(value, min, max) do
    case Integer.parse(value) do
      {number, ""} when number >= min and number <= max -> {:ok, number}
      _other -> :not_fixed
    end
  end

  defp pipeline_name(%{module: module, name: name}), do: "#{inspect(module)}.#{name}"
  defp schedule_name(%{name: name}) when is_atom(name), do: Atom.to_string(name)
  defp schedule_name(%{cron: cron}), do: cron
  defp ref_name({module, name}), do: "#{inspect(module)}.#{name}"
end
