defmodule FavnOrchestrator.Persistence.DeploymentSchedules do
  @moduledoc "Plans durable schedule cursors for an exact workspace deployment."

  alias Favn.Manifest.Index
  alias Favn.Manifest.Version
  alias FavnOrchestrator.Persistence.Commands.DeploymentSchedule
  alias FavnOrchestrator.Persistence.TargetIdentity
  alias FavnOrchestrator.Scheduler.Cron
  alias FavnOrchestrator.Scheduler.ManifestEntries

  @doc "Returns active schedules whose pipelines are customer-selected deployment targets."
  @spec plan(Version.t(), [struct()], DateTime.t()) ::
          {:ok, [DeploymentSchedule.t()]} | {:error, term()}
  def plan(%Version{} = version, targets, %DateTime{} = now) when is_list(targets) do
    selected =
      targets
      |> Enum.filter(&(&1.target_kind == :pipeline))
      |> MapSet.new(& &1.target_id)

    with {:ok, index} <- Index.build_from_version(version),
         {:ok, entries} <- ManifestEntries.discover_all(version, index) do
      entries
      |> Enum.filter(fn entry ->
        entry.schedule.active == true and
          MapSet.member?(selected, TargetIdentity.for_pipeline({entry.module, entry.id}))
      end)
      |> Enum.reduce_while({:ok, []}, fn entry, {:ok, acc} ->
        case Cron.next_due(entry.schedule.cron, entry.schedule.timezone, now) do
          %DateTime{} = next_due ->
            schedule = %DeploymentSchedule{
              pipeline_target_id: TargetIdentity.for_pipeline({entry.module, entry.id}),
              schedule_id: to_string(entry.schedule.name),
              schedule_fingerprint: entry.schedule_fingerprint,
              definition: schedule_definition(entry),
              next_due_at: next_due,
              cursor: %{
                "schedule_fingerprint" => entry.schedule_fingerprint,
                "in_flight_run_id" => nil,
                "queued_due_at" => nil
              }
            }

            {:cont, {:ok, [schedule | acc]}}

          nil ->
            {:halt, {:error, {:invalid_deployment_schedule, entry.module, entry.id}}}
        end
      end)
      |> then(fn
        {:ok, schedules} ->
          {:ok, Enum.sort_by(schedules, &{&1.pipeline_target_id, &1.schedule_id})}

        error ->
          error
      end)
    end
  end

  defp schedule_definition(entry) do
    %{
      "pipeline_module" => Atom.to_string(entry.module),
      "pipeline_name" => Atom.to_string(entry.id),
      "cron" => entry.schedule.cron,
      "timezone" => entry.schedule.timezone,
      "overlap" => Atom.to_string(entry.schedule.overlap),
      "missed" => Atom.to_string(entry.schedule.missed),
      "window" => json_value(entry.window)
    }
  end

  defp json_value(nil), do: nil
  defp json_value(%_{} = value), do: value |> Map.from_struct() |> json_value()

  defp json_value(value) when is_map(value),
    do: Map.new(value, fn {key, item} -> {to_string(key), json_value(item)} end)

  defp json_value(value) when is_list(value), do: Enum.map(value, &json_value/1)
  defp json_value(value) when is_atom(value), do: Atom.to_string(value)
  defp json_value(value), do: value
end
