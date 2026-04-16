defmodule FavnView.Presenters.RunPresenter do
  @moduledoc """
  Stable UI-facing projection for run snapshots and run timeline events.
  """

  alias FavnOrchestrator.RunEvent

  @terminal_statuses [:ok, :error, :cancelled, :timed_out]

  @spec summary(Favn.Run.t()) :: map()
  def summary(run) when is_map(run) do
    %{
      id: Map.get(run, :id),
      status: Map.get(run, :status),
      manifest_version_id: Map.get(run, :manifest_version_id),
      submit_kind: Map.get(run, :submit_kind),
      cancel_enabled: cancel_enabled?(run),
      rerun_enabled: rerun_enabled?(run)
    }
  end

  @spec summaries([Favn.Run.t()]) :: [map()]
  def summaries(runs) when is_list(runs), do: Enum.map(runs, &summary/1)

  @spec timeline_event(RunEvent.t() | map()) :: map()
  def timeline_event(%RunEvent{} = event) do
    %{
      sequence: event.sequence,
      event_type: event.event_type,
      entity: event.entity,
      asset_ref: event.asset_ref,
      stage: event.stage,
      status: event.status,
      label: event_label(event.event_type)
    }
  end

  def timeline_event(event) when is_map(event) do
    timeline_event(RunEvent.from_map(event))
  end

  @spec timeline([RunEvent.t() | map()]) :: [map()]
  def timeline(events) when is_list(events), do: Enum.map(events, &timeline_event/1)

  @spec cancel_enabled?(map()) :: boolean()
  def cancel_enabled?(run) do
    status = Map.get(run, :status)
    not is_nil(status) and status not in @terminal_statuses
  end

  @spec rerun_enabled?(map()) :: boolean()
  def rerun_enabled?(run), do: Map.get(run, :status) in @terminal_statuses

  defp event_label(event_type) when is_atom(event_type) do
    event_type
    |> Atom.to_string()
    |> String.replace("_", " ")
    |> String.capitalize()
  end

  defp event_label(event_type) when is_binary(event_type) do
    event_type
    |> String.replace("_", " ")
    |> String.capitalize()
  end

  defp event_label(_event_type), do: "Event"
end
