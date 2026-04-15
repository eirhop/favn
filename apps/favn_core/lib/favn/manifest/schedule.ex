defmodule Favn.Manifest.Schedule do
  @moduledoc """
  Canonical persisted descriptor for one named schedule.
  """

  alias Favn.Triggers.Schedule, as: TriggerSchedule

  @type t :: %__MODULE__{
          module: module() | nil,
          name: atom() | nil,
          ref: TriggerSchedule.ref() | nil,
          kind: TriggerSchedule.kind(),
          cron: String.t() | nil,
          timezone: String.t() | nil,
          missed: TriggerSchedule.missed_policy(),
          overlap: TriggerSchedule.overlap_policy(),
          active: boolean(),
          origin: :inline | :named
        }

  defstruct module: nil,
            name: nil,
            ref: nil,
            kind: :cron,
            cron: nil,
            timezone: nil,
            missed: :skip,
            overlap: :forbid,
            active: true,
            origin: :named

  @spec from_schedule(module(), atom(), map()) :: t()
  def from_schedule(module, name, schedule)
      when is_atom(module) and is_atom(name) and is_map(schedule) do
    %__MODULE__{
      module: module,
      name: name,
      ref: Map.get(schedule, :ref),
      kind: Map.get(schedule, :kind, :cron),
      cron: Map.get(schedule, :cron),
      timezone: Map.get(schedule, :timezone),
      missed: Map.get(schedule, :missed, :skip),
      overlap: Map.get(schedule, :overlap, :forbid),
      active: Map.get(schedule, :active, true),
      origin: Map.get(schedule, :origin, :named)
    }
  end
end
