defmodule Favn.Manifest.Schedule do
  @moduledoc """
  Canonical persisted descriptor for one named schedule.
  """

  alias Favn.Triggers.Schedule, as: TriggerSchedule
  alias Favn.Manifest.Environment

  @type t :: %__MODULE__{
          module: module() | nil,
          name: atom() | nil,
          ref: TriggerSchedule.ref() | nil,
          kind: TriggerSchedule.kind(),
          cron: String.t() | nil,
          timezone: String.t() | nil,
          timezone_source: :local | :application_default | :utc_fallback,
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
            timezone_source: :utc_fallback,
            missed: :skip,
            overlap: :forbid,
            active: true,
            origin: :named

  @spec from_schedule(module(), atom(), map(), Environment.t()) :: t()
  def from_schedule(module, name, schedule, environment \\ Environment.new!())

  def from_schedule(module, name, schedule, %Environment{} = environment)
      when is_atom(module) and is_atom(name) and is_map(schedule) do
    schedule = resolve_timezone!(schedule, environment)

    %__MODULE__{
      module: module,
      name: name,
      ref: Map.get(schedule, :ref),
      kind: Map.get(schedule, :kind, :cron),
      cron: Map.get(schedule, :cron),
      timezone: Map.get(schedule, :timezone),
      timezone_source: Map.get(schedule, :timezone_source),
      missed: Map.get(schedule, :missed, :skip),
      overlap: Map.get(schedule, :overlap, :forbid),
      active: Map.get(schedule, :active, true),
      origin: Map.get(schedule, :origin, :named)
    }
    |> apply_identity(module, name)
  end

  defp resolve_timezone!(%TriggerSchedule{} = schedule, %Environment{} = environment) do
    case TriggerSchedule.apply_default_timezone(
           schedule,
           environment.default_timezone,
           environment.default_timezone_source
         ) do
      {:ok, resolved} ->
        resolved

      {:error, reason} ->
        raise ArgumentError, "invalid manifest schedule timezone: #{inspect(reason)}"
    end
  end

  defp resolve_timezone!(schedule, _environment), do: schedule

  @doc false
  @spec apply_identity(t(), module(), atom()) :: t()
  def apply_identity(%__MODULE__{} = schedule, module, name)
      when is_atom(module) and is_atom(name) do
    effective_module = schedule.module || module
    effective_name = schedule.name || name

    %{
      schedule
      | module: effective_module,
        name: effective_name,
        ref: schedule.ref || {effective_module, effective_name}
    }
  end
end
