defmodule Favn.Run.PipelineContext do
  @moduledoc """
  Resolved pipeline data exposed to each asset in a pipeline run.

  This is an explicit runner contract. Per-run asset parameters remain on
  `Favn.Run.Context.params`; pipeline settings remain on `:settings`. Schedule
  data is canonicalized to the manifest-backed `Favn.Manifest.Schedule` shape
  before it crosses the runner boundary. `window_selection` preserves the
  trigger's requested anchors, expansion, and effective anchors; the runner
  consumes the concrete plan and never expands the selection.
  """

  alias Favn.Ref

  @type t :: %__MODULE__{
          ref: Ref.t() | nil,
          settings: Favn.Settings.t(),
          metadata: map(),
          resolved_refs: [Ref.t()],
          dependencies: :all | :none,
          trigger: map(),
          anchor_window: Favn.Window.Anchor.t() | nil,
          window_selection: Favn.Window.Selection.t() | nil,
          window: Favn.Window.Policy.t() | nil,
          max_concurrency: pos_integer() | nil,
          execution_pool: atom() | nil,
          resource_recovery: Favn.ResourceRecovery.Policy.t() | nil,
          schedule: Favn.Manifest.Schedule.t() | nil,
          source: atom() | nil,
          outputs: [atom()]
        }

  defstruct ref: nil,
            settings: %{},
            metadata: %{},
            resolved_refs: [],
            dependencies: :all,
            trigger: %{},
            anchor_window: nil,
            window_selection: nil,
            window: nil,
            max_concurrency: nil,
            execution_pool: nil,
            resource_recovery: nil,
            schedule: nil,
            source: nil,
            outputs: []

  @doc false
  @spec from_map(map() | nil) :: t() | nil
  def from_map(nil), do: nil

  def from_map(value) when is_map(value) do
    module = field(value, :module)
    name = field(value, :name)

    %__MODULE__{
      ref: if(is_atom(module) and is_atom(name), do: {module, name}),
      settings: Favn.Settings.normalize!(field(value, :settings, %{})),
      metadata: normalize_metadata(field(value, :metadata, %{})),
      resolved_refs: field(value, :resolved_refs, []),
      dependencies: field(value, :dependencies, :all),
      trigger: field(value, :trigger, %{}),
      anchor_window: Favn.Window.Anchor.from_value!(field(value, :anchor_window)),
      window_selection: window_selection(field(value, :window_selection)),
      window: Favn.Window.Policy.from_value!(field(value, :window)),
      max_concurrency: field(value, :max_concurrency),
      execution_pool: field(value, :execution_pool),
      resource_recovery:
        Favn.ResourceRecovery.Policy.from_value!(field(value, :resource_recovery)),
      schedule: normalize_schedule(field(value, :schedule), module, name),
      source: field(value, :source),
      outputs: field(value, :outputs, [])
    }
  end

  defp field(map, key, default \\ nil),
    do: Map.get(map, key, Map.get(map, Atom.to_string(key), default))

  defp normalize_metadata(value) when is_map(value) do
    Favn.Settings.normalize!(metadata: value).metadata
  end

  defp normalize_metadata(value) do
    raise ArgumentError, "pipeline metadata must be a map, got: #{inspect(value)}"
  end

  defp window_selection(value) do
    case Favn.Window.Selection.from_value(value) do
      {:ok, selection} ->
        selection

      {:error, reason} ->
        raise ArgumentError, "invalid pipeline window selection: #{inspect(reason)}"
    end
  end

  defp normalize_schedule(nil, _module, _name), do: nil

  defp normalize_schedule(%Favn.Manifest.Schedule{} = schedule, module, name)
       when is_atom(module) and is_atom(name),
       do: Favn.Manifest.Schedule.apply_identity(schedule, module, name)

  defp normalize_schedule(%Favn.Triggers.Schedule{} = schedule, module, name) do
    schedule_name = schedule.id || name

    %Favn.Manifest.Schedule{
      module: module,
      name: schedule_name,
      ref: schedule.ref || schedule_ref(module, schedule_name),
      kind: schedule.kind,
      cron: schedule.cron,
      timezone: schedule.timezone,
      missed: schedule.missed,
      overlap: schedule.overlap,
      active: schedule.active,
      origin: schedule.origin
    }
  end

  defp normalize_schedule(value, _module, _name) do
    raise ArgumentError, "pipeline schedule must be a normalized schedule, got: #{inspect(value)}"
  end

  defp schedule_ref(module, name) when is_atom(module) and is_atom(name), do: {module, name}
  defp schedule_ref(_module, _name), do: nil
end
