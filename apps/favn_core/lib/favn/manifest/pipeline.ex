defmodule Favn.Manifest.Pipeline do
  @moduledoc """
  Canonical persisted descriptor for one pipeline definition.

  Tag and category selectors carry manifest labels. Labels are normalized to
  strings for stable selector behavior across JSON persistence.
  """

  alias Favn.Window.Policy
  alias Favn.Manifest.Environment
  alias Favn.Manifest.Schedule
  alias Favn.Triggers.Schedule, as: TriggerSchedule

  @type t :: %__MODULE__{
          module: module() | nil,
          name: atom() | nil,
          selectors: [term()],
          deps: :all | :none,
          schedule: term(),
          window: Favn.Window.Policy.t() | nil,
          retry_policy: Favn.Retry.Policy.t() | nil,
          max_concurrency: pos_integer() | nil,
          execution_pool: atom() | nil,
          resource_recovery: Favn.ResourceRecovery.Policy.t() | nil,
          source: atom() | nil,
          outputs: [atom()],
          settings: Favn.Settings.t(),
          metadata: map()
        }

  defstruct module: nil,
            name: nil,
            selectors: [],
            deps: :all,
            schedule: nil,
            window: nil,
            retry_policy: nil,
            max_concurrency: nil,
            execution_pool: nil,
            resource_recovery: nil,
            source: nil,
            outputs: [],
            settings: %{},
            metadata: %{}

  @spec from_definition(map(), Environment.t()) :: t()
  def from_definition(definition, environment \\ Environment.new!())

  def from_definition(definition, %Environment{} = environment) when is_map(definition) do
    module = Map.get(definition, :module)
    name = Map.get(definition, :name)
    window = resolve_window(Map.get(definition, :window), environment)

    %__MODULE__{
      module: module,
      name: name,
      selectors: normalize_list(Map.get(definition, :selectors, [])),
      deps: normalize_deps(Map.get(definition, :deps, :all)),
      schedule: normalize_schedule(Map.get(definition, :schedule), module, name, environment),
      window: window,
      retry_policy: normalize_retry_policy(Map.get(definition, :retry_policy)),
      max_concurrency: normalize_max_concurrency(Map.get(definition, :max_concurrency)),
      execution_pool: normalize_execution_pool(Map.get(definition, :execution_pool)),
      resource_recovery:
        Favn.ResourceRecovery.Policy.from_value!(Map.get(definition, :resource_recovery)),
      source: Map.get(definition, :source),
      outputs: normalize_atom_list(Map.get(definition, :outputs, [])),
      settings: Favn.Settings.normalize!(Map.get(definition, :settings, %{})),
      metadata: normalize_map(Map.get(definition, :meta, %{}))
    }
  end

  defp normalize_schedule(
         {:inline, %TriggerSchedule{} = schedule},
         module,
         name,
         environment
       )
       when is_atom(module) and is_atom(name),
       do: {:inline, Schedule.from_schedule(module, name, schedule, environment)}

  defp normalize_schedule({:inline, %Schedule{} = schedule}, module, name, _environment)
       when is_atom(module) and is_atom(name),
       do: {:inline, Schedule.apply_identity(schedule, module, name)}

  defp normalize_schedule(schedule, _module, _name, _environment), do: schedule

  defp resolve_window(value, %Environment{} = environment) do
    with {:ok, policy} <- Policy.from_value(value),
         {:ok, policy} <- resolve_window_timezone(policy, environment) do
      policy
    else
      {:error, reason} ->
        raise ArgumentError, "invalid manifest pipeline window: #{inspect(reason)}"
    end
  end

  defp resolve_window_timezone(nil, _environment), do: {:ok, nil}

  defp resolve_window_timezone(%Policy{} = policy, %Environment{} = environment) do
    Policy.resolve_timezone(
      policy,
      environment.default_timezone,
      environment.default_timezone_source
    )
  end

  defp normalize_deps(:all), do: :all
  defp normalize_deps(:none), do: :none
  defp normalize_deps(_other), do: :all

  defp normalize_retry_policy(nil), do: nil
  defp normalize_retry_policy(value), do: Favn.Retry.Policy.new!(value)

  defp normalize_max_concurrency(value) when is_integer(value) and value > 0, do: value
  defp normalize_max_concurrency(_other), do: nil

  defp normalize_execution_pool(value) when is_atom(value), do: value
  defp normalize_execution_pool(_other), do: nil

  defp normalize_atom_list(list) when is_list(list) do
    list
    |> Enum.filter(&is_atom/1)
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp normalize_atom_list(_other), do: []

  defp normalize_list(list) when is_list(list), do: list
  defp normalize_list(_other), do: []

  defp normalize_map(value) when is_map(value), do: value
  defp normalize_map(_other), do: %{}
end
