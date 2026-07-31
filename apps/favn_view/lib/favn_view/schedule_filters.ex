defmodule FavnView.ScheduleFilters do
  @moduledoc """
  Pure filter logic for the schedule list.

  A schedule has two states that matter and they are independent: whether the
  operator has activated it, and what it is doing right now. Six counters used to
  report them in a band above the list, which said what was disabled without
  offering to show it. These are the same six facts as one axis of scopes, so the
  count is both the answer and the way to narrow to it.

  A scope is a shortcut over the activation and runtime selects rather than a
  third filter, so the rail and the selects can never disagree: choosing
  "Running" is choosing `runtime_state=running`, and the rail reads its active
  choice back out of those two values.
  """

  @scopes [
    %{
      id: "all",
      label: "All",
      icon: "hero-queue-list",
      tone: :neutral,
      hint: "Every schedule in the active manifest",
      activation_state: "all",
      runtime_state: "all"
    },
    %{
      id: "enabled",
      label: "Enabled",
      icon: "hero-check-circle",
      tone: :success,
      hint: "Activated by an operator",
      activation_state: "enabled",
      runtime_state: "all"
    },
    %{
      id: "pending_activation",
      label: "Pending",
      icon: "hero-hand-raised",
      tone: :warning,
      hint: "Awaiting an activation decision",
      activation_state: "pending_activation",
      runtime_state: "all"
    },
    %{
      id: "disabled",
      label: "Disabled",
      icon: "hero-pause-circle",
      tone: :neutral,
      hint: "Will not submit until activated",
      activation_state: "disabled",
      runtime_state: "all"
    },
    %{
      id: "running",
      label: "Running",
      icon: "hero-bolt",
      tone: :info,
      hint: "Has a run in flight now",
      activation_state: "all",
      runtime_state: "running"
    },
    %{
      id: "queued",
      label: "Queued",
      icon: "hero-clock",
      tone: :info,
      hint: "Submitted and waiting to start",
      activation_state: "all",
      runtime_state: "queued"
    }
  ]

  @doc "The scope definitions, in the order the rail renders them."
  @spec scopes() :: [map()]
  def scopes, do: @scopes

  @doc """
  The filter values one scope stands for.

  ## Examples

      iex> FavnView.ScheduleFilters.scope_filters("running")
      %{"activation_state" => "all", "runtime_state" => "running"}

      iex> FavnView.ScheduleFilters.scope_filters("nonsense")
      %{"activation_state" => "all", "runtime_state" => "all"}
  """
  @spec scope_filters(String.t()) :: %{String.t() => String.t()}
  def scope_filters(id) do
    scope = Enum.find(@scopes, List.first(@scopes), &(&1.id == id))

    %{"activation_state" => scope.activation_state, "runtime_state" => scope.runtime_state}
  end

  @doc """
  Scope choices for `FavnView.UI.Data.scope_rail/1`.

  Counts come from every schedule rather than the filtered page, so the number on
  a button is the number of rows clicking it produces.

  ## Examples

      iex> schedules = [
      ...>   %{activation_state: :enabled, runtime_state: :running},
      ...>   %{activation_state: :disabled, runtime_state: :inactive}
      ...> ]
      iex> filters = %{"activation_state" => "all", "runtime_state" => "all"}
      iex> choices = FavnView.ScheduleFilters.scope_choices(schedules, filters)
      iex> Enum.map(choices, &{&1.id, &1.count, &1.active?}) |> Enum.take(3)
      [{"all", 2, true}, {"enabled", 1, false}, {"pending_activation", 0, false}]
  """
  @spec scope_choices([map()], map()) :: [map()]
  def scope_choices(schedules, filters) do
    active = active_scope(filters)

    Enum.map(@scopes, fn scope ->
      scope
      |> Map.take([:id, :label, :icon, :tone, :hint])
      |> Map.merge(%{
        count: Enum.count(schedules, &in_scope?(&1, scope)),
        count_label: "schedules",
        active?: scope.id == active
      })
    end)
  end

  @doc """
  Which scope the current activation and runtime values represent.

  A combination no scope covers — narrowed with both selects — reports `nil`, so
  no button claims to be showing the list.

  ## Examples

      iex> FavnView.ScheduleFilters.active_scope(%{
      ...>   "activation_state" => "enabled",
      ...>   "runtime_state" => "all"
      ...> })
      "enabled"

      iex> FavnView.ScheduleFilters.active_scope(%{
      ...>   "activation_state" => "enabled",
      ...>   "runtime_state" => "running"
      ...> })
      nil
  """
  @spec active_scope(map()) :: String.t() | nil
  def active_scope(filters) do
    activation = Map.get(filters, "activation_state", "all")
    runtime = Map.get(filters, "runtime_state", "all")

    Enum.find_value(@scopes, fn scope ->
      if scope.activation_state == activation and scope.runtime_state == runtime,
        do: scope.id
    end)
  end

  defp in_scope?(_schedule, %{activation_state: "all", runtime_state: "all"}), do: true

  defp in_scope?(schedule, %{activation_state: "all", runtime_state: runtime}),
    do: to_string(schedule.runtime_state) == runtime

  defp in_scope?(schedule, %{activation_state: activation}),
    do: to_string(schedule.activation_state) == activation
end
