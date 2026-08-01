defmodule FavnView.Components.Navigation do
  @moduledoc """
  The single source of truth for Favn's primary navigation.

  Every destination here must be a live route in `FavnView.Router`. A
  navigation entry that points nowhere is not a roadmap hint, it is a dead
  control: add the entry in the same change that adds the route.

  Status comes first because it is the only destination that answers "what needs
  me". The rest follow the operator's workflow: define assets, group them into
  pipelines, schedule them, watch runs, repair, and inspect.

  ## Examples

      iex> FavnView.Components.Navigation.items(:runs) |> Enum.find(& &1.active) |> Map.get(:label)
      "Runs"
  """

  @type item :: %{
          required(:label) => String.t(),
          required(:icon) => String.t(),
          required(:href) => String.t(),
          required(:active) => boolean()
        }

  @destinations [
    {:status, "Status", "hero-signal", "/"},
    {:assets, "Assets", "hero-sparkles", "/assets"},
    {:pipelines, "Pipelines", "hero-queue-list", "/pipelines"},
    {:schedules, "Schedules", "hero-calendar-days", "/schedules"},
    {:runs, "Runs", "hero-rocket-launch", "/runs"},
    {:runners, "Runners", "hero-server-stack", "/runners"},
    {:rebuilds, "Rebuilds", "hero-arrow-path-rounded-square", "/rebuilds"},
    {:recoveries, "Recovery", "hero-shield-check", "/recoveries"},
    {:logs, "Logs", "hero-document-text", "/logs"}
  ]

  @admin_destination {:admin, "Admin", "hero-cog-6-tooth", "/admin"}

  @sections Enum.map(@destinations, fn {section, _label, _icon, _href} -> section end)

  @doc """
  The navigation sections, in display order.
  """
  @spec sections() :: [atom()]
  def sections, do: @sections

  @doc """
  Builds the navigation items, marking `active` as the current section.

  Passing a section that does not exist marks nothing active, which is the right
  behaviour for a page that is not reachable from the rail.
  """
  @spec items(atom()) :: [item()]
  def items(active \\ nil) do
    Enum.map(@destinations, fn {section, label, icon, href} ->
      %{label: label, icon: icon, href: href, active: section == active}
    end)
  end

  @doc """
  The administrator-only destination for the primary navigation.
  """
  @spec admin_item(boolean()) :: item()
  def admin_item(active \\ false) do
    {section, label, icon, href} = @admin_destination
    %{label: label, icon: icon, href: href, active: active && section == :admin}
  end
end
