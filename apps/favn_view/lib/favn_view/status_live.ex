defmodule FavnView.StatusLive do
  @moduledoc false

  use FavnView, :live_view

  alias FavnView.Orchestrator
  alias FavnView.Components.Navigation
  alias FavnView.Components.StatusPage
  alias FavnView.StatusConcerns

  # Enough to show that something is wrong without turning this into a list view.
  # The group's own destination is where an operator goes to see all of them.
  @per_group 8

  @impl true
  def mount(_params, _session, socket) do
    %{groups: groups, unavailable: unavailable} =
      socket
      |> operator_context()
      |> read_sources()
      |> StatusConcerns.build(limit: @per_group)

    {:ok,
     assign(socket,
       groups: groups,
       unavailable: unavailable,
       loading: false,
       error: nil,
       nav_items: Navigation.items(:status)
     )}
  end

  @impl true
  def render(assigns) do
    ~H"""
    <StatusPage.status_page
      groups={@groups}
      unavailable={@unavailable}
      loading={@loading}
      error={@error}
      nav_items={@nav_items}
      current_scope={@current_scope}
      operator_workspaces={@operator_workspaces}
      flash={@flash}
    />
    """
  end

  defp read_sources(operator_context) do
    %{
      runs:
        call(:page_execution_groups_fun, &Orchestrator.page_execution_groups/2, [
          operator_context,
          [limit: @per_group, only_failed: true]
        ]),
      assets:
        call(:active_asset_catalogue_fun, &Orchestrator.active_asset_catalogue/1, [
          operator_context
        ]),
      schedules:
        call(
          :page_schedule_list_entries_fun,
          &Orchestrator.page_schedule_list_entries/2,
          [
            operator_context,
            [limit: 500]
          ]
        ),
      rebuilds:
        call(:page_operator_rebuilds_fun, &Orchestrator.page_operator_rebuilds/2, [
          operator_context,
          [limit: @per_group]
        ])
    }
  end

  # The same override seam the other LiveViews use, accepting a stub of either
  # arity so a test can supply one without an operator context.
  defp call(key, default, args) do
    fun = Application.get_env(:favn_view, key, default)

    cond do
      is_function(fun, length(args)) -> apply(fun, args)
      is_function(fun, length(args) - 1) -> apply(fun, tl(args))
      true -> apply(default, args)
    end
  end

  defp operator_context(socket), do: socket.assigns.current_scope.operator_context
end
