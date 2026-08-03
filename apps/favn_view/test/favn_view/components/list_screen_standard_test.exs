defmodule FavnView.Components.ListScreenStandardTest do
  @moduledoc """
  The two rules every list screen follows, asserted for all of them at once.

  Both were inconsistent before: `/assets` and `/pipelines` rendered every row
  twice on a phone — once as a table row and once as a card — because the table
  had no narrow-screen hide, and three screens laid their filters out inline
  where only `/runs` collapsed them. A per-screen test would not have caught
  either, since each screen passed its own tests; the defect was the gap between
  them. So this enumerates the screens and fails when a new one is added without
  the standard.
  """

  use ExUnit.Case, async: true

  import Phoenix.LiveViewTest

  alias FavnView.AssetCatalogueFilters
  alias FavnView.Components.AssetCataloguePage
  alias FavnView.Components.PipelinesPage
  alias FavnView.Components.RunsListPage
  alias FavnView.Components.SchedulesPage
  alias FavnView.Dev.DesignSystem.Fixtures.RunsList
  alias FavnView.Dev.DesignSystem.Fixtures.Schedules
  alias FavnView.RunsFilters

  # Each entry is {screen, desktop rows, narrow cards}, rendered from the same
  # data so "one of the two shows" is a claim about markup and not about content.
  defp row_renderings do
    assets = AssetCataloguePage.sample_assets()
    pipelines = PipelinesPage.sample_pipelines()
    schedules = Schedules.list()
    listing = RunsList.today().listing

    [
      {"/assets", render_component(&AssetCataloguePage.asset_table/1, assets: assets),
       render_component(&AssetCataloguePage.asset_card_list/1, assets: assets)},
      {"/pipelines", render_component(&PipelinesPage.pipeline_table/1, pipelines: pipelines),
       render_component(&PipelinesPage.pipeline_card_list/1, pipelines: pipelines)},
      {"/schedules", render_component(&SchedulesPage.schedules_table/1, schedules: schedules),
       render_component(&SchedulesPage.schedule_cards/1, schedules: schedules)},
      {"/runs", render_component(&RunsListPage.runs_table/1, listing: listing, order: :desc),
       render_component(&RunsListPage.runs_cards/1, listing: listing)}
    ]
  end

  defp toolbar_renderings do
    assets = AssetCataloguePage.sample_assets()
    defaults = AssetCatalogueFilters.defaults()

    [
      {"/assets",
       render_component(&AssetCataloguePage.asset_filters/1,
         filters: defaults,
         connection_options: AssetCataloguePage.connection_options(),
         catalogue_options: AssetCataloguePage.catalogue_options(),
         schema_options: AssetCataloguePage.schema_options(),
         scope_choices: AssetCatalogueFilters.scope_choices(assets, defaults)
       )},
      {"/pipelines",
       render_component(&PipelinesPage.pipeline_filters/1,
         filters: %{search: "", status: "all"},
         status_options: [{"Health", "all"}]
       )},
      {"/runs",
       render_component(&RunsListPage.runs_toolbar/1, filters: %RunsFilters{}, counts: nil)}
    ]
  end

  test "a list screen renders its rows for one viewport at a time" do
    for {screen, table, cards} <- row_renderings() do
      assert table =~ "hidden lg:block",
             "#{screen} does not hide its table below lg, so its rows render twice on a phone"

      assert cards =~ "lg:hidden",
             "#{screen} does not hide its cards at lg, so its rows render twice on a desktop"
    end
  end

  test "a list screen keeps its filters behind one control below lg" do
    for {screen, html} <- toolbar_renderings() do
      assert html =~ ~s(data-testid="toggle-table-filters"),
             "#{screen} has no narrow-screen filter control"

      # The form is laid out at `lg` and hidden below it until the control opens.
      assert html =~ "hidden w-full flex-col lg:flex",
             "#{screen} lays its filters out inline below lg instead of collapsing them"
    end
  end

  test "the control says a filter is in force while the filters are hidden" do
    adjusted =
      render_component(&PipelinesPage.pipeline_filters/1,
        filters: %{search: "orders", status: "all"},
        status_options: [{"Health", "all"}]
      )

    # Otherwise a narrowed list looks like the whole list, with the thing that
    # narrowed it collapsed out of sight.
    assert adjusted =~ "bg-primary"
  end

  test "opening the disclosure lays the filters out" do
    open =
      render_component(&PipelinesPage.pipeline_filters/1,
        filters: %{search: "", status: "all"},
        status_options: [{"Health", "all"}],
        filters_open?: true
      )

    assert open =~ "flex w-full flex-col"
    refute open =~ "hidden w-full flex-col lg:flex"
  end
end
