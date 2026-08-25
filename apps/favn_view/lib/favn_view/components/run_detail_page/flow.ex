defmodule FavnView.Components.RunDetailPage.Flow do
  @moduledoc """
  The bounded asset list for one exact run.

  Rows intentionally contain only the name, state, start, and end values shown
  here. Observed rows link to their separate detail route; planned rows are
  inert because an attempt does not exist yet.
  """

  use FavnView, :html

  attr :assets, :list, required: true
  attr :backfill_parent?, :boolean, default: false

  def flow(assigns) do
    ~H"""
    <section data-testid="run-flow">
      <.empty_state
        :if={@assets == [] && !@backfill_parent?}
        icon="hero-square-3-stack-3d"
        title="No asset work yet"
        description="Assets appear when the persisted run plan is available."
      />

      <.empty_state
        :if={@assets == [] && @backfill_parent?}
        icon="hero-calendar-days"
        title="Asset work runs in the windows"
        description="Open a window run to inspect its exact assets and results."
      />

      <.panel :if={@assets != []} padding={:none}>
        <:header title="Assets" subtitle="Only data shown in this list is loaded." />
        <:actions>
          <.count_badge count={length(@assets)} label="assets" />
        </:actions>

        <.data_table
          id="run-assets"
          rows={@assets}
          row_testid="run-asset-row"
          desktop_only?
        >
          <:col :let={asset} label="Asset">
            <.link
              :if={asset.detail?}
              navigate={~p"/runs/#{asset.run_id}/assets/#{asset.id}"}
              class="font-medium link link-hover"
            >
              {asset.name}
            </.link>
            <span :if={!asset.detail?} class="font-medium">{asset.name}</span>
            <.mono value={asset.asset_ref} truncate class="mt-0.5 text-sm favn-text-subtle" />
          </:col>
          <:col :let={asset} label="State" class="w-32">
            <.status_badge
              tone={status_tone(asset.state)}
              label={status_label(asset.state)}
              size={:sm}
            />
          </:col>
          <:col :let={asset} label="Started" class="w-44 favn-text-subtle">
            {asset.started_at || "-"}
          </:col>
          <:col :let={asset} label="Finished" class="w-44 favn-text-subtle">
            {asset.finished_at || "-"}
          </:col>
        </.data_table>

        <.stack gap={:sm} class="p-3 lg:hidden" data-testid="run-asset-card-list">
          <.asset_card :for={asset <- @assets} asset={asset} />
        </.stack>
      </.panel>
    </section>
    """
  end

  attr :asset, :map, required: true

  defp asset_card(%{asset: %{detail?: true}} = assigns) do
    ~H"""
    <.list_card
      navigate={~p"/runs/#{@asset.run_id}/assets/#{@asset.id}"}
      data-testid="run-asset-card"
    >
      <.asset_card_content asset={@asset} />
    </.list_card>
    """
  end

  defp asset_card(assigns) do
    ~H"""
    <.list_card data-testid="run-asset-card">
      <.asset_card_content asset={@asset} />
    </.list_card>
    """
  end

  attr :asset, :map, required: true

  defp asset_card_content(assigns) do
    ~H"""
    <div class="flex items-start justify-between gap-3">
      <div class="min-w-0">
        <.section_title>{@asset.name}</.section_title>
        <.mono value={@asset.asset_ref} truncate class="mt-1 text-sm favn-text-subtle" />
      </div>
      <.status_badge
        tone={status_tone(@asset.state)}
        label={status_label(@asset.state)}
        size={:sm}
      />
    </div>
    <.inline gap={:sm} class="mt-2 text-sm favn-text-subtle">
      <span>Started {@asset.started_at || "-"}</span>
      <span>Finished {@asset.finished_at || "-"}</span>
    </.inline>
    """
  end

  defp status_tone(status) when status in [:ok, :succeeded, :skipped_fresh], do: :success
  defp status_tone(status) when status in [:error, :failed, :timed_out, :blocked], do: :error
  defp status_tone(status) when status in [:running, :retrying], do: :info
  defp status_tone(status) when status in [:queued, :planned, :pending], do: :warning
  defp status_tone(_status), do: :neutral

  defp status_label(:skipped_fresh), do: "Skipped"

  defp status_label(status),
    do: status |> to_string() |> String.replace("_", " ") |> String.capitalize()
end
