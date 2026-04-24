defmodule FavnReferenceWorkload.Warehouse.Raw.Payments do
  @moduledoc """
  Deterministic payment ingest from a simulated API JSON payload.

  This Elixir asset provides payment lifecycle signals (`succeeded`/`failed`)
  and paid amounts by ingesting JSON rows into `raw.payments`.

  Downstream logic uses this to:

  - include only succeeded payments in fact metrics
  - calculate revenue from payment amounts

  Alternative:

  - Add refund or chargeback rows to simulate more realistic finance behavior.
  """

  use Favn.Namespace
  use Favn.Asset

  alias FavnReferenceWorkload.Client.DuckDBJSONLoader
  alias FavnReferenceWorkload.Client.FakeAPI

  @meta owner: "reference-workload", category: :payments, tags: [:raw, :synthetic]
  @depends FavnReferenceWorkload.Warehouse.Raw.Orders
  @relation true
  def asset(ctx) do
    with {:ok, rows} <- FakeAPI.fetch_rows(:payments) do
      DuckDBJSONLoader.replace_relation_from_rows(
        ctx.asset.relation,
        :payments,
        rows,
        payments_select_sql()
      )
    end
  end

  defp payments_select_sql do
    """
    select
      payment_id,
      order_id,
      payment_status,
      cast(paid_at as timestamp) as paid_at,
      amount_cents
    from __RAW_JSON_SOURCE__
    where order_id in (
      select order_id
      from raw.orders
    )
    """
  end
end
