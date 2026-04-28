defmodule FavnReferenceWorkload.Warehouse.Raw.Orders do
  @moduledoc """
  Deterministic source-system order ingest from a simulated API JSON payload.

  This is the tutorial's canonical source-system raw landing pattern. It reads
  resolved runtime source config from `ctx.config`, calls a narrow source client,
  writes the raw payload to DuckDB through `Favn.SQLClient`, and returns structured
  run metadata for inspection.

  The SQL load validates two foreign-key-like constraints:

  - customer must exist in `raw.customers`
  - channel must exist in `sources.channel_catalog`

  In simple terms: this module shows how to keep integration logic in Elixir,
  raw landing in the raw schema, and business transformations in downstream SQL
  assets.

  Alternative:

  - Add explicit `@depends` only when dependencies are not visible in SQL.
  """

  use Favn.Namespace
  use Favn.Asset

  alias FavnReferenceWorkload.Client.DuckDBJSONLoader
  alias FavnReferenceWorkload.Client.FakeAPI

  source_config(:source_system,
    segment_id: env!("FAVN_REFERENCE_SOURCE_SEGMENT_ID"),
    token: secret_env!("FAVN_REFERENCE_SOURCE_TOKEN")
  )

  @meta owner: "reference-workload", category: :orders, tags: [:raw, :synthetic]
  @depends FavnReferenceWorkload.Warehouse.Raw.Customers
  @depends FavnReferenceWorkload.Warehouse.Sources.ChannelCatalog
  @relation true
  def asset(ctx) do
    relation = ctx.asset.relation
    source_config = ctx.config.source_system

    with {:ok, rows} <- FakeAPI.fetch_rows(:orders, source_config),
         :ok <-
           DuckDBJSONLoader.replace_relation_from_rows(
             relation,
             :orders,
             rows,
             orders_select_sql()
           ) do
      {:ok,
       %{
         rows_written: length(rows),
         mode: :full_refresh,
         relation: relation_name(relation),
         loaded_at: DateTime.utc_now(),
         source: %{
           system: :reference_source,
           segment_id_hash: hash_identity(source_config.segment_id)
         }
       }}
    end
  end

  defp orders_select_sql do
    """
    select
      order_id,
      customer_id,
      channel_code,
      cast(order_date as date) as order_date
    from __RAW_JSON_SOURCE__
    where customer_id in (
      select customer_id
      from raw.customers
    )
      and channel_code in (
        select channel_code
        from sources.channel_catalog
      )
    """
  end

  defp relation_name(relation), do: Enum.join([relation.schema, relation.name], ".")

  defp hash_identity(value) do
    :sha256
    |> :crypto.hash(to_string(value))
    |> Base.encode16(case: :lower)
  end
end
