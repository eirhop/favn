defmodule FavnReferenceWorkload.Client.FakeAPI do
  @moduledoc """
  Deterministic fake API client for reference-workload raw datasets.

  This simulates an external service returning JSON-shaped rows.

  Most datasets need only the dataset name. The source-system orders path also
  accepts a narrow source config map so the tutorial can show resolved runtime
  config flowing from `ctx.config` without passing the full runtime context.

  Best-practice point shown here:

  - do not pass full `ctx` into helpers unless they actually need runtime data
  - pass only the narrow source config a client actually needs
  - keeping the API small makes the client easier to understand and reuse
  """

  @type dataset :: :customers | :products | :orders | :order_items | :payments
  @type source_config :: %{required(:segment_id) => String.t(), optional(atom()) => term()}

  @spec fetch_rows(dataset()) :: {:ok, [map()]} | {:error, term()}
  def fetch_rows(dataset)
      when dataset in [:customers, :products, :orders, :order_items, :payments] do
    {:ok, rows_for(dataset)}
  end

  def fetch_rows(_dataset), do: {:error, :invalid_dataset}

  @spec fetch_rows(dataset(), source_config()) :: {:ok, [map()]} | {:error, term()}
  def fetch_rows(:orders, %{segment_id: "source-failure"}) do
    {:error, {:source_unavailable, :orders}}
  end

  def fetch_rows(:orders, %{segment_id: segment_id}) when is_binary(segment_id),
    do: {:ok, rows_for(:orders)}

  def fetch_rows(:orders, _source_config), do: {:error, :missing_segment_id}

  def fetch_rows(dataset, _source_config), do: fetch_rows(dataset)

  defp rows_for(:customers) do
    [
      %{
        "customer_id" => 1,
        "customer_code" => "C-001",
        "region_code" => "nordic",
        "country_code" => "NO",
        "signup_date" => "2026-01-01"
      },
      %{
        "customer_id" => 2,
        "customer_code" => "C-002",
        "region_code" => "nordic",
        "country_code" => "SE",
        "signup_date" => "2026-01-03"
      },
      %{
        "customer_id" => 3,
        "customer_code" => "C-003",
        "region_code" => "dach",
        "country_code" => "DE",
        "signup_date" => "2026-01-05"
      },
      %{
        "customer_id" => 4,
        "customer_code" => "C-004",
        "region_code" => "dach",
        "country_code" => "AT",
        "signup_date" => "2026-01-07"
      },
      %{
        "customer_id" => 5,
        "customer_code" => "C-005",
        "region_code" => "uk_ie",
        "country_code" => "GB",
        "signup_date" => "2026-01-09"
      },
      %{
        "customer_id" => 6,
        "customer_code" => "C-006",
        "region_code" => "southern_eu",
        "country_code" => "ES",
        "signup_date" => "2026-01-11"
      }
    ]
  end

  defp rows_for(:products) do
    [
      %{
        "product_id" => 101,
        "sku" => "SKU-101",
        "product_name" => "Nordic Trail Jacket",
        "category" => "outerwear",
        "unit_price_cents" => 12_900
      },
      %{
        "product_id" => 102,
        "sku" => "SKU-102",
        "product_name" => "Summit Merino Base Layer",
        "category" => "baselayer",
        "unit_price_cents" => 6_900
      },
      %{
        "product_id" => 103,
        "sku" => "SKU-103",
        "product_name" => "City Rain Shell",
        "category" => "outerwear",
        "unit_price_cents" => 9_900
      },
      %{
        "product_id" => 104,
        "sku" => "SKU-104",
        "product_name" => "Everyday Travel Pack",
        "category" => "accessories",
        "unit_price_cents" => 7_900
      }
    ]
  end

  defp rows_for(:orders) do
    [
      %{
        "order_id" => 1001,
        "customer_id" => 1,
        "channel_code" => "organic_search",
        "order_date" => "2026-02-01"
      },
      %{
        "order_id" => 1002,
        "customer_id" => 2,
        "channel_code" => "paid_social",
        "order_date" => "2026-02-01"
      },
      %{
        "order_id" => 1003,
        "customer_id" => 3,
        "channel_code" => "email",
        "order_date" => "2026-02-02"
      },
      %{
        "order_id" => 1004,
        "customer_id" => 1,
        "channel_code" => "email",
        "order_date" => "2026-02-03"
      },
      %{
        "order_id" => 1005,
        "customer_id" => 4,
        "channel_code" => "affiliate",
        "order_date" => "2026-02-03"
      },
      %{
        "order_id" => 1006,
        "customer_id" => 5,
        "channel_code" => "organic_search",
        "order_date" => "2026-02-04"
      }
    ]
  end

  defp rows_for(:order_items) do
    [
      %{"order_item_id" => 5001, "order_id" => 1001, "product_id" => 101, "quantity" => 1},
      %{"order_item_id" => 5002, "order_id" => 1001, "product_id" => 104, "quantity" => 1},
      %{"order_item_id" => 5003, "order_id" => 1002, "product_id" => 102, "quantity" => 2},
      %{"order_item_id" => 5004, "order_id" => 1003, "product_id" => 103, "quantity" => 1},
      %{"order_item_id" => 5005, "order_id" => 1004, "product_id" => 102, "quantity" => 1},
      %{"order_item_id" => 5006, "order_id" => 1005, "product_id" => 101, "quantity" => 1},
      %{"order_item_id" => 5007, "order_id" => 1006, "product_id" => 104, "quantity" => 2}
    ]
  end

  defp rows_for(:payments) do
    [
      %{
        "payment_id" => 9001,
        "order_id" => 1001,
        "payment_status" => "succeeded",
        "paid_at" => "2026-02-01 09:10:00",
        "amount_cents" => 20_800
      },
      %{
        "payment_id" => 9002,
        "order_id" => 1002,
        "payment_status" => "succeeded",
        "paid_at" => "2026-02-01 10:20:00",
        "amount_cents" => 13_800
      },
      %{
        "payment_id" => 9003,
        "order_id" => 1003,
        "payment_status" => "succeeded",
        "paid_at" => "2026-02-02 11:40:00",
        "amount_cents" => 9_900
      },
      %{
        "payment_id" => 9004,
        "order_id" => 1004,
        "payment_status" => "failed",
        "paid_at" => "2026-02-03 07:55:00",
        "amount_cents" => 6_900
      },
      %{
        "payment_id" => 9005,
        "order_id" => 1004,
        "payment_status" => "succeeded",
        "paid_at" => "2026-02-03 08:10:00",
        "amount_cents" => 6_900
      },
      %{
        "payment_id" => 9006,
        "order_id" => 1005,
        "payment_status" => "succeeded",
        "paid_at" => "2026-02-03 15:25:00",
        "amount_cents" => 12_900
      },
      %{
        "payment_id" => 9007,
        "order_id" => 1006,
        "payment_status" => "succeeded",
        "paid_at" => "2026-02-04 16:00:00",
        "amount_cents" => 15_800
      }
    ]
  end
end
