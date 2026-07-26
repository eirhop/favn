defmodule FavnReferenceWorkload.CRMData do
  @moduledoc """
  Deterministic CRM records used by the temporary example workload.

  The data is intentionally small enough to inspect manually and stable enough
  for focused DuckDB assertions.
  """

  alias FavnReferenceWorkload.SchemaVariant

  @schema_version SchemaVariant.schema_version!()

  @spec schema_version() :: :v1 | :v2
  def schema_version, do: @schema_version

  @spec seed(:v1 | :v2) :: %{
          accounts: [map()],
          contacts: [map()],
          deals: [map()],
          activities: [map()]
        }
  def seed(schema_version \\ @schema_version) when schema_version in [:v1, :v2] do
    %{
      accounts: accounts(schema_version),
      contacts: [
        %{contact_id: "contact_001", account_id: "acct_001", name: "Ada Lovelace"},
        %{contact_id: "contact_002", account_id: "acct_002", name: "Grace Hopper"},
        %{contact_id: "contact_003", account_id: "acct_003", name: "Linus Torvalds"}
      ],
      deals: [
        %{
          deal_id: "deal_001",
          account_id: "acct_001",
          stage: "qualified",
          amount_cents: 12_000,
          occurred_at: "2026-07-22T10:00:00Z"
        },
        %{
          deal_id: "deal_002",
          account_id: "acct_002",
          stage: "proposal",
          amount_cents: 45_000,
          occurred_at: "2026-07-23T11:30:00Z"
        },
        %{
          deal_id: "deal_003",
          account_id: "acct_003",
          stage: "won",
          amount_cents: 8_000,
          occurred_at: "2026-07-23T15:00:00Z"
        }
      ],
      activities: [
        %{
          activity_id: "activity_001",
          account_id: "acct_001",
          activity_type: "call",
          occurred_at: "2026-07-22T09:00:00Z"
        },
        %{
          activity_id: "activity_002",
          account_id: "acct_002",
          activity_type: "email",
          occurred_at: "2026-07-23T09:30:00Z"
        },
        %{
          activity_id: "activity_003",
          account_id: "acct_003",
          activity_type: "meeting",
          occurred_at: "2026-07-23T14:00:00Z"
        }
      ]
    }
  end

  defp accounts(schema_version) do
    accounts = [
      %{account_id: "acct_001", name: "Northwind Labs", segment: "mid_market"},
      %{account_id: "acct_002", name: "Harbor Health", segment: "enterprise"},
      %{account_id: "acct_003", name: "Pinecone Retail", segment: "smb"}
    ]

    case schema_version do
      :v1 ->
        accounts

      :v2 ->
        industries = ["software", "healthcare", "retail"]

        accounts
        |> Enum.zip(industries)
        |> Enum.map(fn {account, industry} -> Map.put(account, :industry, industry) end)
    end
  end
end
