defmodule FavnReferenceWorkload.CRMData do
  @moduledoc """
  Deterministic CRM records used by the temporary example workload.

  The data is intentionally small enough to inspect in the UI and stable enough
  for focused DuckDB assertions.
  """

  @spec seed() :: %{accounts: [map()], contacts: [map()], deals: [map()], activities: [map()]}
  def seed do
    %{
      accounts: [
        %{account_id: "acct_001", name: "Northwind Labs", segment: "mid_market"},
        %{account_id: "acct_002", name: "Harbor Health", segment: "enterprise"},
        %{account_id: "acct_003", name: "Pinecone Retail", segment: "smb"}
      ],
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
end
