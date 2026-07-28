defmodule CrmDemo.Integrations.Crm.Client do
  @moduledoc """
  Stand-in transport for the CRM source system.

  A real client would build HTTP requests, authenticate, page through
  responses, and normalize transport errors. This one serves a fixed dataset
  from module attributes so the tutorial runs with no network, but it keeps the
  shape a real client has:

  - it is constructed from resolved runtime configuration;
  - it returns source-shaped rows with the source system's own field names; and
  - it pages with an opaque cursor.

  Integration code returns source data. It never writes landing files and never
  models data. Replace this module and everything downstream keeps working.
  """

  @enforce_keys [:base_url, :token]
  defstruct [:base_url, :token]

  @type t :: %__MODULE__{base_url: String.t() | nil, token: String.t() | nil}

  @type page :: %{rows: [map()], next_cursor: String.t() | nil}

  @accounts [
    %{
      "AccountId" => "acct_001",
      "Name" => "Northwind Labs",
      "Segment" => "mid_market",
      "Industry" => "software"
    },
    %{
      "AccountId" => "acct_002",
      "Name" => "Harbor Health",
      "Segment" => "enterprise",
      "Industry" => "healthcare"
    },
    %{
      "AccountId" => "acct_003",
      "Name" => "Pinecone Retail",
      "Segment" => "smb",
      "Industry" => "retail"
    },
    %{
      "AccountId" => "acct_004",
      "Name" => "Vela Logistics",
      "Segment" => "mid_market",
      "Industry" => "transport"
    }
  ]

  @contacts [
    %{"ContactId" => "cont_001", "AccountId" => "acct_001", "FullName" => "Ada Lovelace"},
    %{"ContactId" => "cont_002", "AccountId" => "acct_001", "FullName" => "Alan Turing"},
    %{"ContactId" => "cont_003", "AccountId" => "acct_002", "FullName" => "Grace Hopper"},
    %{"ContactId" => "cont_004", "AccountId" => "acct_003", "FullName" => "Linus Torvalds"},
    %{"ContactId" => "cont_005", "AccountId" => "acct_003", "FullName" => "Radia Perlman"}
  ]

  @deals [
    %{
      "DealId" => "deal_001",
      "AccountId" => "acct_001",
      "Stage" => "qualified",
      "AmountCents" => 12_000,
      "OccurredAt" => "2026-07-22T10:00:00Z"
    },
    %{
      "DealId" => "deal_002",
      "AccountId" => "acct_003",
      "Stage" => "proposal",
      "AmountCents" => 30_000,
      "OccurredAt" => "2026-07-22T16:45:00Z"
    },
    %{
      "DealId" => "deal_003",
      "AccountId" => "acct_002",
      "Stage" => "proposal",
      "AmountCents" => 45_000,
      "OccurredAt" => "2026-07-23T11:30:00Z"
    },
    %{
      "DealId" => "deal_004",
      "AccountId" => "acct_003",
      "Stage" => "won",
      "AmountCents" => 8_000,
      "OccurredAt" => "2026-07-23T15:00:00Z"
    },
    %{
      "DealId" => "deal_005",
      "AccountId" => "acct_004",
      "Stage" => "qualified",
      "AmountCents" => 21_500,
      "OccurredAt" => "2026-07-23T17:20:00Z"
    }
  ]

  @activities [
    %{
      "ActivityId" => "act_001",
      "AccountId" => "acct_001",
      "ActivityType" => "call",
      "OccurredAt" => "2026-07-22T09:00:00Z"
    },
    %{
      "ActivityId" => "act_002",
      "AccountId" => "acct_002",
      "ActivityType" => "email",
      "OccurredAt" => "2026-07-23T09:30:00Z"
    },
    %{
      "ActivityId" => "act_003",
      "AccountId" => "acct_003",
      "ActivityType" => "meeting",
      "OccurredAt" => "2026-07-23T14:00:00Z"
    },
    %{
      "ActivityId" => "act_004",
      "AccountId" => "acct_003",
      "ActivityType" => "call",
      "OccurredAt" => "2026-07-23T15:45:00Z"
    },
    %{
      "ActivityId" => "act_005",
      "AccountId" => "acct_004",
      "ActivityType" => "email",
      "OccurredAt" => "2026-07-23T18:10:00Z"
    }
  ]

  @endpoints %{
    "Accounts" => @accounts,
    "Contacts" => @contacts,
    "Deals" => @deals,
    "Activities" => @activities
  }

  @doc """
  Builds a client from resolved runtime configuration.

      iex> CrmDemo.Integrations.Crm.Client.new("https://crm.test/v1", "token").base_url
      "https://crm.test/v1"
  """
  @spec new(String.t() | nil, String.t() | nil) :: t()
  def new(base_url, token), do: %__MODULE__{base_url: base_url, token: token}

  @doc """
  Fetches one page of records.

  Options:

  - `:page_size` - maximum rows in the page
  - `:cursor` - opaque cursor returned by the previous page, or `nil` to start
  - `:date_field` - source field used to select a time range
  - `:changed_since` / `:changed_until` - half-open range applied to `:date_field`

  Returns rows plus the cursor for the next page, or `nil` when the page is the
  last one.
  """
  @spec fetch_page(t(), String.t(), keyword()) :: {:ok, page()}
  def fetch_page(%__MODULE__{}, endpoint, opts) do
    offset = String.to_integer(Keyword.get(opts, :cursor) || "0")
    page_size = Keyword.fetch!(opts, :page_size)

    matching =
      @endpoints
      |> Map.fetch!(endpoint)
      |> select_range(opts)

    rows = Enum.slice(matching, offset, page_size)
    next_offset = offset + length(rows)
    next_cursor = if next_offset < length(matching), do: Integer.to_string(next_offset)

    {:ok, %{rows: rows, next_cursor: next_cursor}}
  end

  defp select_range(rows, opts) do
    case Keyword.get(opts, :changed_since) do
      nil ->
        rows

      since ->
        field = Keyword.fetch!(opts, :date_field)
        until = Keyword.fetch!(opts, :changed_until)

        Enum.filter(rows, fn row ->
          changed_at = parse!(Map.fetch!(row, field))

          DateTime.compare(changed_at, since) != :lt and
            DateTime.compare(changed_at, until) == :lt
        end)
    end
  end

  defp parse!(value) do
    {:ok, at, 0} = DateTime.from_iso8601(value)
    at
  end
end
