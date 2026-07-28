defmodule CrmDemo.Integrations.Crm.ClientTest do
  use ExUnit.Case, async: true

  alias CrmDemo.Integrations.Crm.Client

  doctest Client

  setup do
    {:ok, client: Client.new("https://crm.test/v1", "test-token")}
  end

  test "pages until the cursor runs out", %{client: client} do
    assert {:ok, first} = Client.fetch_page(client, "Accounts", page_size: 3, cursor: nil)
    assert length(first.rows) == 3
    assert first.next_cursor == "3"

    assert {:ok, last} = Client.fetch_page(client, "Accounts", page_size: 3, cursor: "3")
    assert length(last.rows) == 1
    assert last.next_cursor == nil
  end

  test "selects a half-open range on the requested date field", %{client: client} do
    assert {:ok, page} =
             Client.fetch_page(client, "Deals",
               page_size: 100,
               cursor: nil,
               date_field: "OccurredAt",
               changed_since: ~U[2026-07-23 00:00:00Z],
               changed_until: ~U[2026-07-24 00:00:00Z]
             )

    assert Enum.map(page.rows, & &1["DealId"]) == ["deal_003", "deal_004", "deal_005"]
  end

  test "returns source field names unchanged", %{client: client} do
    assert {:ok, page} = Client.fetch_page(client, "Contacts", page_size: 1, cursor: nil)
    assert [row] = page.rows
    assert Map.keys(row) == ["AccountId", "ContactId", "FullName"]
  end
end
