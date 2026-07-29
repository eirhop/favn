defmodule FavnStoragePostgres.CanonicalJSONTest do
  use ExUnit.Case, async: true

  alias FavnStoragePostgres.CanonicalJSON

  test "preserves JSON booleans while stringifying enum atoms" do
    assert {:ok, encoded} =
             CanonicalJSON.encode(%{
               enabled: true,
               disabled: false,
               absent: nil,
               outcome: :safe_failure
             })

    assert Jason.decode!(encoded) == %{
             "absent" => nil,
             "disabled" => false,
             "enabled" => true,
             "outcome" => "safe_failure"
           }
  end
end
