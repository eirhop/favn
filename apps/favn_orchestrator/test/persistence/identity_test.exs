defmodule FavnOrchestrator.Persistence.IdentityTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Identity

  test "accepts identities immediately below and at the byte boundary" do
    assert :ok = Identity.validate(:runner_execution_id, String.duplicate("a", 254))
    assert :ok = Identity.validate(:runner_execution_id, String.duplicate("a", 255))
  end

  test "reports the field and byte boundary above the limit" do
    assert {:error,
            %Error{
              kind: :invalid,
              details: %{
                field: :runner_execution_id,
                actual_bytes: 256,
                max_bytes: 255
              }
            } = error} =
             Identity.validate(:runner_execution_id, String.duplicate("a", 256))

    assert error.message =~ "runner_execution_id"
    assert error.message =~ "256 bytes"
    assert error.message =~ "255 bytes"
  end
end
