defmodule Favn.ResourceRecovery.PolicyTest do
  use ExUnit.Case, async: true

  alias Favn.ResourceRecovery.Policy

  test "builds the bounded retry-remaining policy" do
    assert {:ok, %Policy{mode: :retry_remaining, max_age_ms: 3_600_000}} =
             Policy.new(:retry_remaining, max_age_ms: 3_600_000)

    assert {:ok, %Policy{mode: :retry_remaining, max_age_ms: 3_600_000}} =
             Policy.from_value(%{"mode" => "retry_remaining", "max_age_ms" => 3_600_000})
  end

  test "rejects unknown modes and options" do
    assert {:error, {:invalid_resource_recovery_mode, :restart}} = Policy.new(:restart)

    assert {:error, {:unknown_resource_recovery_options, [:concurrency]}} =
             Policy.new(:retry_remaining, concurrency: 10)
  end
end
