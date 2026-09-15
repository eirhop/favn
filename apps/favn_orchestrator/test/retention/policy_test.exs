defmodule FavnOrchestrator.Retention.PolicyTest do
  use ExUnit.Case, async: true
  alias FavnOrchestrator.Retention.Policy

  test "optional cleanup defaults off while receipt expiry is mandatory" do
    assert {:ok, policy} = Policy.new(%{})
    assert Policy.period(policy, :logs) == :retain_forever
    assert Policy.period(policy, :receipts) == 605_100
    assert {:ok, ^policy} = policy |> Policy.encode() |> Policy.decode()
  end

  test "invalid policies fail closed and holds are canonical" do
    assert {:error, _} = Policy.new(%{unknown: 1})
    assert {:error, _} = Policy.new(%{row_limit: 0})
    assert {:error, _} = Policy.new(%{periods: %{receipts: 1}})
    assert {:error, _} = Policy.new(%{periods: %{logs: 0}})

    assert {:ok, policy} =
             Policy.new(%{
               enabled?: true,
               periods: %{logs: 86_400},
               excluded_workspace_ids: ["b", "a", "b"]
             })

    assert policy.excluded_workspace_ids == ["a", "b"]
    assert Policy.period(policy, :logs) == 605_100
    assert Policy.period(policy, :registry) == :retain_forever
    assert {:ok, ^policy} = policy |> Policy.encode() |> Policy.decode()
  end
end
