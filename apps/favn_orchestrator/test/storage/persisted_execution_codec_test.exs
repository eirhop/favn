defmodule FavnOrchestrator.Storage.PersistedExecutionCodecTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.Storage.ExecutionLeaseCodec

  @now ~U[2026-07-13 12:00:00Z]

  describe "execution lease storage" do
    test "round-trips through JSON and rejects Erlang terms" do
      lease = lease_fixture()

      assert {:ok, payload} = ExecutionLeaseCodec.encode(lease)
      assert {:ok, %{"format" => "json-v1"}} = Jason.decode(payload)
      assert {:ok, ^lease} = ExecutionLeaseCodec.decode(payload)

      erlang_term = lease |> :erlang.term_to_binary() |> Base.encode64()
      assert {:error, _reason} = ExecutionLeaseCodec.decode(erlang_term)
    end

    test "rejects unknown scopes and non-forward expiry" do
      assert {:error, {:invalid_execution_lease_field, :kind}} =
               lease_fixture()
               |> put_in([:scopes], [%{kind: :invented, key: "run-1", limit: 1}])
               |> ExecutionLeaseCodec.normalize()

      assert {:error, {:invalid_execution_lease_field, :expires_at}} =
               lease_fixture()
               |> Map.put(:expires_at, @now)
               |> ExecutionLeaseCodec.normalize()
    end
  end

  defp lease_fixture do
    %{
      lease_id: "lease-1",
      run_id: "run-1",
      asset_step_id: "step-1",
      scopes: [%{kind: :run, key: "run-1", limit: 1}],
      acquired_at: @now,
      expires_at: DateTime.add(@now, 30, :second)
    }
  end
end
