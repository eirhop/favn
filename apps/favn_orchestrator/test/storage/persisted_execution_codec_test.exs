defmodule FavnOrchestrator.Storage.PersistedExecutionCodecTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.ExecutionAdmission.Waiter
  alias FavnOrchestrator.Storage.ExecutionAdmissionWaiterCodec
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

  describe "execution admission waiter storage" do
    test "round-trips through JSON and rejects Erlang terms" do
      waiter = waiter_fixture()

      assert {:ok, payload} = ExecutionAdmissionWaiterCodec.encode(waiter)
      assert {:ok, %{"format" => "json-v1"}} = Jason.decode(payload)
      assert {:ok, ^waiter} = ExecutionAdmissionWaiterCodec.decode(payload)

      erlang_term = waiter |> :erlang.term_to_binary() |> Base.encode64()
      assert {:error, _reason} = ExecutionAdmissionWaiterCodec.decode(erlang_term)
    end

    test "validates blocked scope membership and queue reason" do
      waiter = waiter_fixture()

      assert {:error, {:invalid_execution_admission_waiter_field, :blocked_scope}} =
               waiter
               |> Map.put(:requested_scopes, [%{kind: :run, key: "run-1", limit: 1}])
               |> ExecutionAdmissionWaiterCodec.normalize()

      assert {:error, {:invalid_execution_admission_waiter_field, :queue_reason}} =
               waiter
               |> Map.put(:queue_reason, :pipeline_concurrency)
               |> ExecutionAdmissionWaiterCodec.normalize()
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

  defp waiter_fixture do
    %Waiter{
      waiter_id: "waiter-1",
      run_id: "run-1",
      asset_step_id: "step-1",
      queue_reason: :global_concurrency,
      blocked_scope: %{kind: :global, key: "default", limit: 4},
      requested_scopes: [%{kind: :global, key: "default", limit: 4}],
      stage: 0,
      attempt: 1,
      inserted_at: @now,
      updated_at: @now,
      deadline_at: DateTime.add(@now, 30, :second),
      wake_generation: 0
    }
  end
end
