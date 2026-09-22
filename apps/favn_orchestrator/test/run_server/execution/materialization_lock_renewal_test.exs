defmodule FavnOrchestrator.RunServer.Execution.MaterializationLockRenewalTest do
  use ExUnit.Case, async: false

  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Runtime
  alias FavnOrchestrator.Persistence.Stores
  alias FavnOrchestrator.RunServer.Execution.ActiveTaskSet
  alias FavnOrchestrator.RunState

  defmodule ExpiredLockStore do
    def renew_many(command) do
      send(self(), {:lock_renewal, command})
      {:error, Error.new(:fenced, "target operation lock fence is stale")}
    end
  end

  setup do
    stores = struct(Stores, target_operation_locks: ExpiredLockStore)
    start_supervised!({Runtime, %Runtime{backend: __MODULE__, options: [], stores: stores}})

    claim = %{
      workspace_id: "workspace",
      claim_key: "claim",
      target_operation_lock: %{
        target_id: "target",
        operation_id: "operation",
        lease_owner: "old-owner",
        fencing_token: 7,
        version: 1
      }
    }

    %{entry: %{task_id: "task", materialization_claim: claim}}
  end

  for status <- [:succeeded, :failed, :cancelled, :unknown] do
    test "restored #{status} evidence needs no execution-lock renewal before reconciliation", %{
      entry: entry
    } do
      entry =
        Map.merge(entry, %{
          recovery_pending?: true,
          recovery_evidence: %{
            status: unquote(status),
            assignment_generation: 2,
            result_version: 1
          }
        })

      work_set = work_set(entry)
      assert :ok = ActiveTaskSet.renew_materialization_locks(work_set)
      refute_received {:lock_renewal, _}
      assert work_set.entries["task"].materialization_claim == entry.materialization_claim
      assert work_set.materialization_claims["task"] == entry.materialization_claim
    end
  end

  test "reconciled terminal work keeps its claim but needs no lock renewal", %{entry: entry} do
    work_set = work_set(Map.put(entry, :terminal_task?, true))
    assert :ok = ActiveTaskSet.renew_materialization_locks(work_set)
    refute_received {:lock_renewal, _}
    assert work_set.materialization_claims["task"] == entry.materialization_claim
  end

  test "runnable and unproven recovered work still reject an expired fence", %{entry: entry} do
    for evidence <- [nil, %{}, %{status: :queued}, %{status: :running}] do
      restored = Map.merge(entry, %{recovery_pending?: true, recovery_evidence: evidence})

      assert {:error, %Error{kind: :fenced}} =
               ActiveTaskSet.renew_materialization_locks(work_set(restored))

      assert_received {:lock_renewal, %{locks: [%{target_id: "target", fencing_token: 7}]}}
    end

    assert {:error, %Error{kind: :fenced}} =
             ActiveTaskSet.renew_materialization_locks(work_set(entry))

    assert_received {:lock_renewal, _}

    unproven =
      Map.merge(entry, %{recovery_pending?: false, recovery_evidence: %{status: :succeeded}})

    assert {:error, %Error{kind: :fenced}} =
             ActiveTaskSet.renew_materialization_locks(work_set(unproven))

    assert_received {:lock_renewal, _}
  end

  defp work_set(entry) do
    ActiveTaskSet.from_entries(%RunState{id: "run"}, [entry])
  end
end
