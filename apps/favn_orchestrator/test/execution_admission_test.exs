defmodule FavnOrchestrator.ExecutionAdmissionTest do
  use ExUnit.Case, async: false

  alias FavnOrchestrator.ExecutionAdmission
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage.Adapter.Memory

  setup do
    Memory.reset()

    previous_pools = Application.get_env(:favn, :execution_pools)
    previous_ttl = Application.get_env(:favn, :execution_lease_ttl_ms)

    on_exit(fn ->
      Memory.reset()
      restore_env(:favn, :execution_pools, previous_pools)
      restore_env(:favn, :execution_lease_ttl_ms, previous_ttl)
    end)

    :ok
  end

  test "acquires and releases run and pool scoped leases" do
    Application.put_env(:favn, :execution_pools, github_api: [max_concurrency: 1])

    run = run(max_concurrency: 1)
    entry = %{asset_step_id: "step-1", execution_pool: :github_api}

    assert {:ok, lease} = ExecutionAdmission.acquire(run, entry)

    assert lease.scopes == [
             %{kind: :run, key: run.id, limit: 1},
             %{kind: :pool, key: "github_api", limit: 1}
           ]

    assert {:queued, :pipeline_concurrency, %{kind: :run}} =
             ExecutionAdmission.acquire(run, %{entry | asset_step_id: "step-2"})

    assert :ok = ExecutionAdmission.release(lease)
    assert {:ok, _lease} = ExecutionAdmission.acquire(run, %{entry | asset_step_id: "step-2"})
  end

  test "global pool applies to every admitted step" do
    Application.put_env(:favn, :execution_pools, global: [max_concurrency: 1])

    run_a = run(id: "run-a")
    run_b = run(id: "run-b")

    assert {:ok, _lease} = ExecutionAdmission.acquire(run_a, %{asset_step_id: "step-a"})

    assert {:queued, :global_concurrency, %{kind: :global}} =
             ExecutionAdmission.acquire(run_b, %{asset_step_id: "step-b"})
  end

  defp run(opts) do
    RunState.new(
      id: Keyword.get(opts, :id, "run-admission"),
      manifest_version_id: "mv_admission",
      manifest_content_hash: "hash_admission",
      asset_ref: {MyApp.Asset, :asset},
      submit_kind: :pipeline,
      metadata: %{
        pipeline_execution_policy: %{
          max_concurrency: Keyword.get(opts, :max_concurrency)
        }
      }
    )
  end

  defp restore_env(app, key, nil), do: Application.delete_env(app, key)
  defp restore_env(app, key, value), do: Application.put_env(app, key, value)
end
