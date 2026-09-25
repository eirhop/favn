defmodule FavnStoragePostgres.DistributedRunnerAgentTest do
  use ExUnit.Case, async: true

  alias Favn.Contracts.RunnerTask
  alias FavnOrchestrator.Persistence.Error
  alias FavnStoragePostgres.TestSupport.DistributedRunnerAgent

  test "retryable transport failures replay identical claim and Started commands" do
    owner = self()
    cohort = make_ref()

    task =
      Task.async(fn ->
        DistributedRunnerAgent.claim_and_start(owner, cohort, owner, "runner", "pool", "release")
      end)

    assert_receive {:"$gen_call", from, {:register, _, agent}}, 1_000
    assert agent == task.pid

    GenServer.reply(
      from,
      {:ok,
       %RunnerTask.RegistrationAck{runner_instance_id: "runner", runner_session_generation: 1}}
    )

    assert_receive {:"$gen_call", from, {:request, %RunnerTask.ClaimRequest{} = claim}}, 1_000

    GenServer.reply(
      from,
      {:error, Error.new(:unavailable, "reply unavailable", retryable?: true)}
    )

    assert_receive {:"$gen_call", from, {:request, replayed_claim}}, 1_000
    assert replayed_claim == claim

    GenServer.reply(
      from,
      {:ok,
       %RunnerTask.Assignment{
         command_id: "assignment",
         task_kind: :relation_inspection,
         runner_instance_id: "runner",
         runner_pool: "pool",
         required_runner_release_id: "release",
         assigned_at: claim.issued_at,
         lease_expires_at: DateTime.add(claim.issued_at, 60),
         retry_class: :safe_to_repeat,
         payload: nil,
         workspace_id: "workspace",
         task_id: "task",
         assignment_generation: 1
       }}
    )

    assert_receive {:"$gen_call", from, {:request, %RunnerTask.Started{} = started}}, 1_000

    GenServer.reply(
      from,
      {:error, Error.new(:unavailable, "reply unavailable", retryable?: true)}
    )

    assert_receive {:"$gen_call", from, {:request, replayed_started}}, 1_000
    assert replayed_started == started
    GenServer.reply(from, {:ok, %{status: :running}})

    assert_receive {:distributed_runner_started, ^cohort, ^agent, "runner", "task", "pool", _},
                   1_000

    send(agent, :stop)
    assert Task.await(task) == :ok
  end
end
