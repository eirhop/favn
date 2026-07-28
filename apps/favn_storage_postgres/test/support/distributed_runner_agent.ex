defmodule FavnStoragePostgres.TestSupport.DistributedRunnerAgent do
  @moduledoc false

  alias Favn.Contracts.RunnerTask
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.RunnerGateway

  @retry_attempts 50
  @retry_delay_ms 20

  def claim_and_start(owner, cohort_ref, gateway, runner_id, runner_pool, release_id) do
    registration = %RunnerTask.Registration{
      runner_instance_id: runner_id,
      boot_id: "boot-#{runner_id}",
      beam_node: Atom.to_string(node()),
      runner_pool: runner_pool,
      required_runner_release_id: release_id,
      lifecycle_mode: :elastic,
      supported_task_kinds: [:relation_inspection],
      capabilities: ["relation_inspection"]
    }

    with {:ok, %RunnerTask.RegistrationAck{status: :accepted} = ack} <-
           retry(fn -> RunnerGateway.register(gateway, registration, self()) end),
         {:ok, %RunnerTask.Assignment{} = assignment} <-
           retry(fn ->
             RunnerGateway.request(
               gateway,
               %RunnerTask.ClaimRequest{
                 command_id: "claim-#{runner_id}",
                 issued_at: DateTime.utc_now(),
                 runner_instance_id: runner_id,
                 runner_session_generation: ack.runner_session_generation,
                 runner_pool: runner_pool,
                 required_runner_release_id: release_id,
                 supported_task_kinds: [:relation_inspection],
                 capabilities: ["relation_inspection"]
               }
             )
           end),
         {:ok, %{status: :running}} <-
           retry(fn ->
             RunnerGateway.request(
               gateway,
               %RunnerTask.Started{
                 workspace_id: assignment.workspace_id,
                 task_id: assignment.task_id,
                 runner_instance_id: runner_id,
                 runner_session_generation: ack.runner_session_generation,
                 assignment_generation: assignment.assignment_generation,
                 issued_at: DateTime.utc_now(),
                 occurred_at: DateTime.utc_now()
               }
             )
           end) do
      {:message_queue_len, mailbox_len} = Process.info(self(), :message_queue_len)

      send(
        owner,
        {:distributed_runner_started, cohort_ref, self(), runner_id, assignment.task_id,
         runner_pool, mailbox_len}
      )

      receive do
        :stop -> :ok
      after
        120_000 -> :ok
      end
    else
      failure ->
        send(owner, {:distributed_runner_failed, cohort_ref, runner_id, failure})
    end
  end

  defp retry(fun, attempts \\ @retry_attempts)

  defp retry(fun, attempts) do
    case fun.() do
      {:error, %Error{kind: :unavailable, retryable?: true}} when attempts > 1 ->
        Process.sleep(@retry_delay_ms)
        retry(fun, attempts - 1)

      {:error, :runner_gateway_overloaded} when attempts > 1 ->
        Process.sleep(@retry_delay_ms)
        retry(fun, attempts - 1)

      result ->
        result
    end
  end
end
