defmodule FavnOrchestrator.RunSubmission.WorkerTest do
  use ExUnit.Case, async: false

  alias FavnOrchestrator.Lifecycle
  alias FavnOrchestrator.Persistence.RunSubmissionAuthority
  alias FavnOrchestrator.Persistence.Results.RunSubmission
  alias FavnOrchestrator.RunSubmission.Worker

  defmodule Store do
    def claim_stale(_command), do: {:ok, []}

    def claim(_command) do
      Agent.get_and_update(agent(), fn
        %{submission: submission} = state ->
          {{:ok, [submission]}, state}
      end)
    end

    def renew(_command) do
      Agent.get_and_update(agent(), fn state ->
        submission =
          if state.cancel_on_renew? do
            %{state.submission | cancellation_requested_at: DateTime.utc_now()}
          else
            state.submission
          end

        {{:ok, submission}, %{state | renewals: state.renewals + 1}}
      end)
    end

    defp agent, do: :persistent_term.get({__MODULE__, :agent})
  end

  defmodule SlowProcessor do
    def process(submission, _opts) do
      send(:persistent_term.get({__MODULE__, :test}), {:processor_started, self(), submission})

      if submission.cancellation_requested_at do
        :cancelled
      else
        Process.sleep(80)
        :processed
      end
    end
  end

  defmodule CrashingProcessor do
    def process(_submission, _opts), do: exit(:processor_crashed)
  end

  setup do
    submission = submission()

    {:ok, agent} =
      Agent.start_link(fn -> %{submission: submission, renewals: 0, cancel_on_renew?: false} end)

    :persistent_term.put({Store, :agent}, agent)
    :persistent_term.put({SlowProcessor, :test}, self())

    lifecycle =
      start_supervised!({Lifecycle, name: unique_name(), shutdown_drain_timeout_ms: 1_000})

    :ok = Lifecycle.mark_accepting(lifecycle)

    on_exit(fn ->
      :persistent_term.erase({Store, :agent})
      :persistent_term.erase({SlowProcessor, :test})
    end)

    {:ok, agent: agent, lifecycle: lifecycle}
  end

  test "renews a live claim while preparation runs", fixture do
    assert :processed =
             Worker.run("workspace-a",
               store: Store,
               lifecycle: fixture.lifecycle,
               owner_id: "worker-a",
               lease_duration_ms: 1_000,
               renewal_interval_ms: 10,
               processor: SlowProcessor
             )

    assert Agent.get(fixture.agent, & &1.renewals) >= 2
  end

  test "stops preparation and acknowledges cancellation observed during renewal", fixture do
    Agent.update(fixture.agent, &%{&1 | cancel_on_renew?: true})

    assert :cancelled =
             Worker.run("workspace-a",
               store: Store,
               lifecycle: fixture.lifecycle,
               owner_id: "worker-a",
               lease_duration_ms: 1_000,
               renewal_interval_ms: 10,
               processor: SlowProcessor
             )

    assert_received {:processor_started, _pid, %RunSubmission{cancellation_requested_at: nil}}

    assert_received {:processor_started, _pid,
                     %RunSubmission{cancellation_requested_at: %DateTime{}}}
  end

  test "contains processor crashes instead of taking down the caller", fixture do
    assert {:error, {:run_submission_processor_exit, :processor_crashed}} =
             Worker.run("workspace-a",
               store: Store,
               lifecycle: fixture.lifecycle,
               owner_id: "worker-a",
               lease_duration_ms: 1_000,
               renewal_interval_ms: 100,
               processor: CrashingProcessor
             )
  end

  test "terminates preparation when the owning worker is killed", fixture do
    {worker_pid, worker_monitor} =
      spawn_monitor(fn ->
        Worker.run("workspace-a",
          store: Store,
          lifecycle: fixture.lifecycle,
          owner_id: "worker-a",
          lease_duration_ms: 1_000,
          renewal_interval_ms: 100,
          processor: SlowProcessor
        )
      end)

    assert_receive {:processor_started, processor_pid, %RunSubmission{}}
    processor_monitor = Process.monitor(processor_pid)
    Process.exit(worker_pid, :kill)

    assert_receive {:DOWN, ^worker_monitor, :process, ^worker_pid, :killed}
    assert_receive {:DOWN, ^processor_monitor, :process, ^processor_pid, :killed}
  end

  defp submission do
    now = DateTime.utc_now()

    %RunSubmission{
      workspace_id: "workspace-a",
      submission_id: "submission-a",
      source: :operator,
      idempotency_key: "idempotency-a",
      request_hash: :crypto.hash(:sha256, "request"),
      authority: %RunSubmissionAuthority{
        workspace_id: "workspace-a",
        principal_id: "operator-a",
        roles: [:customer_operator]
      },
      deployment_id: "deployment-a",
      manifest_version_id: "manifest-a",
      target_kind: "asset",
      target_id: "asset-a",
      run_id: "run-a",
      intent: %{"format" => "test"},
      status: :preparing,
      attempt: 1,
      claim_owner: "worker-a",
      claim_generation: 1,
      claim_expires_at: DateTime.add(now, 1_000, :millisecond),
      retry_root_id: "submission-a",
      enqueued_at: now,
      available_at: now,
      inserted_at: now,
      updated_at: now
    }
  end

  defp unique_name, do: :"lifecycle_#{System.unique_integer([:positive])}"
end
