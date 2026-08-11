defmodule FavnOrchestrator.SubscriptionActivationTest do
  use ExUnit.Case, async: false

  alias FavnOrchestrator.Events
  alias FavnOrchestrator.Logs
  alias FavnOrchestrator.Persistence.WorkspaceContext

  setup do
    if is_nil(Process.whereis(FavnOrchestrator.PubSub)) do
      start_supervised!({Phoenix.PubSub, name: FavnOrchestrator.PubSub})
    end

    :ok
  end

  test "an authorized run grant subscribes the calling process" do
    workspace_id = "subscription-#{System.unique_integer([:positive])}"
    run_id = "run-#{System.unique_integer([:positive])}"
    grant = %{kind: :run, workspace_id: workspace_id, run_id: run_id}

    assert :ok = FavnOrchestrator.activate_run_subscription(grant)

    assert :ok =
             Phoenix.PubSub.broadcast(
               Events.pubsub_name(),
               Events.run_topic(workspace_id, run_id),
               :wake
             )

    assert_receive :wake

    operator_context = %FavnOrchestrator.OperatorContext{
      workspace_id: workspace_id,
      actor_id: "actor",
      session_id: "session"
    }

    assert :ok = FavnOrchestrator.deactivate_run_subscription(operator_context, run_id)

    assert :ok =
             Phoenix.PubSub.broadcast(
               Events.pubsub_name(),
               Events.run_topic(workspace_id, run_id),
               :stale
             )

    refute_receive :stale
  end

  test "an authorized log grant owns its forwarder in the local caller" do
    workspace_id = "subscription-#{System.unique_integer([:positive])}"
    {:ok, context} = WorkspaceContext.new(workspace_id, "subscription:test", [:customer_reader])
    {:ok, grant} = Logs.prepare_subscription(context, %{})
    test_pid = self()

    owner =
      spawn(fn ->
        {:ok, subscription} = FavnOrchestrator.activate_logs_subscription(grant)
        send(test_pid, {:ready, self(), subscription})

        receive do
          {:favn_log_entry, entry} = message ->
            send(test_pid, {:forwarded, self(), entry})
            message
        end
      end)

    assert_receive {:ready, ^owner, %{pid: forwarder} = subscription}
    forwarder_ref = Process.monitor(forwarder)

    entry = %{workspace_id: workspace_id, level: :info, source: :orchestrator}

    assert :ok =
             Phoenix.PubSub.broadcast(
               Logs.pubsub_name(),
               Logs.workspace_topic(workspace_id),
               {:favn_log_entry, entry}
             )

    assert_receive {:forwarded, ^owner, ^entry}
    assert_receive {:DOWN, ^forwarder_ref, :process, ^forwarder, :normal}
    assert :ok = FavnOrchestrator.unsubscribe_logs(subscription)
  end
end
