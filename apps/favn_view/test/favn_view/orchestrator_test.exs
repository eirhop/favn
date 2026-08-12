defmodule FavnView.OrchestratorTest do
  use ExUnit.Case, async: false

  alias FavnView.Orchestrator

  @persistent_key {Orchestrator, :config}

  setup do
    previous = :persistent_term.get(@persistent_key, :missing)
    Orchestrator.reset_for_test()

    on_exit(fn ->
      case previous do
        :missing -> Orchestrator.reset_for_test()
        config -> :persistent_term.put(@persistent_key, config)
      end
    end)
  end

  test "every cross-node wrapper targets an existing public facade function" do
    assert Code.ensure_loaded?(FavnOrchestrator)

    for {_kind, calls} <- Orchestrator.facade_calls(), {function, arity} <- calls do
      assert function_exported?(FavnOrchestrator, function, arity),
             "missing FavnOrchestrator.#{function}/#{arity}"
    end
  end

  test "classifies durable session and plan creation as commands" do
    commands = Orchestrator.facade_calls().command

    assert Keyword.get(commands, :operator_external_login) == 2
    assert Keyword.get(commands, :operator_password_login) == 4
    assert Keyword.get(commands, :plan_operator_rebuild) == 4
    assert Keyword.get(commands, :plan_operator_target_recovery) == 4
  end

  test "validates a bounded private remote-node contract" do
    assert {:ok, config} =
             Orchestrator.validate(%{
               "FAVN_CONTROL_PLANE_NODE" => "favn_orchestrator@orchestrator.internal",
               "FAVN_VIEW_ORCHESTRATOR_CALL_TIMEOUT_MS" => "2500"
             })

    assert config == %{
             target_node: :"favn_orchestrator@orchestrator.internal",
             call_timeout_ms: 2_500
           }

    assert {:error, {:missing_env, "FAVN_CONTROL_PLANE_NODE"}} = Orchestrator.validate(%{})

    assert {:error, {:invalid_env, "FAVN_VIEW_ORCHESTRATOR_CALL_TIMEOUT_MS", "100..120000"}} =
             Orchestrator.validate(%{
               "FAVN_CONTROL_PLANE_NODE" => "favn_orchestrator@orchestrator.internal",
               "FAVN_VIEW_ORCHESTRATOR_CALL_TIMEOUT_MS" => "0"
             })
  end

  test "transport loss distinguishes reads from commands without retrying" do
    :persistent_term.put(@persistent_key, %{
      target_node: :"missing@orchestrator.internal",
      call_timeout_ms: 100
    })

    assert {:error, :orchestrator_unavailable} = Orchestrator.active_asset_catalogue(%{})

    assert {:error, :orchestrator_outcome_unknown} =
             Orchestrator.enable_schedule(%{}, "schedule-id", "idempotency-key")

    assert FavnOrchestrator.operator_command_retryable?(:orchestrator_outcome_unknown)
  end

  test "subscriptions remotely authorize before caller-local activation" do
    :persistent_term.put(@persistent_key, %{
      target_node: :"missing@orchestrator.internal",
      call_timeout_ms: 100
    })

    assert {:error, :orchestrator_unavailable} = Orchestrator.subscribe_run(%{}, "run-id")
    assert {:error, :orchestrator_unavailable} = Orchestrator.subscribe_runs(%{})
    assert {:error, :orchestrator_unavailable} = Orchestrator.subscribe_logs(%{}, %{})

    reads = Orchestrator.facade_calls().read
    assert Keyword.get(reads, :authorize_run_subscription) == 2
    assert Keyword.get(reads, :authorize_runs_subscription) == 1
    assert Keyword.get(reads, :authorize_logs_subscription) == 2
  end
end
