defmodule FavnView.TargetRecoveryLiveTest do
  use ExUnit.Case, async: false

  alias FavnView.Auth.Scope
  alias FavnView.TargetRecoveryLive

  @env_keys [
    :plan_operator_target_recovery_fun,
    :get_operator_target_recovery_fun
  ]

  setup do
    previous = Map.new(@env_keys, &{&1, Application.get_env(:favn_view, &1)})

    on_exit(fn ->
      Enum.each(previous, fn
        {key, nil} -> Application.delete_env(:favn_view, key)
        {key, value} -> Application.put_env(:favn_view, key, value)
      end)
    end)

    :ok
  end

  test "retries transient planning with the same durable attempt identity" do
    test_pid = self()

    Application.put_env(:favn_view, :plan_operator_target_recovery_fun, fn
      :operator_context, "asset:orders", "recover interrupted target", opts ->
        send(test_pid, {:plan_attempt, opts})

        case Process.get(:target_recovery_plan_calls, 0) do
          0 ->
            Process.put(:target_recovery_plan_calls, 1)
            {:error, :runner_task_timeout}

          1 ->
            {:ok,
             %{
               plan_id: Keyword.fetch!(opts, :operation_id),
               plan_hash: String.duplicate("a", 64)
             }}
        end
    end)

    Application.put_env(:favn_view, :get_operator_target_recovery_fun, fn
      :operator_context, operation_id ->
        {:ok,
         %{
           operation_id: operation_id,
           target_id: "asset:orders",
           reason: "recover interrupted target",
           state: :planning
         }}
    end)

    assert {:ok, mounted} = TargetRecoveryLive.mount(%{}, %{}, socket())

    browser_key = "target_recovery_plan:browser:01234567-89ab-cdef-0123-456789abcdef"

    params = %{
      "idempotency_key" => browser_key,
      "recovery" => %{
        "target_id" => "asset:orders",
        "reason" => "recover interrupted target"
      }
    }

    assert {:noreply, timed_out} =
             TargetRecoveryLive.handle_event("plan_recovery", params, mounted)

    assert_received {:plan_attempt, first_opts}
    assert timed_out.assigns.planning_attempt.operation_id == first_opts[:operation_id]
    assert first_opts[:idempotency_key] == browser_key
    refute first_opts[:idempotency_key] == first_opts[:operation_id]
    assert timed_out.assigns.operation.state == :planning

    assert {:live, :patch, %{to: patched_path}} = timed_out.redirected
    patched_params = patched_path |> URI.parse() |> Map.fetch!(:query) |> URI.decode_query()

    assert {:ok, remounted} = TargetRecoveryLive.mount(patched_params, %{}, socket())
    assert remounted.assigns.planning_attempt.operation_id == first_opts[:operation_id]
    assert remounted.assigns.reason == "recover interrupted target"

    assert {:noreply, planned} =
             TargetRecoveryLive.handle_event("plan_recovery", params, remounted)

    assert_received {:plan_attempt, second_opts}
    assert second_opts == first_opts
    assert planned.assigns.plan.plan_id == first_opts[:operation_id]
    assert is_nil(planned.assigns.planning_attempt)
  end

  test "does not resurrect an attempt from a stale URL after it becomes planned" do
    operation_id = "target_recovery_ui_" <> String.duplicate("a", 32)

    Application.put_env(:favn_view, :get_operator_target_recovery_fun, fn
      :operator_context, ^operation_id ->
        {:ok,
         %{
           operation_id: operation_id,
           target_id: "asset:orders",
           reason: "recover interrupted target",
           state: :planned
         }}
    end)

    params = %{
      "target_id" => "asset:orders",
      "operation_id" => operation_id,
      "attempt" => String.duplicate("x", 43)
    }

    assert {:ok, mounted} = TargetRecoveryLive.mount(params, %{}, socket())
    assert mounted.assigns.operation.state == :planned
    assert is_nil(mounted.assigns.planning_attempt)
  end

  defp socket do
    %Phoenix.LiveView.Socket{
      transport_pid: self(),
      assigns: %{
        __changed__: %{},
        current_scope: %Scope{operator_context: :operator_context}
      }
    }
  end
end
