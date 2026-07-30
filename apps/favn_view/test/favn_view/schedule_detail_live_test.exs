defmodule FavnView.ScheduleDetailLiveTest do
  use ExUnit.Case, async: false

  alias FavnView.Auth.Scope
  alias FavnView.ScheduleDetailLive

  @env_keys [
    :get_schedule_entry_fun,
    :preview_schedule_occurrences_fun,
    :enable_schedule_fun,
    :disable_schedule_fun
  ]

  @schedule_id "pipeline:CrmDemo.Pipelines.Daily:default"

  setup do
    previous = Map.new(@env_keys, &{&1, Application.get_env(:favn_view, &1)})

    on_exit(fn ->
      Enum.each(previous, fn
        {key, nil} -> Application.delete_env(:favn_view, key)
        {key, value} -> Application.put_env(:favn_view, key, value)
      end)
    end)

    stub_reads(:disabled)

    :ok
  end

  test "activation sends a command id, which the orchestrator requires" do
    stub_activation(:enable_schedule_fun, {:ok, %{effective_state: :enabled}})

    assert {:noreply, enabled} = activate(mount!(), "enable")

    assert_received {:activation, opts}
    assert is_binary(Keyword.fetch!(opts, :command_id))
    assert is_nil(enabled.assigns.activation_error)
  end

  test "a proven runtime refusal lets the next click issue a new command" do
    stub_activation(:enable_schedule_fun, {:error, :runtime_starting})

    assert {:noreply, failed} = activate(mount!(), "enable")
    assert_received {:activation, first_opts}
    assert failed.assigns.activation_error =~ "starting or draining"

    assert {:noreply, _retried} = activate(failed, "enable")
    assert_received {:activation, retry_opts}

    refute retry_opts[:command_id] == first_opts[:command_id]
  end

  test "a succeeded command does not lend its command id to the next one" do
    stub_activation(:enable_schedule_fun, {:ok, %{effective_state: :enabled}})
    stub_activation(:disable_schedule_fun, {:ok, %{effective_state: :disabled}})

    assert {:noreply, enabled} = activate(mount!(), "enable")
    assert_received {:activation, enable_opts}
    assert is_nil(enabled.assigns.activation_attempt)

    assert {:noreply, disabled} = activate(enabled, "disable")
    assert_received {:activation, disable_opts}

    refute disable_opts[:command_id] == enable_opts[:command_id]

    assert {:noreply, _reenabled} = activate(disabled, "enable")
    assert_received {:activation, reenable_opts}

    refute reenable_opts[:command_id] == enable_opts[:command_id]
  end

  test "a refusal drops the command id, because nothing was written" do
    stub_activation(:enable_schedule_fun, {:error, :forbidden})

    assert {:noreply, refused} = activate(mount!(), "enable")

    assert is_nil(refused.assigns.activation_attempt)
    assert refused.assigns.activation_error =~ "Operator role required"
  end

  test "an activation failure is reported as an activation failure" do
    stub_activation(:enable_schedule_fun, {:error, :runtime_starting})

    assert {:noreply, failed} = activate(mount!(), "enable")

    assert failed.assigns.activation_error
    assert is_nil(failed.assigns.occurrence_error)
  end

  defp mount!,
    do: elem(ScheduleDetailLive.mount(%{"schedule_id" => @schedule_id}, %{}, socket()), 1)

  defp activate(socket, action) do
    ScheduleDetailLive.handle_event("set_schedule_activation", %{"action" => action}, socket)
  end

  defp stub_activation(key, result) do
    test_pid = self()

    Application.put_env(:favn_view, key, fn :operator_context, _schedule_id, opts ->
      send(test_pid, {:activation, opts})
      result
    end)
  end

  defp stub_reads(activation_state) do
    Application.put_env(:favn_view, :get_schedule_entry_fun, fn :operator_context, _schedule_id ->
      {:ok, entry(activation_state)}
    end)

    Application.put_env(:favn_view, :preview_schedule_occurrences_fun, fn
      :operator_context, _schedule_id, _opts -> {:ok, []}
    end)
  end

  defp entry(activation_state) do
    %{
      schedule_id: "default",
      pipeline_module: CrmDemo.Pipelines.Daily,
      cron: "0 6 * * *",
      timezone: "Europe/Oslo",
      window: %{kind: :daily, timezone: "Europe/Oslo"},
      overlap: :skip,
      missed: :run_once,
      active: activation_state == :enabled,
      activation_state: activation_state,
      runtime_state: :idle,
      effective_enabled?: activation_state == :enabled,
      next_due_at: nil,
      last_evaluated_at: nil,
      last_due_at: nil,
      last_submitted_due_at: nil,
      queued_due_at: nil,
      updated_at: nil,
      in_flight_run_id: nil,
      last_scheduler_error: nil,
      manifest_version_id: "mv_1",
      manifest_content_hash: nil,
      schedule_fingerprint: String.duplicate("f", 64)
    }
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
