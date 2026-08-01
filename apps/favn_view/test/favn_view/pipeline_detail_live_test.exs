defmodule FavnView.PipelineDetailLiveTest do
  use ExUnit.Case, async: false

  alias FavnView.Auth.Scope
  alias FavnView.PipelineDetailLive

  @env_keys [:submit_operator_run_fun]

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

  test "a windowed pipeline submits without a window for latest complete resolution" do
    test_pid = self()

    Application.put_env(:favn_view, :submit_operator_run_fun, fn
      :operator_context, "mv_1", %{type: :pipeline, id: "pipeline:Example:daily"}, input, opts
      when input == %{} ->
        send(test_pid, {:submitted, opts})
        {:error, :forbidden}
    end)

    pipeline = %{
      id: "pipeline:Example:daily",
      manifest_version_id: "mv_1",
      can_run_without_window?: false
    }

    assert {:noreply, _socket} =
             PipelineDetailLive.handle_event("run_pipeline", %{}, socket(pipeline))

    assert_received {:submitted, opts}
    assert is_binary(opts[:idempotency_key])
  end

  defp socket(pipeline) do
    %Phoenix.LiveView.Socket{
      transport_pid: self(),
      assigns: %{
        __changed__: %{},
        current_scope: %Scope{operator_context: :operator_context},
        pipeline: pipeline,
        run_attempt: nil
      }
    }
  end
end
