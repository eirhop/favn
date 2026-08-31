defmodule FavnView.PipelineDetailLiveTest do
  use ExUnit.Case, async: false

  alias FavnView.Auth.Scope
  alias FavnView.PipelineDetailLive
  alias FavnView.PipelineRunConfig

  @env_keys [:submit_operator_run_fun, :submit_operator_pipeline_backfill_fun]

  @pipeline %{
    id: "pipeline:Example:daily",
    manifest_version_id: "mv_1",
    window: %{"kind" => "day", "timezone" => "Etc/UTC", "combine_windows" => false}
  }

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

  describe "the dialog" do
    test "opens on what the pipeline declares, whatever the last attempt left behind" do
      socket = socket(run_config: %{defaults() | from: "2026-01-01", refresh: "force_all"})

      assert {:noreply, opened} = PipelineDetailLive.handle_event("open_run_dialog", %{}, socket)

      assert opened.assigns.run_dialog_open?
      assert opened.assigns.run_config == defaults()
      refute opened.assigns.run_advanced_open?
    end

    test "a change keeps the disclosure open and reports why it cannot submit" do
      params = %{"run_config" => %{"to" => "2026-01-03"}}

      assert {:noreply, changed} =
               PipelineDetailLive.handle_event("change_run_config", params, socket())

      assert changed.assigns.run_advanced_open?
      refute changed.assigns.run_config_valid?
      assert changed.assigns.run_error == "A range needs a period to start from."
    end

    test "reset returns to the declared configuration" do
      socket = socket(run_config: %{defaults() | from: "2026-01-01"}, run_error: "nope")

      assert {:noreply, reset} = PipelineDetailLive.handle_event("reset_run_config", %{}, socket)

      assert reset.assigns.run_config == defaults()
      assert reset.assigns.run_error == nil
    end
  end

  describe "submitting" do
    test "no period sends no window, so the control plane resolves the latest complete one" do
      expect_run(fn input ->
        refute Map.has_key?(input, :window)
        assert input.refresh_mode == "auto"
      end)

      assert {:noreply, _socket} =
               PipelineDetailLive.handle_event("submit_pipeline_run", %{}, socket())

      assert_received {:submitted, opts}
      assert is_binary(opts[:idempotency_key])
    end

    test "a single period runs that window" do
      expect_run(fn input ->
        assert input.window == %{kind: "day", value: "2026-01-01", timezone: "Etc/UTC"}
      end)

      params = %{"run_config" => %{"from" => "2026-01-01"}}

      assert {:noreply, _socket} =
               PipelineDetailLive.handle_event("submit_pipeline_run", params, socket())

      assert_received {:submitted, _opts}
    end

    test "a range submits a backfill with the combine-windows choice" do
      test_pid = self()

      Application.put_env(:favn_view, :submit_operator_pipeline_backfill_fun, fn
        :operator_context, "mv_1", "pipeline:Example:daily", input, opts ->
          send(test_pid, {:backfill_submitted, input, opts})
          {:error, :forbidden}
      end)

      params = %{
        "run_config" => %{
          "from" => "2026-01-01",
          "to" => "2026-01-03",
          "kind" => "day",
          "refresh" => "force_all",
          "combine_windows" => "true"
        }
      }

      assert {:noreply, _socket} =
               PipelineDetailLive.handle_event("submit_pipeline_run", params, socket())

      assert_received {:backfill_submitted, input, opts}

      assert input.range == %{
               kind: "day",
               from: "2026-01-01",
               to: "2026-01-03",
               timezone: "Etc/UTC"
             }

      assert input.refresh_mode == "force_all"
      assert input.combine_windows
      assert is_binary(opts[:idempotency_key])
    end

    test "an invalid configuration is refused before any command is sent" do
      Application.put_env(:favn_view, :submit_operator_run_fun, fn _c, _m, _t, _i, _o ->
        flunk("submitted an invalid configuration")
      end)

      params = %{"run_config" => %{"to" => "2026-01-03"}}

      assert {:noreply, refused} =
               PipelineDetailLive.handle_event("submit_pipeline_run", params, socket())

      assert refused.assigns.run_error == "A range needs a period to start from."
      refute refused.assigns.run_config_valid?
    end

    test "a rejected submission keeps the dialog open with the reason" do
      expect_run(fn _input -> :ok end)

      assert {:noreply, rejected} =
               PipelineDetailLive.handle_event("submit_pipeline_run", %{}, socket())

      assert rejected.assigns.run_error == "Operator role required to submit runs."
    end
  end

  defp expect_run(assertion) do
    test_pid = self()

    Application.put_env(:favn_view, :submit_operator_run_fun, fn
      :operator_context, "mv_1", %{type: :pipeline, id: "pipeline:Example:daily"}, input, opts ->
        assertion.(input)
        send(test_pid, {:submitted, opts})
        {:error, :forbidden}
    end)
  end

  defp defaults, do: PipelineRunConfig.default(@pipeline)

  defp socket(overrides \\ []) do
    assigns =
      Enum.into(overrides, %{
        __changed__: %{},
        current_scope: %Scope{operator_context: :operator_context},
        pipeline: @pipeline,
        run_attempt: nil,
        run_config: defaults(),
        run_config_defaults: defaults(),
        run_config_valid?: true,
        run_advanced_open?: false,
        run_dialog_open?: true,
        run_error: nil
      })

    %Phoenix.LiveView.Socket{transport_pid: self(), assigns: assigns}
  end
end
