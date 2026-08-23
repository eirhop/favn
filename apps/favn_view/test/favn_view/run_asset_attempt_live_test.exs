defmodule FavnView.RunAssetAttemptLiveTest do
  use ExUnit.Case, async: false

  alias FavnOrchestrator.Persistence.Results.RunAssetAttempt
  alias FavnView.Auth.Scope
  alias FavnView.RunAssetAttemptLive

  setup do
    previous = Application.get_env(:favn_view, :operator_run_asset_attempt_fun)
    on_exit(fn -> restore_env(:operator_run_asset_attempt_fun, previous) end)
  end

  test "reads the full asset payload only when its separate page connects" do
    caller = self()

    Application.put_env(:favn_view, :operator_run_asset_attempt_fun, fn
      :operator_context, "run-one", "step-one" ->
        send(caller, :detail_read)

        {:ok,
         %RunAssetAttempt{
           run_id: "run-one",
           asset_step_id: "step-one",
           asset_ref: "crm.orders",
           status: :ok,
           started_at: ~U[2026-08-23 10:00:00Z],
           finished_at: ~U[2026-08-23 10:00:04Z],
           duration_ms: 4_000,
           attempt_number: 1,
           stage: 0,
           execution_pool: "default",
           output_metadata: %{"rows_written" => 42}
         }}
    end)

    assert {:ok, mounted} =
             RunAssetAttemptLive.mount(
               %{"run_id" => "run-one", "asset_step_id" => "step-one"},
               %{},
               connected_socket()
             )

    assert_receive :detail_read
    assert mounted.assigns.attempt.name == "orders"
    assert mounted.assigns.attempt.output_metadata == %{"rows_written" => 42}
    refute mounted.assigns.loading?
  end

  test "does not duplicate the detail read during disconnected rendering" do
    caller = self()

    Application.put_env(:favn_view, :operator_run_asset_attempt_fun, fn _, _, _ ->
      send(caller, :unexpected_read)
      {:error, :not_found}
    end)

    assert {:ok, mounted} =
             RunAssetAttemptLive.mount(
               %{"run_id" => "run-one", "asset_step_id" => "step-one"},
               %{},
               disconnected_socket()
             )

    refute_receive :unexpected_read
    assert mounted.assigns.loading?
  end

  test "renders not found without exposing backend detail" do
    Application.put_env(:favn_view, :operator_run_asset_attempt_fun, fn _, _, _ ->
      {:error, :not_found}
    end)

    assert {:ok, mounted} =
             RunAssetAttemptLive.mount(
               %{"run_id" => "run-one", "asset_step_id" => "missing"},
               %{},
               connected_socket()
             )

    assert mounted.assigns.error == "Asset run not found."
    refute mounted.assigns.loading?
    assert is_nil(mounted.assigns.attempt)
  end

  test "renders a safe error when the backend read fails" do
    Application.put_env(:favn_view, :operator_run_asset_attempt_fun, fn _, _, _ ->
      {:error, :unavailable}
    end)

    assert {:ok, mounted} =
             RunAssetAttemptLive.mount(
               %{"run_id" => "run-one", "asset_step_id" => "step-one"},
               %{},
               connected_socket()
             )

    assert mounted.assigns.error == "Backend unavailable. Try again later."
    refute mounted.assigns.loading?
  end

  defp connected_socket do
    %Phoenix.LiveView.Socket{
      transport_pid: self(),
      assigns: %{__changed__: %{}, current_scope: scope()}
    }
  end

  defp disconnected_socket do
    %Phoenix.LiveView.Socket{assigns: %{__changed__: %{}, current_scope: scope()}}
  end

  defp scope do
    %Scope{
      operator_context: :operator_context,
      workspace_configuration: %FavnOrchestrator.WorkspaceConfiguration{
        workspace_id: "workspace-one",
        deployment_id: "deployment-one",
        default_timezone: "Etc/UTC",
        default_timezone_source: :application_default
      }
    }
  end

  defp restore_env(key, nil), do: Application.delete_env(:favn_view, key)
  defp restore_env(key, value), do: Application.put_env(:favn_view, key, value)
end
