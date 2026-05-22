defmodule FavnOrchestrator.Auth.OperatorFacadeTest do
  use ExUnit.Case, async: false

  alias FavnOrchestrator
  alias FavnOrchestrator.Auth
  alias FavnOrchestrator.Auth.Store, as: AuthStore
  alias FavnOrchestrator.OperatorCommands.AssetBackfillRequest
  alias FavnOrchestrator.OperatorCommands.AssetRunRequest
  alias FavnOrchestrator.OperatorCommands.PipelineBackfillRequest
  alias FavnOrchestrator.OperatorCommands.PipelineRunRequest

  setup do
    previous_failure_limit = Application.get_env(:favn_orchestrator, :auth_login_failure_limit)

    previous_backoff_seconds =
      Application.get_env(:favn_orchestrator, :auth_login_backoff_seconds)

    auth_start = ensure_auth_store_started()
    :ok = AuthStore.reset()

    on_exit(fn ->
      restore_env(:auth_login_failure_limit, previous_failure_limit)
      restore_env(:auth_login_backoff_seconds, previous_backoff_seconds)
      maybe_stop_auth_store(auth_start)
    end)

    :ok
  end

  test "public operator auth facade logs in introspects revokes and checks roles" do
    assert {:ok, actor} =
             Auth.create_actor("operator", "operator-password-long", "Operator", [:operator])

    assert {:ok, session, ^actor} =
             FavnOrchestrator.operator_password_login("operator", "operator-password-long")

    assert is_binary(session.token)
    refute Map.has_key?(session, :token_hash)

    assert {:ok, introspected_session, ^actor} =
             FavnOrchestrator.introspect_operator_session(session.token)

    assert introspected_session.id == session.id
    refute Map.has_key?(introspected_session, :token_hash)

    assert FavnOrchestrator.operator_has_role?(actor, :viewer)
    assert FavnOrchestrator.operator_has_role?(actor, :operator)
    refute FavnOrchestrator.operator_has_role?(actor, :admin)

    assert :ok = FavnOrchestrator.revoke_operator_session(session.id)

    assert {:error, :invalid_session} =
             FavnOrchestrator.introspect_operator_session(session.token)
  end

  test "public operator password login applies generic rate-limit backoff" do
    Application.put_env(:favn_orchestrator, :auth_login_failure_limit, 2)
    Application.put_env(:favn_orchestrator, :auth_login_backoff_seconds, 3_600)

    assert {:ok, _actor} =
             Auth.create_actor("operator", "operator-password-long", "Operator", [:operator])

    opts = [remote_identity: "127.0.0.1"]

    assert {:error, :invalid_credentials} =
             FavnOrchestrator.operator_password_login("operator", "wrong-password", opts)

    assert {:error, :invalid_credentials} =
             FavnOrchestrator.operator_password_login("operator", "wrong-password", opts)

    assert {:error, :invalid_credentials} =
             FavnOrchestrator.operator_password_login("operator", "operator-password-long", opts)

    assert {:ok, _session, _actor} =
             FavnOrchestrator.operator_password_login("operator", "operator-password-long",
               remote_identity: "127.0.0.2"
             )
  end

  test "operator command wrappers require authenticated operator context" do
    assert {:ok, viewer} =
             Auth.create_actor("viewer", "viewer-password-long", "Viewer", [:viewer])

    assert {:ok, viewer_session, ^viewer} =
             FavnOrchestrator.operator_password_login("viewer", "viewer-password-long")

    viewer_context = %{actor: viewer, session: viewer_session}

    assert {:error, :unauthenticated} =
             FavnOrchestrator.submit_operator_run(
               %{},
               "missing_manifest",
               %{type: :asset, id: "asset:missing"},
               []
             )

    assert {:error, :forbidden} =
             FavnOrchestrator.submit_operator_run(
               viewer_context,
               "missing_manifest",
               %{type: :asset, id: "asset:missing"},
               []
             )

    assert {:error, :forbidden} =
             FavnOrchestrator.submit_operator_run(
               viewer_context,
               "missing_manifest",
               %{type: :pipeline, id: "pipeline:missing"},
               []
             )

    assert {:error, :forbidden} =
             FavnOrchestrator.submit_operator_pipeline_backfill(
               viewer_context,
               "missing_manifest",
               "pipeline:missing",
               []
             )

    assert {:ok, operator} =
             Auth.create_actor("operator", "operator-password-long", "Operator", [:operator])

    assert {:ok, operator_session, ^operator} =
             FavnOrchestrator.operator_password_login("operator", "operator-password-long")

    operator_context = %{actor: operator, session: operator_session}

    assert {:error, :manifest_version_not_found} =
             FavnOrchestrator.submit_operator_run(
               operator_context,
               "missing_manifest",
               %{type: :asset, id: "asset:missing"},
               []
             )

    assert {:error, :manifest_version_not_found} =
             FavnOrchestrator.submit_operator_run(
               operator_context,
               "missing_manifest",
               %{type: :pipeline, id: "pipeline:missing"},
               []
             )

    assert {:error, :manifest_version_not_found} =
             FavnOrchestrator.submit_operator_pipeline_backfill(
               operator_context,
               "missing_manifest",
               "pipeline:missing",
               valid_range_request()
             )
  end

  test "operator command wrappers reject caller-supplied role claims without session context" do
    assert {:ok, viewer} =
             Auth.create_actor("viewer-forged", "viewer-password-long", "Viewer", [:viewer])

    assert {:ok, viewer_session, ^viewer} =
             FavnOrchestrator.operator_password_login("viewer-forged", "viewer-password-long")

    forged_viewer_context = %{actor: %{viewer | roles: [:operator]}, session: viewer_session}

    assert {:error, :forbidden} =
             FavnOrchestrator.submit_operator_pipeline_run(
               forged_viewer_context,
               "missing_manifest",
               "pipeline:missing",
               []
             )

    assert {:ok, operator} =
             Auth.create_actor("operator", "operator-password-long", "Operator", [:operator])

    assert {:ok, operator_session, ^operator} =
             FavnOrchestrator.operator_password_login("operator", "operator-password-long")

    assert {:error, :unauthenticated} =
             FavnOrchestrator.submit_operator_pipeline_run(
               %{actor_id: operator.id, session_id: operator_session.id, roles: ["operator"]},
               "missing_manifest",
               "pipeline:missing",
               []
             )

    assert :ok = FavnOrchestrator.revoke_operator_session(operator_session.id)

    assert {:error, :unauthenticated} =
             FavnOrchestrator.submit_operator_pipeline_run(
               %{actor: operator, session: operator_session},
               "missing_manifest",
               "pipeline:missing",
               []
             )
  end

  test "operator command wrappers validate malformed DTO structs before manifest lookup" do
    operator_context = operator_context("operator-malformed-dto")

    assert {:error, {:invalid_operator_refresh_mode, :bogus}} =
             FavnOrchestrator.submit_operator_asset_run(
               operator_context,
               "missing_manifest",
               "asset:missing",
               %AssetRunRequest{refresh_mode: :bogus}
             )

    assert {:error, {:invalid_operator_refresh_mode, :bogus}} =
             FavnOrchestrator.submit_operator_asset_backfill(
               operator_context,
               "missing_manifest",
               "asset:missing",
               %AssetBackfillRequest{refresh_mode: :bogus}
             )

    assert {:error, {:invalid_operator_refresh_mode, :force_selected}} =
             FavnOrchestrator.submit_operator_pipeline_run(
               operator_context,
               "missing_manifest",
               "pipeline:missing",
               %PipelineRunRequest{refresh_mode: :force_selected}
             )

    assert {:error, {:invalid_operator_refresh_mode, :force_selected}} =
             FavnOrchestrator.submit_operator_pipeline_backfill(
               operator_context,
               "missing_manifest",
               "pipeline:missing",
               %PipelineBackfillRequest{refresh_mode: :force_selected}
             )
  end

  test "operator pipeline run normalizes malformed window input before manifest lookup" do
    operator_context = operator_context("operator-bad-window")

    assert {:error, {:invalid_operator_window, %{mode: "bad"}}} =
             FavnOrchestrator.submit_operator_pipeline_run(
               operator_context,
               "missing_manifest",
               "pipeline:missing",
               %{window: %{mode: "bad"}}
             )
  end

  defp ensure_auth_store_started do
    case Process.whereis(AuthStore) do
      nil ->
        start_supervised!({AuthStore, []})
        :started

      _pid ->
        :existing
    end
  end

  defp maybe_stop_auth_store(:existing), do: :ok

  defp maybe_stop_auth_store(:started) do
    case Process.whereis(AuthStore) do
      nil -> :ok
      pid -> GenServer.stop(pid, :normal)
    end
  end

  defp restore_env(key, nil), do: Application.delete_env(:favn_orchestrator, key)
  defp restore_env(key, value), do: Application.put_env(:favn_orchestrator, key, value)

  defp operator_context(username) do
    assert {:ok, actor} =
             Auth.create_actor(username, "operator-password-long", "Operator", [:operator])

    assert {:ok, session, ^actor} =
             FavnOrchestrator.operator_password_login(username, "operator-password-long")

    %{actor: actor, session: session}
  end

  defp valid_range_request do
    %{range: %{kind: "day", from: "2026-05-01", to: "2026-05-03", timezone: "Etc/UTC"}}
  end
end
