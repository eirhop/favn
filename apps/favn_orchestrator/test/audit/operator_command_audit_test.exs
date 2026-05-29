defmodule FavnOrchestrator.Audit.OperatorCommandAuditTest do
  use ExUnit.Case, async: false

  alias Favn.Manifest
  alias Favn.Manifest.Graph
  alias Favn.Manifest.Version
  alias FavnOrchestrator.Auth
  alias FavnOrchestrator.Auth.Store, as: AuthStore
  alias FavnOrchestrator.Storage.Adapter.Memory

  setup do
    ensure_auth_store_started()
    Memory.reset()
    :ok = AuthStore.reset()

    version = manifest_version("mv_operator_audit")
    assert :ok = FavnOrchestrator.register_manifest(version)

    assert {:ok, actor} =
             Auth.create_actor("audit-operator", "operator-password-long", "Operator", [:operator])

    assert {:ok, session, ^actor} =
             FavnOrchestrator.operator_password_login("audit-operator", "operator-password-long")

    {:ok, context} = FavnOrchestrator.operator_context(actor, session, source: :live_view)

    %{context: context, version: version}
  end

  test "submit_operator_run creates a durable asset audit event", %{
    context: context,
    version: version
  } do
    assert {:ok, run_id} =
             FavnOrchestrator.submit_operator_run(
               context,
               version.manifest_version_id,
               %{type: :asset, id: "asset:Elixir.MyApp.Assets.Gold:asset"},
               %{refresh_mode: :force_all, metadata: %{api_key: "secret", keep: "safe"}}
             )

    assert {:ok, page} = FavnOrchestrator.list_audit_events(limit: 10)
    assert [event] = page.items
    assert event.action == "operator.asset_run.submit"
    assert event.outcome == :accepted
    assert event.resource_id == run_id
    assert event.actor_id == context.actor_id
    assert event.session_id == context.session_id
    assert event.manifest_version_id == version.manifest_version_id
    assert event.target_type == :asset
    assert event.payload["metadata"]["api_key"] == "[REDACTED]"
    assert event.payload["metadata"]["keep"] == "safe"
  end

  defp ensure_auth_store_started do
    case Process.whereis(AuthStore) do
      nil -> start_supervised!({AuthStore, []})
      _pid -> :ok
    end
  end

  defp manifest_version(manifest_version_id) do
    assets = [
      %Favn.Manifest.Asset{
        ref: {MyApp.Assets.Raw, :asset},
        module: MyApp.Assets.Raw,
        name: :asset
      },
      %Favn.Manifest.Asset{
        ref: {MyApp.Assets.Gold, :asset},
        module: MyApp.Assets.Gold,
        name: :asset,
        depends_on: [{MyApp.Assets.Raw, :asset}]
      }
    ]

    {:ok, graph} = Graph.build(assets)

    manifest = %Manifest{
      assets: assets,
      graph: graph,
      pipelines: [
        %Favn.Manifest.Pipeline{
          module: MyApp.Pipelines.Daily,
          name: :daily,
          selectors: [{:asset, {MyApp.Assets.Gold, :asset}}],
          deps: :all,
          schedule: nil,
          metadata: %{}
        }
      ]
    }

    {:ok, version} = Version.new(manifest, manifest_version_id: manifest_version_id)
    version
  end
end
