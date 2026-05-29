defmodule FavnOrchestrator.Audit.OperatorCommandAuditTest do
  use ExUnit.Case, async: false

  alias Favn.Manifest
  alias Favn.Manifest.Graph
  alias Favn.Manifest.Version
  alias Favn.Window.Policy
  alias FavnOrchestrator.Auth
  alias FavnOrchestrator.Auth.Store, as: AuthStore
  alias FavnOrchestrator.Storage.Adapter.Memory

  defmodule MemoryDelegateAdapter do
    defmacro __using__(_opts) do
      callbacks = Favn.Storage.Adapter.behaviour_info(:callbacks)

      delegates =
        for {name, arity} <- callbacks do
          args = Macro.generate_arguments(arity, __MODULE__)

          quote do
            def unquote(name)(unquote_splicing(args)) do
              apply(Memory, unquote(name), [unquote_splicing(args)])
            end

            defoverridable [{unquote(name), unquote(arity)}]
          end
        end

      quote do
        @behaviour Favn.Storage.Adapter

        alias FavnOrchestrator.Storage.Adapter.Memory

        unquote_splicing(delegates)
      end
    end
  end

  defmodule AuditInsertFailureAdapter do
    use MemoryDelegateAdapter

    def put_audit_event(_event, _opts), do: {:error, :audit_insert_failed}

    def persist_run_transition(run, event, opts) do
      if pid = Application.get_env(:favn_orchestrator, :audit_test_pid) do
        send(pid, {:unexpected_runtime_mutation, run.id})
      end

      Memory.persist_run_transition(run, event, opts)
    end
  end

  defmodule AuditUpdateFailureAdapter do
    use MemoryDelegateAdapter

    def update_audit_event_result(_event_id, _attrs, _opts), do: {:error, :audit_update_failed}
  end

  defmodule SubmitFailureAdapter do
    use MemoryDelegateAdapter

    def persist_run_transition(_run, _event, _opts), do: {:error, :audit_submit_failed}
  end

  setup do
    previous_dynamic_env = Application.get_env(:favn_orchestrator, :runtime_config_dynamic_env?)
    previous_storage_adapter = Application.get_env(:favn_orchestrator, :storage_adapter)
    previous_metrics_hook = Application.get_env(:favn_orchestrator, :metrics_hook)
    previous_audit_test_pid = Application.get_env(:favn_orchestrator, :audit_test_pid)

    Application.put_env(:favn_orchestrator, :runtime_config_dynamic_env?, true)
    Application.put_env(:favn_orchestrator, :storage_adapter, Memory)

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

    on_exit(fn ->
      restore_env(:runtime_config_dynamic_env?, previous_dynamic_env)
      restore_env(:storage_adapter, previous_storage_adapter)
      restore_env(:metrics_hook, previous_metrics_hook)
      restore_env(:audit_test_pid, previous_audit_test_pid)
    end)

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

  test "submit errors update the audit event to rejected", %{context: context, version: version} do
    Application.put_env(:favn_orchestrator, :storage_adapter, SubmitFailureAdapter)

    assert {:error, :audit_submit_failed} =
             FavnOrchestrator.submit_operator_run(
               context,
               version.manifest_version_id,
               %{type: :asset, id: "asset:Elixir.MyApp.Assets.Gold:asset"},
               %{refresh_mode: :force_all}
             )

    assert {:ok, page} = FavnOrchestrator.list_audit_events(limit: 10)
    assert [event] = page.items
    assert event.action == "operator.asset_run.submit"
    assert event.outcome == :rejected
    assert event.failure_class == "audit_submit_failed"
    assert event.resource_id == nil
  end

  test "audit insert failure rejects the command before runtime mutation", %{
    context: context,
    version: version
  } do
    Application.put_env(:favn_orchestrator, :storage_adapter, AuditInsertFailureAdapter)
    Application.put_env(:favn_orchestrator, :audit_test_pid, self())

    assert {:error, :audit_insert_failed} =
             FavnOrchestrator.submit_operator_run(
               context,
               version.manifest_version_id,
               %{type: :asset, id: "asset:Elixir.MyApp.Assets.Gold:asset"},
               %{refresh_mode: :force_all}
             )

    refute_receive {:unexpected_runtime_mutation, _run_id}
    assert {:ok, page} = FavnOrchestrator.list_audit_events(limit: 10)
    assert page.items == []
  end

  test "audit result update failure is logged but preserves operator result", %{
    context: context,
    version: version
  } do
    Application.put_env(:favn_orchestrator, :storage_adapter, AuditUpdateFailureAdapter)
    test_pid = self()

    Application.put_env(:favn_orchestrator, :metrics_hook, fn event, measurements, metadata ->
      send(test_pid, {:metrics_hook, event, measurements, metadata})
    end)

    assert {:ok, _run_id} =
             FavnOrchestrator.submit_operator_run(
               context,
               version.manifest_version_id,
               %{type: :asset, id: "asset:Elixir.MyApp.Assets.Gold:asset"},
               %{refresh_mode: :force_all}
             )

    assert_receive {:metrics_hook, :audit_event_result_update_failed, %{}, metadata}
    assert metadata.action == "operator.asset_run.submit"
    assert metadata.reason == :audit_update_failed
    assert metadata.outcome == :accepted

    assert {:ok, page} = FavnOrchestrator.list_audit_events(limit: 10)
    assert [event] = page.items
    assert event.outcome == :accepted
    assert event.resource_id == nil
  end

  test "operator run and backfill commands record expected actions and resource types", %{
    context: context,
    version: version
  } do
    assert {:ok, _run_id} =
             FavnOrchestrator.submit_operator_run(
               context,
               version.manifest_version_id,
               %{type: :pipeline, id: "pipeline:Elixir.MyApp.Pipelines.Daily"},
               %{refresh_mode: :force_all}
             )

    assert {:ok, _backfill_id} =
             FavnOrchestrator.submit_operator_asset_backfill(
               context,
               version.manifest_version_id,
               "asset:Elixir.MyApp.Assets.Gold:asset",
               valid_range_request()
             )

    assert {:ok, _backfill_id} =
             FavnOrchestrator.submit_operator_pipeline_backfill(
               context,
               version.manifest_version_id,
               "pipeline:Elixir.MyApp.Pipelines.Daily",
               valid_range_request()
             )

    assert {:ok, page} = FavnOrchestrator.list_audit_events(limit: 10)

    assert_audit_event(page.items, "operator.pipeline_run.submit", :pipeline, :run)
    assert_audit_event(page.items, "operator.asset_backfill.submit", :asset, :backfill)
    assert_audit_event(page.items, "operator.pipeline_backfill.submit", :pipeline, :backfill)
  end

  test "memory audit event listing uses cursor pagination", %{context: context, version: version} do
    for refresh_mode <- [:auto, :missing, :force_all] do
      assert {:ok, _run_id} =
               FavnOrchestrator.submit_operator_run(
                 context,
                 version.manifest_version_id,
                 %{type: :asset, id: "asset:Elixir.MyApp.Assets.Gold:asset"},
                 %{refresh_mode: refresh_mode}
               )
    end

    assert {:ok, first_page} = FavnOrchestrator.list_audit_events(limit: 2)
    assert length(first_page.items) == 2
    assert first_page.has_more? == true
    assert is_map(first_page.next_cursor)

    assert {:ok, second_page} =
             FavnOrchestrator.list_audit_events(limit: 2, after: first_page.next_cursor)

    assert length(second_page.items) == 1
    assert second_page.has_more? == false

    first_ids = MapSet.new(first_page.items, & &1.id)
    second_ids = MapSet.new(second_page.items, & &1.id)
    assert MapSet.disjoint?(first_ids, second_ids)
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
          window: Policy.new!(:daily, timezone: "UTC", allow_full_load: true),
          metadata: %{}
        }
      ]
    }

    {:ok, version} = Version.new(manifest, manifest_version_id: manifest_version_id)
    version
  end

  defp assert_audit_event(events, action, target_type, resource_type) do
    assert Enum.any?(events, fn event ->
             event.action == action and event.target_type == target_type and
               event.resource_type == resource_type and event.outcome == :accepted and
               is_binary(event.resource_id)
           end)
  end

  defp valid_range_request do
    %{range: %{kind: "day", from: "2026-05-01", to: "2026-05-01", timezone: "Etc/UTC"}}
  end

  defp restore_env(key, nil), do: Application.delete_env(:favn_orchestrator, key)
  defp restore_env(key, value), do: Application.put_env(:favn_orchestrator, key, value)
end
