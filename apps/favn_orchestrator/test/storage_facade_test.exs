defmodule Favn.StorageFacadeTest do
  use ExUnit.Case, async: false

  alias Favn.Run
  alias Favn.Storage
  alias FavnOrchestrator.Storage, as: OrchestratorStorage
  alias FavnOrchestrator.Storage.Adapter.Memory

  defmodule ExactShapeAdapterStub do
    @behaviour Favn.Storage.Adapter

    @impl true
    def child_spec(_opts), do: :none

    @impl true
    def put_manifest_version(_version, _opts), do: :ok

    @impl true
    def get_manifest_version(_manifest_version_id, _opts), do: {:error, :not_found}

    @impl true
    def list_manifest_versions(_opts), do: {:ok, []}

    @impl true
    def set_active_manifest_version(_manifest_version_id, _opts), do: :ok

    @impl true
    def get_active_manifest_version(_opts), do: {:error, :not_found}

    @impl true
    def put_run(_run_state, _opts), do: :ok

    @impl true
    def get_run(_run_id, _opts), do: {:error, :not_found}

    @impl true
    def list_runs(_opts, _adapter_opts), do: {:ok, []}

    @impl true
    def append_run_event(_run_id, _event, _opts), do: :ok

    @impl true
    def list_run_events(_run_id, _opts), do: {:ok, []}

    @impl true
    def put_scheduler_state(_key, _state, _opts), do: :ok

    @impl true
    def get_scheduler_state(_key, _opts), do: {:ok, nil}
  end

  defmodule LegacyShapeAdapterStub do
    def child_spec(_opts), do: :none
    def scheduler_child_spec(_opts), do: :none
    def put_run(_run, _opts), do: :ok
    def get_run(_run_id, _opts), do: {:error, :not_found}
    def list_runs(_opts, _adapter_opts), do: {:ok, []}
    def put_scheduler_state(_state, _opts), do: :ok
    def get_scheduler_state(_module, _schedule_id, _opts), do: {:ok, nil}
  end

  defmodule ChildSpecProbeAdapterStub do
    @behaviour Favn.Storage.Adapter

    @impl true
    def child_spec(opts) do
      test_pid = Keyword.fetch!(opts, :test_pid)
      send(test_pid, :child_spec_called)
      :none
    end

    @impl true
    def put_manifest_version(_version, _opts), do: :ok

    @impl true
    def get_manifest_version(_manifest_version_id, _opts), do: {:error, :not_found}

    @impl true
    def list_manifest_versions(_opts), do: {:ok, []}

    @impl true
    def set_active_manifest_version(_manifest_version_id, _opts), do: :ok

    @impl true
    def get_active_manifest_version(_opts), do: {:error, :not_found}

    @impl true
    def put_run(_run_state, _opts), do: :ok

    @impl true
    def get_run(_run_id, _opts), do: {:error, :not_found}

    @impl true
    def list_runs(_opts, _adapter_opts), do: {:ok, []}

    @impl true
    def append_run_event(_run_id, _event, _opts), do: :ok

    @impl true
    def list_run_events(_run_id, _opts), do: {:ok, []}

    @impl true
    def put_scheduler_state(_key, _state, _opts), do: :ok

    @impl true
    def get_scheduler_state(_key, _opts), do: {:ok, nil}
  end

  setup do
    previous_adapter = Application.get_env(:favn_orchestrator, :storage_adapter)
    previous_opts = Application.get_env(:favn_orchestrator, :storage_adapter_opts)

    Application.put_env(:favn_orchestrator, :storage_adapter, Memory)
    Application.put_env(:favn_orchestrator, :storage_adapter_opts, [])

    Memory.reset()

    on_exit(fn ->
      restore_env(:favn_orchestrator, :storage_adapter, previous_adapter)
      restore_env(:favn_orchestrator, :storage_adapter_opts, previous_opts)
      Memory.reset()
    end)

    :ok
  end

  test "validates adapters that implement the public orchestrator-backed behaviour" do
    assert :ok = Storage.validate_adapter(ExactShapeAdapterStub)
    assert :ok = OrchestratorStorage.validate_adapter(ExactShapeAdapterStub)

    assert {:error, {:store_error, {:invalid_storage_adapter, LegacyShapeAdapterStub}}} =
             Storage.validate_adapter(LegacyShapeAdapterStub)

    assert {:error, {:invalid_storage_adapter, LegacyShapeAdapterStub}} =
             OrchestratorStorage.validate_adapter(LegacyShapeAdapterStub)
  end

  test "reads and writes runs through orchestrator storage facade" do
    now = DateTime.utc_now()

    run =
      %Run{
        id: "run_storage_facade",
        manifest_version_id: "mv_storage_facade",
        manifest_content_hash: "hash_storage_facade",
        asset_ref: {MyApp.Assets.Gold, :asset},
        target_refs: [{MyApp.Assets.Gold, :asset}],
        submit_kind: :asset,
        status: :running,
        started_at: now,
        event_seq: 1
      }

    assert :ok = Storage.put_run(run)

    assert {:ok, projected} = Storage.get_run("run_storage_facade")
    assert projected.id == "run_storage_facade"
    assert projected.asset_ref == {MyApp.Assets.Gold, :asset}

    assert {:ok, run_state} = OrchestratorStorage.get_run("run_storage_facade")
    assert run_state.id == "run_storage_facade"
    assert run_state.manifest_version_id == "mv_storage_facade"
  end

  test "always delegates child spec setup to orchestrator storage" do
    Application.put_env(:favn_orchestrator, :storage_adapter, ChildSpecProbeAdapterStub)
    Application.put_env(:favn_orchestrator, :storage_adapter_opts, test_pid: self())

    assert {:ok, []} = Storage.child_specs()
    assert_receive :child_spec_called
  end

  defp restore_env(app, key, nil), do: Application.delete_env(app, key)
  defp restore_env(app, key, value), do: Application.put_env(app, key, value)
end
