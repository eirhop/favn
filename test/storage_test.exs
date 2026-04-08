defmodule Favn.StorageTest do
  use ExUnit.Case, async: false

  alias Favn.Run
  alias Favn.Scheduler.State, as: SchedulerState
  alias Favn.Scheduler.Storage, as: SchedulerStorage
  alias Favn.Storage

  defmodule RawErrorStore do
    @behaviour Favn.Storage.Adapter

    @impl true
    def child_spec(_opts), do: {:error, :child_spec_failed}

    @impl true
    def put_run(_run, _opts), do: {:error, :write_failed}

    @impl true
    def get_run(_run_id, _opts), do: {:error, :read_failed}

    @impl true
    def list_runs(_opts, _adapter_opts), do: {:error, :list_failed}
    def scheduler_child_spec(_opts), do: :none
    def put_scheduler_state(_state, _opts), do: :ok
    def get_scheduler_state(_pipeline_module, _opts), do: {:ok, nil}
  end

  defmodule NormalizedErrorStore do
    @behaviour Favn.Storage.Adapter

    @impl true
    def child_spec(_opts), do: {:error, {:store_error, :already_normalized}}

    @impl true
    def put_run(_run, _opts), do: {:error, {:store_error, :already_normalized}}

    @impl true
    def get_run(_run_id, _opts), do: {:error, {:store_error, :already_normalized}}

    @impl true
    def list_runs(_opts, _adapter_opts), do: {:error, {:store_error, :already_normalized}}

    @impl true
    def scheduler_child_spec(_opts), do: :none

    @impl true
    def put_scheduler_state(_state, _opts), do: {:error, {:store_error, :already_normalized}}

    @impl true
    def get_scheduler_state(_pipeline_module, _opts),
      do: {:error, {:store_error, :already_normalized}}
  end

  defmodule CanonicalErrorStore do
    @behaviour Favn.Storage.Adapter

    @impl true
    def child_spec(_opts), do: :none

    @impl true
    def put_run(_run, _opts), do: {:error, :invalid_opts}

    @impl true
    def get_run(_run_id, _opts), do: {:error, :not_found}

    @impl true
    def list_runs(_opts, _adapter_opts), do: {:error, :invalid_opts}
    def scheduler_child_spec(_opts), do: :none
    def put_scheduler_state(_state, _opts), do: :ok
    def get_scheduler_state(_pipeline_module, _opts), do: {:ok, nil}
  end

  defmodule InvalidSchedulerChildSpecStore do
    @behaviour Favn.Storage.Adapter

    @impl true
    def child_spec(_opts), do: :none

    @impl true
    def scheduler_child_spec(_opts), do: :invalid

    @impl true
    def put_run(_run, _opts), do: :ok

    @impl true
    def get_run(_run_id, _opts), do: {:error, :not_found}

    @impl true
    def list_runs(_opts, _adapter_opts), do: {:ok, []}

    @impl true
    def put_scheduler_state(_state, _opts), do: :ok

    @impl true
    def get_scheduler_state(_pipeline_module, _opts), do: {:ok, nil}
  end

  defmodule RaisingSchedulerStore do
    @behaviour Favn.Storage.Adapter

    @impl true
    def child_spec(_opts), do: :none

    @impl true
    def scheduler_child_spec(_opts), do: :none

    @impl true
    def put_run(_run, _opts), do: :ok

    @impl true
    def get_run(_run_id, _opts), do: {:error, :not_found}

    @impl true
    def list_runs(_opts, _adapter_opts), do: {:ok, []}

    @impl true
    def put_scheduler_state(_state, _opts), do: raise("scheduler boom")

    @impl true
    def get_scheduler_state(_pipeline_module, _opts), do: throw(:scheduler_throw)
  end

  setup do
    previous_store = Application.get_env(:favn, :storage_adapter)
    previous_store_opts = Application.get_env(:favn, :storage_adapter_opts)

    on_exit(fn ->
      restore_env(:storage_adapter, previous_store)
      restore_env(:storage_adapter_opts, previous_store_opts)
    end)

    :ok
  end

  test "child_specs/0 does not double-wrap normalized store errors" do
    Application.put_env(:favn, :storage_adapter, NormalizedErrorStore)

    assert {:error, {:store_error, :already_normalized}} = Storage.child_specs()
  end

  test "storage entrypoints wrap raw adapter errors as store_error" do
    Application.put_env(:favn, :storage_adapter, RawErrorStore)

    assert {:error, {:store_error, :child_spec_failed}} = Storage.child_specs()
    assert {:error, {:store_error, :write_failed}} = Storage.put_run(sample_run())
    assert {:error, {:store_error, :read_failed}} = Storage.get_run("run-1")
    assert {:error, {:store_error, :list_failed}} = Storage.list_runs()
  end

  test "storage entrypoints preserve canonical error shapes" do
    Application.put_env(:favn, :storage_adapter, CanonicalErrorStore)

    assert :ok = Storage.put_run(sample_run()) |> expect_error(:invalid_opts)
    assert :ok = Storage.get_run("missing") |> expect_error(:not_found)
    assert :ok = Storage.list_runs() |> expect_error(:invalid_opts)
  end

  test "list_runs/1 validates invalid options before adapter call" do
    Application.put_env(:favn, :storage_adapter, RawErrorStore)

    assert {:error, :invalid_opts} = Storage.list_runs(status: :pending)
    assert {:error, :invalid_opts} = Storage.list_runs(limit: 0)
  end

  test "scheduler child_specs normalize malformed adapter responses" do
    Application.put_env(:favn, :storage_adapter, InvalidSchedulerChildSpecStore)

    assert {:error, {:store_error, {:invalid_child_spec_response, :invalid}}} =
             SchedulerStorage.child_specs()
  end

  test "scheduler storage wraps adapter raises/throws as store_error" do
    Application.put_env(:favn, :storage_adapter, RaisingSchedulerStore)

    assert {:error, {:store_error, {:raised, %RuntimeError{message: "scheduler boom"}}}} =
             SchedulerStorage.put_state(%SchedulerState{pipeline_module: __MODULE__})

    assert {:error, {:store_error, {:thrown, :scheduler_throw}}} =
             SchedulerStorage.get_state(__MODULE__)
  end

  test "invalid adapter configuration is normalized as store_error" do
    Application.put_env(:favn, :storage_adapter, Missing.Adapter)

    assert {:error, {:store_error, {:invalid_storage_adapter, Missing.Adapter}}} =
             Storage.get_run("run-1")
  end

  defp sample_run do
    %Run{id: "run-1", target_refs: [], plan: nil, started_at: DateTime.utc_now()}
  end

  defp expect_error({:error, reason}, expected) when reason == expected, do: :ok

  defp restore_env(key, nil), do: Application.delete_env(:favn, key)
  defp restore_env(key, value), do: Application.put_env(:favn, key, value)
end
