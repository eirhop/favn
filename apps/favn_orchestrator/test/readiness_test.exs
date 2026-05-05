defmodule FavnOrchestrator.ReadinessTest do
  use ExUnit.Case, async: false

  alias Favn.Contracts.RunnerClient
  alias FavnOrchestrator.Auth.ServiceTokens
  alias FavnOrchestrator.Readiness
  alias FavnOrchestrator.Storage.Adapter.Memory

  defmodule RunnerClientStub do
    @behaviour RunnerClient

    @impl true
    def register_manifest(_version, _opts), do: :ok

    @impl true
    def submit_work(_work, _opts), do: {:ok, "execution"}

    @impl true
    def await_result(_execution_id, _timeout, _opts), do: {:error, :not_used}

    @impl true
    def cancel_work(_execution_id, _reason, _opts), do: :ok

    @impl true
    def inspect_relation(_request, _opts), do: {:error, :not_used}
  end

  defmodule RunnerRuntimeStub do
    def readiness, do: :ok
  end

  defmodule RaisingRunnerRuntimeStub do
    def readiness, do: raise("runner secret should not leak")
  end

  defmodule StorageReadinessFailureAdapterStub do
    @behaviour Favn.Storage.Adapter

    @impl true
    def child_spec(_opts), do: :none

    @impl true
    def readiness(opts) do
      case Keyword.fetch!(opts, :readiness_result) do
        :raise -> raise("storage secret should not leak")
        result -> result
      end
    end

    @impl true
    def put_manifest_version(_version, _opts), do: :ok

    @impl true
    def get_manifest_version(_manifest_version_id, _opts), do: {:error, :not_found}

    @impl true
    def get_manifest_version_by_content_hash(_content_hash, _opts), do: {:error, :not_found}

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
    def persist_run_transition(_run_state, _event, _opts), do: :ok

    @impl true
    def append_run_event(_run_id, _event, _opts), do: :ok

    @impl true
    def list_run_events(_run_id, _opts), do: {:ok, []}

    @impl true
    def list_global_run_events(_filters, _opts), do: {:ok, []}

    @impl true
    def put_scheduler_state(_key, _state, _opts), do: :ok

    @impl true
    def get_scheduler_state(_key, _opts), do: {:ok, nil}

    @impl true
    def put_coverage_baseline(_baseline, _opts), do: :ok

    @impl true
    def get_coverage_baseline(_baseline_id, _opts), do: {:error, :not_found}

    @impl true
    def list_coverage_baselines(_filters, _opts), do: {:ok, []}

    @impl true
    def put_backfill_window(_window, _opts), do: :ok

    @impl true
    def get_backfill_window(_backfill_run_id, _pipeline_module, _window_key, _opts),
      do: {:error, :not_found}

    @impl true
    def list_backfill_windows(_filters, _opts), do: {:ok, []}

    @impl true
    def put_asset_window_state(_state, _opts), do: :ok

    @impl true
    def get_asset_window_state(_asset_ref_module, _asset_ref_name, _window_key, _opts),
      do: {:error, :not_found}

    @impl true
    def list_asset_window_states(_filters, _opts), do: {:ok, []}

    @impl true
    def replace_backfill_read_models(_scope, _baselines, _windows, _states, _opts), do: :ok
  end

  setup do
    keys = [
      :api_server,
      :api_service_tokens,
      :api_service_tokens_env,
      :storage_adapter,
      :storage_adapter_opts,
      :scheduler,
      :runner_client,
      :runner_client_opts
    ]

    previous = Map.new(keys, &{&1, Application.get_env(:favn_orchestrator, &1)})

    Application.put_env(:favn_orchestrator, :api_server,
      enabled: true,
      host: "127.0.0.1",
      port: 4101
    )

    Application.put_env(:favn_orchestrator, :api_service_tokens, [
      [
        service_identity: "favn_web",
        token_hash: ServiceTokens.hash_token(String.duplicate("a", 32)),
        enabled: true
      ]
    ])

    Application.put_env(:favn_orchestrator, :storage_adapter, Memory)
    Application.put_env(:favn_orchestrator, :storage_adapter_opts, [])
    Application.put_env(:favn_orchestrator, :scheduler, enabled: false)
    Application.put_env(:favn_orchestrator, :runner_client, RunnerClientStub)

    on_exit(fn -> Enum.each(previous, fn {key, value} -> restore_env(key, value) end) end)

    :ok
  end

  test "liveness is always ok" do
    assert %{status: :ok, checks: [%{name: :process, status: :ok}]} = Readiness.liveness()
  end

  test "readiness is ready when configured checks pass" do
    assert %{status: :ready, checks: checks} = Readiness.readiness()
    assert Enum.all?(checks, &(&1.status == :ok))
  end

  test "readiness reports failed checks without secrets" do
    Application.put_env(:favn_orchestrator, :api_service_tokens, [])
    Application.delete_env(:favn_orchestrator, :api_service_tokens_env)
    Application.delete_env(:favn_orchestrator, :runner_client)

    assert %{status: :not_ready, checks: checks} = Readiness.readiness()
    assert Enum.any?(checks, &(&1.name == :api and &1.status == :error))
    assert Enum.any?(checks, &(&1.name == :runner and &1.status == :error))
    refute inspect(checks) =~ String.duplicate("a", 32)
  end

  test "readiness reports storage diagnostics failures without crashing" do
    secret = "storage-secret-should-not-leak"

    Application.put_env(:favn_orchestrator, :storage_adapter, StorageReadinessFailureAdapterStub)

    Application.put_env(:favn_orchestrator, :storage_adapter_opts,
      readiness_result: {:error, %{reason: :schema_not_ready, secret: secret}}
    )

    assert %{status: :not_ready, checks: checks} = Readiness.readiness()
    assert Enum.any?(checks, &(&1.name == :storage and &1.status == :error))
    refute inspect(checks) =~ secret
  end

  test "readiness isolates raised storage checks" do
    Application.put_env(:favn_orchestrator, :storage_adapter, StorageReadinessFailureAdapterStub)
    Application.put_env(:favn_orchestrator, :storage_adapter_opts, readiness_result: :raise)

    assert %{status: :not_ready, checks: checks} = Readiness.readiness()

    assert Enum.any?(checks, fn check ->
             check.name == :storage and check.status == :error and
               check.error == %{kind: :raised, exception: "Elixir.RuntimeError"}
           end)

    refute inspect(checks) =~ "storage secret should not leak"
  end

  test "local-node runner readiness checks the runner runtime" do
    Application.put_env(
      :favn_orchestrator,
      :runner_client,
      FavnOrchestrator.RunnerClient.LocalNode
    )

    Application.put_env(:favn_orchestrator, :runner_client_opts, runner_module: RunnerRuntimeStub)

    assert %{status: :ready} = Readiness.readiness()

    Application.put_env(:favn_orchestrator, :runner_client_opts,
      runner_module: MissingRunnerRuntime
    )

    assert %{status: :not_ready, checks: checks} = Readiness.readiness()
    assert Enum.any?(checks, &(&1.name == :runner and &1.error == :runner_runtime_not_available))
  end

  test "readiness isolates raised runner checks" do
    Application.put_env(
      :favn_orchestrator,
      :runner_client,
      FavnOrchestrator.RunnerClient.LocalNode
    )

    Application.put_env(:favn_orchestrator, :runner_client_opts,
      runner_module: RaisingRunnerRuntimeStub
    )

    assert %{status: :not_ready, checks: checks} = Readiness.readiness()

    assert Enum.any?(checks, fn check ->
             check.name == :runner and check.status == :error and
               check.error == %{kind: :raised, exception: "Elixir.RuntimeError"}
           end)

    refute inspect(checks) =~ "runner secret should not leak"
  end

  defp restore_env(key, nil), do: Application.delete_env(:favn_orchestrator, key)
  defp restore_env(key, value), do: Application.put_env(:favn_orchestrator, key, value)
end
