defmodule FavnOrchestrator.ReadinessTest do
  use ExUnit.Case, async: false

  alias Favn.Contracts.RunnerClient
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

  setup do
    keys = [
      :api_server,
      :api_service_tokens,
      :storage_adapter,
      :storage_adapter_opts,
      :scheduler,
      :runner_client
    ]

    previous = Map.new(keys, &{&1, Application.get_env(:favn_orchestrator, &1)})

    Application.put_env(:favn_orchestrator, :api_server,
      enabled: true,
      host: "127.0.0.1",
      port: 4101
    )

    Application.put_env(:favn_orchestrator, :api_service_tokens, [String.duplicate("a", 32)])
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
    Application.delete_env(:favn_orchestrator, :runner_client)

    assert %{status: :not_ready, checks: checks} = Readiness.readiness()
    assert Enum.any?(checks, &(&1.name == :api and &1.status == :error))
    assert Enum.any?(checks, &(&1.name == :runner and &1.status == :error))
    refute inspect(checks) =~ String.duplicate("a", 32)
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

  defp restore_env(key, nil), do: Application.delete_env(:favn_orchestrator, key)
  defp restore_env(key, value), do: Application.put_env(:favn_orchestrator, key, value)
end
