defmodule FavnOrchestrator.RuntimeConfigTest do
  use ExUnit.Case, async: false

  alias FavnOrchestrator.RuntimeConfig
  alias FavnOrchestrator.Storage.Adapter.Memory

  defmodule RunnerClientStub do
    @behaviour Favn.Contracts.RunnerClient

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

  test "normalizes runtime dependency env into one explicit contract" do
    config =
      RuntimeConfig.normalize(
        runner_client: RunnerClientStub,
        runner_client_opts: [runner_node: :runner@local],
        storage_adapter: Memory,
        storage_adapter_opts: [server: __MODULE__.Storage],
        log_redaction_policy: [fields: [:secret]]
      )

    assert %RuntimeConfig{
             runner_client: RunnerClientStub,
             runner_client_opts: [runner_node: :runner@local],
             storage_adapter: Memory,
             storage_adapter_opts: [server: __MODULE__.Storage],
             log_redaction_policy: [fields: [:secret]]
           } = config
  end

  test "supervised runtime config is stable after app env mutation" do
    name = __MODULE__.RuntimeConfig

    start_supervised!(
      {RuntimeConfig,
       config: RuntimeConfig.normalize(runner_client: RunnerClientStub, storage_adapter: Memory),
       name: name}
    )

    previous_runner_client = Application.get_env(:favn_orchestrator, :runner_client)

    try do
      Application.put_env(:favn_orchestrator, :runner_client, :mutated_after_startup)

      assert %RuntimeConfig{runner_client: RunnerClientStub} = RuntimeConfig.current(name)
    after
      restore_env(:runner_client, previous_runner_client)
    end
  end

  defp restore_env(key, nil), do: Application.delete_env(:favn_orchestrator, key)
  defp restore_env(key, value), do: Application.put_env(:favn_orchestrator, key, value)
end
