defmodule FavnOrchestrator.RuntimeConfigTest do
  use ExUnit.Case, async: false

  alias FavnOrchestrator.RuntimeConfig
  alias FavnOrchestrator.Storage
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
    assert {:ok,
            %RuntimeConfig{
              runner_client: RunnerClientStub,
              runner_client_opts: [runner_node: :runner@local],
              storage_adapter: Memory,
              storage_adapter_opts: [server: __MODULE__.Storage],
              log_redaction_policy: [fields: [:secret]]
            }} =
             RuntimeConfig.normalize(
               runner_client: RunnerClientStub,
               runner_client_opts: [runner_node: :runner@local],
               storage_adapter: Memory,
               storage_adapter_opts: [server: __MODULE__.Storage],
               log_redaction_policy: [fields: [:secret]]
             )
  end

  test "rejects invalid runtime dependency option shapes" do
    assert {:error, {:invalid_runtime_config, {:runner_client_opts, :bad_opts}}} =
             RuntimeConfig.normalize(runner_client_opts: :bad_opts)

    assert {:error, {:invalid_runtime_config, {:storage_adapter_opts, %{server: :bad}}}} =
             RuntimeConfig.normalize(storage_adapter_opts: %{server: :bad})

    assert_raise ArgumentError, ~r/invalid orchestrator runtime config/, fn ->
      RuntimeConfig.normalize!(storage_adapter_opts: ["server"])
    end
  end

  test "supervised runtime config is stable after app env mutation" do
    name = __MODULE__.RuntimeConfig

    start_supervised!(
      {RuntimeConfig,
       config: RuntimeConfig.normalize!(runner_client: RunnerClientStub, storage_adapter: Memory),
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

  test "current reads the published config without synchronously calling its owner" do
    name = __MODULE__.PublishedRuntimeConfig

    pid =
      start_supervised!(
        {RuntimeConfig,
         config:
           RuntimeConfig.normalize!(runner_client: RunnerClientStub, storage_adapter: Memory),
         name: name}
      )

    :sys.suspend(pid)

    try do
      task = Task.async(fn -> RuntimeConfig.current(name) end)
      assert %RuntimeConfig{runner_client: RunnerClientStub} = Task.await(task, 100)
    after
      :sys.resume(pid)
    end
  end

  test "default runtime config freezes storage and runner lookups after startup" do
    keys = [
      :runtime_config_dynamic_env?,
      :runner_client,
      :runner_client_opts,
      :storage_adapter,
      :storage_adapter_opts
    ]

    previous = Map.new(keys, &{&1, Application.get_env(:favn_orchestrator, &1)})

    try do
      Application.put_env(:favn_orchestrator, :runtime_config_dynamic_env?, false)

      frozen_config =
        ensure_default_runtime_config(
          RuntimeConfig.normalize!(
            runner_client: RunnerClientStub,
            runner_client_opts: [runner_node: :frozen_runner],
            storage_adapter: Memory,
            storage_adapter_opts: [server: __MODULE__.FrozenMemory]
          )
        )

      mutated_runner_client =
        case frozen_config.runner_client do
          RunnerClientStub -> nil
          _other -> RunnerClientStub
        end

      Application.put_env(:favn_orchestrator, :runner_client, mutated_runner_client)
      Application.put_env(:favn_orchestrator, :runner_client_opts, runner_node: :mutated)
      Application.put_env(:favn_orchestrator, :storage_adapter, :mutated_after_startup)
      Application.put_env(:favn_orchestrator, :storage_adapter_opts, server: :mutated)

      assert Storage.adapter_module() == frozen_config.storage_adapter
      assert Storage.adapter_opts() == frozen_config.storage_adapter_opts

      assert runner_check_matches_config?(
               FavnOrchestrator.Readiness.readiness().checks,
               frozen_config.runner_client
             )
    after
      Enum.each(previous, fn {key, value} -> restore_env(key, value) end)
    end
  end

  defp ensure_default_runtime_config(%RuntimeConfig{} = config) do
    case Process.whereis(RuntimeConfig) do
      nil ->
        start_supervised!({RuntimeConfig, config: config})
        config

      _pid ->
        RuntimeConfig.current()
    end
  end

  defp runner_check_matches_config?(checks, nil) do
    Enum.any?(checks, fn check ->
      check.name == :runner and check.status == :error and
        check.error in [:runner_client_not_available, :nofile]
    end)
  end

  defp runner_check_matches_config?(checks, module) when is_atom(module) do
    expected_module = Atom.to_string(module)

    Enum.any?(checks, fn check ->
      check.name == :runner and check.status == :ok and
        get_in(check, [:details, :module]) == expected_module
    end)
  end

  defp restore_env(key, nil), do: Application.delete_env(:favn_orchestrator, key)
  defp restore_env(key, value), do: Application.put_env(:favn_orchestrator, key, value)
end
