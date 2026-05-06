defmodule FavnOrchestrator.RunnerClient.LocalNodeTest do
  use ExUnit.Case, async: false

  alias Favn.Contracts.RunnerResult
  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest.Version
  alias FavnOrchestrator.RunnerClient.LocalNode

  defmodule StubRunner do
    def register_manifest(_version, _opts), do: :ok
    def submit_work(_work, _opts), do: {:ok, "exec_1"}
    def await_result(_execution_id, _timeout, _opts), do: {:ok, %RunnerResult{status: :ok}}
    def cancel_work(_execution_id, _reason, _opts), do: :ok
    def inspect_relation(_request, _opts), do: {:error, :not_supported}
  end

  defmodule SlowRunner do
    def await_result(_execution_id, _timeout, opts) do
      opts
      |> Keyword.fetch!(:test_pid)
      |> send({:await_result_called, self()})

      Process.sleep(Keyword.fetch!(opts, :block_ms))
      {:ok, %RunnerResult{status: :ok}}
    end
  end

  defmodule RaisingRunner do
    def cancel_work(_execution_id, _reason, _opts), do: raise("runner failed")
  end

  defmodule ExitingRunner do
    def cancel_work(_execution_id, _reason, _opts), do: exit(:runner_exited)
  end

  test "dispatches runner calls to configured runner module" do
    manifest = %{
      schema_version: 1,
      runner_contract_version: 1,
      assets: [],
      pipelines: [],
      schedules: [],
      graph: %{},
      metadata: %{}
    }

    {:ok, version} = Version.new(manifest, manifest_version_id: "mv_local_node")

    work =
      %RunnerWork{
        run_id: "run_1",
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash
      }

    opts = [runner_module: StubRunner]

    assert :ok = LocalNode.register_manifest(version, opts)
    assert {:ok, "exec_1"} = LocalNode.submit_work(work, opts)
    assert {:ok, %RunnerResult{status: :ok}} = LocalNode.await_result("exec_1", 1_000, opts)
    assert :ok = LocalNode.cancel_work("exec_1", %{}, opts)

    assert {:error, :not_supported} =
             LocalNode.inspect_relation(%Favn.Contracts.RelationInspectionRequest{}, opts)
  end

  test "returns error when runner module is missing" do
    assert {:error, {:runner_function_undefined, MissingRunnerModule, :cancel_work, 3}} =
             LocalNode.cancel_work("exec_2", %{}, runner_module: MissingRunnerModule)
  end

  test "normalizes local runner exceptions into error tuples" do
    assert {:error,
            {:runner_dispatch_failed,
             %{
               runner_module: RaisingRunner,
               function: :cancel_work,
               arity: 3,
               kind: :error,
               reason: {RuntimeError, "runner failed"}
             }}} = LocalNode.cancel_work("exec_2", %{}, runner_module: RaisingRunner)
  end

  test "normalizes local runner exits into error tuples" do
    assert {:error,
            {:runner_dispatch_failed,
             %{
               runner_module: ExitingRunner,
               function: :cancel_work,
               arity: 3,
               kind: :exit,
               reason: :runner_exited
             }}} = LocalNode.cancel_work("exec_2", %{}, runner_module: ExitingRunner)
  end

  test "remote dispatch does not require local runner module exports" do
    runner_node = :definitely_missing_runner@localhost

    assert {:error, {reason, node}} =
             LocalNode.cancel_work("exec_2", %{},
               runner_module: MissingRunnerModule,
               runner_node: runner_node
             )

    assert reason in [:runner_node_unreachable, :runner_node_ignored]
    assert node == runner_node
  end

  test "normalizes remote runner call failures into error tuples" do
    case ensure_distributed_node() do
      :ok ->
        assert {:error,
                {:runner_dispatch_failed,
                 %{
                   runner_module: RaisingRunner,
                   function: :cancel_work,
                   arity: 3,
                   kind: :error,
                   reason: {RuntimeError, "runner failed"}
                 }}} =
                 LocalNode.cancel_work("exec_2", %{},
                   runner_module: RaisingRunner,
                   runner_node: Node.self()
                 )

      {:error, reason} ->
        IO.puts(
          "Skipping remote LocalNode dispatch test: distributed Erlang unavailable: #{inspect(reason)}"
        )
    end
  end

  test "remote await_result uses requested await timeout instead of dispatch default" do
    case ensure_distributed_node() do
      :ok ->
        assert {:ok, %RunnerResult{status: :ok}} =
                 LocalNode.await_result("exec_2", 50,
                   runner_module: SlowRunner,
                   runner_node: Node.self(),
                   runner_dispatch_timeout_ms: 10,
                   runner_await_timeout_buffer_ms: 20,
                   block_ms: 25,
                   test_pid: self()
                 )

        assert_received {:await_result_called, _pid}

      {:error, reason} ->
        IO.puts(
          "Skipping remote LocalNode await timeout test: distributed Erlang unavailable: #{inspect(reason)}"
        )
    end
  end

  defp ensure_distributed_node do
    case Node.start(:favn_orchestrator_local_node_test, :shortnames) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end
end
