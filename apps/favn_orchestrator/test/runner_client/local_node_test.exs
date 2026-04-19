defmodule FavnOrchestrator.RunnerClient.LocalNodeTest do
  use ExUnit.Case, async: true

  alias Favn.Contracts.RunnerResult
  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest.Version
  alias FavnOrchestrator.RunnerClient.LocalNode

  defmodule StubRunner do
    def register_manifest(_version, _opts), do: :ok
    def submit_work(_work, _opts), do: {:ok, "exec_1"}
    def await_result(_execution_id, _timeout, _opts), do: {:ok, %RunnerResult{status: :ok}}
    def cancel_work(_execution_id, _reason, _opts), do: :ok
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
  end

  test "returns error when runner module is missing" do
    assert {:error, {:runner_module_unavailable, MissingRunnerModule, _reason}} =
             LocalNode.cancel_work("exec_2", %{}, runner_module: MissingRunnerModule)
  end
end
