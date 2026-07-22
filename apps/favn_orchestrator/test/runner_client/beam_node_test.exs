defmodule FavnOrchestrator.RunnerClient.BeamNodeTest do
  use ExUnit.Case, async: false

  alias Favn.Contracts.GenerationActivationRequest
  alias Favn.Contracts.GenerationMarker
  alias Favn.Contracts.GenerationReconciliationRequest
  alias Favn.Contracts.RunnerError
  alias Favn.Contracts.RunnerResult
  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest.Version
  alias Favn.RelationRef
  alias FavnOrchestrator.RunnerClient.BeamNode

  defmodule StubRunner do
    def register_manifest(_version, opts) do
      if test_pid = Keyword.get(opts, :test_pid), do: send(test_pid, {:remote_opts, opts})
      :ok
    end

    def ensure_manifest(_version, _opts), do: :ok
    def submit_work(_work, _opts), do: {:ok, "exec_1"}
    def await_result(_execution_id, _timeout, _opts), do: {:ok, %RunnerResult{status: :ok}}
    def cancel_work(_execution_id, _reason, _opts), do: :ok
    def inspect_relation(_request, _opts), do: {:error, :not_supported}

    def diagnostics(_opts) do
      {:ok,
       %{
         available?: true,
         ready?: true,
         status: :ready,
         runner_release_id: FavnTestSupport.runner_release_id(),
         favn_version: Favn.RunnerRelease.current_favn_version(),
         runner_contract_version: Favn.Manifest.Compatibility.current_runner_contract_version(),
         self_verified?: true,
         node_name: Atom.to_string(Node.self())
       }}
    end
  end

  defmodule TimeoutRunner do
    def activate_generation(_request, _opts), do: :erlang.error({:erpc, :timeout})
    def await_result(_execution_id, _timeout, _opts), do: :erlang.error({:erpc, :timeout})
    def reconcile_generation(_request, _opts), do: :erlang.error({:erpc, :timeout})
    def submit_work(_work, _opts), do: :erlang.error({:erpc, :timeout})
  end

  defmodule RaisingRunner do
    def cancel_work(_execution_id, _reason, _opts), do: raise("remote secret must not leak")
  end

  defmodule ExitingRunner do
    def cancel_work(_execution_id, _reason, _opts), do: exit(:runner_exited)
  end

  defmodule InternalUndefRunner do
    def cancel_work(_execution_id, _reason, _opts),
      do: apply(Module.concat(["FavnOrchestrator.RunnerClient.MissingHelper"]), :call, [])
  end

  test "dispatches only through an explicitly configured BEAM node" do
    {:ok, version} = Version.new(manifest(), manifest_version_id: "mv_beam_node")

    work = %RunnerWork{
      required_runner_release_id: FavnTestSupport.runner_release_id(),
      run_id: "run_1",
      manifest_version_id: version.manifest_version_id,
      manifest_content_hash: version.content_hash
    }

    opts = beam_opts(StubRunner, test_pid: self())

    assert :ok = BeamNode.register_manifest(version, opts)
    assert_received {:remote_opts, remote_opts}
    refute Keyword.has_key?(remote_opts, :runner_node)
    refute Keyword.has_key?(remote_opts, :runner_module)
    refute Keyword.has_key?(remote_opts, :runner_rpc_timeout_ms)

    assert :ok = BeamNode.ensure_manifest(version, opts)
    assert {:ok, "exec_1"} = BeamNode.submit_work(work, opts)
    assert {:ok, %RunnerResult{status: :ok}} = BeamNode.await_result("exec_1", 1_000, opts)
    assert :ok = BeamNode.cancel_work("exec_1", %{}, opts)

    assert {:error, :not_supported} =
             BeamNode.inspect_relation(%Favn.Contracts.RelationInspectionRequest{}, opts)

    assert {:ok, %{status: :ready}} = BeamNode.diagnostics(opts)
  end

  test "never falls back to a local function call when runner_node is missing" do
    assert {:error,
            %RunnerError{
              type: :runner_node_not_configured,
              retryable?: false,
              outcome: :safe_failure
            }} = BeamNode.cancel_work("exec_2", %{}, runner_module: StubRunner)
  end

  test "normalizes unsupported remote functions" do
    assert {:error,
            %RunnerError{
              type: :runner_function_undefined,
              retryable?: false,
              outcome: :safe_failure
            }} = BeamNode.cancel_work("exec_2", %{}, beam_opts(MissingRunnerModule))

    assert {:error,
            %RunnerError{
              type: :runner_remote_failure,
              retryable?: false,
              outcome: :unknown
            }} = BeamNode.cancel_work("exec_2", %{}, beam_opts(InternalUndefRunner))
  end

  test "normalizes remote exceptions and exits without their payloads" do
    assert {:error, %RunnerError{type: :runner_remote_failure} = raised} =
             BeamNode.cancel_work("exec_2", %{}, beam_opts(RaisingRunner))

    refute inspect(raised) =~ "remote secret"

    assert {:error, %RunnerError{type: :runner_remote_failure} = exited} =
             BeamNode.cancel_work("exec_2", %{}, beam_opts(ExitingRunner))

    refute inspect(exited) =~ "runner_exited"
  end

  test "normalizes an unreachable runner before attempting RPC" do
    runner_node = :definitely_missing_runner@localhost

    assert {:error,
            %RunnerError{
              type: type,
              retryable?: true,
              outcome: :safe_failure
            }} =
             BeamNode.cancel_work("exec_2", %{},
               runner_module: StubRunner,
               runner_node: runner_node,
               runner_rpc_timeout_ms: 100
             )

    assert type in [:runner_node_unreachable, :runner_distribution_unavailable]
  end

  test "await timeout is a retryable safe read while mutation timeout has unknown outcome" do
    read_opts =
      beam_opts(TimeoutRunner,
        runner_rpc_timeout_ms: 10,
        runner_await_timeout_buffer_ms: 0
      )

    assert {:error,
            %RunnerError{
              type: :runner_rpc_timeout,
              retryable?: true,
              outcome: :safe_failure
            }} = BeamNode.await_result("exec_2", 20, read_opts)

    work = %RunnerWork{
      required_runner_release_id: FavnTestSupport.runner_release_id(),
      run_id: "run_timeout",
      manifest_version_id: "mv_timeout",
      manifest_content_hash: String.duplicate("a", 64)
    }

    mutation_opts = beam_opts(TimeoutRunner, runner_rpc_timeout_ms: 10)

    assert {:error,
            %RunnerError{
              type: :runner_rpc_timeout,
              retryable?: false,
              outcome: :unknown
            }} = BeamNode.submit_work(work, mutation_opts)

    assert {:error, %RunnerError{retryable?: false, outcome: :unknown}} =
             BeamNode.activate_generation(generation_activation_request(), mutation_opts)

    assert {:error, %RunnerError{retryable?: true, outcome: :safe_failure}} =
             BeamNode.reconcile_generation(
               %GenerationReconciliationRequest{activation: generation_activation_request()},
               mutation_opts
             )
  end

  test "connects to a real peer and bounds remote diagnostics, timeouts, and exceptions" do
    started_distribution? = not Node.alive?()

    if started_distribution? do
      assert {_, 0} = System.cmd("epmd", ["-daemon"], stderr_to_stdout: true)
      client_name = String.to_atom("favn_beam_client_#{System.unique_integer([:positive])}")
      assert {:ok, _pid} = Node.start(client_name, :shortnames)
    end

    peer_name = "favn_beam_runner_#{System.unique_integer([:positive])}"

    assert {:ok, peer, peer_node} =
             :peer.start_link(%{
               name: peer_name,
               connection: :standard_io,
               wait_boot: 10_000
             })

    try do
      assert :ok = :peer.call(peer, :code, :add_paths, [:code.get_path()], 10_000)
      Node.disconnect(peer_node)
      refute peer_node in Node.list()

      peer_diagnostics = %{
        available?: true,
        ready?: true,
        status: :ready,
        runner_release_id: FavnTestSupport.runner_release_id(),
        favn_version: Favn.RunnerRelease.current_favn_version(),
        runner_contract_version: Favn.Manifest.Compatibility.current_runner_contract_version(),
        self_verified?: true,
        node_name: Atom.to_string(peer_node)
      }

      opts =
        beam_opts(FavnTestSupport.BeamNodeRunner,
          runner_node: peer_node,
          runner_diagnostics_timeout_ms: 1_000,
          peer_diagnostics: peer_diagnostics
        )

      assert {:ok, ^peer_diagnostics} = BeamNode.diagnostics(opts)
      assert peer_node in Node.list()

      assert {:error,
              %RunnerError{
                type: :runner_rpc_timeout,
                retryable?: true,
                outcome: :safe_failure
              }} =
               BeamNode.await_result(
                 "exec_peer",
                 20,
                 Keyword.merge(opts,
                   runner_rpc_timeout_ms: 20,
                   runner_await_timeout_buffer_ms: 0,
                   peer_delay_ms: 200
                 )
               )

      secret = "remote peer secret must not leak"

      assert {:error,
              %RunnerError{
                type: :runner_remote_failure,
                retryable?: false,
                outcome: :unknown
              } = error} =
               BeamNode.cancel_work(
                 "exec_peer",
                 %{},
                 Keyword.put(opts, :peer_exception_message, secret)
               )

      refute inspect(error) =~ secret
    after
      :peer.stop(peer)
      if started_distribution?, do: Node.stop()
    end
  end

  defp beam_opts(runner_module, extra \\ []) do
    [runner_node: Node.self(), runner_module: runner_module, runner_rpc_timeout_ms: 100]
    |> Keyword.merge(extra)
  end

  defp generation_activation_request do
    relation = %RelationRef{connection: :warehouse, schema: "main", name: "target"}

    %GenerationActivationRequest{
      manifest_version_id: "mv_generation_rpc",
      manifest_content_hash: String.duplicate("a", 64),
      required_runner_release_id: FavnTestSupport.runner_release_id(),
      rebuild_operation_id: "rebuild_rpc",
      rebuild_action_id: "action_rpc",
      target_id: "asset:rpc",
      previous_generation_id: "11111111-1111-4111-8111-111111111111",
      candidate_generation_id: "22222222-2222-4222-8222-222222222222",
      active_relation: relation,
      candidate_relation: %{relation | name: "target_candidate"},
      retired_relation: %{relation | name: "target_retired"},
      expected_candidate_fingerprint: String.duplicate("b", 64),
      activation_token: "activation_rpc",
      expected_marker: %GenerationMarker{
        target_id: "asset:rpc",
        active_relation: relation,
        active_generation_id: "11111111-1111-4111-8111-111111111111",
        activation_operation_id: "previous_rpc",
        activation_token: "previous_token_rpc",
        activated_at: ~U[2026-07-22 09:00:00Z]
      }
    }
  end

  defp manifest do
    FavnTestSupport.with_manifest_contract(%{
      assets: [],
      pipelines: [],
      schedules: [],
      graph: %{},
      metadata: %{}
    })
  end
end
