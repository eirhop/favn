defmodule FavnOrchestrator.RunServer.Execution.PlanPreflightTest do
  use ExUnit.Case, async: false

  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Graph
  alias Favn.Manifest.Version
  alias Favn.Plan
  alias FavnOrchestrator.RunServer.Execution
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.RunState

  defmodule RunnerClient do
    @behaviour Favn.Contracts.RunnerClient

    def register_manifest(_version, _opts), do: :ok
    def ensure_manifest(_version, _opts), do: :ok

    def acquire_manifest(_version, lease_id, _expires_at, planned_asset_refs, opts) do
      send(
        Keyword.fetch!(opts, :test_pid),
        {:manifest_acquired, lease_id, planned_asset_refs, opts}
      )

      :ok
    end

    def renew_manifest(_lease_id, _expires_at, _opts), do: :ok

    def release_manifest(lease_id, opts) do
      send(Keyword.fetch!(opts, :test_pid), {:manifest_released, lease_id})
      :ok
    end

    def submit_work(_work, _opts), do: {:error, :not_executed}
    def await_result(_execution_id, _timeout, _opts), do: {:error, :not_executed}
    def cancel_work(_execution_id, _reason, _opts), do: {:error, :not_executed}
    def inspect_relation(_request, _opts), do: {:error, :not_supported}

    def diagnostics(opts) do
      {:ok,
       %{
         available?: true,
         ready?: true,
         status: :ready,
         runner_release_id: Keyword.fetch!(opts, :runner_release_id),
         favn_version: Favn.RunnerRelease.current_favn_version(),
         runner_contract_version: Favn.Manifest.Compatibility.current_runner_contract_version(),
         identity_source: :operator,
         node_name: "runner@runner.internal"
       }}
    end
  end

  setup do
    previous_client = Application.get_env(:favn_orchestrator, :runner_client)
    previous_opts = Application.get_env(:favn_orchestrator, :runner_client_opts)

    Application.put_env(:favn_orchestrator, :runner_client, RunnerClient)

    Application.put_env(:favn_orchestrator, :runner_client_opts,
      test_pid: self(),
      runner_release_id: FavnTestSupport.runner_release_id()
    )

    on_exit(fn ->
      restore_env(:runner_client, previous_client)
      restore_env(:runner_client_opts, previous_opts)
    end)

    :ok
  end

  test "acquires one manifest lease with the complete wide-plan preflight scope" do
    ref = {__MODULE__.Source, :asset}
    wide_refs = List.duplicate(ref, 10_000)
    node_key = {ref, nil}

    asset = %Asset{
      ref: ref,
      module: elem(ref, 0),
      name: elem(ref, 1),
      type: :source,
      relation: %{name: "wide_source"}
    }

    manifest =
      FavnTestSupport.with_manifest_contract(%Manifest{
        assets: [asset],
        graph: %Graph{nodes: [ref], topo_order: [ref]}
      })

    {:ok, version} = Version.new(manifest, manifest_version_id: "mv-wide-plan-preflight")

    plan = %Plan{
      target_refs: [ref],
      target_node_keys: [node_key],
      topo_order: wide_refs,
      nodes: %{
        node_key => %{
          ref: ref,
          node_key: node_key,
          window: nil,
          upstream: [],
          downstream: [],
          stage: 0,
          execution_pool: :default,
          action: :observe,
          retry_policy: nil,
          retry_policy_source: :default
        }
      }
    }

    run =
      RunState.new(
        id: "run-wide-plan-preflight",
        workspace_id: "workspace-wide-plan-preflight",
        deployment_id: "deployment-wide-plan-preflight",
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        required_runner_release_id: version.required_runner_release_id,
        asset_ref: ref,
        target_refs: [ref],
        plan: plan
      )

    assert {:ok, _execution_state} = Execution.start_state(run, version)

    assert_receive {:manifest_acquired, lease_id, ^wide_refs, _opts}
    refute_receive {:manifest_acquired, _other_lease_id, _other_refs, _other_opts}

    assert :ok = Execution.release_manifest_lease(run)
    assert_receive {:manifest_released, ^lease_id}
  end

  test "rejects an oversized planned asset step before acquiring or dispatching" do
    ref = {__MODULE__.Source, :asset}
    node_key = String.duplicate("s", 256)

    asset = %Asset{
      ref: ref,
      module: elem(ref, 0),
      name: elem(ref, 1),
      type: :source,
      relation: %{name: "source"}
    }

    manifest =
      FavnTestSupport.with_manifest_contract(%Manifest{
        assets: [asset],
        graph: %Graph{nodes: [ref], topo_order: [ref]}
      })

    {:ok, version} = Version.new(manifest, manifest_version_id: "mv-identity-preflight")

    plan = %Plan{
      target_refs: [ref],
      target_node_keys: [node_key],
      topo_order: [ref],
      nodes: %{
        node_key => %{
          ref: ref,
          node_key: node_key,
          window: nil,
          upstream: [],
          downstream: [],
          stage: 0,
          execution_pool: :default,
          action: :observe,
          retry_policy: nil,
          retry_policy_source: :default
        }
      }
    }

    run =
      RunState.new(
        id: "run-identity-preflight",
        workspace_id: "workspace-identity-preflight",
        deployment_id: "deployment-identity-preflight",
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        required_runner_release_id: version.required_runner_release_id,
        asset_ref: ref,
        target_refs: [ref],
        plan: plan
      )

    assert {:terminal, %RunState{error: %Error{} = error}} = Execution.start_state(run, version)
    assert error.details == %{field: :asset_step_id, actual_bytes: 256, max_bytes: 255}
    refute_receive {:manifest_acquired, _lease_id, _refs, _opts}
  end

  test "rejects a forged run release before acquiring the manifest" do
    ref = {__MODULE__.Source, :asset}

    asset = %Asset{
      ref: ref,
      module: elem(ref, 0),
      name: elem(ref, 1),
      type: :source,
      relation: %{name: "source"}
    }

    manifest =
      FavnTestSupport.with_manifest_contract(%Manifest{
        assets: [asset],
        graph: %Graph{nodes: [ref], topo_order: [ref]}
      })

    {:ok, version} = Version.new(manifest, manifest_version_id: "mv-forged-release")
    alternate = FavnTestSupport.runner_release_id(:alternate)

    run =
      RunState.new(
        id: "run-forged-release",
        workspace_id: "workspace-forged-release",
        deployment_id: "deployment-forged-release",
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        required_runner_release_id: alternate,
        asset_ref: ref,
        target_refs: [ref]
      )

    assert {:terminal,
            %RunState{
              status: :error,
              error: {:runner_release_mismatch, ^alternate, required}
            }} = Execution.start_state(run, version)

    assert required == version.required_runner_release_id
    refute_receive {:manifest_acquired, _lease_id, _refs, _opts}
  end

  defp restore_env(key, nil), do: Application.delete_env(:favn_orchestrator, key)
  defp restore_env(key, value), do: Application.put_env(:favn_orchestrator, key, value)
end

defmodule FavnOrchestrator.RunServer.Execution.PlanPreflightTest.Source do
end
