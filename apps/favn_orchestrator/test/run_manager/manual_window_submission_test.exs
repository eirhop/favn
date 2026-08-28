defmodule FavnOrchestrator.RunManager.ManualWindowSubmissionTest do
  use ExUnit.Case, async: false

  alias Favn.Coverage.Effective
  alias Favn.Coverage.Spec, as: CoverageSpec
  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Pipeline
  alias Favn.Manifest.Version
  alias Favn.Window.Policy
  alias Favn.Window.Spec, as: WindowSpec
  alias FavnOrchestrator.MemoryCapacity
  alias FavnOrchestrator.MemoryCapacity.Budget
  alias FavnOrchestrator.Persistence.Queries.GetDeploymentManifest
  alias FavnOrchestrator.Persistence.Queries.GetRun
  alias FavnOrchestrator.Persistence.Queries.GetRuntimeState
  alias FavnOrchestrator.Persistence.Results.RuntimeState
  alias FavnOrchestrator.Persistence.Runtime
  alias FavnOrchestrator.Persistence.Stores
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.RunManager.SubmissionBuilder

  @asset_ref {__MODULE__.Events, :events}
  @pipeline_ref {__MODULE__.Daily, :daily}
  @evaluated_at ~U[2026-07-01 00:30:00Z]

  defmodule Store do
    def get_runtime_state(%GetRuntimeState{}) do
      version = Process.get(:manual_window_version)

      {:ok,
       %RuntimeState{
         workspace_id: "workspace",
         deployment_id: "deployment",
         manifest_version_id: version.manifest_version_id,
         revision: 1
       }}
    end

    def get_deployment_manifest(%GetDeploymentManifest{}),
      do: {:ok, Process.get(:manual_window_version)}

    def get_manifest_size(_selector), do: {:ok, 1_024}
    def get_deployment_configuration(_query), do: {:ok, %{}}

    def get_run_size(%GetRun{}), do: {:ok, 1_024}

    def get_run(%GetRun{}),
      do: {:ok, Application.fetch_env!(:favn_orchestrator, :manual_window_source_run)}

    def get_evidence_bindings(query) do
      {:ok, Enum.map(query.target_ids, &evidence_binding/1)}
    end

    defp evidence_binding(target_id) do
      digest = target_id |> then(&:crypto.hash(:sha256, &1)) |> Base.encode16(case: :lower)
      %{target_id: target_id, evidence_generation_id: "ag_" <> digest}
    end
  end

  setup do
    version = manifest_version()
    Process.put(:manual_window_version, version)

    on_exit(fn ->
      Application.delete_env(:favn_orchestrator, :manual_window_source_run)
    end)

    stores =
      struct(Stores,
        registry: Store,
        runs: Store,
        run_ownership: Store,
        scheduler: Store,
        admission: Store,
        resource_circuits: Store,
        target_generations: Store,
        rebuilds: Store,
        target_operation_locks: Store,
        materialization: Store,
        backfills: Store,
        operator_reads: Store,
        logs: Store,
        identity: Store,
        maintenance: Store
      )

    start_supervised!({Runtime, %Runtime{backend: __MODULE__, options: [], stores: stores}})
    {:ok, context} = WorkspaceContext.new("workspace", "operator", [:customer_operator])

    %{context: context}
  end

  test "builder persists one default selection and rerun preserves it", %{context: context} do
    {:ok, token} = MemoryCapacity.acquire(Budget.index_max(), kind: :test_run_submission)
    on_exit(fn -> MemoryCapacity.release(token) end)

    assert {:ok, submission} =
             SubmissionBuilder.persisted_target(
               context,
               :pipeline,
               @pipeline_ref,
               "deployment",
               "manual-window-manifest",
               "manual-window-run",
               window_evaluated_at: @evaluated_at,
               _memory_capacity_token: token
             )

    source = submission.run_state
    selection = source.metadata.window_selection

    assert [anchor] = selection.requested_anchors
    assert anchor.start_at == ~U[2026-06-29 00:00:00Z]

    assert source.metadata.manual_window_resolution == %{
             mode: :latest_complete,
             evaluated_at: "2026-07-01T00:30:00Z",
             availability_delay_seconds: 3_600
           }

    legacy_source = %{
      source
      | status: :ok,
        metadata:
          source.metadata
          |> Map.put("execution_pool_policy", %{"spoofed" => %{}})
          |> Map.put("connection_circuit_policy", %{"spoofed" => %{}})
    }

    Application.put_env(:favn_orchestrator, :manual_window_source_run, legacy_source)

    assert {:ok, rerun} =
             SubmissionBuilder.persisted_rerun(
               context,
               source.id,
               "deployment",
               "manual-window-manifest",
               "manual-window-rerun",
               window_evaluated_at: ~U[2030-01-01 00:00:00Z],
               _memory_capacity_token: token
             )

    assert rerun.run_state.metadata.window_selection == selection

    assert rerun.run_state.metadata.manual_window_resolution ==
             source.metadata.manual_window_resolution

    assert rerun.run_state.metadata.execution_pool_policy == %{}
    assert rerun.run_state.metadata.connection_circuit_policy == %{}
    refute Map.has_key?(rerun.run_state.metadata, "execution_pool_policy")
    refute Map.has_key?(rerun.run_state.metadata, "connection_circuit_policy")
  end

  defp manifest_version do
    window = WindowSpec.new!(:day, timezone: "Etc/UTC")

    coverage =
      Effective.resolve(
        CoverageSpec.new!(
          from: ~D[2026-01-01],
          availability_delay: {:hours, 1}
        ),
        window,
        nil
      )
      |> elem(1)

    manifest = %Manifest{
      assets: [
        %Asset{
          ref: @asset_ref,
          module: elem(@asset_ref, 0),
          name: elem(@asset_ref, 1),
          window: window,
          coverage: coverage
        }
      ],
      pipelines: [
        %Pipeline{
          module: elem(@pipeline_ref, 0),
          name: elem(@pipeline_ref, 1),
          selectors: [{:asset, @asset_ref}],
          window: Policy.new!(:daily, timezone: "Etc/UTC")
        }
      ]
    }

    {:ok, version} =
      Version.new(
        manifest
        |> FavnTestSupport.with_manifest_graph()
        |> FavnTestSupport.with_manifest_contract(),
        manifest_version_id: "manual-window-manifest"
      )

    version
  end
end
