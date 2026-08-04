defmodule FavnOrchestrator.RunManager.RefreshPolicyDependenciesTest do
  @moduledoc """
  Covers the rule that forcing upstream assets requires traversing dependencies.

  `FavnOrchestrator.RunSubmission.AssetOptions` refuses the pair for callers that
  describe a refresh by operator mode, and its own test covers that. A caller passing
  the runtime `{:force_assets, refs, include_upstream: true}` tuple bypasses those
  clauses entirely, so the builder is the only thing standing between it and run
  metadata claiming to force an upstream asset that was never planned.

  That matters beyond the submission: `Catalogue.Timeline` reads the pair back out of
  run metadata to prefill a retry, and the run dialog refuses it. Were the pair ever
  persisted, the dialog would open on a configuration it will not submit.
  """

  use ExUnit.Case, async: false

  alias Favn.Coverage.Effective
  alias Favn.Coverage.Spec, as: CoverageSpec
  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Version
  alias Favn.Window.Spec, as: WindowSpec
  alias FavnOrchestrator.Persistence.Queries.GetDeploymentManifest
  alias FavnOrchestrator.Persistence.Queries.GetRuntimeState
  alias FavnOrchestrator.Persistence.Results.RuntimeState
  alias FavnOrchestrator.Persistence.Runtime
  alias FavnOrchestrator.Persistence.Stores
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.RunManager.SubmissionBuilder

  @asset_ref {__MODULE__.Orders, :orders}

  defmodule Store do
    def get_runtime_state(%GetRuntimeState{}) do
      version = Process.get(:refresh_policy_version)

      {:ok,
       %RuntimeState{
         workspace_id: "workspace",
         deployment_id: "deployment",
         manifest_version_id: version.manifest_version_id,
         revision: 1
       }}
    end

    def get_deployment_manifest(%GetDeploymentManifest{}),
      do: {:ok, Process.get(:refresh_policy_version)}

    def get_evidence_bindings(query),
      do: {:ok, Enum.map(query.target_ids, &%{target_id: &1, evidence_generation_id: "ag_1"})}
  end

  setup do
    Process.put(:refresh_policy_version, manifest_version())

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

  test "forcing upstream is refused when dependencies are excluded", %{context: context} do
    assert {:error, {:refresh_include_upstream_requires_dependencies, :all}} =
             build(context, dependencies: :none, refresh: force_upstream())
  end

  test "forcing upstream is accepted when dependencies are traversed", %{context: context} do
    assert {:ok, submission} =
             build(context, dependencies: :all, refresh: force_upstream())

    assert submission.run_state.metadata.refresh_policy.include_upstream? == true
    assert submission.run_state.metadata.asset_dependencies == :all
  end

  # Forcing this asset alone carries no claim about upstream, so excluding dependencies
  # is coherent and the metadata says so.
  test "forcing only the selected asset is accepted without dependencies", %{context: context} do
    assert {:ok, submission} =
             build(context,
               dependencies: :none,
               refresh: {:force_assets, [@asset_ref], [include_upstream: false]}
             )

    assert submission.run_state.metadata.refresh_policy.include_upstream? == false
    assert submission.run_state.metadata.asset_dependencies == :none
  end

  defp force_upstream, do: {:force_assets, [@asset_ref], [include_upstream: true]}

  defp build(context, opts) do
    SubmissionBuilder.persisted_target(
      context,
      :asset,
      @asset_ref,
      "deployment",
      "refresh-policy-manifest",
      "refresh-policy-run-#{System.unique_integer([:positive])}",
      opts
    )
  end

  defp manifest_version do
    window = WindowSpec.new!(:day, timezone: "Etc/UTC")

    coverage =
      CoverageSpec.new!(from: ~D[2026-01-01])
      |> Effective.resolve(window, nil)
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
      pipelines: []
    }

    {:ok, version} =
      Version.new(
        manifest
        |> FavnTestSupport.with_manifest_graph()
        |> FavnTestSupport.with_manifest_contract(),
        manifest_version_id: "refresh-policy-manifest"
      )

    version
  end
end
