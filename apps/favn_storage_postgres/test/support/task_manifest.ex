defmodule FavnStoragePostgres.TestSupport.TaskManifest do
  @moduledoc false
  alias Favn.Manifest
  alias Favn.Manifest.Version
  alias FavnOrchestrator.Persistence.Commands, as: C
  alias FavnOrchestrator.Persistence.Queries.ManifestSelector.ById
  alias FavnStoragePostgres.Registry.Store

  def prepare(fixture, payload, pool, release) do
    version =
      case payload.manifest_version_id &&
             Store.get_manifest(%ById{manifest_version_id: payload.manifest_version_id}) do
        {:ok, version} -> version
        _missing -> version(pool, release, Map.get(payload, :asset_ref))
      end

    retain(fixture, version)

    payload = %{
      payload
      | manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        required_runner_release_id: release
    }

    payload =
      if is_struct(payload, Favn.Contracts.RunnerWork),
        do: %{
          payload
          | runner_pool: String.to_atom(to_string(pool)),
            asset_ref: payload.asset_ref || hd(version.manifest.assets).ref
        },
        else: payload

    payload =
      if is_struct(payload, Favn.Contracts.RelationInspectionRequest) and
           is_nil(payload.asset_ref) and is_nil(payload.relation),
         do: %{payload | relation: Favn.RelationRef.new!(connection: :default, name: "fixture")},
         else: payload

    {payload, version}
  end

  def version(pool, release, ref \\ nil) do
    {module, name} = ref || {__MODULE__.Asset, :asset}

    manifest =
      %Manifest{
        assets: [
          %Manifest.Asset{
            ref: {module, name},
            module: module,
            name: name,
            runner_pool: String.to_atom(to_string(pool))
          }
        ],
        pipelines: []
      }
      |> FavnTestSupport.with_manifest_contract(release)
      |> FavnTestSupport.with_manifest_graph()

    {:ok, version} = Version.new(manifest)
    version
  end

  def sql_work(fixture) do
    ref = {__MODULE__.SQL, :write_test}
    relation = Favn.RelationRef.new!(connection: :default, name: "write_test")
    sql = "SELECT 1 AS value"

    {:ok, package} =
      Manifest.ExecutionPackage.new(ref, %Manifest.SQLExecution{
        sql: sql,
        template: Favn.SQL.Template.compile!(sql, file: "write_test.sql", line: 1)
      })

    asset =
      %Manifest.Asset{
        ref: ref,
        module: elem(ref, 0),
        name: elem(ref, 1),
        type: :sql,
        runner_pool: String.to_atom(fixture.runner_pool),
        relation: relation,
        materialization: :table,
        execution_package_hash: package.content_hash
      }
      |> FavnTestSupport.with_target_descriptor()

    manifest =
      %Manifest{assets: [asset], pipelines: []}
      |> FavnTestSupport.with_manifest_contract()
      |> FavnTestSupport.with_manifest_graph()

    {:ok, version} = Version.new(manifest)

    :ok =
      Store.register_execution_packages(%C.RegisterExecutionPackages{
        platform_context: fixture.platform_context,
        packages: [package]
      })

    retain(fixture, version)

    {:ok, %{generation: generation}} =
      FavnStoragePostgres.TargetGenerations.Store.ensure_writable(
        %C.EnsureWritableTargetGeneration{
          workspace_context: fixture.workspace_context,
          command_id: "generation:" <> fixture.workspace_id,
          target_id: asset.target_descriptor.target_id,
          manifest_version_id: version.manifest_version_id,
          descriptor: asset.target_descriptor,
          occurred_at: fixture.now
        }
      )

    work = %Favn.Contracts.RunnerWork{
      run_id: "run-" <> fixture.workspace_id,
      asset_ref: ref,
      asset_step_id: "step",
      runner_pool: asset.runner_pool,
      manifest_version_id: version.manifest_version_id,
      manifest_content_hash: version.content_hash,
      required_runner_release_id: FavnTestSupport.runner_release_id(),
      execution_package: package,
      target_operation: :normal_materialization,
      logical_target_id: asset.target_descriptor.target_id,
      target_descriptor_hash: asset.target_descriptor.descriptor_hash,
      target_generation_id: generation.target_generation_id,
      active_relation: relation,
      write_relation: relation
    }

    {version, work}
  end

  def ownership_claim(fixture, version, work, purpose \\ :ownership_only) do
    deployment_id = retain(fixture, version)
    {module, name} = work.asset_ref

    run =
      FavnOrchestrator.RunState.new(
        id: work.run_id,
        workspace_id: fixture.workspace_id,
        deployment_id: deployment_id,
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        runner_releases: version.runner_releases,
        asset_ref: work.asset_ref,
        target_refs: [work.asset_ref],
        plan: Map.get(fixture, :plan),
        timeout_ms: Map.get(fixture, :timeout_ms, 300_000)
      )

    {:ok, _} =
      FavnStoragePostgres.Runs.Store.create_run(%C.CreateRun{
        workspace_context: fixture.workspace_context,
        command_id: "create:" <> run.id,
        deployment_id: deployment_id,
        run: run,
        targets: [
          %C.RunTarget{
            target_kind: :asset,
            target_id: work.logical_target_id,
            target_module: Atom.to_string(module),
            target_name: Atom.to_string(name),
            is_primary: true
          }
        ],
        event: %{
          run_id: run.id,
          sequence: 1,
          event_type: :run_submitted,
          status: :pending,
          occurred_at: run.inserted_at
        }
      })

    {:ok, %{status: :claimed, claim: claim}} =
      FavnStoragePostgres.Materialization.Store.claim(%C.ClaimMaterialization{
        workspace_context: fixture.workspace_context,
        command_id: "claim:" <> run.id,
        claim_key: "claim:" <> run.id,
        purpose: purpose,
        deployment_id: deployment_id,
        target_kind: :asset,
        target_id: work.logical_target_id,
        target_generation_id: work.target_generation_id,
        evidence_generation_id: work.target_generation_id,
        partition_key: "latest",
        run_id: run.id,
        owner_id: "fixture-owner",
        lease_duration_ms: 60_000,
        occurred_at: fixture.now
      })

    Map.from_struct(claim)
  end

  def retain(fixture, version) do
    {:ok, _} =
      Store.register_manifest(%C.RegisterManifest{
        platform_context: fixture.platform_context,
        version: version
      })

    id = "task-fixture-" <> version.content_hash

    targets =
      Enum.map(version.manifest.assets, fn asset ->
        target = Favn.TargetIdentity.for_asset(asset.ref)

        %C.DeploymentTarget{
          target_kind: :asset,
          target_id: target,
          selection_source: :common,
          customer_visible: true,
          descriptor: %{"target_id" => target, "label" => target}
        }
      end)

    {:ok, _} =
      Store.deploy_manifest(%C.DeployManifest{
        platform_context: fixture.platform_context,
        workspace_context: fixture.workspace_context,
        deployment_id: id,
        manifest_version_id: version.manifest_version_id,
        configuration: %{"resources" => %{}},
        targets: targets,
        target_compatibilities:
          for(
            %{target_descriptor: %Manifest.TargetDescriptor{} = d} <- version.manifest.assets,
            do: %C.DeploymentTargetCompatibility{
              target_id: d.target_id,
              desired_descriptor_hash: d.descriptor_hash,
              compatibility_status: :uninitialized,
              reason_code: "no_active_generation",
              compatibility_diff: %{},
              expected_binding_version: nil,
              expected_active_generation_id: nil,
              active_physical_fingerprint: nil
            }
          ),
        occurred_at: fixture.now
      })

    id
  end
end
