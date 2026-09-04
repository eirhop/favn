defmodule FavnTestSupport.RunnerTaskPersistence do
  @moduledoc false
  alias Favn.Contracts, as: C
  alias Favn.Manifest
  alias Favn.Manifest.Version
  alias Favn.RelationRef

  def version(
        module_name \\ "Elixir.CrashRecoveryFixture",
        asset_name \\ "asset",
        pool \\ :default
      ) do
    module = String.to_atom(module_name)
    name = String.to_atom(asset_name)
    pool = if is_binary(pool), do: String.to_atom(pool), else: pool

    manifest =
      %Manifest{
        assets: [
          %Manifest.Asset{ref: {module, name}, module: module, name: name, runner_pool: pool}
        ],
        pipelines: []
      }
      |> FavnTestSupport.with_manifest_contract()
      |> FavnTestSupport.with_manifest_graph()

    {:ok, version} = Version.new(manifest)
    version
  end

  def package_version(module_name, asset_name, resolver_name) do
    version = version(module_name, asset_name)
    asset = hd(version.manifest.assets)
    sql = "SELECT 1 AS value"

    execution = %Manifest.SQLExecution{
      sql: sql,
      template: Favn.SQL.Template.compile!(sql, file: "recovery.sql", line: 1),
      runtime_inputs: Favn.RuntimeInputResolver.Ref.new!(String.to_atom(resolver_name))
    }

    {:ok, package} = Manifest.ExecutionPackage.new(asset.ref, execution)
    asset = %{asset | type: :sql, execution_package_hash: package.content_hash}
    {:ok, version} = Version.new(%{version.manifest | assets: [asset]})
    {version, package}
  end

  def tasks(version) do
    ref = hd(version.manifest.assets).ref
    release = FavnTestSupport.runner_release_id()

    pin = %{
      manifest_version_id: version.manifest_version_id,
      manifest_content_hash: version.content_hash,
      required_runner_release_id: release
    }

    target = Favn.TargetIdentity.for_asset(ref)
    previous = "11111111-1111-4111-8111-111111111111"
    candidate = "22222222-2222-4222-8222-222222222222"
    fingerprint = String.duplicate("a", 64)
    now = ~U[2026-09-04 12:34:56.123456Z]
    relation = RelationRef.new!(connection: :default, schema: "public", name: "target")

    marker = %C.GenerationMarker{
      target_id: target,
      active_relation: relation,
      active_generation_id: previous,
      activation_operation_id: "initialize",
      activation_token: "initial-token",
      activated_at: now
    }

    initialize =
      struct!(
        C.GenerationMarkerInitializationRequest,
        Map.merge(pin, %{
          target_id: target,
          target_generation_id: previous,
          active_relation: relation,
          expected_physical_fingerprint: fingerprint,
          initialization_operation_id: "initialize",
          initialization_token: "initial-token"
        })
      )

    activation =
      struct!(
        C.GenerationActivationRequest,
        Map.merge(pin, %{
          target_id: target,
          rebuild_operation_id: "rebuild",
          rebuild_action_id: "activate",
          previous_generation_id: previous,
          candidate_generation_id: candidate,
          active_relation: relation,
          candidate_relation: %{relation | name: "candidate"},
          retired_relation: %{relation | name: "retired"},
          expected_candidate_fingerprint: fingerprint,
          activation_token: "activate-token",
          expected_marker: marker
        })
      )

    candidate_marker = %{
      marker
      | active_generation_id: candidate,
        activation_operation_id: "rebuild",
        activation_token: "activate-token"
    }

    discard =
      struct!(
        C.GenerationDiscardRequest,
        Map.merge(pin, %{
          target_id: target,
          rebuild_operation_id: "rebuild",
          rebuild_action_id: "discard",
          candidate_generation_id: candidate,
          candidate_relation: activation.candidate_relation,
          active_relation: relation,
          discard_token: "discard-token"
        })
      )

    work =
      struct!(
        C.RunnerWork,
        Map.merge(pin, %{
          asset_ref: ref,
          asset_refs: [ref],
          runner_pool: hd(version.manifest.assets).runner_pool,
          run_id: "run",
          asset_step_id: "step",
          run_started_at: now,
          params: %{
            "exact" =>
              {<<0, 255>>, ~D[2026-09-04], ~T[12:34:56.123], ~N[2026-09-04 12:34:56.123456], now,
               Decimal.new("1234.500"), MapSet.new([1, "1"])}
          }
        })
      )

    [
      {:asset_attempt, work,
       struct!(
         C.RunnerResult,
         Map.merge(pin, %{
           run_id: "run",
           status: :ok,
           asset_results: [
             %C.RunnerAssetResult{
               ref: ref,
               status: :ok,
               meta: %{
                 group_replacement: %Favn.SQL.GroupReplacementResult{
                   operation: :replaced,
                   scope_group_count: 1,
                   candidate_row_count: 2,
                   inserted_row_count: 2,
                   deleted_row_count: :unavailable
                 },
                 contract_validation: %Favn.SQL.ContractValidation{
                   status: :passed,
                   expected_columns: [],
                   observed_columns: [],
                   differences: [],
                   observed_column_count: 0
                 }
               }
             }
           ]
         })
       )},
      {:relation_inspection,
       struct!(
         C.RelationInspectionRequest,
         Map.merge(
           pin,
           %{asset_ref: ref, relation: relation, include: [:columns], sample_limit: 0}
         )
       ),
       %C.RelationInspectionResult{
         asset_ref: ref,
         required_runner_release_id: release,
         relation_ref: relation,
         inspected_at: now,
         adapter: "Elixir.FixtureSQLAdapter",
         relation: %Favn.SQL.Relation{name: "target", type: :table},
         columns: [
           %Favn.SQL.Column{
             name: "id",
             data_type: "BIGINT",
             nullable?: false,
             metadata: %{contract_nullability: :reliable}
           }
         ],
         sample: %{"values" => [<<255>>, ~D[2026-09-04]]}
       }},
      {:generation_capabilities,
       %C.GenerationCapabilitiesRequest{manifest: version, asset_ref: ref},
       %C.GenerationCapabilitiesResult{
         capabilities: %{
           transactional_ddl: :supported,
           isolated_candidates: :supported,
           physical_inspection: :supported,
           atomic_swap: :supported,
           marker_reconciliation: :supported,
           idempotent_discard: :supported,
           snapshots: :unsupported,
           max_identifier_bytes: 128
         }
       }},
      {:generation_marker_read,
       %C.GenerationMarkerReadRequest{
         manifest: Version.identity(version),
         asset_ref: ref,
         require_relation_instance?: false
       }, %C.GenerationMarkerReadResult{marker: marker}},
      {:generation_marker_initialize, initialize,
       %C.GenerationMarkerInitializationResult{
         required_runner_release_id: release,
         target_id: target,
         target_generation_id: previous,
         initialization_token: "initial-token",
         outcome: :succeeded,
         observed_marker: marker,
         physical_fingerprint: fingerprint,
         completed_at: now
       }},
      {:generation_activate, activation,
       %C.GenerationActivationResult{
         required_runner_release_id: release,
         target_id: target,
         candidate_generation_id: candidate,
         activation_token: "activate-token",
         outcome: :succeeded,
         observed_marker: candidate_marker,
         candidate_fingerprint: fingerprint,
         physical_fingerprint: fingerprint,
         retired_relation: activation.retired_relation,
         completed_at: now
       }},
      {:generation_reconcile, %C.GenerationReconciliationRequest{activation: activation},
       %C.GenerationReconciliationResult{
         required_runner_release_id: release,
         target_id: target,
         candidate_generation_id: candidate,
         activation_token: "activate-token",
         disposition: :candidate_active,
         observed_marker: candidate_marker,
         candidate_present: false,
         physical_fingerprint: fingerprint,
         reconciled_at: now
       }},
      {:generation_discard, discard,
       %C.GenerationDiscardResult{
         required_runner_release_id: release,
         target_id: target,
         candidate_generation_id: candidate,
         discard_token: "discard-token",
         outcome: :already_absent,
         candidate_present: false,
         observed_marker: marker,
         completed_at: now
       }}
    ]
  end

  def failed_result(version) do
    {:asset_attempt, _work, result} = hd(tasks(version))
    validation = List.last(validations())

    error =
      C.RunnerError.new(
        type: :contract_violation,
        phase: :before_materialize,
        outcome: :safe_failure,
        details: %{contract_validation: validation}
      )

    %{
      result
      | status: :error,
        asset_results: [
          %C.RunnerAssetResult{ref: hd(version.manifest.assets).ref, status: :error, error: error}
        ]
    }
  end

  def validations do
    contract = %Favn.SQL.Contract{
      columns: [Favn.SQL.Contract.Column.new!(:value, :integer, null: false)]
    }

    good = [
      %Favn.SQL.Column{
        name: "value",
        data_type: "BIGINT",
        nullable?: false,
        metadata: %{contract_nullability: :reliable}
      }
    ]

    bad = [
      %Favn.SQL.Column{
        name: "value",
        data_type: "VARCHAR",
        nullable?: true,
        metadata: %{contract_nullability: :reliable}
      }
    ]

    [
      Favn.SQL.ContractValidation.compare(contract, good),
      Favn.SQL.ContractValidation.compare(contract, bad)
    ]
  end

  def digest(value),
    do: :crypto.hash(:sha256, :erlang.term_to_binary(value, [:deterministic])) |> Base.encode16()
end
