defmodule FavnRunner.GenerationOperationsTest do
  use ExUnit.Case, async: false

  alias Favn.Connection.Registry
  alias Favn.Connection.Resolved
  alias Favn.Contracts.GenerationActivationRequest
  alias Favn.Contracts.GenerationActivationResult
  alias Favn.Contracts.GenerationDiscardRequest
  alias Favn.Contracts.GenerationDiscardResult
  alias Favn.Contracts.GenerationMarkerInitializationRequest
  alias Favn.Contracts.GenerationMarkerInitializationResult
  alias Favn.Contracts.GenerationReconciliationRequest
  alias Favn.Contracts.GenerationReconciliationResult
  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Graph
  alias Favn.Manifest.TargetDescriptor
  alias Favn.Manifest.Version
  alias Favn.RelationRef
  alias Favn.SQL.GenerationRelation

  @previous_generation_id "018f47a0-7b0d-4b1a-8d8b-e18a9a987654"
  @candidate_generation_id "018f47a0-7b0d-4b1a-8d8b-e18a9a987655"
  @candidate_fingerprint String.duplicate("c", 64)
  @active_fingerprint String.duplicate("d", 64)

  defmodule Adapter do
    @behaviour Favn.SQL.Adapter
    @behaviour Favn.SQL.GenerationAdapter

    alias Favn.SQL.GenerationActivationResult
    alias Favn.SQL.GenerationCapabilities
    alias Favn.SQL.GenerationInspection
    alias Favn.SQL.GenerationMarker
    alias Favn.SQL.GenerationMarkerInitializationResult

    @impl true
    def connect(%Resolved{}, _opts), do: {:ok, :generation_conn}

    @impl true
    def disconnect(:generation_conn, _opts), do: :ok

    @impl true
    def capabilities(%Resolved{}, _opts), do: {:ok, %Favn.SQL.Capabilities{}}

    @impl true
    def execute(:generation_conn, _statement, _opts),
      do: {:ok, %Favn.SQL.Result{kind: :execute}}

    @impl true
    def query(:generation_conn, _statement, _opts),
      do: {:ok, %Favn.SQL.Result{kind: :query}}

    @impl true
    def introspection_query(_kind, _payload, _opts), do: {:ok, "SELECT 1"}

    @impl true
    def materialization_statements(_plan, _capabilities, _opts), do: {:ok, []}

    @impl Favn.SQL.GenerationAdapter
    def generation_capabilities(%Resolved{}, _opts) do
      {:ok,
       %GenerationCapabilities{
         transactional_ddl: :supported,
         isolated_candidates: :supported,
         physical_inspection: :supported,
         atomic_swap: :supported,
         marker_reconciliation: :supported,
         idempotent_discard: :supported,
         max_identifier_bytes: 128
       }}
    end

    @impl Favn.SQL.GenerationAdapter
    def initialize_generation_marker(:generation_conn, request, _opts) do
      marker = %GenerationMarker{
        logical_target_id: request.logical_target_id,
        active_relation: request.stable_relation,
        active_generation_id: request.active_generation_id,
        activation_operation_id: request.initialization_operation_id,
        activation_token: request.initialization_token,
        activated_at: request.initialized_at
      }

      Application.put_env(:favn_runner, :generation_operations_test_marker, marker)

      {:ok,
       %GenerationMarkerInitializationResult{
         marker: marker,
         physical_fingerprint: request.expected_physical_fingerprint,
         inspection: inspection(request.stable_relation)
       }}
    end

    @impl Favn.SQL.GenerationAdapter
    def activate_generation(:generation_conn, request, _opts) do
      marker = %GenerationMarker{
        logical_target_id: request.logical_target_id,
        active_relation: request.stable_relation,
        active_generation_id: request.candidate_generation_id,
        activation_operation_id: request.activation_operation_id,
        activation_token: request.activation_token,
        activated_at: request.activated_at
      }

      Application.put_env(:favn_runner, :generation_operations_test_marker, marker)

      {:ok,
       %GenerationActivationResult{
         marker: marker,
         candidate_fingerprint: request.expected_candidate_fingerprint,
         physical_fingerprint: String.duplicate("d", 64),
         inspection: inspection(request.stable_relation)
       }}
    end

    @impl Favn.SQL.GenerationAdapter
    def reconcile_generation(:generation_conn, _request, _opts),
      do: {:ok, Application.get_env(:favn_runner, :generation_operations_test_marker)}

    @impl Favn.SQL.GenerationAdapter
    def inspect_generation(:generation_conn, relation, _opts) do
      marker = Application.get_env(:favn_runner, :generation_operations_test_marker)

      if marker && relation.name != marker.active_relation.name,
        do: {:ok, :not_found},
        else: {:ok, inspection(relation)}
    end

    @impl Favn.SQL.GenerationAdapter
    def discard_generation(:generation_conn, _request, _opts) do
      case Application.get_env(:favn_runner, :generation_operations_test_discard_error) do
        nil -> :ok
        %Favn.SQL.Error{} = error -> {:error, error}
      end
    end

    defp inspection(relation) do
      %GenerationInspection{
        relation_ref: relation,
        relation: %Favn.SQL.Relation{
          catalog: relation.catalog,
          schema: relation.schema,
          name: relation.name,
          type: :table
        },
        columns: [],
        physical_fingerprint: %Favn.TargetCompatibility.PhysicalFingerprint{
          adapter: Atom.to_string(__MODULE__),
          relation: %{
            catalog: relation.catalog,
            schema: relation.schema,
            name: relation.name,
            kind: "table"
          },
          columns: [],
          fingerprint: String.duplicate("d", 64)
        }
      }
    end
  end

  setup do
    previous = Registry.list(registry_name: FavnRunner.ConnectionRegistry)
    Application.delete_env(:favn_runner, :generation_operations_test_marker)
    Application.delete_env(:favn_runner, :generation_operations_test_discard_error)

    Registry.reload(
      %{
        generation_warehouse: %Resolved{
          name: :generation_warehouse,
          adapter: Adapter,
          module: __MODULE__,
          config: %{}
        }
      },
      registry_name: FavnRunner.ConnectionRegistry
    )

    on_exit(fn ->
      Application.delete_env(:favn_runner, :generation_operations_test_marker)
      Application.delete_env(:favn_runner, :generation_operations_test_discard_error)

      Registry.reload(Map.new(previous, &{&1.name, &1}),
        registry_name: FavnRunner.ConnectionRegistry
      )
    end)

    :ok
  end

  test "runner maps activation and reconciliation through exact boundary contracts" do
    {version, asset} = registered_target()
    manifest_identity = %{version | manifest: nil}

    assert {:ok, capabilities} =
             FavnRunner.generation_capabilities(manifest_identity, asset.ref)

    assert capabilities.atomic_swap == :supported
    assert capabilities.marker_reconciliation == :supported

    initialization = initialization_request(version, asset)

    assert {:ok, %GenerationMarkerInitializationResult{outcome: :succeeded} = initialized} =
             FavnRunner.initialize_generation_marker(initialization)

    assert initialized.physical_fingerprint == @active_fingerprint

    assert {:ok, initialized.observed_marker} ==
             FavnRunner.generation_marker(manifest_identity, asset.ref)

    request = activation_request(version, asset, initialized.observed_marker)

    assert {:ok, %GenerationActivationResult{outcome: :succeeded} = result} =
             FavnRunner.activate_generation(request)

    assert result.candidate_fingerprint == @candidate_fingerprint
    assert result.physical_fingerprint == @active_fingerprint
    assert result.observed_marker.activation_operation_id == request.rebuild_operation_id
    assert :ok = GenerationActivationResult.validate(result, request)

    reconciliation = %GenerationReconciliationRequest{activation: request}

    assert {:ok, %GenerationReconciliationResult{disposition: :candidate_active} = observed} =
             FavnRunner.reconcile_generation(reconciliation)

    assert observed.physical_fingerprint == @active_fingerprint
    assert :ok = GenerationReconciliationResult.validate(observed, reconciliation)

    retired_discard = %GenerationDiscardRequest{
      manifest_version_id: version.manifest_version_id,
      manifest_content_hash: version.content_hash,
      required_runner_release_id: version.required_runner_release_id,
      rebuild_operation_id: request.rebuild_operation_id,
      rebuild_action_id: request.rebuild_action_id,
      target_id: request.target_id,
      candidate_generation_id: @previous_generation_id,
      active_relation: asset.relation,
      candidate_relation:
        GenerationRelation.retired(asset.relation, @previous_generation_id, 128),
      discard_token: "cleanup-retired-generation-operations",
      relation_kind: :retired
    }

    assert {:ok, %GenerationDiscardResult{outcome: :already_absent} = retired_result} =
             FavnRunner.discard_generation(retired_discard)

    assert :ok = GenerationDiscardResult.validate(retired_result, retired_discard)

    discard = %GenerationDiscardRequest{
      manifest_version_id: version.manifest_version_id,
      manifest_content_hash: version.content_hash,
      required_runner_release_id: version.required_runner_release_id,
      rebuild_operation_id: request.rebuild_operation_id,
      rebuild_action_id: request.rebuild_action_id,
      target_id: request.target_id,
      candidate_generation_id: request.candidate_generation_id,
      active_relation: %{asset.relation | name: "another_target"},
      candidate_relation: request.candidate_relation,
      discard_token: "discard-generation-operations"
    }

    assert {:error, :generation_relation_mismatch} = FavnRunner.discard_generation(discard)

    Application.put_env(
      :favn_runner,
      :generation_operations_test_discard_error,
      %Favn.SQL.Error{
        type: :introspection_mismatch,
        message: "candidate is active",
        retryable?: false,
        details: %{classification: :active_generation_discard_forbidden}
      }
    )

    active_discard = %{discard | active_relation: asset.relation}

    assert {:ok, %GenerationDiscardResult{outcome: :outcome_unknown} = discard_result} =
             FavnRunner.discard_generation(active_discard)

    assert discard_result.observed_marker.active_generation_id == @candidate_generation_id
    assert discard_result.candidate_present == nil
    assert :ok = GenerationDiscardResult.validate(discard_result, active_discard)
  end

  test "full manifest generation reads do not depend on manifest cache residency" do
    {version, asset} = registered_target()
    {:ok, empty_store} = start_supervised({FavnRunner.ManifestStore, name: nil})

    assert {:ok, capabilities} =
             FavnRunner.generation_capabilities(version, asset.ref, manifest_store: empty_store)

    assert capabilities.atomic_swap == :supported
  end

  defp registered_target do
    ref = {MyApp.GenerationTarget, :asset}
    relation = RelationRef.new!(connection: :generation_warehouse, schema: "main", name: "target")

    asset = %Asset{
      ref: ref,
      module: elem(ref, 0),
      name: elem(ref, 1),
      type: :sql,
      relation: relation,
      materialization: :table,
      execution_package_hash: String.duplicate("a", 64)
    }

    descriptor =
      TargetDescriptor.from_asset(Map.from_struct(asset),
        connection_definitions: %{
          generation_warehouse: %{adapter: Adapter, module: __MODULE__}
        },
        manifest_schema_version: 11,
        runner_contract_version: 11
      )

    asset = %{asset | target_descriptor: descriptor}

    manifest =
      %Manifest{
        schema_version: 11,
        runner_contract_version: 11,
        required_runner_release_id: FavnTestSupport.runner_release_id(),
        assets: [asset],
        graph: %Graph{nodes: [ref], topo_order: [ref]}
      }

    {:ok, version} =
      Version.new(manifest,
        manifest_version_id:
          "mv_generation_operations_" <>
            Base.encode16(:crypto.strong_rand_bytes(6), case: :lower)
      )

    :ok = FavnRunner.register_manifest(version)
    {version, hd(version.manifest.assets)}
  end

  defp initialization_request(version, asset) do
    %GenerationMarkerInitializationRequest{
      manifest_version_id: version.manifest_version_id,
      manifest_content_hash: version.content_hash,
      required_runner_release_id: version.required_runner_release_id,
      target_id: asset.target_descriptor.target_id,
      target_generation_id: @previous_generation_id,
      active_relation: asset.relation,
      expected_physical_fingerprint: @active_fingerprint,
      initialization_operation_id: "initialization-generation-operations",
      initialization_token: "initialization-token-generation-operations"
    }
  end

  defp activation_request(version, asset, expected_marker) do
    max_identifier_bytes = 128

    %GenerationActivationRequest{
      manifest_version_id: version.manifest_version_id,
      manifest_content_hash: version.content_hash,
      required_runner_release_id: version.required_runner_release_id,
      rebuild_operation_id: "rebuild-generation-operations",
      rebuild_action_id: "action-generation-operations",
      target_id: asset.target_descriptor.target_id,
      previous_generation_id: @previous_generation_id,
      candidate_generation_id: @candidate_generation_id,
      expected_candidate_fingerprint: @candidate_fingerprint,
      active_relation: asset.relation,
      candidate_relation:
        GenerationRelation.candidate(
          asset.relation,
          @candidate_generation_id,
          max_identifier_bytes
        ),
      retired_relation:
        GenerationRelation.retired(
          asset.relation,
          @previous_generation_id,
          max_identifier_bytes
        ),
      activation_token: "activation-generation-operations",
      expected_marker: expected_marker
    }
  end
end
