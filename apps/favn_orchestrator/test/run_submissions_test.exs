defmodule FavnOrchestrator.RunSubmissionsTest do
  use ExUnit.Case, async: false

  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Version
  alias FavnOrchestrator.Persistence.Commands.EnqueueRunSubmission
  alias FavnOrchestrator.Persistence.Commands.RequestRunSubmissionCancellation
  alias FavnOrchestrator.Persistence.Queries.GetDeploymentManifest
  alias FavnOrchestrator.Persistence.Queries.GetEvidenceBindings
  alias FavnOrchestrator.Persistence.Queries.GetRuntimeState
  alias FavnOrchestrator.Persistence.Results.RunSubmission
  alias FavnOrchestrator.Persistence.Results.RuntimeState
  alias FavnOrchestrator.Persistence.Runtime
  alias FavnOrchestrator.Persistence.Stores
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.MemoryCapacity
  alias FavnOrchestrator.MemoryCapacity.Budget
  alias FavnOrchestrator.ExecutionPoolPolicy
  alias FavnOrchestrator.ConnectionCircuitPolicy
  alias FavnOrchestrator.WorkspaceConfiguration
  alias FavnOrchestrator.RunSubmission.Intent
  alias FavnOrchestrator.RunSubmission.Preparation
  alias FavnOrchestrator.RunManager
  alias FavnOrchestrator.RunManager.SubmissionBuilder
  alias FavnOrchestrator.RunSubmissions

  @asset_ref {__MODULE__.Events, :events}

  defmodule Store do
    def get_runtime_state(%GetRuntimeState{}) do
      version = Process.get(:run_submissions_version)

      {:ok,
       %RuntimeState{
         workspace_id: "workspace",
         deployment_id: "deployment",
         manifest_version_id: version.manifest_version_id,
         revision: 1
       }}
    end

    def get_deployment_manifest(%GetDeploymentManifest{}),
      do: {:ok, Process.get(:run_submissions_version)}

    def get_manifest_size(_selector), do: {:ok, 1_024}

    def get_deployment_configuration(_query),
      do: {:ok, Process.get(:run_submissions_configuration)}

    def get_evidence_bindings(%GetEvidenceBindings{target_ids: target_ids}) do
      {:ok,
       Enum.map(target_ids, fn target_id ->
         %{target_id: target_id, evidence_generation_id: "ag_run_submissions"}
       end)}
    end

    def enqueue(%EnqueueRunSubmission{} = command) do
      now = DateTime.utc_now()

      submission =
        struct(RunSubmission,
          workspace_id: command.workspace_context.workspace_id,
          submission_id: command.submission_id,
          source: command.source,
          idempotency_key: command.idempotency_key,
          request_hash: command.request_hash,
          deployment_id: command.deployment_id,
          manifest_version_id: command.manifest_version_id,
          target_kind: command.target_kind,
          target_id: command.target_id,
          run_id: command.run_id,
          intent: command.intent,
          status: :queued,
          attempt: 0,
          claim_generation: 0,
          retry_root_id: command.submission_id,
          enqueued_at: now,
          available_at: now,
          inserted_at: now,
          updated_at: now
        )

      Process.put(:run_submission_command, command)
      Process.put(:run_submission, submission)
      {:ok, submission}
    end

    def get_by_run_id(_query), do: {:ok, Process.get(:run_submission)}

    def request_cancellation(%RequestRunSubmissionCancellation{} = command) do
      Process.put(:run_submission_cancellation, command)
      {:ok, %{Process.get(:run_submission) | status: :cancelled}}
    end
  end

  setup do
    version = manifest_version()

    assert {:ok, policy} =
             ExecutionPoolPolicy.resolve(version.manifest, %{}, %{}, %{
               approve_manifest_defaults: true
             })

    assert {:ok, configuration} =
             WorkspaceConfiguration.put(policy.configuration, version.manifest)

    assert {:ok, configuration} =
             ConnectionCircuitPolicy.put(configuration, version.manifest)

    Process.put(:run_submissions_version, version)
    Process.put(:run_submissions_configuration, configuration)

    stores =
      struct(Stores,
        registry: Store,
        runs: Store,
        run_submissions: Store,
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

  test "producer persists a frozen intent and returns before planning", %{context: context} do
    assert {:ok, "run-reserved"} =
             RunSubmissions.enqueue_asset(context, @asset_ref,
               run_id: "run-reserved",
               submission_source: :api,
               dependencies: :none,
               metadata: %{
                 "execution_pool_policy" => %{"untrusted" => %{"max_concurrency" => 99}},
                 "connection_circuit_policy" => %{
                   "untrusted" => %{"failure_threshold" => 99}
                 },
                 requested_by: :test
               }
             )

    assert %EnqueueRunSubmission{} = command = Process.get(:run_submission_command)
    assert command.run_id == "run-reserved"
    assert command.source == :api
    assert command.deployment_id == "deployment"
    assert command.manifest_version_id == "run-submissions-manifest"
    target_id = command.target_id

    assert {:ok,
            {:asset, ^target_id,
             [
               dependencies: :none,
               metadata: %{
                 "execution_pool_policy" => %{
                   "untrusted" => %{"max_concurrency" => 99}
                 },
                 "connection_circuit_policy" => %{
                   "untrusted" => %{"failure_threshold" => 99}
                 },
                 requested_by: :test
               }
             ]}} =
             Intent.decode(command.intent)

    {:ok, token} = MemoryCapacity.acquire(Budget.index_max(), kind: :test_run_submission)
    on_exit(fn -> MemoryCapacity.release(token) end)

    assert {:ok, prepared, summary} =
             Preparation.prepare(context, Process.get(:run_submission),
               memory_capacity_token: token
             )

    assert prepared.run_state.id == "run-reserved"
    assert prepared.run_state.deployment_id == "deployment"
    assert prepared.run_state.manifest_version_id == "run-submissions-manifest"

    assert get_in(prepared.run_state.metadata, [
             :execution_pool_policy,
             "partner_api",
             "max_concurrency"
           ]) == 3

    refute Map.has_key?(prepared.run_state.metadata, "execution_pool_policy")
    refute Map.has_key?(prepared.run_state.metadata, "connection_circuit_policy")

    assert get_in(prepared.run_state.metadata, [
             :connection_circuit_policy,
             "warehouse",
             "failure_threshold"
           ]) == 5

    assert summary["run_id"] == "run-reserved"
  end

  test "remaining-run retry selectors survive the bounded intent codec", %{context: context} do
    assert {:ok, "run-retry"} =
             RunSubmissions.enqueue_asset(context, @asset_ref,
               run_id: "run-retry",
               target_refs: [@asset_ref],
               replay_node_keys: [{{@asset_ref, nil}, nil}]
             )

    command = Process.get(:run_submission_command)

    assert {:ok,
            {:asset, _selector,
             [
               target_refs: [@asset_ref],
               replay_node_keys: [{{@asset_ref, nil}, nil}]
             ]}} = Intent.decode(command.intent)
  end

  test "cancellation applies to a reserved run before admission", %{context: context} do
    assert {:ok, "run-cancel-before-admission"} =
             RunSubmissions.enqueue_asset(context, @asset_ref,
               run_id: "run-cancel-before-admission"
             )

    assert :ok =
             FavnOrchestrator.cancel_run(
               context,
               "run-cancel-before-admission",
               %{actor_id: "operator"}
             )

    assert %RequestRunSubmissionCancellation{} =
             command = Process.get(:run_submission_cancellation)

    assert command.submission_id == Process.get(:run_submission).submission_id
    assert Jason.decode!(command.reason) == %{"actor_id" => "operator"}
  end

  test "legacy synchronous producer entrypoints are absent" do
    for {module, function, arity} <- [
          {RunManager, :submit_asset_run, 3},
          {RunManager, :submit_pipeline_run, 3},
          {RunManager, :submit_pipeline_module_run, 3},
          {RunManager, :submit_pipeline_ref_run, 3},
          {RunManager, :rerun, 3},
          {RunManager, :prepare_rerun, 3},
          {RunManager, :admit_prepared_submission, 1},
          {SubmissionBuilder, :asset, 3},
          {SubmissionBuilder, :pipeline, 3},
          {SubmissionBuilder, :pipeline_module, 3},
          {SubmissionBuilder, :pipeline_ref, 3},
          {SubmissionBuilder, :rerun, 3}
        ] do
      refute function_exported?(module, function, arity),
             "legacy synchronous producer remains: #{inspect(module)}.#{function}/#{arity}"
    end
  end

  defp manifest_version do
    manifest = %Manifest{
      execution_pools: %{partner_api: %{max_concurrency: 3}},
      connection_circuits: %{
        "warehouse" => %{failure_threshold: 5, probe_after_ms: 10_000}
      },
      assets: [
        %Asset{
          ref: @asset_ref,
          module: elem(@asset_ref, 0),
          name: elem(@asset_ref, 1),
          execution_pool: :partner_api
        }
      ]
    }

    {:ok, version} =
      Version.new(
        manifest
        |> FavnTestSupport.with_manifest_graph()
        |> FavnTestSupport.with_manifest_contract(),
        manifest_version_id: "run-submissions-manifest"
      )

    version
  end
end
