# Elastic runners legacy inventory

This Step 0 inventory is the reproducible removal baseline for
[`elastic-runners.md`](elastic-runners.md). It covers live source, tests,
configuration, and current documentation. Historical archive, report, and
refactor material is excluded. The plan and implementation log are excluded so
they can name legacy concepts without keeping the baseline nonzero.

Generated from baseline `b8cb26771729edd154820099a70bd9a49689daf6` with:

```powershell
$pattern = 'Favn\.Contracts\.RunnerClient|RunnerClient\.BeamNode|RunnerClientValidator|RunnerDispatch|RunnerHealth|RunnerDiagnostics|RunnerManifestRegistration|ActiveManifestReconciler|RunnerLogBridge|RunnerReplacement|RunExecutionOwnership|RunnerExecutionIdentity|ExecutionOwnershipCodec|RunnerReleaseCompatibility|RuntimeStarter|FavnRunner\.Server|FavnRunner\.ResultRetention|FavnRunner\.ExecutionLifecycle|FavnRunner\.Shutdown|FavnLocal\.(RunnerMain|RunnerChild|Lifecycle)|runner_executions|runner_execution_id|inflight_execution_ids|runner_ref|FAVN_RUNNER_NODE'

rg -l --sort path `
  -g '!docs/archive/**' `
  -g '!docs/refactor/**' `
  -g '!docs/report/**' `
  -g '!docs/architecture/elastic-runners*.md' `
  $pattern apps config docs
```

Baseline count: 124 files.

```text
apps\favn\priv\templates\deployment\compose.yml
apps\favn\priv\templates\deployment\env.sh.eex
apps\favn_core\lib\favn\contracts\runner_client.ex
apps\favn_core\lib\favn\log\entry.ex
apps\favn_core\lib\favn\log\filter.ex
apps\favn_core\lib\favn\run\node_result.ex
apps\favn_core\test\contracts\runner_client_test.exs
apps\favn_core\test\run\node_result_test.exs
apps\favn_local\lib\favn_local\config.ex
apps\favn_local\lib\favn_local\lifecycle.ex
apps\favn_local\lib\favn_local\runner_child.ex
apps\favn_local\lib\favn_local\runner_main.ex
apps\favn_local\lib\favn_local\supervisor.ex
apps\favn_local\lib\favn_local.ex
apps\favn_local\test\acceptance\docker_free_local_lifecycle_test.exs
apps\favn_orchestrator\lib\favn\run.ex
apps\favn_orchestrator\lib\favn_orchestrator\active_manifest_reconciler.ex
apps\favn_orchestrator\lib\favn_orchestrator\application.ex
apps\favn_orchestrator\lib\favn_orchestrator\diagnostics.ex
apps\favn_orchestrator\lib\favn_orchestrator\initial_target_generation_reconciler.ex
apps\favn_orchestrator\lib\favn_orchestrator\log_writer.ex
apps\favn_orchestrator\lib\favn_orchestrator\logs.ex
apps\favn_orchestrator\lib\favn_orchestrator\manifests.ex
apps\favn_orchestrator\lib\favn_orchestrator\materialization_claim.ex
apps\favn_orchestrator\lib\favn_orchestrator\persistence\commands\run_ownership.ex
apps\favn_orchestrator\lib\favn_orchestrator\persistence\run_ownership_store.ex
apps\favn_orchestrator\lib\favn_orchestrator\production_runtime_config.ex
apps\favn_orchestrator\lib\favn_orchestrator\projector.ex
apps\favn_orchestrator\lib\favn_orchestrator\readiness.ex
apps\favn_orchestrator\lib\favn_orchestrator\rebuild\runtime_inputs.ex
apps\favn_orchestrator\lib\favn_orchestrator\rebuild_dispatcher.ex
apps\favn_orchestrator\lib\favn_orchestrator\rebuilds.ex
apps\favn_orchestrator\lib\favn_orchestrator\run_cancellation.ex
apps\favn_orchestrator\lib\favn_orchestrator\run_execution_cleanup.ex
apps\favn_orchestrator\lib\favn_orchestrator\run_execution_ownership.ex
apps\favn_orchestrator\lib\favn_orchestrator\run_manager.ex
apps\favn_orchestrator\lib\favn_orchestrator\run_read_model\step_projection.ex
apps\favn_orchestrator\lib\favn_orchestrator\run_read_model.ex
apps\favn_orchestrator\lib\favn_orchestrator\run_server\execution\result_builder.ex
apps\favn_orchestrator\lib\favn_orchestrator\run_server\execution\run_work_set.ex
apps\favn_orchestrator\lib\favn_orchestrator\run_server\execution\sequential.ex
apps\favn_orchestrator\lib\favn_orchestrator\run_server\execution\stage_admission.ex
apps\favn_orchestrator\lib\favn_orchestrator\run_server\execution\stage_entry.ex
apps\favn_orchestrator\lib\favn_orchestrator\run_server\execution\stage_result.ex
apps\favn_orchestrator\lib\favn_orchestrator\run_server\execution.ex
apps\favn_orchestrator\lib\favn_orchestrator\run_server\recovery.ex
apps\favn_orchestrator\lib\favn_orchestrator\run_server\snapshots.ex
apps\favn_orchestrator\lib\favn_orchestrator\run_server.ex
apps\favn_orchestrator\lib\favn_orchestrator\run_state.ex
apps\favn_orchestrator\lib\favn_orchestrator\runner_client\beam_node.ex
apps\favn_orchestrator\lib\favn_orchestrator\runner_client_validator.ex
apps\favn_orchestrator\lib\favn_orchestrator\runner_diagnostics.ex
apps\favn_orchestrator\lib\favn_orchestrator\runner_dispatch.ex
apps\favn_orchestrator\lib\favn_orchestrator\runner_execution_identity.ex
apps\favn_orchestrator\lib\favn_orchestrator\runner_health.ex
apps\favn_orchestrator\lib\favn_orchestrator\runner_log_bridge.ex
apps\favn_orchestrator\lib\favn_orchestrator\runner_manifest_registration.ex
apps\favn_orchestrator\lib\favn_orchestrator\runner_release_compatibility.ex
apps\favn_orchestrator\lib\favn_orchestrator\runner_replacement.ex
apps\favn_orchestrator\lib\favn_orchestrator\runtime_input_pins.ex
apps\favn_orchestrator\lib\favn_orchestrator\runtime_starter.ex
apps\favn_orchestrator\lib\favn_orchestrator\storage\execution_ownership_codec.ex
apps\favn_orchestrator\lib\favn_orchestrator\storage\log_entry_codec.ex
apps\favn_orchestrator\lib\favn_orchestrator\storage\materialization_claim_codec.ex
apps\favn_orchestrator\lib\favn_orchestrator\storage\run_snapshot_codec.ex
apps\favn_orchestrator\lib\favn_orchestrator\storage\run_state_codec.ex
apps\favn_orchestrator\lib\favn_orchestrator\target_compatibility_planner.ex
apps\favn_orchestrator\lib\favn_orchestrator\transition_writer.ex
apps\favn_orchestrator\lib\favn_orchestrator.ex
apps\favn_orchestrator\test\active_manifest_reconciler_test.exs
apps\favn_orchestrator\test\api\dto_test.exs
apps\favn_orchestrator\test\application_test.exs
apps\favn_orchestrator\test\initial_target_generation_reconciler_test.exs
apps\favn_orchestrator\test\persistence\identity_test.exs
apps\favn_orchestrator\test\production_runtime_config_test.exs
apps\favn_orchestrator\test\run_server\execution\plan_preflight_test.exs
apps\favn_orchestrator\test\run_server\execution\result_builder_test.exs
apps\favn_orchestrator\test\run_server\execution\run_work_set_test.exs
apps\favn_orchestrator\test\run_server\execution\stage_entry_test.exs
apps\favn_orchestrator\test\run_server\recovery_test.exs
apps\favn_orchestrator\test\runner_client\beam_node_test.exs
apps\favn_orchestrator\test\runner_client_validator_test.exs
apps\favn_orchestrator\test\runner_dispatch_test.exs
apps\favn_orchestrator\test\runner_execution_identity_test.exs
apps\favn_orchestrator\test\runner_health_test.exs
apps\favn_orchestrator\test\runner_release_compatibility_test.exs
apps\favn_orchestrator\test\storage\execution_ownership_codec_test.exs
apps\favn_orchestrator\test\storage\materialization_claim_codec_test.exs
apps\favn_orchestrator\test\storage\run_snapshot_codec_test.exs
apps\favn_runner\lib\favn_runner\application.ex
apps\favn_runner\lib\favn_runner\execution_lifecycle\execution.ex
apps\favn_runner\lib\favn_runner\execution_lifecycle.ex
apps\favn_runner\lib\favn_runner\production_runtime_config.ex
apps\favn_runner\lib\favn_runner\result_retention.ex
apps\favn_runner\lib\favn_runner\runtime_starter.ex
apps\favn_runner\lib\favn_runner\server.ex
apps\favn_runner\lib\favn_runner\shutdown.ex
apps\favn_runner\lib\favn_runner\worker.ex
apps\favn_runner\lib\favn_runner.ex
apps\favn_runner\test\application_test.exs
apps\favn_runner\test\execution_lifecycle_test.exs
apps\favn_runner\test\favn_runner_test.exs
apps\favn_runner\test\production_runtime_config_test.exs
apps\favn_runner\test\server_test.exs
apps\favn_runner\test\shutdown_test.exs
apps\favn_storage_postgres\lib\favn_storage_postgres\logs\store.ex
apps\favn_storage_postgres\lib\favn_storage_postgres\migrations\add_commit_safe_log_replay_v2.ex
apps\favn_storage_postgres\lib\favn_storage_postgres\migrations\create_storage_v2.ex
apps\favn_storage_postgres\lib\favn_storage_postgres\migrations\harden_identifier_bounds_v2.ex
apps\favn_storage_postgres\lib\favn_storage_postgres\migrations\harden_payload_bounds_v2.ex
apps\favn_storage_postgres\lib\favn_storage_postgres\migrations\optimize_runner_execution_paging_v2.ex
apps\favn_storage_postgres\lib\favn_storage_postgres\run_ownership\store.ex
apps\favn_storage_postgres\lib\favn_storage_postgres\schemas\coordination.ex
apps\favn_storage_postgres\lib\favn_storage_postgres\schemas\operations.ex
apps\favn_storage_postgres\lib\favn_storage_postgres\storage_v2\migrations.ex
apps\favn_storage_postgres\test\storage_v2\core_authority_test.exs
apps\favn_storage_postgres\test\storage_v2\performance_contract_test.exs
apps\favn_view\lib\favn_view\logs_view_model.ex
apps\favn_view\test\favn_view\control_plane_runtime_config_test.exs
docs\production\control_plane_environment.md
docs\production\network_and_proxy.md
docs\storage\postgresql\data-model.md
docs\structure\favn_orchestrator.md
docs\structure\favn_runner.md
```

Migration ownership:

- Steps 4-8 own manifest/release identity, run snapshots, task storage, log
  identity, materialization ownership, and asset dispatch.
- Steps 9-13 own runner registration, compatibility, runtime startup,
  execution lifecycle, result retention, shutdown, and asset transport.
- Steps 14-16 own inspection, generation, and rebuild callers.
- Step 19 owns `FavnLocal.RunnerMain`, `RunnerChild`, `Lifecycle`, local
  supervision/configuration, deployment templates, and their tests.
- Step 23 removes every remaining singleton symbol and updates storage
  inventories, API/view projections, current structure/production docs, and
  legacy tests.

Step 23 reruns the exact command. Its final live-code/documentation result must
be empty.
