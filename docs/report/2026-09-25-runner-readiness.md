# Packaged runner readiness qualification — 2026-09-25

This is point-in-time evidence for the focused fix on
`codex/runner-health-and-latency`, implementation commit `7b39ef54`.
The current contract is [container readiness](../production/runner_releases.md#container-readiness).

## Root cause and scope

The previous Docker and customer Compose probes used release RPC against runners
configured as dynamic, non-listening distributed nodes. Retained containers
reported `:noconnection` despite having executed assets successfully. The probe
failed before evaluating application readiness.

The fix publishes an atomic, expiring local readiness snapshot and probes it with
shell tools. Checks have a one-second deadline; late results are rejected. Startup
invalidates old snapshots. No inbound listener, persistent storage, new dependency,
or control-plane scheduling change is introduced. The code and tests remain below
the user's 300-line change-record threshold, so no change record was required.

## Verification

- Runner owning-layer suite: 294 passed. Its first run had one registration test
  timing failure at `runner_agent_test.exs:2221`; the isolated test and complete
  suite passed with the original seed `897193`. No unrelated test was changed.
- Focused readiness/facade tests: 15 passed; deployment artifact acceptance:
  1 passed; deployment/template tests: 10 passed.
- Compilation with warnings as errors, formatting, test-tag coverage, shell syntax,
  and whitespace checks passed.
- Built the actual customer runner and matching manifest/operator image from the
  committed source using the existing OrbStack builder.
- Image contract passed with the existing local `+JMsingle true` emulation setting.
  The unadjusted invocation failed in OTP terminal initialization on this ARM host;
  no production defaults were changed to accommodate emulation.
- Five corrected runners registered and reported healthy against the retained
  orchestrator with `NanoCpus=250000000`. Sample successful Docker probes took
  38–70 ms, including Docker exec overhead. This is not a production CPU benchmark.

Runner image: `favn-763-runner:7b39ef544f12-d64dfe69`.
Runner release: `rr_dcd5923c39ef3958d06528a089485e61180eb7904f3f687414216291540f68ee`.
The existing control-plane image remained `favn-763-control:84248a5c74f4-310e55c3`.
No authenticated View/browser workload or 100-asset run was exercised.

## Workload limitation and follow-up

Two 35-asset submissions were rejected before asset execution with
`operator_decision_required` / `physical_inspection_unavailable`. The activation
response reported 34 unresolved inspections; repeating its requested activation
left 33 unresolved inspections. The five runners were healthy. An inspection of
recent durable tasks found 68 succeeded tasks and no non-null task errors.
This does not establish why the activation planner could not use those results.

Affected run IDs:

- `run_api_8615ccb12a2f1e7f241e5a0ca66909fa`
- `run_api_d62f2f2d7bb7ff4231da812ba4b107a6`

Do not count these submissions as a successful stress rerun. Diagnose activation
across runner-release replacement, retain its underlying inspection errors, and
rerun the workload after resolving that blocker. The finding is tracked in the
[roadmap](../ROADMAP.md). Existing volumes and old runner containers were retained;
old containers were renamed with `before-health` to preserve their evidence.

Local build and load evidence is under `.favn/registration-stress/health-case/`.

## Actual Docker health transitions

The final disconnect/reconnect/restart check passed with the image's unmodified
five-second interval and 20-failure threshold:

- 06:23:25 UTC: runner 5 disconnected from the test network.
- 06:25:56 UTC: Docker reported unhealthy; the direct probe failed because the
  readiness file was absent. Network-loss detection plus Docker's failure policy
  took approximately 151 seconds. The probe does not bypass connection detection.
- 06:26:22 UTC: after reconnection, Docker reported healthy again.
- 06:26:32 UTC: after a container restart, Docker again reported healthy.

All five corrected runners were healthy afterward. A preliminary observation
budget of 150 seconds was too short for the combined failure-detection policies;
the final check allowed 250 seconds per transition. No deployment threshold was
changed. Network cleanup ran between attempts. The exact final transcript is
retained locally as `.favn/registration-stress/health-case/transitions.log`.
