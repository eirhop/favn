#!/usr/bin/env python3
"""Submit bounded, back-to-back short-asset runs to the isolated local stack."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import subprocess
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[3]
TARGET = "pipeline:Elixir.CrmDemo.RegistrationStress.Pipeline:registration_stress"


def now():
    return datetime.now(timezone.utc).isoformat()


def query(project, sql):
    command = ["docker", "--context", "orbstack", "exec", "-i", project + "-postgres-1",
               "sh", "-ec", 'export PGPASSWORD="$POSTGRES_PASSWORD" '
               'PGSSLROOTCERT=/var/lib/postgresql/certs/ca.crt PGSSLMODE=verify-full; '
               'exec psql -h postgres -U favn_bootstrap -d favn -AtX -v ON_ERROR_STOP=1']
    result = subprocess.run(command, input=sql, text=True, capture_output=True, check=True)
    return json.loads(result.stdout)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", required=True)
    parser.add_argument("--runs", type=int, default=10)
    parser.add_argument("--max-in-flight", type=int, default=2)
    parser.add_argument("--timeout", type=int, default=900)
    parser.add_argument("--key-prefix", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not re.fullmatch(r"favn-763-[a-z0-9-]+", args.project):
        parser.error("Use an isolated favn-763-* project")
    if not re.fullmatch(r"[A-Za-z0-9-]{1,100}", args.key_prefix):
        parser.error("Use a unique alphanumeric/hyphen key prefix")
    if not (1 <= args.runs <= 50 and 1 <= args.max_in_flight <= 5 and 1 <= args.timeout <= 3600):
        parser.error("Runs <= 50, in-flight <= 5, timeout <= 3600s")
    env = dict(line.split("=", 1) for line in
               (ROOT / "deployment/docker-compose/.env.local").read_text().splitlines()
               if "=" in line and not line.startswith("#"))
    api_port = int(env.get("FAVN_API_HOST_PORT", "4101"))
    ports = json.loads(subprocess.check_output(
        ["docker", "--context", "orbstack", "inspect", "--format",
         '{{json (index .NetworkSettings.Ports "4101/tcp")}}',
         args.project + "-control-plane-1"], text=True)) or []
    if not any(p["HostIp"] == "127.0.0.1" and p["HostPort"] == str(api_port) for p in ports):
        parser.error("The API port does not belong to this isolated project's control plane")
    # Stored API idempotency keys are hashed. Reserve the raw key namespace
    # locally, then correlate durable submissions by the returned run IDs.
    cases = ROOT / ".favn/registration-stress/cases"
    cases.mkdir(parents=True, exist_ok=True)
    ledger = cases / f"{args.project}-{args.key_prefix}.json"
    if ledger.exists():
        parser.error(f"Key prefix is reserved; inspect {ledger} before resolving any uncertain request")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x") as evidence, ledger.open("x") as reservation:
        json.dump({"project": args.project, "key_prefix": args.key_prefix,
                   "evidence": str(args.output.resolve()), "reserved_at": now()}, reservation)
        reservation.flush()
        os.fsync(reservation.fileno())
        def emit(event, **fields):
            record = dict(event=event, at=now(), **fields)
            line = json.dumps(record, sort_keys=True)
            evidence.write(line + "\n")
            evidence.flush()
            os.fsync(evidence.fileno())
            print(line, flush=True)

        started = time.monotonic()
        deadline = started + args.timeout
        accepted, terminal = {}, {}
        emit("load_started", project=args.project, requested_runs=args.runs,
             max_in_flight=args.max_in_flight, asset_sleep_ms=0, target=TARGET, workload_model="bounded_backlog")
        while time.monotonic() < deadline:
            while len(accepted) < args.runs and len(accepted) - len(terminal) < args.max_in_flight:
                sequence = len(accepted) + 1
                key = f"{args.key_prefix}-{sequence}"
                payload = {"target": {"type": "pipeline", "id": TARGET}, "refresh": "force_all",
                           "metadata": {"local_stress_sequence": sequence,
                                        "local_stress_key_prefix": args.key_prefix}}
                # Persist intent before sending. An uncertain response stops submission;
                # rerunning with a fresh key must never hide a possibly accepted request.
                emit("submission_intent", idempotency_key=key, payload=payload)
                request = urllib.request.Request(
                    f"http://127.0.0.1:{api_port}/api/orchestrator/v1/runs",
                    data=json.dumps(payload).encode(),
                    headers={"Authorization": "Bearer " + env["FAVN_PLATFORM_TOKEN"],
                             "X-Favn-Workspace-Id": "elastic-simulation",
                             "Idempotency-Key": key, "Content-Type": "application/json"})
                began = time.monotonic()
                try:
                    with urllib.request.urlopen(request, timeout=30) as response:
                        body = json.load(response)
                        status = response.status
                        run_id = body["data"]["run"]["id"]
                        if not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", run_id) or run_id in accepted:
                            raise ValueError("Invalid or repeated run identity")
                except urllib.error.HTTPError as error:
                    uncertain = error.code >= 500 or error.code == 408
                    emit("submission_outcome_unknown" if uncertain else "submission_rejected",
                         idempotency_key=key, http_status=error.code)
                    raise SystemExit("Submission stopped; inspect the original key before retrying")
                except (TimeoutError, urllib.error.URLError, OSError, ValueError, KeyError, TypeError) as error:
                    emit("submission_outcome_unknown", idempotency_key=key,
                         error_type=type(error).__name__)
                    raise SystemExit("Unknown submission outcome; resolve the original key before retrying")
                emit("submission_accepted", idempotency_key=key, http_status=status,
                     request_ms=round((time.monotonic() - began) * 1000, 3), response=body)
                accepted[run_id] = key
            run_ids = ",".join("'" + run_id + "'" for run_id in accepted)
            states = query(args.project, """
              SELECT COALESCE(jsonb_agg(jsonb_build_object(
                'submission_status',s.status,'run_id',s.run_id,
                'run_status',r.status,'enqueued_at',s.enqueued_at,
                'started_at',r.inserted_at,'terminal_at',r.terminal_at,
                'failure_kind',s.failure_kind)), '[]'::jsonb)
              FROM favn_control.run_submissions s LEFT JOIN favn_control.runs r
                ON r.workspace_id=s.workspace_id AND r.run_id=s.run_id
              WHERE s.workspace_id='elastic-simulation' AND s.run_id IN (""" + run_ids + ");")
            for state in states:
                finished = state["terminal_at"] or state["submission_status"] in ["failed", "cancelled"]
                if finished and state["run_id"] not in terminal:
                    terminal[state["run_id"]] = state
                    emit("run_terminal", idempotency_key=accepted[state["run_id"]], **state)
            if len(terminal) == args.runs:
                ok = sum(state["run_status"] == "ok" for state in terminal.values())
                emit("load_finished", submitted=len(accepted), successful=ok,
                     failed=len(terminal) - ok, elapsed_seconds=time.monotonic() - started)
                return 0 if ok == args.runs else 1
            # Only the external observer waits; asset execution has no artificial delay.
            time.sleep(1)
        emit("load_timeout", submitted=len(accepted), terminal=len(terminal))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
