#!/usr/bin/env python3
"""Manual local controls and bounded evidence capture for issue 763."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import time
import urllib.request

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]


def timestamp():
    return datetime.now(timezone.utc).isoformat()


def command(args, **kwargs):
    kwargs.setdefault("check", True)
    return subprocess.run(args, cwd=ROOT, text=True, **kwargs)


def compose(args):
    return ["docker", "--context", "orbstack", "compose", "--env-file",
            str(HERE.parent / ".env.local"), "--env-file",
            str(ROOT / ".favn/registration-stress/build.env"),
            "--project-name", ARGS.project, "-f", str(HERE.parent / "compose.yml"),
            "-f", str(HERE / "compose.yml"),
            *(["-f", str(ARGS.compose_override.resolve())] if ARGS.compose_override else []), *args]


def psql_command():
    # Credentials stay inside the disposable PostgreSQL container.
    return compose(["exec", "-T", "postgres", "sh", "-ec",
                    'export PGPASSWORD="$POSTGRES_PASSWORD"; '
                    'export PGSSLROOTCERT=/var/lib/postgresql/certs/ca.crt; '
                    'export PGSSLMODE=verify-full; '
                    'exec psql -h postgres -U favn_bootstrap -d favn -AtX -v ON_ERROR_STOP=1'])


def query(sql):
    result = command(psql_command(), input=sql, capture_output=True)
    return json.loads(result.stdout.strip())


def verify_proxy_owner():
    records = json.loads(command(["docker", "--context", "orbstack", "inspect",
        ARGS.project + "-postgres-1", ARGS.project + "-database-proxy-1"],
        capture_output=True).stdout)
    postgres, proxy_container = records
    for record, service in [(postgres, "postgres"), (proxy_container, "database-proxy")]:
        labels = record["Config"]["Labels"]
        if (labels.get("com.docker.compose.project") != ARGS.project or
                labels.get("com.docker.compose.service") != service or
                not record["State"]["Running"]):
            raise RuntimeError("Proxy containers do not belong to this running project")
    ports = postgres["NetworkSettings"]["Ports"].get("8474/tcp") or []
    if not any(p["HostIp"] == "127.0.0.1" and p["HostPort"] == str(ARGS.proxy_port) for p in ports):
        raise RuntimeError("Proxy port does not belong to this project's PostgreSQL container")
    if proxy_container["HostConfig"]["NetworkMode"] != "container:" + postgres["Id"]:
        raise RuntimeError("Proxy does not share this project's PostgreSQL network namespace")


def proxy(path="", data=None, method=None):
    verify_proxy_owner()
    request = urllib.request.Request(
        f"http://127.0.0.1:{ARGS.proxy_port}/proxies/control_database{path}",
        data=json.dumps(data).encode() if data is not None else None,
        headers={"Content-Type": "application/json"}, method=method)
    with urllib.request.urlopen(request, timeout=5) as response:
        body = response.read()
        return json.loads(body) if body else None


def clear_faults():
    for toxic in proxy()["toxics"]:
        proxy("/toxics/" + toxic["name"], method="DELETE")
    proxy(data={"enabled": True})


def capacity():
    env = dict(line.split("=", 1) for line in (HERE.parent / ".env.local").read_text().splitlines()
               if "=" in line and not line.startswith("#"))
    port = int(env.get("FAVN_API_HOST_PORT", "4101"))
    inspected = command(["docker", "--context", "orbstack", "inspect", "--format",
        '{{json (index .NetworkSettings.Ports "4101/tcp")}}',
        ARGS.project + "-control-plane-1"], capture_output=True, check=False)
    ports = json.loads(inspected.stdout) if inspected.returncode == 0 else []
    if not any(p["HostIp"] == "127.0.0.1" and p["HostPort"] == str(port) for p in (ports or [])):
        return {"unavailable": "project_api_not_published"}
    request = urllib.request.Request(f"http://127.0.0.1:{port}/api/orchestrator/v1/runner-capacity",
        headers={"Authorization": "Bearer " + env["FAVN_PLATFORM_TOKEN"]})
    try:
        with urllib.request.urlopen(request, timeout=3) as response:
            return json.load(response)["data"]
    except (OSError, ValueError, KeyError) as error:
        return {"unavailable": type(error).__name__}


def container_states():
    ids = command(compose(["ps", "--all", "--quiet"]), capture_output=True).stdout.split()
    if not ids:
        return []
    template = ('{"id":{{json .Id}},"name":{{json .Name}},"image_id":{{json .Image}},'
                '"status":{{json .State.Status}},"started_at":{{json .State.StartedAt}},'
                '"restart_count":{{.RestartCount}},"nano_cpus":{{.HostConfig.NanoCpus}},'
                '"health":{{with (index .State "Health")}}{{json .Status}}{{else}}null{{end}}}')
    rows = command(["docker", "--context", "orbstack", "inspect", "--format", template, *ids],
                   capture_output=True).stdout.splitlines()
    return [json.loads(row) for row in rows]


def snapshot():
    result = query((HERE / "snapshot.sql").read_text())
    result["observed_at"] = timestamp()
    result["proxy"] = proxy()
    result["containers"] = container_states()
    result["runner_capacity"] = capacity()
    container = command(compose(["ps", "-q", "control-plane"]), capture_output=True).stdout.strip()
    if container:
        limits = command(["docker", "--context", "orbstack", "exec", container, "sh", "-ec",
                          'cat /sys/fs/cgroup/cpu.max /sys/fs/cgroup/cpu.stat /sys/fs/cgroup/memory.current'],
                         capture_output=True, check=False)
        result["orchestrator_cgroup"] = limits.stdout.splitlines() if limits.returncode == 0 else None
    return result


def emit(record, handle=None):
    line = json.dumps(record, sort_keys=True)
    print(line, flush=True)
    if handle:
        handle.write(line + "\n")
        handle.flush()


def start_runners(count):
    for number in range(1, count + 1):
        name = f"{ARGS.project}-runner-{number}.favn.local"
        env = dict(os.environ, FAVN_RUNNER_NODE_HOST_ALIAS=name)
        command(compose(["run", "-d", "--no-deps", "--name", name,
                         "-e", "FAVN_RUNNER_INSTANCE_ID=" + name,
                         "-e", "FAVN_RUNNER_NODE_HOST_ALIAS=" + name, "runner"]), env=env)


def trigger_outage():
    if not proxy()["enabled"]:
        raise SystemExit("Restore the proxy before arming an outage")
    sql = (HERE / "trigger.sql").read_text()
    sql = sql.replace(":generation_state", "'" + ARGS.generation_state + "'")
    if ARGS.next_run:
        if query("SELECT count(*) FROM favn_control.runs;") != 0:
            raise SystemExit("--next-run requires a fresh project with no runs")
        sql = sql.replace("t.run_id = :run_id", "TRUE")
    else:
        if not re.fullmatch(r"[a-zA-Z0-9_-]{1,128}", ARGS.run_id):
            raise SystemExit("Invalid run ID")
        sql = sql.replace(":run_id", "'" + ARGS.run_id + "'")
    predicate = "m.materialization_id IS NOT NULL" if ARGS.phase == "materialized" else "m.materialization_id IS NULL"
    sql = sql.replace(":phase_predicate", predicate)
    if ARGS.output.exists():
        raise SystemExit("Refusing to overwrite existing fault evidence")
    ARGS.output.parent.mkdir(parents=True, exist_ok=True)
    # One persistent direct SQL session; no polling on the constrained BEAM/API.
    process = subprocess.Popen(psql_command(), cwd=ROOT, text=True, stdin=subprocess.PIPE,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=1)
    process.stdin.write(sql + "\n\\watch 0.1\n")
    process.stdin.flush()
    deadline = time.monotonic() + ARGS.timeout
    triggered = False
    import selectors
    selector = selectors.DefaultSelector()
    with ARGS.output.open("x") as evidence:
        emit({"event": "armed", "at": timestamp(), "run_id": ARGS.run_id,
              "phase": ARGS.phase, "outage_seconds": ARGS.seconds}, evidence)
        try:
            selector.register(process.stdout, selectors.EVENT_READ)
            while time.monotonic() < deadline and process.poll() is None:
                if not selector.select(timeout=1):
                    continue
                line = process.stdout.readline().strip()
                if not line.startswith("{"):
                    continue
                candidate = json.loads(line)
                if not candidate.get("candidate"):
                    continue
                emit({"event": "durable_trigger", "at": timestamp(), **candidate}, evidence)
                triggered = True
                proxy(data={"enabled": False})
                emit({"event": "outage_started", "at": timestamp(), "proxy": proxy()}, evidence)
                process.terminate()
                time.sleep(ARGS.seconds)
                break
            if not triggered:
                raise RuntimeError("No qualifying durable trigger; no outage was injected")
        finally:
            try:
                if triggered:
                    for attempt in range(3):
                        try:
                            proxy(data={"enabled": True})
                            restored = proxy()
                            if not restored["enabled"]:
                                raise RuntimeError("Proxy remains disabled")
                            emit({"event": "outage_ended", "at": timestamp(), "proxy": restored}, evidence)
                            break
                        except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
                            if attempt == 2:
                                emit({"event": "restoration_failed", "at": timestamp(),
                                      "error_type": type(error).__name__}, evidence)
                                raise
                            time.sleep(1)
            finally:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
                selector.close()


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--project", default="favn-763-local")
parser.add_argument("--proxy-port", type=int, default=8476)
parser.add_argument("--compose-override", type=Path, help="Case-specific quota and bootstrap configuration")
sub = parser.add_subparsers(dest="action", required=True)
sub.add_parser("snapshot")
sub.add_parser("up")
sub.add_parser("clear-faults")
latency = sub.add_parser("latency")
latency.add_argument("--ms", type=int, required=True, help="Latency per direction, not round-trip")
latency.add_argument("--jitter", type=int, default=0)
runners = sub.add_parser("start-runners")
runners.add_argument("--count", type=int, default=5)
observe = sub.add_parser("observe")
observe.add_argument("--seconds", type=int, default=600)
observe.add_argument("--interval", type=float, default=5)
observe.add_argument("--output", type=Path, required=True)
fault = sub.add_parser("outage-after-receipt")
fault_run = fault.add_mutually_exclusive_group(required=True)
fault_run.add_argument("--run-id")
fault_run.add_argument("--next-run", action="store_true")
fault.add_argument("--generation-state", choices=["building", "active"], default="building",
                   help="building for the historical defect; active for atomic-publication candidates")
fault.add_argument("--phase", choices=["receipt", "materialized"], default="materialized")
fault.add_argument("--seconds", type=int, default=90)
fault.add_argument("--timeout", type=int, default=300)
fault.add_argument("--output", type=Path, required=True)
ARGS = parser.parse_args()
def interrupted(signum, frame):
    raise SystemExit(128 + signum)
signal.signal(signal.SIGTERM, interrupted)
if not re.fullmatch(r"favn-763-[a-z0-9-]+", ARGS.project):
    parser.error("Use an isolated favn-763-* Compose project")

if ARGS.action == "up":
    command(compose(["up", "-d", "--no-build", "postgres", "database-proxy", "data-init"]))
    command(compose(["run", "--rm", "database-bootstrap"]))
    command(psql_command(), input="SELECT 'CREATE DATABASE stress_lake' WHERE NOT EXISTS "
            "(SELECT FROM pg_database WHERE datname='stress_lake')\\gexec\n")
    command(compose(["run", "--rm", "--no-deps", "catalog-tool", "init"]))
    command(compose(["up", "-d", "--no-build", "--wait", "--wait-timeout", "120", "control-plane"]))
    command(compose(["run", "--rm", "--no-deps", "operator", "publish"]))
    start_runners(5)
    for attempt in range(60):
        if capacity().get("registered_runners") == 5:
            break
        time.sleep(1)
    else:
        raise SystemExit("Five distinct live runner sessions did not register")
    command(compose(["run", "--rm", "--no-deps", "operator", "activate"]))
    status = snapshot()
    unresolved = query("SELECT count(*) FROM favn_control.asset_target_bindings "
        "WHERE workspace_id='elastic-simulation' AND compatibility_status NOT IN ('ready','uninitialized');")
    if unresolved:
        raise SystemExit("Activation left unresolved targets; inspect evidence before submitting work")
    if status["runner_capacity"].get("registered_runners") != 5:
        raise SystemExit("Activation lost runner presence; inspect evidence before submitting work")
    command(compose(["up", "-d", "--no-build", "--wait", "view", "https-proxy"]))
    emit(status)
elif ARGS.action == "snapshot":
    emit(snapshot())
elif ARGS.action == "clear-faults":
    clear_faults()
    emit(proxy())
elif ARGS.action == "latency":
    if not 0 <= ARGS.jitter <= ARGS.ms <= 1000:
        parser.error("Require 0 <= jitter <= latency <= 1000 ms")
    clear_faults()
    for direction in ["upstream", "downstream"]:
        proxy("/toxics", {"name": "latency_" + direction, "type": "latency", "stream": direction,
                          "toxicity": 1, "attributes": {"latency": ARGS.ms, "jitter": ARGS.jitter}})
    emit(proxy())
elif ARGS.action == "start-runners":
    if not 1 <= ARGS.count <= 5:
        parser.error("The local profile supports one to five runners")
    start_runners(ARGS.count)
elif ARGS.action == "observe":
    if ARGS.interval < 1 or not 1 <= ARGS.seconds <= 3600:
        parser.error("Observation interval must be >= 1s and duration <= 1h")
    ARGS.output.parent.mkdir(parents=True, exist_ok=True)
    with ARGS.output.open("x") as evidence:
        deadline = time.monotonic() + ARGS.seconds
        while time.monotonic() < deadline:
            emit(snapshot(), evidence)
            time.sleep(ARGS.interval)
elif ARGS.action == "outage-after-receipt":
    if not 1 <= ARGS.seconds <= 120 or not 1 <= ARGS.timeout <= 900:
        parser.error("Outage is limited to 120s, trigger wait to 900s")
    trigger_outage()
