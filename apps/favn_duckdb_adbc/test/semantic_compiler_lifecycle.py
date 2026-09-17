"""Exercise the real supervisor using a blocked/crashing stand-in for native C."""
import ctypes
import importlib.util
import json
import os
from pathlib import Path
import select
import signal
import sys
import time

sys.dont_write_bytecode = True

source = Path(__file__).parents[1] / "lib/favn_duckdb_adbc/semantic_compiler/worker.py"
spec = importlib.util.spec_from_file_location("worker", source)
worker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(worker)

# Adopt and reap the native grandchild when testing supervisor death.
assert ctypes.CDLL(None).prctl(36, 1, 0, 0, 0) == 0
worker.STARTUP_TIMEOUT = 0.15
worker.EXPRESSION_TIMEOUT = 0.15
worker.TERM_TIMEOUT = 0.1
worker.KILL_TIMEOUT = 0.2


def read_line(fd):
    assert select.select([fd], [], [], 3)[0], "worker did not reply"
    data = b""
    while not data.endswith(b"\n"):
        chunk = os.read(fd, 1)
        assert chunk, "worker pipe closed without receipt"
        data += chunk
    return data


def case(mode):
    control_r, control_w = os.pipe()
    receipt_r, receipt_w = os.pipe()
    pid_r, pid_w = os.pipe()
    pid = os.fork()
    if pid == 0:
        os.close(control_w)
        os.close(receipt_r)
        os.close(pid_r)
        os.dup2(control_r, 0)
        os.close(control_r)

        def native(_request, output):
            os.write(pid_w, str(os.getpid()).encode() + b"\n")
            if mode == "crash":
                os._exit(11)
            if mode in {"success", "unknown_cleanup"}:
                return {"ok": True}
            if mode != "startup_timeout":
                os.write(output, b"ready\n")
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            while True:
                time.sleep(1)

        worker.native = native
        if mode == "unknown_cleanup":
            worker.reap = lambda _pid, _timeout: None
        try:
            result = worker.supervise()
        except BaseException:
            result = {"error": "worker_failed"}
        os.write(receipt_w, json.dumps(result).encode() + b"\n")
        os._exit(0)
    os.close(control_r)
    os.close(receipt_w)
    os.close(pid_w)
    os.write(control_w, b"{}\n")
    native_pid = int(read_line(pid_r))
    if mode == "owner_lost":
        os.close(control_w)
    if mode == "supervisor_killed":
        os.kill(pid, signal.SIGKILL)
        os.waitpid(pid, 0)
        # PDEATHSIG must kill the blocked native process even without supervisor cleanup.
        status = worker.reap(native_pid, 2)
        assert status is not None and os.WIFSIGNALED(status)
        assert os.WTERMSIG(status) == signal.SIGKILL
    else:
        result = json.loads(read_line(receipt_r))
        os.waitpid(pid, 0)
        expected = {"success": {"ok": True}, "crash": {"error": "worker_failed"},
                    "owner_lost": {"error": "owner_lost"},
                    "unknown_cleanup": {"error": "cleanup_unconfirmed"}}
        assert result == expected.get(mode, {"error": "timeout"}), result
        if mode == "unknown_cleanup":
            assert worker.reap(native_pid, 2) is not None
        assert not Path("/proc", str(native_pid)).exists(), "native child was not reaped"
    if mode != "owner_lost":
        os.close(control_w)
    os.close(receipt_r)
    os.close(pid_r)


for mode in ["success", "startup_timeout", "expression_timeout", "owner_lost", "crash", "supervisor_killed", "unknown_cleanup"]:
    case(mode)
print("ok")
