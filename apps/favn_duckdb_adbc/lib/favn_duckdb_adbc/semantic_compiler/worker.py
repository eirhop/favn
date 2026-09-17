"""One request, one forked native process; stdout contains only a bounded receipt."""
import ctypes as C
import json
import os
import select
import signal
import sys
import time

STARTUP_TIMEOUT = 10
EXPRESSION_TIMEOUT = 5
TERM_TIMEOUT = 2
KILL_TIMEOUT = 3


class Invalid(Exception):
    pass


def require(condition):
    if not condition:
        raise Invalid()


class Result(C.Structure):
    _fields_ = [("columns", C.c_uint64), ("rows", C.c_uint64),
                ("changed", C.c_uint64), ("column_data", C.c_void_p),
                ("error", C.c_void_p), ("internal", C.c_void_p)]


class DuckDB:
    def __init__(self, path):
        self.lib = C.CDLL(path)
        signatures = {
            "duckdb_library_version": (C.c_char_p, []),
            "duckdb_create_config": (C.c_int, [C.POINTER(C.c_void_p)]),
            "duckdb_set_config": (C.c_int, [C.c_void_p, C.c_char_p, C.c_char_p]),
            "duckdb_open_ext": (C.c_int, [C.c_char_p, C.POINTER(C.c_void_p), C.c_void_p, C.POINTER(C.c_void_p)]),
            "duckdb_destroy_config": (None, [C.POINTER(C.c_void_p)]),
            "duckdb_connect": (C.c_int, [C.c_void_p, C.POINTER(C.c_void_p)]),
            "duckdb_query": (C.c_int, [C.c_void_p, C.c_char_p, C.POINTER(Result)]),
            "duckdb_value_varchar": (C.c_void_p, [C.POINTER(Result), C.c_uint64, C.c_uint64]),
            "duckdb_row_count": (C.c_uint64, [C.POINTER(Result)]),
            "duckdb_destroy_result": (None, [C.POINTER(Result)]),
            "duckdb_free": (None, [C.c_void_p]),
            "duckdb_disconnect": (None, [C.POINTER(C.c_void_p)]),
            "duckdb_close": (None, [C.POINTER(C.c_void_p)]),
        }
        for name, (restype, args) in signatures.items():
            fn = getattr(self.lib, name)
            fn.restype, fn.argtypes = restype, args
        self.version = self.lib.duckdb_library_version().decode()
        if self.version not in ("v1.5.2", "v1.5.5"):
            raise RuntimeError("unsupported_runtime")
        config, self.db, self.conn = C.c_void_p(), C.c_void_p(), C.c_void_p()
        require(self.lib.duckdb_create_config(C.byref(config)) == 0)
        try:
            for key, value in [("enable_external_access", "false"),
                               ("autoinstall_known_extensions", "false"),
                               ("autoload_known_extensions", "false"),
                               ("threads", "1"), ("memory_limit", "128MB"),
                               ("max_expression_depth", "128")]:
                require(self.lib.duckdb_set_config(config, key.encode(), value.encode()) == 0)
            require(self.lib.duckdb_open_ext(b":memory:", C.byref(self.db), config, None) == 0)
            require(self.lib.duckdb_connect(self.db, C.byref(self.conn)) == 0)
        finally:
            self.lib.duckdb_destroy_config(C.byref(config))

    def query(self, sql, column=0):
        result = Result()
        try:
            if self.lib.duckdb_query(self.conn, sql.encode(), C.byref(result)):
                raise RuntimeError("bind_failed")
            require(self.lib.duckdb_row_count(C.byref(result)) == 1)
            value = self.lib.duckdb_value_varchar(C.byref(result), column, 0)
            require(bool(value))
            try:
                text = C.string_at(value)
                require(len(text) <= 2_000_000)
                return text.decode()
            finally:
                self.lib.duckdb_free(value)
        finally:
            self.lib.duckdb_destroy_result(C.byref(result))

    def close(self):
        self.lib.duckdb_disconnect(C.byref(self.conn))
        self.lib.duckdb_close(C.byref(self.db))


BASE = {"class", "type", "alias", "query_location"}
AGGREGATES = {"sum", "min", "max", "avg", "count"}
SCALARS = {"nullif", "abs", "round"}
OPERATORS = {"+", "-", "*", "/", "%"}
TYPES = {"NULL", "BOOLEAN", "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT",
         "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT", "UHUGEINT", "FLOAT", "DOUBLE",
         "DECIMAL", "VARCHAR", "DATE", "TIMESTAMP", "TIMESTAMP_TZ", "TIME", "INTERVAL"}
PROFILE = {"integer": "BIGINT", "float": "DOUBLE", "decimal": "DECIMAL(18,2)",
           "string": "VARCHAR", "boolean": "BOOLEAN", "date": "DATE",
           "datetime": "TIMESTAMP", "binary": "BLOB", "time": "TIME",
           "json": "JSON", "uuid": "UUID"}


def logical_type(value):
    require(isinstance(value, dict) and set(value) == {"id", "type_info"})
    require(value["id"] in TYPES)
    info = value["type_info"]
    if info is not None:
        require(value["id"] == "DECIMAL" and set(info) ==
                {"type", "alias", "extension_info", "width", "scale"})
        require(info["type"] == "DECIMAL_TYPE_INFO" and info["alias"] == ""
                and info["extension_info"] is None)
        require(1 <= info["width"] <= 38 and 0 <= info["scale"] <= info["width"])


def expression(node, names, locations, depth=0, inside_aggregate=False):
    require(isinstance(node, dict) and depth <= 64 and node.get("alias") == "")
    kind, typ = node.get("class"), node.get("type")
    children, aggregate = [], False
    if kind == "COLUMN_REF":
        extra = {"column_names"}
        require(typ == "COLUMN_REF" and node["column_names"] in [[n] for n in names])
        name = node["column_names"][0]
        if name in locations:
            require(node["query_location"] - 7 in locations[name])
    elif kind == "CONSTANT":
        extra = {"value"}
        require(typ == "VALUE_CONSTANT")
        value = node["value"]
        require(set(value) <= {"type", "is_null", "value"})
        logical_type(value["type"])
        require(value.get("is_null") in (True, False))
    elif kind == "FUNCTION":
        extra = {"function_name", "schema", "children", "filter", "order_bys",
                 "distinct", "is_operator", "export_state", "catalog"}
        name = node["function_name"]
        require(typ == "FUNCTION" and name in AGGREGATES | SCALARS | OPERATORS)
        require(node["schema"] == node["catalog"] == "" and node["export_state"] is False)
        require(node["order_bys"] == {"type": "ORDER_MODIFIER", "orders": []})
        require(node["is_operator"] == (name in OPERATORS))
        aggregate = name in AGGREGATES
        require(not (aggregate and inside_aggregate))
        require(aggregate or (node["filter"] is None and node["distinct"] is False))
        children = node["children"] + ([] if node["filter"] is None else [node["filter"]])
    elif kind == "CAST":
        extra = {"child", "cast_type", "try_cast"}
        require(typ == "OPERATOR_CAST" and node["try_cast"] is False)
        logical_type(node["cast_type"])
        children = [node["child"]]
    elif kind == "CASE":
        extra = {"case_checks", "else_expr"}
        require(typ == "CASE_EXPR")
        for check in node["case_checks"]:
            require(set(check) == {"when_expr", "then_expr"})
            children.extend([check["when_expr"], check["then_expr"]])
        children.append(node["else_expr"])
    elif kind == "COMPARISON":
        extra = {"left", "right"}
        require(typ in {"COMPARE_EQUAL", "COMPARE_NOTEQUAL", "COMPARE_LESSTHAN",
                        "COMPARE_GREATERTHAN", "COMPARE_LESSTHANOREQUALTO",
                        "COMPARE_GREATERTHANOREQUALTO", "COMPARE_DISTINCT_FROM",
                        "COMPARE_NOT_DISTINCT_FROM"})
        children = [node["left"], node["right"]]
    elif kind in {"CONJUNCTION", "OPERATOR"}:
        extra = {"children"}
        allowed = ({"CONJUNCTION_AND", "CONJUNCTION_OR"} if kind == "CONJUNCTION" else
                   {"OPERATOR_NOT", "OPERATOR_IS_NULL", "OPERATOR_IS_NOT_NULL", "OPERATOR_COALESCE"})
        require(typ in allowed)
        children = node["children"]
    else:
        raise Invalid()
    require(set(node) == BASE | extra)
    for child in children:
        aggregate = expression(child, names, locations, depth + 1, inside_aggregate or kind == "FUNCTION"
                               and node["function_name"] in AGGREGATES) or aggregate
    return aggregate


def validate_tree(parsed, names, locations):
    require(set(parsed) == {"error", "statements"} and parsed["error"] is False)
    require(len(parsed["statements"]) == 1)
    statement = parsed["statements"][0]
    require(set(statement) == {"node", "named_param_map"} and statement["named_param_map"] == [])
    node = statement["node"]
    expected = {"type": "SELECT_NODE", "modifiers": [], "cte_map": {"map": []},
                "where_clause": None, "group_expressions": [], "group_sets": [],
                "aggregate_handling": "STANDARD_HANDLING", "having": None,
                "sample": None, "qualify": None}
    require(set(node) == set(expected) | {"select_list", "from_table"})
    require(all(node[key] == value for key, value in expected.items()))
    table = node["from_table"]
    require(set(table) == {"type", "alias", "sample", "query_location"})
    require(table["type"] == "EMPTY" and table["alias"] == "" and table["sample"] is None)
    require(len(node["select_list"]) == 1)
    require(expression(node["select_list"][0], names, locations))


def child_ownership(parent):
    # Kernel ownership survives even a supervisor SIGKILL; close the race at fork.
    libc = C.CDLL(None)
    require(libc.prctl(1, signal.SIGKILL, 0, 0, 0) == 0)
    require(os.getppid() == parent and parent != 1)
    os.setsid()
    os.close(0)


def native(request, output):
    db = DuckDB(request["driver"])
    os.write(output, b"ready\n")
    try:
        names = [item["name"] for item in request["inputs"]]
        locations = {item["name"]: set(item["locations"]) for item in request["inputs"] if "locations" in item}
        require(len(names) == len(set(n.lower() for n in names)))
        profile = {item["name"]: PROFILE[item["type"]] for item in request["inputs"]}
        sql = request["sql"]
        parsed = json.loads(db.query("SELECT json_serialize_sql('SELECT " + sql.replace("'", "''") + "')"))
        validate_tree(parsed, names, locations)
        columns = ", ".join("CAST(NULL AS " + typ + ') AS "' + name.replace('"', '""') + '"'
                            for name, typ in profile.items())
        native_type = db.query("DESCRIBE SELECT " + sql + " AS result FROM (SELECT " + columns + " WHERE FALSE) AS inputs", 1)
        result = {"ok": True, "native_type": native_type, "runtime_version": db.version,
                  "validation_profile": profile}
    finally:
        db.close()
    return result


def reap(pid, timeout):
    end = time.monotonic() + timeout
    while True:
        found, status = os.waitpid(pid, os.WNOHANG)
        if found:
            return status
        if time.monotonic() >= end:
            return None
        time.sleep(0.01)


def terminate(pid):
    for sig, seconds in ((signal.SIGTERM, TERM_TIMEOUT), (signal.SIGKILL, KILL_TIMEOUT)):
        try:
            os.kill(pid, sig)
        except ProcessLookupError:
            pass
        status = reap(pid, seconds)
        if status is not None:
            return True
    return False


def supervise(report=lambda _receipt: None):
    # Initial request is bounded; continue watching the pipe for owner death.
    request_bytes = b""
    while not request_bytes.endswith(b"\n"):
        require(len(request_bytes) <= 262_144)
        ready, _, _ = select.select([0], [], [], STARTUP_TIMEOUT)
        require(ready)
        chunk = os.read(0, 4096)
        require(chunk)
        request_bytes += chunk
    request = json.loads(request_bytes)
    read_fd, write_fd = os.pipe()
    parent = os.getpid()
    pid = os.fork()
    if pid == 0:
        os.close(read_fd)
        try:
            child_ownership(parent)
            result = native(request, write_fd)
        except Invalid:
            result = {"error": "invalid_expression"}
        except RuntimeError as error:
            result = {"error": str(error) if str(error) in {"bind_failed", "unsupported_runtime"} else "worker_failed"}
        except BaseException:
            result = {"error": "worker_failed"}
        os.write(write_fd, json.dumps(result).encode() + b"\n")
        os.close(write_fd)
        os._exit(0)
    os.close(write_fd)
    identity = {"supervisor_pid": parent, "worker_pid": pid, "process_group": None}
    report({"process": identity})
    data, deadline, live = b"", time.monotonic() + STARTUP_TIMEOUT, True
    try:
        while live:
            ready, _, _ = select.select([0, read_fd], [], [], max(0, deadline - time.monotonic()))
            if not ready:
                return {"error": "timeout"} if terminate(pid) else {"error": "cleanup_unconfirmed", "process": identity}
            if 0 in ready:
                os.read(0, 4096)
                terminate(pid)
                return {"error": "owner_lost"}
            chunk = os.read(read_fd, 16_384)
            data += chunk
            require(len(data) <= 16_384)
            if data.startswith(b"ready\n"):
                data = data[6:]
                identity["process_group"] = pid
                report({"process": identity})
                deadline = time.monotonic() + EXPRESSION_TIMEOUT
            if not chunk:
                status = reap(pid, KILL_TIMEOUT)
                if status is None:
                    return {"error": "worker_failed"} if terminate(pid) else {"error": "cleanup_unconfirmed", "process": identity}
                live = False
                require(status == 0)
                return json.loads(data)
    finally:
        os.close(read_fd)
        if live:
            try:
                terminate(pid)
            except ChildProcessError:
                pass


if __name__ == "__main__":
    def report(receipt):
        sys.stdout.write(json.dumps(receipt) + "\n")
        sys.stdout.flush()
    try:
        receipt = supervise(report)
    except BaseException:
        receipt = {"error": "worker_failed"}
    report(receipt)
