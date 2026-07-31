import {readFileSync} from "node:fs";
import {Evidence} from "./lib/evidence.mjs";

const apiUrl = required("FAVN_SECURITY_API_URL");
const workspaceId = required("FAVN_SECURITY_WORKSPACE_ID");
const platformToken = required("FAVN_SECURITY_PLATFORM_TOKEN");
const capacityToken = required("FAVN_SECURITY_CAPACITY_TOKEN");
const adminUsername = required("FAVN_SECURITY_ADMIN_USERNAME");
const adminPassword = readFileSync(required("FAVN_SECURITY_ADMIN_PASSWORD_FILE"), "utf8").trim();
const evidence = new Evidence(required("FAVN_SECURITY_EVIDENCE_DIR"), "api");
const catalog = JSON.parse(readFileSync("/security/catalog.json", "utf8"));

let failures = 0;
const actor = await loginAdministrator();

const endpoints = catalog.api.toSorted((left, right) => {
  if (left.id === "API-012") return 1;
  if (right.id === "API-012") return -1;
  return left.id.localeCompare(right.id);
});

for (const endpoint of endpoints) {
  const path = expand(endpoint.path, {
    ...catalog.placeholders,
    session_id: endpoint.id === "API-012" ? actor.sessionId : catalog.placeholders.session_id
  });
  const anonymous = await request(endpoint, path, undefined);
  const anonymousAllowed = endpoint.access === "public"
    ? [200, 503].includes(anonymous.status)
    : anonymous.status === 401;
  assert(`${endpoint.id}-ANON`, anonymousAllowed, {
    method: endpoint.method,
    path: endpoint.path,
    status: anonymous.status,
    expected: endpoint.access === "public" ? "200 or readiness 503" : "401"
  });

  if (endpoint.access === "public") continue;

  const forged = await request(endpoint, path, "not-a-configured-token");
  assert(`${endpoint.id}-FORGED`, forged.status === 401, {
    method: endpoint.method,
    path: endpoint.path,
    status: forged.status,
    expected: "401"
  });

  if (endpoint.access === "capacity") {
    const wrongScope = await request(endpoint, path, platformToken);
    assert(`${endpoint.id}-SCOPE`, wrongScope.status === 403, {
      method: endpoint.method,
      path: endpoint.path,
      status: wrongScope.status,
      expected: "403"
    });

    const authorized = await request(endpoint, path, capacityToken);
    assert(`${endpoint.id}-AUTHORIZED`, authorizedStatus(authorized.status), {
      method: endpoint.method,
      path: endpoint.path,
      status: authorized.status
    });
    continue;
  }

  if (["actor", "actor_stream"].includes(endpoint.access)) {
    const missingActor = await request(endpoint, path, platformToken);
    assert(`${endpoint.id}-ACTOR-REQUIRED`, missingActor.status === 401, {
      method: endpoint.method,
      path: endpoint.path,
      status: missingActor.status,
      expected: "401"
    });

    const authorized = await request(endpoint, path, platformToken, actor);
    assert(`${endpoint.id}-AUTHORIZED`, authorizedStatus(authorized.status), {
      method: endpoint.method,
      path: endpoint.path,
      status: authorized.status
    });
    continue;
  }

  if (endpoint.access === "platform") {
    const wrongScope = await request(endpoint, path, capacityToken);
    assert(`${endpoint.id}-SCOPE`, wrongScope.status === 403, {
      method: endpoint.method,
      path: endpoint.path,
      status: wrongScope.status,
      expected: "403"
    });
  }

  if (["service", "platform"].includes(endpoint.access)) {
    const authorized = await request(endpoint, path, platformToken);
    assert(`${endpoint.id}-AUTHORIZED`, authorizedStatus(authorized.status), {
      method: endpoint.method,
      path: endpoint.path,
      status: authorized.status
    });
  }
}

const oversized = await fetch(`${apiUrl}/api/orchestrator/v1/health`, {
  headers: {"x-favn-workspace-id": "x".repeat(4096)}
});
assert("API-LIMIT-001", oversized.status < 500, {
  status: oversized.status,
  property: "oversized header does not crash API"
});

const unknown = await fetch(`${apiUrl}/api/orchestrator/v1/definitely-not-a-route`, {
  headers: authHeaders(platformToken)
});
assert("API-ROUTE-001", unknown.status === 404, {
  status: unknown.status,
  property: "unknown route fails closed"
});

if (failures > 0) {
  throw new Error(`${failures} API security assertions failed`);
}

console.log(`API security qualification passed for ${catalog.api.length} catalogued endpoints`);

async function request(endpoint, path, token, actorContext) {
  const headers = {
    "accept": endpoint.access === "actor_stream" ? "application/json" : "application/json",
    "content-type": "application/json",
    "x-favn-workspace-id": workspaceId,
    "x-request-id": `security-${endpoint.id.toLowerCase()}`
  };
  if (token) headers.authorization = `Bearer ${token}`;
  if (actorContext) {
    headers["x-favn-actor-id"] = actorContext.actorId;
    headers["x-favn-session-token"] = actorContext.sessionToken;
  }

  const response = await fetch(`${apiUrl}${path}`, {
    method: endpoint.method,
    headers,
    body: ["POST", "PUT", "PATCH"].includes(endpoint.method) ? "{}" : undefined,
    signal: AbortSignal.timeout(10_000)
  });
  await response.body?.cancel();
  return response;
}

async function loginAdministrator() {
  const response = await fetch(`${apiUrl}/api/orchestrator/v1/auth/password/sessions`, {
    method: "POST",
    headers: {
      ...authHeaders(platformToken),
      "content-type": "application/json"
    },
    body: JSON.stringify({username: adminUsername, password: adminPassword}),
    signal: AbortSignal.timeout(10_000)
  });
  const payload = await response.json();
  const actorId = payload?.data?.actor?.id;
  const sessionId = payload?.data?.session?.id;
  const sessionToken = payload?.data?.session_token;
  assert("API-ACTOR-001", response.status === 201 && actorId && sessionId && sessionToken, {
    status: response.status,
    actor_id_present: Boolean(actorId),
    session_id_present: Boolean(sessionId),
    session_token_present: Boolean(sessionToken)
  });
  return {actorId, sessionId, sessionToken};
}

function authHeaders(token) {
  return {
    authorization: `Bearer ${token}`,
    "x-favn-workspace-id": workspaceId
  };
}

function authorizedStatus(status) {
  return (status >= 200 && status < 300) || (status >= 400 && status < 500 && ![401, 403].includes(status));
}

function expand(path, placeholders) {
  return path.replace(/\{([^}]+)\}/g, (_match, key) => {
    if (!(key in placeholders)) throw new Error(`missing placeholder: ${key}`);
    return encodeURIComponent(placeholders[key]);
  });
}

function assert(id, condition, details) {
  evidence.record(id, condition ? "pass" : "fail", details);
  if (!condition) {
    failures += 1;
    console.error(`${id} failed`, details);
  }
}

function required(name) {
  const value = process.env[name];
  if (!value) throw new Error(`${name} is required`);
  return value;
}
