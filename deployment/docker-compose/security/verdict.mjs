import {readFileSync, readdirSync, statSync, writeFileSync} from "node:fs";
import {join} from "node:path";

const resultsDirectory = required("FAVN_SECURITY_EVIDENCE_DIR");
const catalog = JSON.parse(readFileSync("/security/catalog.json", "utf8"));
const assertions = readAssertions(resultsDirectory);
const expected = expectedAssertionIds(catalog);
const observed = new Map();

for (const assertion of assertions) {
  if (observed.has(assertion.assertion_id)) {
    throw new Error(`duplicate security assertion: ${assertion.assertion_id}`);
  }
  observed.set(assertion.assertion_id, assertion);
}

const missing = [...expected].filter((id) => !observed.has(id));
const unexpected = [...observed.keys()].filter((id) => !expected.has(id));
const failed = assertions.filter(({outcome}) => outcome !== "pass");

if (missing.length > 0 || unexpected.length > 0 || failed.length > 0) {
  throw new Error(
    `incomplete security evidence; missing=${missing.join(",") || "none"}; ` +
      `unexpected=${unexpected.join(",") || "none"}; ` +
      `failed=${failed.map(({assertion_id}) => assertion_id).join(",") || "none"}`
  );
}

scanForGeneratedSecrets(resultsDirectory);

const sourceState = required("FAVN_SECURITY_SOURCE_STATE");
const verdict = sourceState === "clean" ? "pass" : "diagnostic_pass";
writeFileSync(
  join(resultsDirectory, "verdict.json"),
  `${JSON.stringify({
    schema_version: 1,
    qualification_profile: "http_boundary_v1",
    source_revision: required("FAVN_SOURCE_REVISION"),
    source_state: sourceState,
    verdict,
    passed_assertions: assertions.length,
    failed_assertions: 0
  })}\n`,
  {mode: 0o600}
);

console.log(`security evidence is complete: ${assertions.length} unique assertions`);

function readAssertions(directory) {
  return files(directory)
    .filter((path) => path.endsWith("assertions.jsonl"))
    .flatMap((path) =>
      readFileSync(path, "utf8")
        .split(/\r?\n/)
        .filter(Boolean)
        .map((line) => JSON.parse(line))
    );
}

function expectedAssertionIds(surface) {
  const ids = new Set([
    ...numbered("TOPO", 8),
    ...numbered("HARDEN", 8),
    ...numbered("TP", 4),
    ...numbered("WEB-NET", 8),
    ...numbered("WEB-HEADERS", 6),
    "STATE-001",
    "WEB-REDIRECT-001",
    "WEB-WS-001",
    "WEB-WS-002",
    "WEB-004-AUTHORIZED",
    "WEB-CSRF-SESSION-001",
    "WEB-COOKIE-001",
    "WEB-006-AUTHORIZED",
    "WEB-SESSION-001",
    "API-ACTOR-001",
    "API-LIMIT-001",
    "API-ROUTE-001"
  ]);

  for (const endpoint of surface.browser) {
    if (endpoint.method !== "GET") {
      ids.add(`${endpoint.id}-CSRF`);
      continue;
    }
    if (["public", "anonymous"].includes(endpoint.access)) {
      ids.add(`${endpoint.id}-PUBLIC`);
      continue;
    }
    ids.add(`${endpoint.id}-ANON`);
    ids.add(`${endpoint.id}-AUTH`);
    ids.add(`${endpoint.id}-A11Y`);
  }

  for (const endpoint of surface.api) {
    ids.add(`${endpoint.id}-ANON`);
    if (endpoint.access === "public") continue;

    ids.add(`${endpoint.id}-FORGED`);
    if (["capacity", "platform"].includes(endpoint.access)) {
      ids.add(`${endpoint.id}-SCOPE`);
    }
    if (["actor", "actor_stream"].includes(endpoint.access)) {
      ids.add(`${endpoint.id}-ACTOR-REQUIRED`);
    }
    ids.add(`${endpoint.id}-AUTHORIZED`);
  }

  return ids;
}

function numbered(prefix, count) {
  return Array.from({length: count}, (_unused, index) => {
    return `${prefix}-${String(index + 1).padStart(3, "0")}`;
  });
}

function scanForGeneratedSecrets(directory) {
  const composeEnvironment = readFileSync(
    required("FAVN_SECURITY_COMPOSE_ENV_FILE"),
    "utf8"
  );
  const secretValues = composeEnvironment
    .split(/\r?\n/)
    .map((line) => line.match(/^([^=]+)=(.*)$/))
    .filter((match) => match && /PASSWORD|TOKEN|COOKIE|KEY/i.test(match[1]))
    .map((match) => ({name: match[1], value: match[2]}));

  secretValues.push({
    name: "FAVN_SECURITY_ADMIN_PASSWORD",
    value: readFileSync(required("FAVN_SECURITY_ADMIN_PASSWORD_FILE"), "utf8").trim()
  });

  for (const path of files(directory)) {
    const content = readFileSync(path);
    for (const secret of secretValues) {
      if (secret.value.length >= 8 && content.includes(Buffer.from(secret.value))) {
        throw new Error(`generated secret ${secret.name} leaked into ${path}`);
      }
    }
  }
}

function files(directory) {
  const result = [];
  for (const entry of readdirSync(directory)) {
    const path = join(directory, entry);
    if (statSync(path).isDirectory()) {
      result.push(...files(path));
    } else {
      result.push(path);
    }
  }
  return result;
}

function required(name) {
  const value = process.env[name];
  if (!value) throw new Error(`${name} is required`);
  return value;
}
