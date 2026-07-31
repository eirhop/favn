import {readFileSync} from "node:fs";
import {resolve} from "node:path";

const repositoryRoot = resolve(import.meta.dirname, "../../..");
const catalog = JSON.parse(readFileSync(resolve(import.meta.dirname, "catalog.json"), "utf8"));

const actualBrowser = routes(
  "apps/favn_view/lib/favn_view/router.ex",
  "",
  readProductionViewRouter
);

const apiRouterPath = "apps/favn_orchestrator/lib/favn_orchestrator/api/router.ex";
const apiRouterSource = readFileSync(resolve(repositoryRoot, apiRouterPath), "utf8");
const actualApi = [
  ...routes(apiRouterPath, ""),
  ...forwardedRoutes(apiRouterSource)
];

compare("browser", actualBrowser, catalog.browser.map(signature));
compare("API", actualApi, catalog.api.map(signature));

const ids = [...catalog.browser, ...catalog.api].map(({id}) => id);
if (new Set(ids).size !== ids.length) {
  throw new Error("security surface catalog contains duplicate assertion IDs");
}

console.log(
  `security surface catalog covers ${actualBrowser.length} browser routes and ${actualApi.length} API routes`
);

function routes(relativePath, prefix, transform = (source) => source) {
  const source = transform(readFileSync(resolve(repositoryRoot, relativePath), "utf8"));
  const result = [];
  const routePattern = /^\s*(get|post|put|patch|delete|live)\s*(?:\(|)\s*"([^"]+)"/gm;
  for (const match of source.matchAll(routePattern)) {
    const method = match[1] === "live" ? "GET" : match[1].toUpperCase();
    result.push(`${method} ${normalize(`${prefix}${match[2]}`)}`);
  }
  return result;
}

function forwardedRoutes(source) {
  const result = [];
  const forwardPattern = /^\s*forward\("([^"]+)",\s*to:\s*([A-Z][A-Za-z0-9_]*)\)/gm;

  for (const match of source.matchAll(forwardPattern)) {
    const fileName = `${snakeCase(match[2])}.ex`;
    const relativePath = `apps/favn_orchestrator/lib/favn_orchestrator/api/${fileName}`;
    result.push(...routes(relativePath, match[1]));
  }

  return result;
}

function snakeCase(value) {
  return value
    .replace(/([a-z0-9])([A-Z])/g, "$1_$2")
    .replace(/([A-Z]+)([A-Z][a-z])/g, "$1_$2")
    .toLowerCase();
}

function readProductionViewRouter(source) {
  return source
    .split("  # Other scopes may use custom stacks.")[0]
    .replace('get "/health/live"', 'get "/api/web/v1/health/live"')
    .replace('get "/health/ready"', 'get "/api/web/v1/health/ready"');
}

function signature(endpoint) {
  return `${endpoint.method} ${normalize(endpoint.path.replace(/\{([^}]+)\}/g, ":$1"))}`;
}

function normalize(path) {
  const normalized = path.replace(/\/+/g, "/");
  return normalized.length > 1 ? normalized.replace(/\/$/, "") : normalized;
}

function compare(name, actual, expected) {
  const actualSet = new Set(actual);
  const expectedSet = new Set(expected);
  const missing = actual.filter((route) => !expectedSet.has(route));
  const stale = expected.filter((route) => !actualSet.has(route));

  if (missing.length > 0 || stale.length > 0) {
    throw new Error(
      `${name} security catalog drift\n` +
        `uncatalogued: ${missing.join(", ") || "none"}\n` +
        `stale: ${stale.join(", ") || "none"}`
    );
  }
}
