import {readFileSync} from "node:fs";
import {createConnection} from "node:net";
import AxeBuilder from "@axe-core/playwright";
import {expect, test} from "@playwright/test";
import {Evidence} from "./lib/evidence.mjs";

const catalog = JSON.parse(readFileSync("/security/catalog.json", "utf8"));
const password = readFileSync(required("FAVN_SECURITY_ADMIN_PASSWORD_FILE"), "utf8").trim();
const workspaceId = required("FAVN_SECURITY_WORKSPACE_ID");
const username = required("FAVN_SECURITY_ADMIN_USERNAME");
const evidence = new Evidence(required("FAVN_SECURITY_EVIDENCE_DIR"), "browser");

test("anonymous routes, proxy headers, CSRF, and authenticated browser surface", async ({
  browser,
  page
}) => {
  let failures = 0;

  failures += check("WEB-NET-001", await portReachable("https-proxy", 443), {
    destination: "HTTPS proxy",
    port: 443,
    expected: "reachable"
  });

  for (const [id, host, port, destination] of [
    ["WEB-NET-003", "control-plane", 4000, "direct View"],
    ["WEB-NET-004", "control-plane", 4101, "private API"],
    ["WEB-NET-005", "control-plane", 4369, "EPMD"],
    ["WEB-NET-006", "control-plane", 9100, "BEAM distribution"],
    ["WEB-NET-007", "postgres", 5432, "PostgreSQL"],
    ["WEB-NET-008", "proxy-header-receiver", 8080, "proxy test instrumentation"]
  ]) {
    failures += check(id, !(await portReachable(host, port)), {
      destination,
      port,
      expected: "unreachable"
    });
  }

  failures += check("WEB-NET-002", await portReachable("https-proxy", 80), {
    destination: "proxy canonical redirect listener",
    port: 80,
    expected: "reachable only for HTTPS redirect"
  });
  const redirectPage = await page.context().newPage();
  const finalRedirectResponse = await redirectPage.goto("http://favn.localhost/");
  let redirectRequest = finalRedirectResponse?.request();
  while (redirectRequest?.redirectedFrom()) {
    redirectRequest = redirectRequest.redirectedFrom();
  }
  const redirectResponse = redirectRequest && (await redirectRequest.response());
  const finalRedirectUrl = new URL(redirectPage.url());
  failures += check(
    "WEB-REDIRECT-001",
    redirectResponse &&
      [301, 302, 307, 308].includes(redirectResponse.status()) &&
      redirectResponse.headers()["location"] === "https://favn.localhost/" &&
      finalRedirectUrl.protocol === "https:" &&
      finalRedirectUrl.host === "favn.localhost",
    {
      status: redirectResponse?.status(),
      location: redirectResponse?.headers()["location"],
      final_url: redirectPage.url(),
      expected: "canonical HTTPS origin"
    }
  );
  await redirectPage.close();

  for (const endpoint of catalog.browser.filter((entry) => entry.method === "GET")) {
    const path = expand(endpoint.path, catalog.placeholders);

    if (["public", "anonymous"].includes(endpoint.access)) {
      const response = await page.goto(path);
      failures += check(`${endpoint.id}-PUBLIC`, response?.status() === 200, {
        path,
        status: response?.status(),
        expected_status: 200
      });
      continue;
    }

    const response = await page.goto(path);
    failures += check(
      `${endpoint.id}-ANON`,
      response?.status() === 200 && new URL(page.url()).pathname === "/login",
      {
        path,
        final_path: new URL(page.url()).pathname,
        status: response?.status(),
        expected_status: 200
      }
    );
  }

  const loginResponse = await page.goto("/login");
  const headers = loginResponse.headers();
  failures += check("WEB-HEADERS-001", headers["strict-transport-security"]?.includes("max-age="), {
    header: "strict-transport-security",
    present: Boolean(headers["strict-transport-security"])
  });
  failures += check("WEB-HEADERS-002", headers["content-security-policy"]?.includes("frame-ancestors 'none'"), {
    header: "content-security-policy",
    present: Boolean(headers["content-security-policy"])
  });
  failures += check("WEB-HEADERS-003", headers["x-content-type-options"] === "nosniff", {
    header: "x-content-type-options",
    value: headers["x-content-type-options"]
  });
  failures += check("WEB-HEADERS-004", headers["x-frame-options"] === "DENY", {
    header: "x-frame-options",
    value: headers["x-frame-options"]
  });
  failures += check("WEB-HEADERS-005", headers["referrer-policy"] === "no-referrer", {
    header: "referrer-policy",
    value: headers["referrer-policy"]
  });
  failures += check(
    "WEB-HEADERS-006",
    headers["permissions-policy"] === "camera=(), geolocation=(), microphone=()",
    {
      header: "permissions-policy",
      value: headers["permissions-policy"]
    }
  );

  const csrfResponse = await page.request.post("/login", {
    form: {
      "operator[workspace_id]": workspaceId,
      "operator[username]": username,
      "operator[password]": password
    },
    failOnStatusCode: false
  });
  failures += check("WEB-004-CSRF", [403, 422].includes(csrfResponse.status()), {
    path: "/login",
    status: csrfResponse.status()
  });

  // The page request context shares the browser cookie jar. Reload so the
  // rendered form token is paired with the post-probe session cookie.
  await page.goto("/login");
  await page.locator('[name="operator[workspace_id]"]').fill(workspaceId);
  await page.locator('[name="operator[username]"]').fill(username);
  await page.locator('[name="operator[password]"]').fill(password);
  await page.getByTestId("operator-login-submit").click();
  await expect(page).not.toHaveURL(/\/login(?:\?|$)/);
  evidence.record("WEB-004-AUTHORIZED", "pass", {
    property: "administrator password login"
  });

  const sessionCookies = await page.context().cookies();
  const protectedCookies = sessionCookies.filter((cookie) => cookie.httpOnly);
  failures += check(
    "WEB-COOKIE-001",
    protectedCookies.length > 0 &&
      protectedCookies.every(
        (cookie) => cookie.secure && ["Lax", "Strict"].includes(cookie.sameSite)
      ),
    {
      count: protectedCookies.length,
      properties: protectedCookies.map(({httpOnly, secure, sameSite}) => ({
        httpOnly,
        secure,
        sameSite
      }))
    }
  );

  failures += check(
    "WEB-WS-001",
    await websocketOpened(page, "wss://favn.localhost/live/websocket?vsn=2.0.0"),
    {property: "LiveView WebSocket accepts the canonical HTTPS origin"}
  );

  const hostilePage = await page.context().newPage();
  await hostilePage.route("https://attacker.example/**", (route) =>
    route.fulfill({status: 200, contentType: "text/html", body: "<!doctype html><title>attacker</title>"})
  );
  await hostilePage.goto("https://attacker.example/");
  failures += check(
    "WEB-WS-002",
    !(await websocketOpened(hostilePage, "wss://favn.localhost/live/websocket?vsn=2.0.0")),
    {property: "LiveView WebSocket rejects a hostile browser Origin"}
  );
  await hostilePage.close();

  for (const endpoint of catalog.browser.filter(
    (entry) => entry.method !== "GET" && entry.id !== "WEB-004"
  )) {
    const path = expand(endpoint.path, catalog.placeholders);
    const status = await page.evaluate(
      async ({method, path}) => {
        const response = await fetch(path, {
          method,
          credentials: "same-origin",
          headers: {"content-type": "application/x-www-form-urlencoded"},
          body: ""
        });
        // Fully consume the rejected response. Cancelling a response body can
        // reset a pooled HTTP/2 stream at the proxy and make the next probe see
        // an unrelated 502 instead of the application's CSRF status.
        await response.text();
        return response.status;
      },
      {method: endpoint.method, path}
    );
    failures += check(`${endpoint.id}-CSRF`, [403, 422].includes(status), {
      method: endpoint.method,
      path: endpoint.path,
      status,
      expected: "403 or 422"
    });
  }

  await page.goto("/");
  failures += check("WEB-CSRF-SESSION-001", new URL(page.url()).pathname === "/", {
    property: "rejected CSRF probes do not revoke the authenticated session",
    final_path: new URL(page.url()).pathname
  });

  for (const endpoint of catalog.browser.filter(
    (entry) => entry.method === "GET" && !["public", "anonymous"].includes(entry.access)
  )) {
    const path = expand(endpoint.path, catalog.placeholders);
    const response = await page.goto(path);
    const finalPath = new URL(page.url()).pathname;
    failures += check(
      `${endpoint.id}-AUTH`,
      response?.status() === 200 && finalPath !== "/login",
      {
        path,
        final_path: finalPath,
        status: response?.status(),
        expected_status: 200
      }
    );

    const results = await new AxeBuilder({page})
      .withTags(["wcag2a", "wcag2aa", "wcag21a", "wcag21aa", "wcag22aa"])
      .analyze();
    const blocking = results.violations.filter(({impact}) => ["critical", "serious"].includes(impact));
    failures += check(`${endpoint.id}-A11Y`, blocking.length === 0, {
      path,
      violations: blocking.map(({id, impact, nodes}) => ({
        id,
        impact,
        nodes: nodes.slice(0, 5).map(({target, failureSummary}) => ({
          target,
          failure_summary: failureSummary
        }))
      }))
    });
  }

  const staleCookies = await page.context().cookies();
  // The administrator page intentionally uses a reduced shell without the
  // global sign-out control. Return to the ordinary operator shell first.
  await page.goto("/");
  await page.getByRole("button", {name: "Sign out"}).click();
  await expect(page).toHaveURL(/\/login(?:\?|$)/);
  evidence.record("WEB-006-AUTHORIZED", "pass", {
    property: "browser logout revokes the current session"
  });

  const replayContext = await browser.newContext({ignoreHTTPSErrors: true});
  await replayContext.addCookies(staleCookies);
  const replayPage = await replayContext.newPage();
  await replayPage.goto("/");
  failures += check("WEB-SESSION-001", new URL(replayPage.url()).pathname === "/login", {
    property: "revoked session cookie cannot be replayed",
    final_path: new URL(replayPage.url()).pathname
  });
  await replayContext.close();

  expect(failures, `${failures} security assertions failed; see the evidence artifact`).toBe(0);
});

function check(id, condition, details) {
  evidence.record(id, condition ? "pass" : "fail", details);
  return condition ? 0 : 1;
}

function expand(path, placeholders) {
  return path.replace(/\{([^}]+)\}/g, (_match, key) => {
    if (!(key in placeholders)) throw new Error(`missing placeholder: ${key}`);
    return encodeURIComponent(placeholders[key]);
  });
}

function required(name) {
  const value = process.env[name];
  if (!value) throw new Error(`${name} is required`);
  return value;
}

function websocketOpened(browserPage, url) {
  return browserPage.evaluate((target) => {
    return new Promise((resolve) => {
      const socket = new WebSocket(target);
      const timeout = setTimeout(() => {
        socket.close();
        resolve(false);
      }, 5_000);

      socket.addEventListener("open", () => {
        clearTimeout(timeout);
        socket.close();
        resolve(true);
      });
      socket.addEventListener("error", () => {
        clearTimeout(timeout);
        resolve(false);
      });
    });
  }, url);
}

function portReachable(host, port) {
  return new Promise((resolve) => {
    const socket = createConnection({host, port});
    let settled = false;
    const finish = (reachable) => {
      if (settled) return;
      settled = true;
      socket.destroy();
      resolve(reachable);
    };

    socket.setTimeout(1_000);
    socket.once("connect", () => finish(true));
    socket.once("error", () => finish(false));
    socket.once("timeout", () => finish(false));
  });
}
