import {defineConfig} from "@playwright/test";

const retainDiagnostics = process.env.FAVN_SECURITY_BROWSER_DIAGNOSTICS === "1";

export default defineConfig({
  testDir: "/security",
  testMatch: "browser.spec.mjs",
  fullyParallel: false,
  forbidOnly: true,
  retries: 0,
  workers: 1,
  timeout: 120_000,
  expect: {timeout: 10_000},
  reporter: [["line"]],
  outputDir: "/results/browser/playwright",
  use: {
    baseURL: process.env.FAVN_SECURITY_BASE_URL,
    browserName: "chromium",
    ignoreHTTPSErrors: true,
    launchOptions: {
      args: ["--host-resolver-rules=MAP favn.localhost 172.31.58.2"]
    },
    trace: retainDiagnostics ? "retain-on-failure" : "off",
    screenshot: retainDiagnostics ? "only-on-failure" : "off"
  }
});
