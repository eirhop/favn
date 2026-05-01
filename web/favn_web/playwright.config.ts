import { defineConfig } from '@playwright/test';

export default defineConfig({
	workers: 1,
	use: {
		baseURL: 'http://127.0.0.1:4173'
	},
	webServer: {
		command: 'node ./tests/e2e/start-e2e-stack.mjs',
		env: {
			FAVN_WEB_ORCHESTRATOR_BASE_URL: 'http://127.0.0.1:4101',
			FAVN_WEB_ORCHESTRATOR_SERVICE_TOKEN: 'playwright-test-token-32-char-minimum',
			FAVN_WEB_SESSION_SECRET: 'playwright-session-secret-32-char-minimum'
		},
		url: 'http://127.0.0.1:4173',
		reuseExistingServer: false,
		timeout: 120_000,
		gracefulShutdown: {
			signal: 'SIGTERM',
			timeout: 10_000
		}
	},
	testMatch: '**/*.e2e.{ts,js}'
});
