import { expect, test } from '@playwright/test';
import { spawn } from 'node:child_process';

const VALID_USERNAME = 'alice';
const VALID_PASSWORD = 'password123';

async function login(page: import('@playwright/test').Page): Promise<void> {
	await page.goto('/login');
	await page.getByLabel('Username').fill(VALID_USERNAME);
	await page.getByLabel('Password').fill(VALID_PASSWORD);
	await page.getByRole('button', { name: 'Log in' }).click();
	await expect(page).toHaveURL(/\/runs$/);
}

async function pageGetJson(page: import('@playwright/test').Page, path: string) {
	return page.evaluate(async (pathArg) => {
		const response = await fetch(pathArg);
		return {
			status: response.status,
			body: await response.json(),
			headers: Object.fromEntries(response.headers.entries())
		};
	}, path);
}

async function setMockReadiness(status: number): Promise<void> {
	const response = await fetch('http://127.0.0.1:4101/__mock/readiness', {
		method: 'POST',
		headers: { 'content-type': 'application/json' },
		body: JSON.stringify({ status })
	});

	expect(response.status).toBe(200);
}

test.afterEach(async () => {
	await setMockReadiness(200);
});

test.describe('web health and readiness', () => {
	test('liveness is cheap and readiness checks orchestrator reachability', async ({ page }) => {
		await login(page);

		const live = await pageGetJson(page, '/api/web/v1/health/live');
		const ready = await pageGetJson(page, '/api/web/v1/health/ready');

		expect(live.status).toBe(200);
		expect(live.body).toEqual({ service: 'favn_web', status: 'ok' });
		expect(live.headers['cache-control']).toBe('no-store');

		expect(ready.status).toBe(200);
		expect(ready.body).toEqual({
			service: 'favn_web',
			status: 'ready',
			checks: [
				{ check: 'web_config', status: 'ok' },
				{ check: 'orchestrator', status: 'ok' }
			]
		});
		expect(ready.headers['cache-control']).toBe('no-store');
	});

	test('readiness returns 503 when orchestrator readiness is degraded', async ({ page }) => {
		await setMockReadiness(503);
		await login(page);

		const response = await pageGetJson(page, '/api/web/v1/health/ready');

		expect(response.status).toBe(503);
		expect(response.body).toMatchObject({
			service: 'favn_web',
			status: 'not_ready',
			checks: [
				{ check: 'web_config', status: 'ok' },
				{
					check: 'orchestrator',
					status: 'degraded',
					reason: 'orchestrator_not_ready',
					details: { status: 503 }
				}
			]
		});
	});
});

test('production server fails before serving traffic when required env is missing', async () => {
	const child = spawn(process.execPath, ['build'], {
		cwd: process.cwd(),
		env: {
			...process.env,
			NODE_ENV: 'production',
			HOST: '127.0.0.1',
			PORT: '4199',
			FAVN_WEB_ORCHESTRATOR_BASE_URL: 'http://127.0.0.1:4101',
			FAVN_WEB_PUBLIC_ORIGIN: 'http://127.0.0.1:4199',
			FAVN_WEB_ORCHESTRATOR_SERVICE_TOKEN: ''
		}
	});

	let output = '';
	child.stdout.on('data', (chunk) => (output += String(chunk)));
	child.stderr.on('data', (chunk) => (output += String(chunk)));

	const result = await new Promise<{ code: number | null; signal: NodeJS.Signals | null }>(
		(resolve) => {
			const timeout = setTimeout(() => {
				child.kill('SIGTERM');
			}, 5_000);

			child.on('exit', (code, signal) => {
				clearTimeout(timeout);
				resolve({ code, signal });
			});
		}
	);

	expect(result.signal).toBeNull();
	expect(result.code).not.toBe(0);
	expect(output).toContain('FAVN_WEB_ORCHESTRATOR_SERVICE_TOKEN');
});
