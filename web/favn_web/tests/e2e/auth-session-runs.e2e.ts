import { expect, test, type Page } from '@playwright/test';

const VALID_USERNAME = 'alice';
const VALID_PASSWORD = 'password123';

const FAVN_WEB_SESSION_COOKIE = 'favn_web_session';
const BASE_URL = 'http://127.0.0.1:4173';

function encodeSessionCookie(payload: Record<string, unknown>): string {
	return Buffer.from(JSON.stringify(payload), 'utf8').toString('base64url');
}

async function loginAsValidUser(page: Page) {
	await page.goto('/login');
	await page.getByLabel('Username').fill(VALID_USERNAME);
	await page.getByLabel('Password').fill(VALID_PASSWORD);
	await page.getByRole('button', { name: 'Log in' }).click();
}

async function loginAndReachHome(page: Page): Promise<void> {
	await loginAsValidUser(page);
	await expect(page).toHaveURL(/\/runs$/);
}

async function pageGetJson(
	page: Page,
	path: string,
	headers: Record<string, string> = {}
): Promise<{ status: number; body: unknown }> {
	return page.evaluate(
		async ({ pathArg, headersArg }) => {
			const response = await fetch(pathArg, { method: 'GET', headers: headersArg });
			const body = await response.json();
			return { status: response.status, body };
		},
		{ pathArg: path, headersArg: headers }
	);
}

async function pagePostJson(
	page: Page,
	path: string,
	body?: Record<string, unknown>
): Promise<{ status: number; body: unknown; headers: Record<string, string> }> {
	return page.evaluate(
		async ({ pathArg, bodyArg }) => {
			const headers: Record<string, string> = {
				accept: 'application/json'
			};

			let payload: string | undefined;
			if (bodyArg) {
				headers['content-type'] = 'application/json';
				payload = JSON.stringify(bodyArg);
			}

			const response = await fetch(pathArg, {
				method: 'POST',
				headers,
				...(payload ? { body: payload } : {})
			});

			const body = await response.json();
			return {
				status: response.status,
				body,
				headers: Object.fromEntries(response.headers.entries())
			};
		},
		{ pathArg: path, bodyArg: body }
	);
}

test.describe('auth/session/runs flow', () => {
	test('unauthenticated user visiting / is redirected to /login', async ({ page }) => {
		await page.goto('/');

		await expect(page).toHaveURL(/\/login$/);
		await expect(page.getByRole('heading', { name: 'Login' })).toBeVisible();
	});

	test('login failure keeps username and displays backend error message', async ({ page }) => {
		await page.goto('/login');
		await page.getByLabel('Username').fill('not-a-real-user');
		await page.getByLabel('Password').fill('wrong-password');
		await page.getByRole('button', { name: 'Log in' }).click();

		await expect(page).toHaveURL(/\/login$/);
		await expect(page.getByText('Invalid username or password')).toBeVisible();
		await expect(page.getByLabel('Username')).toHaveValue('not-a-real-user');
	});

	test('login success redirects to /runs and shows the run inspector', async ({ page }) => {
		await loginAsValidUser(page);

		await expect(page).toHaveURL(/\/runs$/);
		await expect(page.getByRole('heading', { name: 'Runs' })).toBeVisible();
		await expect(page.getByText('local-operator')).toBeVisible();
		await expect(page.getByText(/Active manifest:/)).toBeVisible();
		await expect(page.getByRole('row', { name: /run_001/ })).toContainText('succeeded');
		await expect(page.getByRole('row', { name: /run_002/ })).toContainText('failed');
	});

	test('login to runs list and open failed run detail', async ({ page }) => {
		await loginAndReachHome(page);

		await page
			.getByRole('row', { name: /run_002/ })
			.getByRole('link', { name: 'run_002' })
			.click();

		await expect(page).toHaveURL(/\/runs\/run_002$/);
		await expect(page.getByRole('heading', { name: 'Run run_002' })).toBeVisible();
		await expect(page.getByText('Run failed in asset Staging.CustomerOrders')).toBeVisible();
		await expect(
			page.getByText('DuckDB query failed: column "customer_id" not found')
		).toBeVisible();
		await expect(page.getByRole('button', { name: 'Assets' })).toBeVisible();
		await page.getByRole('button', { name: 'Outputs' }).click();
		await expect(page.getByText('staging.customer_orders')).toBeVisible();
	});

	test('logout returns to /login and / remains protected afterward', async ({ page }) => {
		await loginAsValidUser(page);
		await expect(page).toHaveURL(/\/runs$/);

		await page.getByText('local-operator').click();
		await page.getByRole('button', { name: 'Log out' }).click();

		await expect(page).toHaveURL(/\/login$/);
		await page.goto('/');
		await expect(page).toHaveURL(/\/login$/);
	});

	test('expired session cookie is cleared and user is redirected to /login', async ({ page }) => {
		const expiredCookie = encodeSessionCookie({
			session_id: 'sess_expired',
			actor_id: 'actor_expired',
			provider: 'password_local',
			expires_at: '2000-01-01T00:00:00.000Z',
			issued_at: '1999-12-31T00:00:00.000Z'
		});

		await page.context().addCookies([
			{
				name: FAVN_WEB_SESSION_COOKIE,
				value: expiredCookie,
				domain: '127.0.0.1',
				path: '/'
			}
		]);

		await page.goto('/');

		await expect(page).toHaveURL(/\/login$/);
		await expect
			.poll(async () => {
				const cookies = await page.context().cookies('http://127.0.0.1:4173');
				return cookies.some((cookie) => cookie.name === FAVN_WEB_SESSION_COOKIE);
			})
			.toBe(false);
	});

	test('web BFF operator API smoke for runs, manifests, and schedules', async ({ page }) => {
		await loginAndReachHome(page);

		const runsList = await pageGetJson(page, '/api/web/v1/runs');
		expect(runsList.status).toBe(200);
		expect(runsList.body).toMatchObject({
			data: expect.objectContaining({
				items: expect.arrayContaining([
					expect.objectContaining({ id: 'run_001', status: 'succeeded' }),
					expect.objectContaining({ id: 'run_002', status: 'failed' }),
					expect.objectContaining({ id: 'run_003', status: 'running' })
				])
			})
		});

		const runDetail = await pageGetJson(page, '/api/web/v1/runs/run_001');
		expect(runDetail.status).toBe(200);
		expect(runDetail.body).toEqual({
			data: expect.objectContaining({ id: 'run_001', status: 'succeeded' })
		});

		const submitRun = await pagePostJson(page, '/api/web/v1/runs', {
			target: { type: 'asset', id: 'asset.orders' },
			manifest_selection: { active: true }
		});
		expect(submitRun.status).toBe(202);
		expect(submitRun.body).toEqual({
			data: expect.objectContaining({ run_id: 'run_submitted_001', status: 'queued' })
		});

		const cancelRun = await pagePostJson(page, '/api/web/v1/runs/run_002/cancel');
		expect(cancelRun.status).toBe(200);
		expect(cancelRun.body).toEqual({
			data: expect.objectContaining({ run_id: 'run_002', status: 'cancelling' })
		});

		const rerunRun = await pagePostJson(page, '/api/web/v1/runs/run_001/rerun');
		expect(rerunRun.status).toBe(202);
		expect(rerunRun.body).toEqual({
			data: expect.objectContaining({ run_id: 'run_001_rerun_001', status: 'queued' })
		});

		const manifestsList = await pageGetJson(page, '/api/web/v1/manifests');
		expect(manifestsList.status).toBe(200);
		expect(manifestsList.body).toEqual({
			data: {
				items: expect.arrayContaining([
					expect.objectContaining({ manifest_version_id: 'manifest_v1' }),
					expect.objectContaining({ manifest_version_id: 'manifest_v2', status: 'active' })
				])
			}
		});

		const activeManifest = await pageGetJson(page, '/api/web/v1/manifests/active');
		expect(activeManifest.status).toBe(200);
		expect(activeManifest.body).toEqual({
			data: expect.objectContaining({
				manifest: expect.objectContaining({ manifest_version_id: 'manifest_v2', status: 'active' }),
				targets: expect.arrayContaining([
					expect.objectContaining({ type: 'asset', id: 'asset.orders' })
				])
			})
		});

		const activateManifest = await pagePostJson(page, '/api/web/v1/manifests/manifest_v1/activate');
		expect(activateManifest.status).toBe(200);
		expect(activateManifest.body).toEqual({
			data: expect.objectContaining({ manifest_version_id: 'manifest_v1', status: 'active' })
		});

		const schedulesList = await pageGetJson(page, '/api/web/v1/schedules');
		expect(schedulesList.status).toBe(200);
		expect(schedulesList.body).toEqual({
			data: {
				items: expect.arrayContaining([
					expect.objectContaining({ schedule_id: 'sched_001', enabled: true }),
					expect.objectContaining({ schedule_id: 'sched_002', enabled: false })
				])
			}
		});

		const scheduleDetail = await pageGetJson(page, '/api/web/v1/schedules/sched_001');
		expect(scheduleDetail.status).toBe(200);
		expect(scheduleDetail.body).toEqual({
			data: expect.objectContaining({
				schedule_id: 'sched_001',
				enabled: true,
				cron: '*/5 * * * *'
			})
		});
	});

	test('run stream relay smoke includes Last-Event-ID passthrough and validation', async ({
		page
	}) => {
		await loginAndReachHome(page);

		const invalidLastEventId = await pageGetJson(page, '/api/web/v1/streams/runs/run_002', {
			'Last-Event-ID': 'bad value!'
		});
		expect(invalidLastEventId.status).toBe(400);
		expect(invalidLastEventId.body).toEqual({
			error: {
				code: 'validation_failed',
				message: 'Invalid Last-Event-ID'
			}
		});

		const stream = await page.evaluate(async () => {
			const response = await fetch('/api/web/v1/streams/runs/run_002', {
				headers: { 'Last-Event-ID': 'evt_123' }
			});

			const reader = response.body?.getReader();
			let body = '';

			if (reader) {
				const decoder = new TextDecoder();
				const firstChunk = await reader.read();

				if (!firstChunk.done && firstChunk.value) {
					body = decoder.decode(firstChunk.value);
				}

				await reader.cancel();
			}

			return {
				status: response.status,
				contentType: response.headers.get('content-type'),
				body
			};
		});

		expect(stream.status).toBe(200);
		expect(stream.contentType).toContain('text/event-stream');
		const body = stream.body;
		expect(body).toContain('id: evt_001');
		expect(body).toContain('event: run_status');
		expect(body).toContain('"run_id":"run_002"');
		expect(body).toContain('"last_event_id_received":"evt_123"');
	});

	test('unauthenticated web BFF API returns current unauthorized envelope', async ({ page }) => {
		const response = await page.request.get(`${BASE_URL}/api/web/v1/runs`);

		expect(response.status()).toBe(401);
		expect(await response.json()).toEqual({
			error: {
				code: 'unauthorized',
				message: 'Authentication required'
			}
		});
	});
});
