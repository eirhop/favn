import { expect, test } from '@playwright/test';

const BASE_URL = 'http://127.0.0.1:4173';

const PUBLIC_ROUTES = [
	{ method: 'GET', path: '/login' },
	{ method: 'POST', path: '/login' }
] as const;

const PROTECTED_PAGE_ROUTES = [
	'/',
	'/runs',
	'/runs/run_001',
	'/assets',
	'/assets/Staging.CustomerOrders%3Aasset',
	'/assets/window-states',
	'/backfills',
	'/backfills/bf_001',
	'/backfills/coverage-baselines',
	'/demo',
	'/demo/playwright'
] as const;

const PROTECTED_API_ROUTES = [
	{ method: 'GET', path: '/api/web/v1/health/live' },
	{ method: 'GET', path: '/api/web/v1/health/ready' },
	{ method: 'GET', path: '/api/web/v1/runs' },
	{ method: 'POST', path: '/api/web/v1/runs' },
	{ method: 'GET', path: '/api/web/v1/runs/run_001' },
	{ method: 'POST', path: '/api/web/v1/runs/run_001/rerun' },
	{ method: 'POST', path: '/api/web/v1/runs/run_001/cancel' },
	{ method: 'GET', path: '/api/web/v1/manifests' },
	{ method: 'GET', path: '/api/web/v1/manifests/active' },
	{ method: 'POST', path: '/api/web/v1/manifests/manifest_v1/activate' },
	{ method: 'GET', path: '/api/web/v1/schedules' },
	{ method: 'GET', path: '/api/web/v1/schedules/sched_001' },
	{ method: 'GET', path: '/api/web/v1/streams/runs' },
	{ method: 'GET', path: '/api/web/v1/streams/runs/run_001' },
	{ method: 'GET', path: '/api/web/v1/assets/window-states' },
	{ method: 'GET', path: '/api/web/v1/backfills' },
	{ method: 'POST', path: '/api/web/v1/backfills' },
	{ method: 'GET', path: '/api/web/v1/backfills/bf_001/windows' },
	{ method: 'POST', path: '/api/web/v1/backfills/bf_001/windows/rerun' },
	{ method: 'GET', path: '/api/web/v1/backfills/coverage-baselines' },
	{
		method: 'GET',
		path: '/api/web/v1/manifests/manifest_v2/assets/asset%3AStaging.CustomerOrders%3Aasset/inspection'
	}
] as const;

test.describe('deny-by-default route inventory', () => {
	test('documents the intentionally tiny public route allowlist', () => {
		expect(PUBLIC_ROUTES).toEqual([
			{ method: 'GET', path: '/login' },
			{ method: 'POST', path: '/login' }
		]);
	});

	test('GET /login is reachable without a session', async ({ request }) => {
		const response = await request.get('/login');

		expect(response.status()).toBe(200);
		expect(response.headers()['content-type']).toContain('text/html');
		expect(await response.text()).toContain('Login');
	});

	test('POST /login remains public but still requires same-origin proof', async ({ request }) => {
		const csrfRejected = await request.post('/login', {
			form: { username: '', password: '' },
			headers: { accept: 'application/json' }
		});
		expect(csrfRejected.status()).toBe(403);
		expect(await csrfRejected.json()).toEqual({
			message: 'Cross-site POST form submissions are forbidden'
		});

		const sameOrigin = await request.post('/login', {
			form: { username: '', password: '' },
			headers: { origin: BASE_URL }
		});
		expect(sameOrigin.status()).toBe(200);
		expect(sameOrigin.headers()['content-type']).toContain('application/json');
		const sameOriginBody = await sameOrigin.text();
		expect(sameOriginBody).toContain('Username and password are required');
		expect(sameOriginBody).not.toContain('Authentication required');
	});

	for (const path of PROTECTED_PAGE_ROUTES) {
		test(`GET ${path} redirects unauthenticated page requests to login`, async ({ page }) => {
			await page.goto(path);

			await expect(page).toHaveURL(/\/login\?next=/);
			await expect(page.getByRole('heading', { name: 'Login' })).toBeVisible();
		});
	}

	test('GET /runs preserves a safe relative next path', async ({ page }) => {
		await page.goto('/runs');

		await expect(page).toHaveURL(`${BASE_URL}/login?next=%2Fruns`);
	});

	for (const route of PROTECTED_API_ROUTES) {
		test(`${route.method} ${route.path} returns JSON 401 without a session`, async ({
			request
		}) => {
			const response = await request.fetch(route.path, {
				method: route.method,
				headers: {
					accept: 'application/json',
					origin: BASE_URL,
					'content-type': 'application/json'
				},
				data: route.method === 'GET' ? undefined : {}
			});

			expect(response.status()).toBe(401);
			expect(response.headers()['content-type']).toContain('application/json');
			expect(await response.json()).toEqual({
				error: {
					code: 'unauthorized',
					message: 'Authentication required'
				}
			});
		});
	}
});
