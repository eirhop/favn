import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { RequestEvent } from '@sveltejs/kit';
import { resetAllRateLimits } from '$lib/server/rate_limit';
import { handle, isPublicRoute, isWebApiRoute, unauthenticatedResponse } from './hooks.server';

const LOCAL_ORIGIN = 'http://localhost';
const CLIENT_ADDRESS = '198.51.100.7';

function event(method: string, path: string): RequestEvent {
	return {
		request: new Request(`${LOCAL_ORIGIN}${path}`, { method }),
		url: new URL(`${LOCAL_ORIGIN}${path}`),
		locals: { session: null }
	} as RequestEvent;
}

function hookEvent(method: string, path: string, headers: HeadersInit = {}): RequestEvent {
	return {
		request: new Request(`${LOCAL_ORIGIN}${path}`, { method, headers }),
		url: new URL(`${LOCAL_ORIGIN}${path}`),
		locals: { session: null },
		cookies: {
			get: () => undefined,
			getAll: () => [],
			set: vi.fn(),
			delete: vi.fn(),
			serialize: vi.fn()
		},
		getClientAddress: () => CLIENT_ADDRESS
	} as unknown as RequestEvent;
}

async function runHook(method: string, path: string, headers: HeadersInit = {}) {
	const resolve = vi.fn(async () => new Response('resolved', { status: 200 }));
	const requestEvent = hookEvent(method, path, headers);
	const response = await handle({
		event: requestEvent,
		resolve
	} as Parameters<typeof handle>[0]);

	return { response, resolve, event: requestEvent };
}

beforeEach(() => {
	resetAllRateLimits();
});

afterEach(() => {
	vi.unstubAllEnvs();
});

function enableTrustedLocalDevAuth(extra: Record<string, string> = {}) {
	vi.stubEnv('FAVN_WEB_LOCAL_DEV_TRUSTED_AUTH', '1');
	vi.stubEnv('FAVN_WEB_PUBLIC_ORIGIN', 'http://127.0.0.1:4199');
	vi.stubEnv('FAVN_WEB_ORCHESTRATOR_BASE_URL', 'http://127.0.0.1:4101');

	for (const [key, value] of Object.entries(extra)) {
		vi.stubEnv(key, value);
	}
}

describe('web hook route classification', () => {
	it('keeps the public route allowlist exact and method-aware', () => {
		expect(isPublicRoute(event('GET', '/login'))).toBe(true);
		expect(isPublicRoute(event('POST', '/login'))).toBe(true);
		expect(isPublicRoute(event('GET', '/login/help'))).toBe(false);
		expect(isPublicRoute(event('GET', '/api/web/v1/health/live'))).toBe(false);
		expect(isPublicRoute(event('GET', '/runs'))).toBe(false);
	});

	it('classifies only the web BFF API prefix as API', () => {
		expect(isWebApiRoute('/api/web/v1/runs')).toBe(true);
		expect(isWebApiRoute('/api/web/v1/health/ready')).toBe(true);
		expect(isWebApiRoute('/api/orchestrator/v1/runs')).toBe(false);
		expect(isWebApiRoute('/runs')).toBe(false);
	});

	it('returns JSON 401 for unauthenticated web API requests', async () => {
		const response = unauthenticatedResponse(event('GET', '/api/web/v1/runs'));

		expect(response.status).toBe(401);
		expect(response.headers.get('content-type')).toContain('application/json');
		expect(await response.json()).toEqual({
			error: { code: 'unauthorized', message: 'Authentication required' }
		});
	});

	it('redirects unauthenticated page requests to login with a same-origin next path', () => {
		const response = unauthenticatedResponse(event('GET', '/runs?status=failed'));

		expect(response.status).toBe(303);
		expect(response.headers.get('location')).toBe('/login?next=%2Fruns%3Fstatus%3Dfailed');
	});

	it('rate limits unauthenticated unsafe protected API requests before returning 401', async () => {
		const headers = { origin: LOCAL_ORIGIN, accept: 'application/json' };

		for (let attempt = 0; attempt < 120; attempt += 1) {
			const { response, resolve } = await runHook('POST', '/api/web/v1/runs', headers);

			expect(response.status).toBe(401);
			expect(await response.json()).toEqual({
				error: { code: 'unauthorized', message: 'Authentication required' }
			});
			expect(resolve).not.toHaveBeenCalled();
		}

		const { response, resolve } = await runHook('POST', '/api/web/v1/runs', headers);

		expect(response.status).toBe(429);
		expect(response.headers.get('retry-after')).toBeTruthy();
		expect(await response.json()).toEqual({
			error: { code: 'rate_limited', message: 'Too many requests' }
		});
		expect(resolve).not.toHaveBeenCalled();
	});

	it('does not apply mutation rate limiting to POST /login', async () => {
		const headers = { origin: LOCAL_ORIGIN, accept: 'application/json' };

		for (let attempt = 0; attempt < 120; attempt += 1) {
			await runHook('POST', '/api/web/v1/runs', headers);
		}

		const { response, resolve } = await runHook('POST', '/login', headers);

		expect(response.status).toBe(200);
		expect(await response.text()).toBe('resolved');
		expect(resolve).toHaveBeenCalledOnce();
	});

	it('allows protected page requests in trusted loopback local dev auth mode', async () => {
		enableTrustedLocalDevAuth();

		const { response, resolve, event } = await runHook('GET', '/runs');

		expect(response.status).toBe(200);
		expect(await response.text()).toBe('resolved');
		expect(resolve).toHaveBeenCalledOnce();
		expect(event.locals.session).toEqual({
			session_token: '',
			session_id: 'local-dev-cli',
			actor_id: 'local-dev-cli',
			provider: 'local_dev_trusted',
			expires_at: null,
			issued_at: null
		});
	});

	it('keeps protected pages behind login unless trusted local dev auth is explicitly loopback', async () => {
		const withoutFlag = await runHook('GET', '/runs');
		expect(withoutFlag.response.status).toBe(303);
		expect(withoutFlag.response.headers.get('location')).toBe('/login?next=%2Fruns');
		expect(withoutFlag.resolve).not.toHaveBeenCalled();

		enableTrustedLocalDevAuth({ FAVN_WEB_PUBLIC_ORIGIN: 'https://favn.example.com' });
		const publicOriginNotLoopback = await runHook('GET', '/runs');
		expect(publicOriginNotLoopback.response.status).toBe(303);
		expect(publicOriginNotLoopback.resolve).not.toHaveBeenCalled();
	});

	it('rejects cross-site unsafe requests before mutation rate limiting', async () => {
		const headers = { origin: LOCAL_ORIGIN, accept: 'application/json' };

		for (let attempt = 0; attempt < 120; attempt += 1) {
			await runHook('POST', '/api/web/v1/runs', headers);
		}

		const { response, resolve } = await runHook('POST', '/api/web/v1/runs', {
			origin: 'https://attacker.example',
			accept: 'application/json'
		});

		expect(response.status).toBe(403);
		expect(await response.json()).toEqual({
			error: { code: 'csrf_rejected', message: 'Request Origin does not match the web origin' }
		});
		expect(resolve).not.toHaveBeenCalled();
	});
});
