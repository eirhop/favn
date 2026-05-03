import { describe, expect, it } from 'vitest';
import type { RequestEvent } from '@sveltejs/kit';
import { isPublicRoute, isWebApiRoute, unauthenticatedResponse } from './hooks.server';

function event(method: string, path: string): RequestEvent {
	return {
		request: new Request(`http://localhost${path}`, { method }),
		url: new URL(`http://localhost${path}`),
		locals: { session: null }
	} as RequestEvent;
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
});
