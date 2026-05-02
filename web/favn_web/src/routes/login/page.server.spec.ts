import { beforeEach, describe, expect, it, vi } from 'vitest';
import { isActionFailure } from '@sveltejs/kit';
import { actions } from './+page.server';
import { orchestratorLoginPassword } from '$lib/server/orchestrator';
import {
	setWebSessionCookie,
	webSessionFromLoginPayload,
	type WebSession
} from '$lib/server/session';

vi.mock('$lib/server/orchestrator', () => ({
	orchestratorLoginPassword: vi.fn()
}));

vi.mock('$lib/server/session', () => ({
	setWebSessionCookie: vi.fn(),
	webSessionFromLoginPayload: vi.fn()
}));

function createRequest(formValues: Record<string, string>): Request {
	return new Request('http://localhost/login', {
		method: 'POST',
		body: new URLSearchParams(formValues)
	});
}

const baseSession: WebSession = {
	session_id: 'sess-1',
	actor_id: 'actor-1',
	provider: 'password_local',
	expires_at: '2999-01-01T00:00:00.000Z',
	issued_at: '2026-01-01T00:00:00.000Z'
};

describe('login page actions', () => {
	beforeEach(() => {
		vi.clearAllMocks();
	});

	it('returns validation failure when username or password is missing', async () => {
		const result = await actions.default({
			request: createRequest({ username: '   ', password: '' }),
			cookies: {},
			locals: { session: null }
		} as never);

		expect(orchestratorLoginPassword).not.toHaveBeenCalled();
		expect(isActionFailure(result)).toBe(true);

		if (isActionFailure(result)) {
			expect(result.status).toBe(400);
			expect(result.data).toEqual({
				message: 'Username and password are required',
				username: ''
			});
		}
	});

	it('returns upstream failure payload message for invalid credentials', async () => {
		vi.mocked(orchestratorLoginPassword).mockResolvedValue(
			new Response(JSON.stringify({ error: { message: 'Invalid credentials' } }), {
				status: 401,
				headers: { 'content-type': 'application/json' }
			})
		);

		const result = await actions.default({
			request: createRequest({ username: 'alice', password: 'wrong-password' }),
			cookies: {},
			locals: { session: null }
		} as never);

		expect(isActionFailure(result)).toBe(true);

		if (isActionFailure(result)) {
			expect(result.status).toBe(401);
			expect(result.data).toEqual({
				message: 'Invalid credentials',
				username: 'alice'
			});
		}
	});

	it('sets session cookie and redirects on successful login', async () => {
		vi.mocked(orchestratorLoginPassword).mockResolvedValue(
			new Response(JSON.stringify({ data: { session_id: 'sess-1', actor_id: 'actor-1' } }), {
				status: 201,
				headers: { 'content-type': 'application/json' }
			})
		);
		vi.mocked(webSessionFromLoginPayload).mockReturnValue(baseSession);

		const cookies = {};
		const locals = { session: null as WebSession | null };

		await expect(
			actions.default({
				request: createRequest({ username: 'alice', password: 'good-password' }),
				cookies,
				locals
			} as never)
		).rejects.toMatchObject({ status: 303, location: '/runs' });

		expect(webSessionFromLoginPayload).toHaveBeenCalledWith({
			data: { session_id: 'sess-1', actor_id: 'actor-1' }
		});
		expect(setWebSessionCookie).toHaveBeenCalledWith(cookies, baseSession);
		expect(locals.session).toEqual(baseSession);
	});

	it('requires a valid orchestrator response before creating a web session', async () => {
		vi.mocked(orchestratorLoginPassword).mockResolvedValue(
			new Response(JSON.stringify({ data: { session_id: 'sess-1', actor_id: 'actor-1' } }), {
				status: 201,
				headers: { 'content-type': 'application/json' }
			})
		);
		vi.mocked(webSessionFromLoginPayload).mockReturnValue(baseSession);

		const cookies = {};
		const locals = { session: null as WebSession | null };

		await expect(
			actions.default({
				request: createRequest({ username: 'alice', password: 'good-password' }),
				cookies,
				locals
			} as never)
		).rejects.toMatchObject({ status: 303, location: '/runs' });

		expect(orchestratorLoginPassword).toHaveBeenCalledWith({
			username: 'alice',
			password: 'good-password'
		});
		expect(setWebSessionCookie).toHaveBeenCalledWith(cookies, baseSession);
		expect(locals.session).toEqual(
			expect.objectContaining({ actor_id: 'actor-1', provider: 'password_local' })
		);
	});

	it('returns orchestrator rejection instead of creating a synthetic local session', async () => {
		vi.mocked(orchestratorLoginPassword).mockResolvedValue(
			new Response(JSON.stringify({ error: { message: 'Invalid credentials' } }), {
				status: 401,
				headers: { 'content-type': 'application/json' }
			})
		);

		const cookies = {};
		const locals = { session: null as WebSession | null };

		const result = await actions.default({
			request: createRequest({ username: 'admin', password: 'admin-password' }),
			cookies,
			locals
		} as never);

		expect(orchestratorLoginPassword).toHaveBeenCalledWith({
			username: 'admin',
			password: 'admin-password'
		});
		expect(isActionFailure(result)).toBe(true);

		if (isActionFailure(result)) {
			expect(result.status).toBe(401);
			expect(result.data).toEqual({
				message: 'Invalid credentials',
				username: 'admin'
			});
		}

		expect(webSessionFromLoginPayload).not.toHaveBeenCalled();
		expect(setWebSessionCookie).not.toHaveBeenCalled();
		expect(locals.session).toBeNull();
		expect(cookies).toEqual({});
	});

	it('returns an error when orchestrator is unavailable instead of creating a synthetic local session', async () => {
		vi.mocked(orchestratorLoginPassword).mockRejectedValue(new Error('connection refused'));

		const cookies = {};
		const locals = { session: null as WebSession | null };

		const result = await actions.default({
			request: createRequest({ username: 'admin', password: 'admin-password' }),
			cookies,
			locals
		} as never);

		expect(isActionFailure(result)).toBe(true);

		if (isActionFailure(result)) {
			expect(result.status).toBe(502);
			expect(result.data).toEqual({
				message: 'Login service is temporarily unavailable',
				username: 'admin'
			});
		}

		expect(webSessionFromLoginPayload).not.toHaveBeenCalled();
		expect(setWebSessionCookie).not.toHaveBeenCalled();
		expect(locals.session).toBeNull();
		expect(cookies).toEqual({});
	});

	it('sanitizes upstream server errors from login responses', async () => {
		vi.mocked(orchestratorLoginPassword).mockResolvedValue(
			new Response(JSON.stringify({ error: { message: 'database password leaked' } }), {
				status: 500,
				headers: { 'content-type': 'application/json' }
			})
		);

		const cookies = {};
		const locals = { session: null as WebSession | null };

		const result = await actions.default({
			request: createRequest({ username: 'admin', password: 'admin-password' }),
			cookies,
			locals
		} as never);

		expect(isActionFailure(result)).toBe(true);

		if (isActionFailure(result)) {
			expect(result.status).toBe(502);
			expect(result.data).toEqual({
				message: 'Login service is temporarily unavailable',
				username: 'admin'
			});
			expect(JSON.stringify(result.data)).not.toContain('password leaked');
		}

		expect(webSessionFromLoginPayload).not.toHaveBeenCalled();
		expect(setWebSessionCookie).not.toHaveBeenCalled();
		expect(locals.session).toBeNull();
		expect(cookies).toEqual({});
	});
});
