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
		delete process.env.FAVN_WEB_ADMIN_USERNAME;
		delete process.env.FAVN_WEB_ADMIN_PASSWORD;
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
		).rejects.toMatchObject({ status: 303, location: '/' });

		expect(webSessionFromLoginPayload).toHaveBeenCalledWith({
			data: { session_id: 'sess-1', actor_id: 'actor-1' }
		});
		expect(setWebSessionCookie).toHaveBeenCalledWith(cookies, baseSession);
		expect(locals.session).toEqual(baseSession);
	});

	it('prefers orchestrator session when credentials are valid both upstream and locally', async () => {
		process.env.FAVN_WEB_ADMIN_USERNAME = 'alice';
		process.env.FAVN_WEB_ADMIN_PASSWORD = 'good-password';

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
		).rejects.toMatchObject({ status: 303, location: '/' });

		expect(orchestratorLoginPassword).toHaveBeenCalledWith({
			username: 'alice',
			password: 'good-password'
		});
		expect(setWebSessionCookie).toHaveBeenCalledWith(cookies, baseSession);
		expect(locals.session).toEqual(
			expect.objectContaining({ actor_id: 'actor-1', provider: 'password_local' })
		);
	});

	it('sets a local admin session from .env credentials when orchestrator rejects', async () => {
		process.env.FAVN_WEB_ADMIN_USERNAME = 'admin';
		process.env.FAVN_WEB_ADMIN_PASSWORD = 'admin-password';
		vi.mocked(orchestratorLoginPassword).mockResolvedValue(
			new Response(JSON.stringify({ error: { message: 'Invalid credentials' } }), {
				status: 401,
				headers: { 'content-type': 'application/json' }
			})
		);

		const cookies = {};
		const locals = { session: null as WebSession | null };

		await expect(
			actions.default({
				request: createRequest({ username: 'admin', password: 'admin-password' }),
				cookies,
				locals
			} as never)
		).rejects.toMatchObject({ status: 303, location: '/' });

		expect(orchestratorLoginPassword).toHaveBeenCalledWith({
			username: 'admin',
			password: 'admin-password'
		});
		expect(webSessionFromLoginPayload).not.toHaveBeenCalled();
		expect(setWebSessionCookie).toHaveBeenCalledWith(
			cookies,
			expect.objectContaining({
				actor_id: 'admin:admin',
				provider: 'web_local_admin'
			})
		);
		expect(locals.session).toEqual(
			expect.objectContaining({ actor_id: 'admin:admin', provider: 'web_local_admin' })
		);
	});

	it('sets a local admin session from .env credentials when orchestrator is unavailable', async () => {
		process.env.FAVN_WEB_ADMIN_USERNAME = 'admin';
		process.env.FAVN_WEB_ADMIN_PASSWORD = 'admin-password';
		vi.mocked(orchestratorLoginPassword).mockRejectedValue(new Error('connection refused'));

		const cookies = {};
		const locals = { session: null as WebSession | null };

		await expect(
			actions.default({
				request: createRequest({ username: 'admin', password: 'admin-password' }),
				cookies,
				locals
			} as never)
		).rejects.toMatchObject({ status: 303, location: '/' });

		expect(setWebSessionCookie).toHaveBeenCalledWith(
			cookies,
			expect.objectContaining({
				actor_id: 'admin:admin',
				provider: 'web_local_admin'
			})
		);
		expect(locals.session).toEqual(
			expect.objectContaining({ actor_id: 'admin:admin', provider: 'web_local_admin' })
		);
	});
});
