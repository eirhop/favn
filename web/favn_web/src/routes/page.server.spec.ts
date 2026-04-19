import { beforeEach, describe, expect, it, vi } from 'vitest';
import { load } from './+page.server';
import { orchestratorListRuns } from '$lib/server/orchestrator';
import { clearWebSessionCookie, type WebSession } from '$lib/server/session';

vi.mock('$lib/server/orchestrator', () => ({
	orchestratorListRuns: vi.fn()
}));

vi.mock('$lib/server/session', () => ({
	clearWebSessionCookie: vi.fn()
}));

const authSession: WebSession = {
	session_id: 'sess-1',
	actor_id: 'actor-1',
	provider: 'password_local',
	expires_at: '2999-01-01T00:00:00.000Z',
	issued_at: '2026-01-01T00:00:00.000Z'
};

describe('home page server load', () => {
	beforeEach(() => {
		vi.clearAllMocks();
	});

	it('redirects to login when unauthenticated', async () => {
		await expect(
			load({
				locals: { session: null },
				cookies: {}
			} as never)
		).rejects.toMatchObject({ status: 303, location: '/login' });

		expect(orchestratorListRuns).not.toHaveBeenCalled();
	});

	it('clears session and redirects when orchestrator returns 401', async () => {
		vi.mocked(orchestratorListRuns).mockResolvedValue(new Response(null, { status: 401 }));
		const cookies = {};
		const locals = { session: authSession as WebSession | null };

		await expect(load({ locals, cookies } as never)).rejects.toMatchObject({
			status: 303,
			location: '/login'
		});

		expect(clearWebSessionCookie).toHaveBeenCalledWith(cookies);
		expect(locals.session).toBeNull();
	});

	it('returns normalized runs on successful response payload', async () => {
		vi.mocked(orchestratorListRuns).mockResolvedValue(
			new Response(
				JSON.stringify({
					data: {
						items: [{ id: 'run-1', status: 'completed' }, { run_id: 'run-2', status: '' }, 42]
					}
				}),
				{ status: 200, headers: { 'content-type': 'application/json' } }
			)
		);

		const result = await load({
			locals: { session: authSession },
			cookies: {}
		} as never);

		expect(result).toEqual({
			session: authSession,
			runs: [
				{ id: 'run-1', status: 'completed' },
				{ id: 'run-2', status: null },
				{ id: 'run-3', status: null }
			]
		});
	});
});
