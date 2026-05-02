import { afterEach, describe, expect, it, vi } from 'vitest';
import type { WebSession } from './session';

const session: WebSession = {
	session_id: 'sess_test',
	actor_id: 'actor_test',
	provider: 'password_local',
	expires_at: new Date(Date.now() + 60_000).toISOString(),
	issued_at: new Date().toISOString()
};

function setValidEnv(extra: Record<string, string | undefined> = {}) {
	vi.stubEnv('FAVN_WEB_ORCHESTRATOR_BASE_URL', 'https://orchestrator.internal:4101');
	vi.stubEnv('FAVN_WEB_ORCHESTRATOR_SERVICE_TOKEN', 'orchestrator-service-token-32-char-minimum');
	vi.stubEnv('FAVN_WEB_SESSION_SECRET', 'web-session-secret-32-char-minimum');
	if (extra.FAVN_WEB_ORCHESTRATOR_TIMEOUT_MS !== undefined) {
		vi.stubEnv('FAVN_WEB_ORCHESTRATOR_TIMEOUT_MS', extra.FAVN_WEB_ORCHESTRATOR_TIMEOUT_MS);
	}
}

afterEach(() => {
	vi.useRealTimers();
	vi.unstubAllGlobals();
	vi.unstubAllEnvs();
	vi.resetModules();
});

describe('orchestrator client', () => {
	it('adds service and actor headers to orchestrator requests', async () => {
		setValidEnv();
		const fetchMock = vi.fn().mockResolvedValue(new Response('{}', { status: 200 }));
		vi.stubGlobal('fetch', fetchMock);
		const { orchestratorListRuns } = await import('./orchestrator');

		const response = await orchestratorListRuns(session);

		expect(response.status).toBe(200);
		expect(fetchMock).toHaveBeenCalledOnce();
		const [url, init] = fetchMock.mock.calls[0] as [URL, RequestInit];
		expect(url.toString()).toBe('https://orchestrator.internal:4101/api/orchestrator/v1/runs');
		const headers = new Headers(init.headers);
		expect(headers.get('authorization')).toBe('Bearer orchestrator-service-token-32-char-minimum');
		expect(headers.get('x-favn-service')).toBe('favn_web');
		expect(headers.get('x-favn-actor-id')).toBe('actor_test');
		expect(headers.get('x-favn-session-id')).toBe('sess_test');
	});

	it('returns a sanitized response when orchestrator is unreachable', async () => {
		setValidEnv();
		vi.stubGlobal('fetch', vi.fn().mockRejectedValue(new Error('connect ECONNREFUSED secret')));
		const { orchestratorListRuns } = await import('./orchestrator');

		const response = await orchestratorListRuns(session);
		const body = await response.json();

		expect(response.status).toBe(502);
		expect(body).toEqual({
			error: {
				code: 'orchestrator_unavailable',
				message: 'Orchestrator service is unavailable'
			}
		});
		expect(JSON.stringify(body)).not.toContain('secret');
	});

	it('returns a sanitized timeout response when orchestrator does not respond', async () => {
		setValidEnv({ FAVN_WEB_ORCHESTRATOR_TIMEOUT_MS: '100' });
		vi.useFakeTimers();
		vi.stubGlobal(
			'fetch',
			vi.fn((_url: URL, init: RequestInit) => {
				return new Promise((_resolve, reject) => {
					init.signal?.addEventListener('abort', () =>
						reject(new DOMException('aborted', 'AbortError'))
					);
				});
			})
		);
		const { orchestratorListRuns } = await import('./orchestrator');

		const responsePromise = orchestratorListRuns(session);
		await vi.advanceTimersByTimeAsync(100);
		const response = await responsePromise;
		const body = await response.json();

		expect(response.status).toBe(504);
		expect(body).toEqual({
			error: {
				code: 'orchestrator_timeout',
				message: 'Orchestrator service did not respond in time'
			}
		});

		vi.useRealTimers();
	});
});
