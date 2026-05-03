import { afterEach, describe, expect, it, vi } from 'vitest';
import type { WebSession } from './session';

const session: WebSession = {
	session_token: 'opaque-session-token-1',
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
		expect(headers.get('x-favn-session-token')).toBe('opaque-session-token-1');
		expect(headers.has('x-favn-session-id')).toBe(false);
		expect(headers.has('Idempotency-Key')).toBe(false);
	});

	it('adds stable idempotency keys to mutating command requests without secrets', async () => {
		setValidEnv();
		const fetchMock = vi.fn().mockResolvedValue(new Response('{}', { status: 200 }));
		vi.stubGlobal('fetch', fetchMock);
		const {
			orchestratorActivateManifest,
			orchestratorCancelRun,
			orchestratorRerunBackfillWindow,
			orchestratorRerunRun,
			orchestratorSubmitBackfill,
			orchestratorSubmitRun
		} = await import('./orchestrator');

		const submitRunPayload = {
			target: { type: 'asset' as const, id: 'asset_a' },
			dependencies: 'all' as const
		};
		const submitBackfillPayload = {
			pipeline_module: 'Demo.Pipeline',
			window_kind: 'day',
			window_from: '2026-01-01',
			window_to: '2026-01-02'
		};

		await orchestratorSubmitRun(session, submitRunPayload);
		await orchestratorCancelRun(session, 'run_1');
		await orchestratorRerunRun(session, 'run_1');
		await orchestratorActivateManifest(session, 'manifest_1');
		await orchestratorSubmitBackfill(session, submitBackfillPayload);
		await orchestratorRerunBackfillWindow(session, 'backfill_1', { window_key: 'day:2026-01-01' });

		expect(fetchMock).toHaveBeenCalledTimes(6);
		const idempotencyKeys = fetchMock.mock.calls.map(([, init]) => {
			const headers = new Headers((init as RequestInit).headers);
			return headers.get('Idempotency-Key');
		});

		expect(idempotencyKeys).toHaveLength(6);
		for (const key of idempotencyKeys) {
			expect(key).toMatch(/^favn-web-[a-z-]+-[a-f0-9]{64}$/);
			expect(key).not.toContain('opaque-session-token-1');
			expect(key).not.toContain('orchestrator-service-token-32-char-minimum');
		}
		expect(new Set(idempotencyKeys).size).toBe(6);

		await orchestratorSubmitRun(session, submitRunPayload);
		const repeatHeaders = new Headers((fetchMock.mock.calls[6][1] as RequestInit).headers);
		expect(repeatHeaders.get('Idempotency-Key')).toBe(idempotencyKeys[0]);
	});

	it('distinguishes boolean and string command input values in idempotency keys', async () => {
		setValidEnv();
		const fetchMock = vi.fn().mockResolvedValue(new Response('{}', { status: 200 }));
		vi.stubGlobal('fetch', fetchMock);
		const { orchestratorSubmitBackfill } = await import('./orchestrator');

		await orchestratorSubmitBackfill(session, { dry_run: true });
		await orchestratorSubmitBackfill(session, { dry_run: 'true' });
		await orchestratorSubmitBackfill(session, { dry_run: true });

		const idempotencyKeys = fetchMock.mock.calls.map(([, init]) => {
			const headers = new Headers((init as RequestInit).headers);
			return headers.get('Idempotency-Key');
		});

		expect(idempotencyKeys[0]).toMatch(/^favn-web-submit-backfill-[a-f0-9]{64}$/);
		expect(idempotencyKeys[1]).toMatch(/^favn-web-submit-backfill-[a-f0-9]{64}$/);
		expect(idempotencyKeys[0]).not.toBe(idempotencyKeys[1]);
		expect(idempotencyKeys[2]).toBe(idempotencyKeys[0]);
	});

	it('revokes the durable session with service auth and session token header', async () => {
		setValidEnv();
		const fetchMock = vi.fn().mockResolvedValue(new Response('{}', { status: 200 }));
		vi.stubGlobal('fetch', fetchMock);
		const { orchestratorRevokeSession } = await import('./orchestrator');

		await orchestratorRevokeSession(session);

		expect(fetchMock).toHaveBeenCalledOnce();
		const [url, init] = fetchMock.mock.calls[0] as [URL, RequestInit];
		const headers = new Headers(init.headers);

		expect(url.toString()).toBe(
			'https://orchestrator.internal:4101/api/orchestrator/v1/auth/sessions/revoke'
		);
		expect(init.method).toBe('POST');
		expect(init.body).toBeUndefined();
		expect(headers.get('authorization')).toBe('Bearer orchestrator-service-token-32-char-minimum');
		expect(headers.get('x-favn-service')).toBe('favn_web');
		expect(headers.get('x-favn-session-token')).toBe('opaque-session-token-1');
		expect(headers.has('x-favn-session-id')).toBe(false);
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
	});
});
