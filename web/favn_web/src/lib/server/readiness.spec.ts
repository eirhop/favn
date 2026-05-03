import { afterEach, describe, expect, it, vi } from 'vitest';

function setValidEnv(extra: Record<string, string | undefined> = {}) {
	vi.stubEnv('FAVN_WEB_ORCHESTRATOR_BASE_URL', 'https://orchestrator.internal:4101');
	vi.stubEnv('FAVN_WEB_ORCHESTRATOR_SERVICE_TOKEN', 'orchestrator-service-token-32-char-minimum');
	vi.stubEnv('FAVN_WEB_PUBLIC_ORIGIN', 'https://favn.example.com');
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

describe('checkWebReadiness', () => {
	it('is ready when web config is valid and orchestrator readiness passes', async () => {
		setValidEnv();
		const fetchMock = vi
			.fn()
			.mockResolvedValue(new Response('{"status":"ready"}', { status: 200 }));
		vi.stubGlobal('fetch', fetchMock);
		const { checkWebReadiness } = await import('./readiness');

		const report = await checkWebReadiness();

		expect(report).toEqual({
			service: 'favn_web',
			status: 'ready',
			checks: [
				{ check: 'web_config', status: 'ok' },
				{ check: 'orchestrator', status: 'ok' }
			]
		});
		expect(fetchMock).toHaveBeenCalledOnce();
	});

	it('reports not ready when orchestrator readiness is degraded', async () => {
		setValidEnv();
		vi.stubGlobal(
			'fetch',
			vi.fn().mockResolvedValue(new Response('{"status":"not_ready"}', { status: 503 }))
		);
		const { checkWebReadiness } = await import('./readiness');

		const report = await checkWebReadiness();

		expect(report).toMatchObject({
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

	it('reports not ready when orchestrator readiness times out', async () => {
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
		const { checkWebReadiness } = await import('./readiness');

		const reportPromise = checkWebReadiness();
		await vi.advanceTimersByTimeAsync(100);
		const report = await reportPromise;

		expect(report).toMatchObject({
			status: 'not_ready',
			checks: [
				{ check: 'web_config', status: 'ok' },
				{ check: 'orchestrator', status: 'degraded', reason: 'timeout' }
			]
		});
	});

	it('reports sanitized web config readiness failures', async () => {
		vi.stubEnv('FAVN_WEB_ORCHESTRATOR_BASE_URL', 'https://orchestrator.internal');
		vi.stubEnv('FAVN_WEB_ORCHESTRATOR_SERVICE_TOKEN', 'short-secret');
		const { checkWebReadiness } = await import('./readiness');

		const report = await checkWebReadiness();

		expect(report.status).toBe('not_ready');
		expect(report.checks).toEqual([
			{
				check: 'web_config',
				status: 'degraded',
				reason: 'invalid_config',
				details: {
					issues: expect.arrayContaining([
						expect.objectContaining({
							variable: 'FAVN_WEB_ORCHESTRATOR_SERVICE_TOKEN',
							value: '[redacted]'
						}),
						expect.objectContaining({ variable: 'FAVN_WEB_PUBLIC_ORIGIN', value: '[missing]' })
					])
				}
			}
		]);
		expect(JSON.stringify(report)).not.toContain('short-secret');
	});
});
