import { createHash } from 'node:crypto';
import type { WebSession } from './session';
import { markSanitizedResponse } from './sanitized_response';
import { currentWebRuntimeConfig } from './runtime_config';

export type OrchestratorFailureCode = 'orchestrator_unavailable' | 'orchestrator_timeout';

export function orchestratorFailureResponse(code: OrchestratorFailureCode): Response {
	const status = code === 'orchestrator_timeout' ? 504 : 502;
	const message =
		code === 'orchestrator_timeout'
			? 'Orchestrator service did not respond in time'
			: 'Orchestrator service is unavailable';
	return markSanitizedResponse(
		new Response(
			JSON.stringify({
				error: {
					code,
					message
				}
			}),
			{
				status,
				headers: { 'content-type': 'application/json; charset=utf-8' }
			}
		)
	);
}

function timeoutFailure(signal: AbortSignal): OrchestratorFailureCode {
	return signal.aborted ? 'orchestrator_timeout' : 'orchestrator_unavailable';
}

function canonicalize(value: unknown): unknown {
	if (value === null) return ['null'];
	if (typeof value === 'boolean') return ['boolean', value];
	if (typeof value === 'string') return ['string', value];
	if (typeof value === 'number') return [Number.isInteger(value) ? 'integer' : 'number', value];
	if (Array.isArray(value)) return ['array', value.map(canonicalize)];
	if (value && typeof value === 'object') {
		return [
			'object',
			Object.entries(value)
				.filter(([, nested]) => nested !== undefined)
				.sort(([left], [right]) => left.localeCompare(right))
				.map(([key, nested]) => [key, canonicalize(nested)])
		];
	}
	return [typeof value, String(value)];
}

function idempotencyKey(operation: string, session: WebSession, input: unknown): string {
	const fingerprint = JSON.stringify(
		canonicalize({
			operation,
			actor_id: session.actor_id,
			session_id: session.session_id,
			provider: session.provider,
			service: 'favn_web',
			input
		})
	);
	const digest = createHash('sha256').update(fingerprint).digest('hex');
	return `favn-web-${operation}-${digest}`;
}

function commandHeaders(
	session: WebSession,
	operation: string,
	input: unknown,
	headers: HeadersInit = {}
): Headers {
	const output = new Headers(headers);
	output.set('Idempotency-Key', idempotencyKey(operation, session, input));
	return output;
}

async function orchestratorRequest(
	pathname: string,
	init: RequestInit = {},
	session?: WebSession
): Promise<Response> {
	const config = currentWebRuntimeConfig();
	const headers = new Headers(init.headers);
	const controller = new AbortController();
	const timeout = setTimeout(() => controller.abort(), config.orchestratorTimeoutMs);

	headers.set('authorization', `Bearer ${config.orchestratorServiceToken}`);
	headers.set('x-favn-service', 'favn_web');

	if (session) {
		headers.set('x-favn-actor-id', session.actor_id);
		headers.set('x-favn-session-token', session.session_token);
	}

	try {
		return await fetch(new URL(pathname, config.orchestratorBaseUrl), {
			...init,
			headers,
			signal: controller.signal
		});
	} catch {
		return orchestratorFailureResponse(timeoutFailure(controller.signal));
	} finally {
		clearTimeout(timeout);
	}
}

export function orchestratorLoginPassword(payload: {
	username: string;
	password: string;
}): Promise<Response> {
	return orchestratorRequest('/api/orchestrator/v1/auth/password/sessions', {
		method: 'POST',
		headers: {
			accept: 'application/json',
			'content-type': 'application/json'
		},
		body: JSON.stringify(payload)
	});
}

export function orchestratorAuthed(
	pathname: string,
	session: WebSession,
	init: RequestInit = {}
): Promise<Response> {
	return orchestratorRequest(pathname, init, session);
}

export function orchestratorGetMe(session: WebSession): Promise<Response> {
	return orchestratorAuthed('/api/orchestrator/v1/me', session, {
		headers: { accept: 'application/json' }
	});
}

export function orchestratorRevokeSession(session: WebSession): Promise<Response> {
	return orchestratorAuthed('/api/orchestrator/v1/auth/sessions/revoke', session, {
		method: 'POST',
		headers: { accept: 'application/json' }
	});
}

export function orchestratorListRuns(session: WebSession): Promise<Response> {
	return orchestratorAuthed('/api/orchestrator/v1/runs', session, {
		headers: { accept: 'application/json' }
	});
}

export function orchestratorGetRun(session: WebSession, runId: string): Promise<Response> {
	return orchestratorAuthed(`/api/orchestrator/v1/runs/${encodeURIComponent(runId)}`, session, {
		headers: { accept: 'application/json' }
	});
}

export function orchestratorSubmitRun(
	session: WebSession,
	payload: {
		target: { type: 'asset' | 'pipeline'; id: string };
		manifest_selection?: unknown;
		dependencies?: 'all' | 'none';
		window?: {
			mode: 'single';
			kind: 'hour' | 'day' | 'month' | 'year';
			value: string;
			timezone?: string | null;
		};
	}
): Promise<Response> {
	return orchestratorAuthed('/api/orchestrator/v1/runs', session, {
		method: 'POST',
		headers: commandHeaders(session, 'submit-run', payload, {
			accept: 'application/json',
			'content-type': 'application/json'
		}),
		body: JSON.stringify(payload)
	});
}

export function orchestratorCancelRun(session: WebSession, runId: string): Promise<Response> {
	return orchestratorAuthed(
		`/api/orchestrator/v1/runs/${encodeURIComponent(runId)}/cancel`,
		session,
		{
			method: 'POST',
			headers: commandHeaders(
				session,
				'cancel-run',
				{ run_id: runId },
				{ accept: 'application/json' }
			)
		}
	);
}

export function orchestratorRerunRun(session: WebSession, runId: string): Promise<Response> {
	return orchestratorAuthed(
		`/api/orchestrator/v1/runs/${encodeURIComponent(runId)}/rerun`,
		session,
		{
			method: 'POST',
			headers: commandHeaders(
				session,
				'rerun-run',
				{ run_id: runId },
				{ accept: 'application/json' }
			)
		}
	);
}

export function orchestratorListManifests(session: WebSession): Promise<Response> {
	return orchestratorAuthed('/api/orchestrator/v1/manifests', session, {
		headers: { accept: 'application/json' }
	});
}

export function orchestratorGetActiveManifest(session: WebSession): Promise<Response> {
	return orchestratorAuthed('/api/orchestrator/v1/manifests/active', session, {
		headers: { accept: 'application/json' }
	});
}

export function orchestratorActivateManifest(
	session: WebSession,
	manifestVersionId: string
): Promise<Response> {
	return orchestratorAuthed(
		`/api/orchestrator/v1/manifests/${encodeURIComponent(manifestVersionId)}/activate`,
		session,
		{
			method: 'POST',
			headers: commandHeaders(
				session,
				'activate-manifest',
				{ manifest_version_id: manifestVersionId },
				{ accept: 'application/json' }
			)
		}
	);
}

export function orchestratorGetAssetInspection(
	session: WebSession,
	manifestVersionId: string,
	targetId: string,
	limit: number
): Promise<Response> {
	const integerLimit = Number.isFinite(limit) ? Math.trunc(limit) : 20;
	const cappedLimit = Math.min(Math.max(integerLimit, 1), 20);
	const pathname = `/api/orchestrator/v1/manifests/${encodeURIComponent(
		manifestVersionId
	)}/assets/${encodeURIComponent(targetId)}/inspection?limit=${cappedLimit}`;

	return orchestratorAuthed(pathname, session, {
		headers: { accept: 'application/json' }
	});
}

export function orchestratorListSchedules(session: WebSession): Promise<Response> {
	return orchestratorAuthed('/api/orchestrator/v1/schedules', session, {
		headers: { accept: 'application/json' }
	});
}

export function orchestratorSubmitBackfill(
	session: WebSession,
	payload: unknown
): Promise<Response> {
	return orchestratorAuthed('/api/orchestrator/v1/backfills', session, {
		method: 'POST',
		headers: commandHeaders(session, 'submit-backfill', payload, {
			accept: 'application/json',
			'content-type': 'application/json'
		}),
		body: JSON.stringify(payload)
	});
}

function queryString(params: URLSearchParams): string {
	const value = params.toString();
	return value ? `?${value}` : '';
}

function forwardedSearchParams(input: URLSearchParams, allowed: string[]): URLSearchParams {
	const output = new URLSearchParams();
	for (const key of allowed) {
		const value = input.get(key);
		if (value !== null && value !== '') output.set(key, value);
	}
	return output;
}

export function orchestratorListBackfillWindows(
	session: WebSession,
	backfillRunId: string,
	searchParams: URLSearchParams
): Promise<Response> {
	const forwarded = forwardedSearchParams(searchParams, [
		'limit',
		'offset',
		'status',
		'pipeline_module',
		'window_key'
	]);
	return orchestratorAuthed(
		`/api/orchestrator/v1/backfills/${encodeURIComponent(backfillRunId)}/windows${queryString(forwarded)}`,
		session,
		{ headers: { accept: 'application/json' } }
	);
}

export function orchestratorRerunBackfillWindow(
	session: WebSession,
	backfillRunId: string,
	payload: { window_key: string }
): Promise<Response> {
	return orchestratorAuthed(
		`/api/orchestrator/v1/backfills/${encodeURIComponent(backfillRunId)}/windows/rerun`,
		session,
		{
			method: 'POST',
			headers: commandHeaders(
				session,
				'rerun-backfill-window',
				{ backfill_run_id: backfillRunId, ...payload },
				{
					accept: 'application/json',
					'content-type': 'application/json'
				}
			),
			body: JSON.stringify(payload)
		}
	);
}

export function orchestratorListCoverageBaselines(
	session: WebSession,
	searchParams: URLSearchParams = new URLSearchParams()
): Promise<Response> {
	const forwarded = forwardedSearchParams(searchParams, [
		'limit',
		'offset',
		'status',
		'pipeline_module',
		'source_key',
		'segment_key_hash'
	]);
	return orchestratorAuthed(
		`/api/orchestrator/v1/backfills/coverage-baselines${queryString(forwarded)}`,
		session,
		{ headers: { accept: 'application/json' } }
	);
}

export function orchestratorListAssetWindowStates(
	session: WebSession,
	searchParams: URLSearchParams = new URLSearchParams()
): Promise<Response> {
	const forwarded = forwardedSearchParams(searchParams, [
		'limit',
		'offset',
		'status',
		'pipeline_module',
		'asset_ref_module',
		'asset_ref_name',
		'window_key'
	]);
	return orchestratorAuthed(
		`/api/orchestrator/v1/assets/window-states${queryString(forwarded)}`,
		session,
		{ headers: { accept: 'application/json' } }
	);
}
