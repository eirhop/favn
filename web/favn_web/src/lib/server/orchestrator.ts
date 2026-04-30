import { env } from '$env/dynamic/private';
import type { WebSession } from './session';

const DEFAULT_BASE_URL = 'http://127.0.0.1:4101';

function orchestratorBaseUrl(): string {
	return env.FAVN_ORCHESTRATOR_BASE_URL || DEFAULT_BASE_URL;
}

function orchestratorServiceToken(): string {
	const token = env.FAVN_ORCHESTRATOR_SERVICE_TOKEN;

	if (token && token.length > 0) {
		return token;
	}

	throw new Error('Missing FAVN_ORCHESTRATOR_SERVICE_TOKEN for orchestrator service auth');
}

function orchestratorUrl(pathname: string): URL {
	return new URL(pathname, orchestratorBaseUrl());
}

function orchestratorUnavailableResponse(): Response {
	return new Response(
		JSON.stringify({
			error: {
				code: 'bad_gateway',
				message: 'Unable to reach orchestrator service'
			}
		}),
		{
			status: 502,
			headers: { 'content-type': 'application/json; charset=utf-8' }
		}
	);
}

async function orchestratorRequest(
	pathname: string,
	init: RequestInit = {},
	session?: WebSession
): Promise<Response> {
	const headers = new Headers(init.headers);

	headers.set('authorization', `Bearer ${orchestratorServiceToken()}`);
	headers.set('x-favn-service', 'favn_web');

	if (session) {
		headers.set('x-favn-actor-id', session.actor_id);
		headers.set('x-favn-session-id', session.session_id);
	}

	try {
		return await fetch(orchestratorUrl(pathname), {
			...init,
			headers
		});
	} catch {
		return orchestratorUnavailableResponse();
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
		headers: {
			accept: 'application/json',
			'content-type': 'application/json'
		},
		body: JSON.stringify(payload)
	});
}

export function orchestratorCancelRun(session: WebSession, runId: string): Promise<Response> {
	return orchestratorAuthed(
		`/api/orchestrator/v1/runs/${encodeURIComponent(runId)}/cancel`,
		session,
		{
			method: 'POST',
			headers: { accept: 'application/json' }
		}
	);
}

export function orchestratorRerunRun(session: WebSession, runId: string): Promise<Response> {
	return orchestratorAuthed(
		`/api/orchestrator/v1/runs/${encodeURIComponent(runId)}/rerun`,
		session,
		{
			method: 'POST',
			headers: { accept: 'application/json' }
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
			headers: { accept: 'application/json' }
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
		headers: {
			accept: 'application/json',
			'content-type': 'application/json'
		},
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
			headers: {
				accept: 'application/json',
				'content-type': 'application/json'
			},
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
