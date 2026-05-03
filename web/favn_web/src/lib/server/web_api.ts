import type { RequestEvent } from '@sveltejs/kit';
import { isSanitizedResponse } from './sanitized_response';
import { validateWebSession } from './session_guard';
import { sanitizeUpstreamPayload } from './upstream_errors';

type JsonRecord = Record<string, unknown>;

function isRecord(value: unknown): value is JsonRecord {
	return typeof value === 'object' && value !== null && !Array.isArray(value);
}

export function jsonError(status: number, code: string, message: string): Response {
	return new Response(JSON.stringify({ error: { code, message } }), {
		status,
		headers: { 'content-type': 'application/json; charset=utf-8' }
	});
}

export function rateLimitedResponse(retryAfterSeconds: number): Response {
	const response = jsonError(429, 'rate_limited', 'Too many requests');
	response.headers.set('retry-after', String(retryAfterSeconds));
	return response;
}

export async function requireSession(event: RequestEvent): Promise<Response | null> {
	const session = await validateWebSession(event);

	if (session) return null;

	return jsonError(401, 'unauthorized', 'Authentication required');
}

export async function relayJson(upstream: Response): Promise<Response> {
	if (upstream.status === 204 || upstream.status === 205 || upstream.status === 304) {
		return new Response(null, { status: upstream.status });
	}

	if (isSanitizedResponse(upstream)) {
		return upstream;
	}

	if (upstream.status >= 500) {
		await upstream.body?.cancel().catch(() => undefined);
		return jsonError(502, 'bad_gateway', 'Orchestrator service returned an unavailable response');
	}

	let payload: unknown;
	let status = upstream.status;

	try {
		payload = await upstream.json();
	} catch {
		payload = { error: { code: 'bad_gateway', message: 'Invalid upstream response' } };
		status = 502;
	}

	const headers = new Headers();
	headers.set('content-type', 'application/json; charset=utf-8');

	const sanitized = sanitizeUpstreamPayload(status, payload);

	return new Response(JSON.stringify(sanitized.payload), {
		status: sanitized.status,
		headers
	});
}

export async function readJsonBody(request: Request): Promise<JsonRecord | null> {
	try {
		const body = (await request.json()) as unknown;
		return isRecord(body) ? body : null;
	} catch {
		return null;
	}
}
