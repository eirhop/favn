import type { RequestEvent } from '@sveltejs/kit';

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

export function requireSession(event: RequestEvent): Response | null {
	if (event.locals.session) {
		return null;
	}

	return jsonError(401, 'unauthorized', 'Authentication required');
}

export async function relayJson(upstream: Response): Promise<Response> {
	if (upstream.status === 204 || upstream.status === 205 || upstream.status === 304) {
		return new Response(null, { status: upstream.status });
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

	return new Response(JSON.stringify(payload), {
		status,
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
