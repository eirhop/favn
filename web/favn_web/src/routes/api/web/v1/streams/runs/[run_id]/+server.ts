import type { RequestHandler } from './$types';
import { orchestratorAuthed } from '$lib/server/orchestrator';
import { jsonError } from '$lib/server/web_api';

function normalizeLastEventId(value: string | null): string | null {
	if (!value) return null;

	const trimmed = value.trim();
	if (trimmed.length === 0) return null;

	if (!/^[a-zA-Z0-9:_\-.]{1,128}$/.test(trimmed)) {
		return null;
	}

	return trimmed;
}

export const GET: RequestHandler = async ({ locals, request, params }) => {
	if (!locals.session) {
		return jsonError(401, 'unauthorized', 'Authentication required');
	}

	const requestedLastEventId = request.headers.get('last-event-id');
	const lastEventId = normalizeLastEventId(requestedLastEventId);

	if (requestedLastEventId && !lastEventId) {
		return jsonError(400, 'validation_failed', 'Invalid Last-Event-ID');
	}

	const upstream = await orchestratorAuthed(
		`/api/orchestrator/v1/streams/runs/${encodeURIComponent(params.run_id)}`,
		locals.session!,
		{
			method: 'GET',
			headers: {
				accept: 'text/event-stream',
				...(lastEventId ? { 'last-event-id': lastEventId } : {})
			}
		}
	);

	if (!upstream.body) {
		const text = await upstream.text();
		return new Response(text || 'Bad gateway', {
			status: upstream.ok ? 502 : upstream.status,
			headers: { 'content-type': 'text/plain; charset=utf-8' }
		});
	}

	const headers = new Headers();
	headers.set('content-type', upstream.headers.get('content-type') ?? 'text/event-stream');
	headers.set('cache-control', upstream.headers.get('cache-control') ?? 'no-cache');

	return new Response(upstream.body, {
		status: upstream.status,
		statusText: upstream.statusText,
		headers
	});
};
