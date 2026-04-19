import type { RequestHandler } from './$types';
import { orchestratorAuthed } from '$lib/server/orchestrator';
import { relayJson, requireSession } from '$lib/server/web_api';

export const GET: RequestHandler = async (event) => {
	const unauthorized = requireSession(event);
	if (unauthorized) return unauthorized;

	const upstream = await orchestratorAuthed(
		`/api/orchestrator/v1/schedules/${encodeURIComponent(event.params.schedule_id)}`,
		event.locals.session!,
		{ headers: { accept: 'application/json' } }
	);

	return relayJson(upstream);
};
