import type { RequestHandler } from './$types';
import { orchestratorListRuns, orchestratorSubmitRun } from '$lib/server/orchestrator';
import { parseSubmitPayload } from '$lib/server/run_submit_payload';
import { jsonError, readJsonBody, relayJson, requireSession } from '$lib/server/web_api';

export const GET: RequestHandler = async (event) => {
	const unauthorized = await requireSession(event);
	if (unauthorized) return unauthorized;

	const upstream = await orchestratorListRuns(event.locals.session!);
	return relayJson(upstream);
};

export const POST: RequestHandler = async (event) => {
	const unauthorized = await requireSession(event);
	if (unauthorized) return unauthorized;

	const body = await readJsonBody(event.request);
	if (!body) {
		return jsonError(422, 'validation_failed', 'Invalid JSON body');
	}

	const payload = parseSubmitPayload(body);
	if (!payload) {
		return jsonError(
			422,
			'validation_failed',
			'Expected target with type "asset"|"pipeline", non-empty id, optional dependencies "all"|"none" for asset targets only, and optional window { mode: "single", kind: "hour"|"day"|"month"|"year", value, timezone? } for pipeline targets only'
		);
	}

	const upstream = await orchestratorSubmitRun(event.locals.session!, payload);
	return relayJson(upstream);
};
