import type { RequestHandler } from './$types';
import { orchestratorGetRun } from '$lib/server/orchestrator';
import { relayJson, requireSession } from '$lib/server/web_api';

export const GET: RequestHandler = async (event) => {
	const unauthorized = requireSession(event);
	if (unauthorized) return unauthorized;

	const upstream = await orchestratorGetRun(event.locals.session!, event.params.run_id);
	return relayJson(upstream);
};
