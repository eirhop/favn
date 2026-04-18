import type { RequestHandler } from './$types';
import { orchestratorListSchedules } from '$lib/server/orchestrator';
import { relayJson, requireSession } from '$lib/server/web_api';

export const GET: RequestHandler = async (event) => {
	const unauthorized = requireSession(event);
	if (unauthorized) return unauthorized;

	const upstream = await orchestratorListSchedules(event.locals.session!);
	return relayJson(upstream);
};
