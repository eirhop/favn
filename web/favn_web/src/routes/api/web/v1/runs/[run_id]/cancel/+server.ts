import { randomUUID } from 'node:crypto';
import type { RequestHandler } from './$types';
import { orchestratorCancelRun } from '$lib/server/orchestrator';
import { relayJson, requireSession } from '$lib/server/web_api';

export const POST: RequestHandler = async (event) => {
	const unauthorized = requireSession(event);
	if (unauthorized) return unauthorized;

	const upstream = await orchestratorCancelRun(event.locals.session!, event.params.run_id, randomUUID());
	return relayJson(upstream);
};
