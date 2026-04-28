import type { RequestHandler } from './$types';
import { orchestratorGetActiveManifest } from '$lib/server/orchestrator';
import { relayJson, requireSession } from '$lib/server/web_api';

export const GET: RequestHandler = async (event) => {
	const unauthorized = await requireSession(event);
	if (unauthorized) return unauthorized;

	const upstream = await orchestratorGetActiveManifest(event.locals.session!);
	return relayJson(upstream);
};
