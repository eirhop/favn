import type { RequestHandler } from './$types';
import { orchestratorListCoverageBaselines } from '$lib/server/orchestrator';
import { relayJson, requireSession } from '$lib/server/web_api';

export const GET: RequestHandler = async (event) => {
	const unauthorized = await requireSession(event);
	if (unauthorized) return unauthorized;

	return relayJson(
		await orchestratorListCoverageBaselines(event.locals.session!, event.url.searchParams)
	);
};
