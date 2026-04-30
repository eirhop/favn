import type { RequestHandler } from './$types';
import { orchestratorListBackfillWindows } from '$lib/server/orchestrator';
import { relayJson, requireSession } from '$lib/server/web_api';

export const GET: RequestHandler = async (event) => {
	const unauthorized = await requireSession(event);
	if (unauthorized) return unauthorized;

	return relayJson(
		await orchestratorListBackfillWindows(
			event.locals.session!,
			event.params.backfill_run_id,
			event.url.searchParams
		)
	);
};
