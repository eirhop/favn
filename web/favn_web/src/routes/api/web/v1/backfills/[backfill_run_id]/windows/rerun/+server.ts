import type { RequestHandler } from './$types';
import { orchestratorRerunBackfillWindow } from '$lib/server/orchestrator';
import { jsonError, readJsonBody, relayJson, requireSession } from '$lib/server/web_api';

export const POST: RequestHandler = async (event) => {
	const unauthorized = await requireSession(event);
	if (unauthorized) return unauthorized;

	const body = await readJsonBody(event.request);
	const windowKey = typeof body?.window_key === 'string' ? body.window_key.trim() : '';
	if (!windowKey) return jsonError(422, 'validation_failed', 'Expected non-empty window_key');

	return relayJson(
		await orchestratorRerunBackfillWindow(event.locals.session!, event.params.backfill_run_id, {
			window_key: windowKey
		})
	);
};
