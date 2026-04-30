import type { RequestHandler } from './$types';
import { orchestratorSubmitBackfill } from '$lib/server/orchestrator';
import { parseBackfillSubmitPayload } from '$lib/server/backfill_submit_payload';
import { jsonError, readJsonBody, relayJson, requireSession } from '$lib/server/web_api';

export const POST: RequestHandler = async (event) => {
	const unauthorized = await requireSession(event);
	if (unauthorized) return unauthorized;

	const body = await readJsonBody(event.request);
	if (!body) return jsonError(422, 'validation_failed', 'Invalid JSON body');

	const payload = parseBackfillSubmitPayload(body);
	if (!payload) {
		return jsonError(
			422,
			'validation_failed',
			'Expected pipeline target, active manifest selection, range { from, to, kind: "hour"|"day"|"month"|"year", timezone }, and optional coverage_baseline_id/max_attempts/retry_backoff_ms/timeout_ms'
		);
	}

	return relayJson(await orchestratorSubmitBackfill(event.locals.session!, payload));
};
