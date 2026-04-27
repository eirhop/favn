import type { RequestHandler } from './$types';
import { orchestratorActivateManifest } from '$lib/server/orchestrator';
import { relayJson, requireSession } from '$lib/server/web_api';

export const POST: RequestHandler = async (event) => {
	const unauthorized = requireSession(event);
	if (unauthorized) return unauthorized;

	const upstream = await orchestratorActivateManifest(
		event.locals.session!,
		event.params.manifest_version_id
	);

	return relayJson(upstream);
};
