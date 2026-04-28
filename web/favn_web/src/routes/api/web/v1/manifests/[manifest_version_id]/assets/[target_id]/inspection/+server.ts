import type { RequestHandler } from './$types';
import { orchestratorGetAssetInspection } from '$lib/server/orchestrator';
import { relayJson, requireSession } from '$lib/server/web_api';

function inspectionLimit(value: string | null): number {
	const parsed = Number.parseInt(value ?? '20', 10);
	if (!Number.isFinite(parsed)) return 20;
	return Math.min(Math.max(parsed, 1), 20);
}

export const GET: RequestHandler = async (event) => {
	const unauthorized = await requireSession(event);
	if (unauthorized) return unauthorized;

	const upstream = await orchestratorGetAssetInspection(
		event.locals.session!,
		event.params.manifest_version_id,
		event.params.target_id,
		inspectionLimit(event.url.searchParams.get('limit'))
	);

	return relayJson(upstream);
};
