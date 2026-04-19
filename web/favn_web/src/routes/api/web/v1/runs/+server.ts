import type { RequestHandler } from './$types';
import { orchestratorListRuns, orchestratorSubmitRun } from '$lib/server/orchestrator';
import { jsonError, readJsonBody, relayJson, requireSession } from '$lib/server/web_api';

function parseSubmitPayload(value: Record<string, unknown>): {
	target: { type: 'asset' | 'pipeline'; id: string };
	manifest_selection?: unknown;
} | null {
	const targetValue = value.target;

	if (typeof targetValue !== 'object' || targetValue === null || Array.isArray(targetValue)) {
		return null;
	}

	const target = targetValue as Record<string, unknown>;

	const type = target.type;
	const id = typeof target.id === 'string' ? target.id.trim() : target.id;

	if ((type !== 'asset' && type !== 'pipeline') || typeof id !== 'string' || id.length === 0) {
		return null;
	}

	return {
		target: { type, id },
		...('manifest_selection' in value ? { manifest_selection: value.manifest_selection } : {})
	};
}

export const GET: RequestHandler = async (event) => {
	const unauthorized = requireSession(event);
	if (unauthorized) return unauthorized;

	const upstream = await orchestratorListRuns(event.locals.session!);
	return relayJson(upstream);
};

export const POST: RequestHandler = async (event) => {
	const unauthorized = requireSession(event);
	if (unauthorized) return unauthorized;

	const body = await readJsonBody(event.request);
	if (!body) {
		return jsonError(422, 'validation_failed', 'Invalid JSON body');
	}

	const payload = parseSubmitPayload(body);
	if (!payload) {
		return jsonError(
			422,
			'validation_failed',
			'Expected target with type "asset"|"pipeline" and non-empty id'
		);
	}

	const upstream = await orchestratorSubmitRun(event.locals.session!, payload);
	return relayJson(upstream);
};
