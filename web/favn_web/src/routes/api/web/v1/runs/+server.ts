import type { RequestHandler } from './$types';
import { orchestratorListRuns, orchestratorSubmitRun } from '$lib/server/orchestrator';
import { jsonError, readJsonBody, relayJson, requireSession } from '$lib/server/web_api';

const windowKinds = new Set(['hour', 'day', 'month', 'year']);

function parseWindowPayload(value: unknown): {
	mode: 'single';
	kind: 'hour' | 'day' | 'month' | 'year';
	value: string;
	timezone?: string | null;
} | null {
	if (typeof value !== 'object' || value === null || Array.isArray(value)) return null;

	const record = value as Record<string, unknown>;
	const mode = record.mode;
	const kind = record.kind;
	const rawValue = typeof record.value === 'string' ? record.value.trim() : record.value;
	const timezone = record.timezone;

	if (mode !== 'single') return null;
	if (typeof kind !== 'string' || !windowKinds.has(kind)) return null;
	if (typeof rawValue !== 'string' || rawValue.length === 0) return null;
	if (
		'timezone' in record &&
		timezone !== undefined &&
		timezone !== null &&
		(typeof timezone !== 'string' || timezone.trim().length === 0)
	) {
		return null;
	}
	const normalizedTimezone: string | null | undefined =
		typeof timezone === 'string'
			? timezone.trim()
			: timezone === null || timezone === undefined
				? timezone
				: undefined;

	return {
		mode,
		kind: kind as 'hour' | 'day' | 'month' | 'year',
		value: rawValue,
		...(normalizedTimezone === undefined ? {} : { timezone: normalizedTimezone })
	};
}

export function parseSubmitPayload(value: Record<string, unknown>): {
	target: { type: 'asset' | 'pipeline'; id: string };
	manifest_selection?: unknown;
	dependencies?: 'all' | 'none';
	window?: {
		mode: 'single';
		kind: 'hour' | 'day' | 'month' | 'year';
		value: string;
		timezone?: string | null;
	};
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

	const dependencies = value.dependencies;
	if (
		'dependencies' in value &&
		dependencies !== undefined &&
		dependencies !== 'all' &&
		dependencies !== 'none'
	) {
		return null;
	}

	if (type === 'pipeline' && 'dependencies' in value && dependencies !== undefined) {
		return null;
	}

	const window =
		'window' in value && value.window !== undefined ? parseWindowPayload(value.window) : null;
	if ('window' in value && value.window !== undefined && !window) {
		return null;
	}

	if (type === 'asset' && window) {
		return null;
	}

	return {
		target: { type, id },
		...('manifest_selection' in value ? { manifest_selection: value.manifest_selection } : {}),
		...(dependencies === 'all' || dependencies === 'none' ? { dependencies } : {}),
		...(window ? { window } : {})
	};
}

export const GET: RequestHandler = async (event) => {
	const unauthorized = await requireSession(event);
	if (unauthorized) return unauthorized;

	const upstream = await orchestratorListRuns(event.locals.session!);
	return relayJson(upstream);
};

export const POST: RequestHandler = async (event) => {
	const unauthorized = await requireSession(event);
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
			'Expected target with type "asset"|"pipeline", non-empty id, optional dependencies "all"|"none" for asset targets only, and optional window { mode: "single", kind: "hour"|"day"|"month"|"year", value, timezone? } for pipeline targets only'
		);
	}

	const upstream = await orchestratorSubmitRun(event.locals.session!, payload);
	return relayJson(upstream);
};
