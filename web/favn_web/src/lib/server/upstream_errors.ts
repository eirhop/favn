type JsonRecord = Record<string, unknown>;

const SAFE_MESSAGES_BY_CODE: Record<string, Set<string>> = {
	validation_failed: new Set([
		'Invalid JSON body',
		'Expected non-empty window_key',
		'Expected target with type "asset"|"pipeline", non-empty id, optional dependencies "all"|"none" for asset targets only, and optional window { mode: "single", kind: "hour"|"day"|"month"|"year", value, timezone? } for pipeline targets only',
		'Expected pipeline target, active manifest selection, range { from, to, kind: "hour"|"day"|"month"|"year", timezone }, and optional coverage_baseline_id/max_attempts/retry_backoff_ms/timeout_ms'
	]),
	not_found: new Set(['No local materialization is available for this asset'])
};

function isRecord(value: unknown): value is JsonRecord {
	return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function stringValue(value: unknown): string | null {
	return typeof value === 'string' && value.length > 0 ? value : null;
}

function upstreamError(payload: unknown): { code: string | null; message: string | null } {
	if (!isRecord(payload) || !isRecord(payload.error)) return { code: null, message: null };

	return {
		code: stringValue(payload.error.code),
		message: stringValue(payload.error.message)
	};
}

function fallbackForStatus(status: number): { status: number; code: string; message: string } {
	if (status === 401) return { status, code: 'unauthorized', message: 'Authentication required' };
	if (status === 403) return { status, code: 'forbidden', message: 'Request is not allowed' };
	if (status === 404) return { status, code: 'not_found', message: 'Resource was not found' };
	if (status === 409)
		return { status, code: 'conflict', message: 'Request conflicts with existing state' };
	if (status === 422)
		return { status, code: 'validation_failed', message: 'Request validation failed' };
	if (status === 429) return { status, code: 'rate_limited', message: 'Too many requests' };
	return {
		status: 502,
		code: 'bad_gateway',
		message: 'Orchestrator service returned an unavailable response'
	};
}

function safeEnvelope(code: string, message: string): unknown {
	return { error: { code, message } };
}

export function sanitizeUpstreamPayload(
	status: number,
	payload: unknown
): { status: number; payload: unknown } {
	if (status < 400) return { status, payload };

	const fallback = fallbackForStatus(status);
	const upstream = upstreamError(payload);
	const safeMessages = upstream.code ? SAFE_MESSAGES_BY_CODE[upstream.code] : undefined;

	if (upstream.code && upstream.message && safeMessages?.has(upstream.message)) {
		return {
			status,
			payload: safeEnvelope(upstream.code, upstream.message)
		};
	}

	return {
		status: fallback.status,
		payload: safeEnvelope(fallback.code, fallback.message)
	};
}
