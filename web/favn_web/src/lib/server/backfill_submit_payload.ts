import type { BackfillSubmitPayload } from '$lib/backfill_submission';
import type { WindowKind } from '$lib/pipeline_run_submission';

type JsonRecord = Record<string, unknown>;

const windowKinds = new Set<WindowKind>(['hour', 'day', 'month', 'year']);

function isRecord(value: unknown): value is JsonRecord {
	return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function nonEmptyString(value: unknown): string | null {
	return typeof value === 'string' && value.trim().length > 0 ? value.trim() : null;
}

function optionalInteger(value: unknown): number | undefined | null {
	if (value === undefined || value === null || value === '') return undefined;
	if (typeof value !== 'number' || !Number.isFinite(value) || value < 0) return null;
	return Math.trunc(value);
}

export function parseBackfillSubmitPayload(body: JsonRecord): BackfillSubmitPayload | null {
	const target = isRecord(body.target) ? body.target : null;
	if (!target || target.type !== 'pipeline') return null;

	const targetId = nonEmptyString(target.id);
	if (!targetId) return null;

	const manifestSelection = isRecord(body.manifest_selection) ? body.manifest_selection : null;
	if (!manifestSelection || manifestSelection.mode !== 'active') return null;

	const range = isRecord(body.range) ? body.range : null;
	if (!range) return null;

	const from = nonEmptyString(range.from);
	const to = nonEmptyString(range.to);
	const kind = nonEmptyString(range.kind);
	const timezone = nonEmptyString(range.timezone);
	if (!from || !to || !kind || !windowKinds.has(kind as WindowKind) || !timezone) return null;

	const payload: BackfillSubmitPayload & {
		max_attempts?: number;
		retry_backoff_ms?: number;
		timeout_ms?: number;
	} = {
		target: { type: 'pipeline', id: targetId },
		manifest_selection: { mode: 'active' },
		range: { from, to, kind: kind as WindowKind, timezone }
	};

	const coverageBaselineId = nonEmptyString(body.coverage_baseline_id);
	if (coverageBaselineId) payload.coverage_baseline_id = coverageBaselineId;

	for (const key of ['max_attempts', 'retry_backoff_ms', 'timeout_ms'] as const) {
		const parsed = optionalInteger(body[key]);
		if (parsed === null) return null;
		if (parsed !== undefined) payload[key] = parsed;
	}

	return payload;
}
