import type { PipelineTargetView, WindowKind } from '$lib/pipeline_run_submission';

export type BackfillSubmitPayload = {
	target: { type: 'pipeline'; id: string };
	manifest_selection: { mode: 'active' };
	range: {
		from: string;
		to: string;
		kind: WindowKind;
		timezone: string;
	};
	coverage_baseline_id?: string;
};

export type BackfillSubmitPayloadResult =
	| { ok: true; payload: BackfillSubmitPayload }
	| { ok: false; error: string };

export function buildBackfillSubmitPayload(input: {
	pipeline: PipelineTargetView | null;
	from: string;
	to: string;
	kind: WindowKind;
	timezone: string;
	coverageBaselineId?: string | null;
}): BackfillSubmitPayloadResult {
	if (!input.pipeline) return { ok: false, error: 'Choose a pipeline to backfill.' };

	const from = input.from.trim();
	if (!from) return { ok: false, error: 'Enter the range start.' };

	const to = input.to.trim();
	if (!to) return { ok: false, error: 'Enter the range end.' };

	const timezone = input.timezone.trim() || 'Etc/UTC';
	const coverageBaselineId = input.coverageBaselineId?.trim();

	return {
		ok: true,
		payload: {
			target: { type: 'pipeline', id: input.pipeline.targetId },
			manifest_selection: { mode: 'active' },
			range: { from, to, kind: input.kind, timezone },
			...(coverageBaselineId ? { coverage_baseline_id: coverageBaselineId } : {})
		}
	};
}

type JsonRecord = Record<string, unknown>;

function isRecord(value: unknown): value is JsonRecord {
	return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function dataPayload(payload: unknown): unknown {
	return isRecord(payload) && 'data' in payload ? payload.data : payload;
}

function asString(value: unknown): string | null {
	return typeof value === 'string' && value.length > 0 ? value : null;
}

export function extractSubmittedBackfill(value: unknown): {
	id: string | null;
	status: string | null;
} {
	const data = dataPayload(value);
	const record = isRecord(data) && isRecord(data.run) ? data.run : data;
	if (!isRecord(record)) return { id: null, status: null };

	return {
		id: asString(record.id) ?? asString(record.run_id) ?? asString(record.backfill_run_id),
		status: asString(record.status) ?? asString(record.state)
	};
}
