export type WindowKind = 'hour' | 'day' | 'month' | 'year';

export type PipelineWindowPolicyView = {
	kind: WindowKind;
	anchor: string | null;
	timezone: string | null;
	allowFullLoad: boolean;
};

export type PipelineTargetView = {
	targetId: string;
	label: string;
	module: string | null;
	windowPolicy: PipelineWindowPolicyView | null;
};

export type PipelineRunPayload = {
	target: { type: 'pipeline'; id: string };
	window?: {
		mode: 'single';
		kind: WindowKind;
		value: string;
		timezone?: string;
	};
};

export type PipelineRunPayloadResult =
	| { ok: true; payload: PipelineRunPayload }
	| { ok: false; error: string };

type JsonRecord = Record<string, unknown>;

const kindAliases: Record<string, WindowKind> = {
	hour: 'hour',
	hourly: 'hour',
	day: 'day',
	daily: 'day',
	month: 'month',
	monthly: 'month',
	year: 'year',
	yearly: 'year'
};

function isRecord(value: unknown): value is JsonRecord {
	return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function dataPayload(payload: unknown): unknown {
	return isRecord(payload) && 'data' in payload ? payload.data : payload;
}

function asNonEmptyString(value: unknown): string | null {
	return typeof value === 'string' && value.trim().length > 0 ? value.trim() : null;
}

function firstString(record: JsonRecord, keys: string[]): string | null {
	for (const key of keys) {
		const value = asNonEmptyString(record[key]);
		if (value) return value;
	}
	return null;
}

function firstBoolean(record: JsonRecord, keys: string[]): boolean | null {
	for (const key of keys) {
		if (typeof record[key] === 'boolean') return record[key];
	}
	return null;
}

function normalizeWindowKind(value: unknown): WindowKind | null {
	const raw = asNonEmptyString(value)?.replace(/^:/, '').toLowerCase();
	return raw ? (kindAliases[raw] ?? null) : null;
}

export function normalizePipelineWindowPolicy(value: unknown): PipelineWindowPolicyView | null {
	if (!isRecord(value)) return null;

	const kind = normalizeWindowKind(value.kind ?? value.window_kind ?? value.windowKind);
	if (!kind) return null;

	return {
		kind,
		anchor: firstString(value, ['anchor', 'mode', 'strategy']),
		timezone: firstString(value, ['timezone', 'time_zone', 'timeZone']),
		allowFullLoad:
			firstBoolean(value, ['allow_full_load', 'allowFullLoad', 'full_load_allowed']) ?? false
	};
}

function activeManifestData(payload: unknown): JsonRecord {
	const data = dataPayload(payload);
	return isRecord(data) ? data : {};
}

function manifestSummary(payload: unknown): JsonRecord {
	const manifest = activeManifestData(payload).manifest;
	return isRecord(manifest) ? manifest : {};
}

function activeTargets(payload: unknown): JsonRecord {
	const targets = activeManifestData(payload).targets;
	return isRecord(targets) ? targets : {};
}

function manifestPipelineLookup(payload: unknown): Map<string, JsonRecord> {
	const lookup = new Map<string, JsonRecord>();
	const pipelines = manifestSummary(payload).pipelines;
	if (!Array.isArray(pipelines)) return lookup;

	for (const pipeline of pipelines) {
		if (!isRecord(pipeline)) continue;
		for (const key of pipelineLookupKeys(pipeline)) lookup.set(key, pipeline);
	}

	return lookup;
}

function pipelineLookupKeys(record: JsonRecord): string[] {
	return [
		firstString(record, ['target_id', 'targetId', 'id']),
		firstString(record, ['label', 'name', 'module', 'pipeline_module'])
	].filter((value): value is string => Boolean(value));
}

function pipelineRecords(payload: unknown): JsonRecord[] {
	const data = activeManifestData(payload);
	const lookup = manifestPipelineLookup(payload);

	if (Array.isArray(data.targets)) {
		return data.targets
			.filter((target): target is JsonRecord => isRecord(target) && target.type === 'pipeline')
			.map((target) => ({ ...(lookup.get(pipelineLookupKeys(target)[0] ?? '') ?? {}), ...target }));
	}

	const targets = activeTargets(payload);
	if (Array.isArray(targets.pipelines)) {
		return targets.pipelines
			.filter(isRecord)
			.map((target) => ({ ...(lookup.get(pipelineLookupKeys(target)[0] ?? '') ?? {}), ...target }));
	}

	return Array.from(lookup.values());
}

function normalizePipelineTarget(record: JsonRecord, index: number): PipelineTargetView {
	const targetId =
		firstString(record, ['target_id', 'targetId', 'id']) ??
		`pipeline:${firstString(record, ['label', 'name', 'module', 'pipeline_module']) ?? index + 1}`;
	const label =
		firstString(record, ['label', 'name', 'module', 'pipeline_module']) ??
		targetId.replace(/^pipeline:/, '');
	const policySource = record.window ?? record.window_policy ?? record.windowPolicy;

	return {
		targetId,
		label,
		module: firstString(record, ['module', 'pipeline_module', 'pipelineModule']),
		windowPolicy: normalizePipelineWindowPolicy(policySource)
	};
}

export function normalizePipelineTargets(activeManifestPayload: unknown): PipelineTargetView[] {
	return pipelineRecords(activeManifestPayload).map(normalizePipelineTarget);
}

export function buildPipelineRunPayload(input: {
	pipeline: PipelineTargetView | null;
	windowValue: string;
	timezone: string;
	fullLoad: boolean;
}): PipelineRunPayloadResult {
	if (!input.pipeline) return { ok: false, error: 'Choose a pipeline to run.' };

	const payload: PipelineRunPayload = { target: { type: 'pipeline', id: input.pipeline.targetId } };
	const policy = input.pipeline.windowPolicy;
	if (!policy) return { ok: true, payload };

	if (input.fullLoad) {
		return policy.allowFullLoad
			? { ok: true, payload }
			: { ok: false, error: 'This pipeline requires a window; full load is not allowed.' };
	}

	const value = input.windowValue.trim();
	if (!value) return { ok: false, error: `Enter a ${policy.kind} window value.` };

	const timezone = input.timezone.trim();
	return {
		ok: true,
		payload: {
			...payload,
			window: {
				mode: 'single',
				kind: policy.kind,
				value,
				...(timezone ? { timezone } : {})
			}
		}
	};
}

export function extractSubmittedRun(value: unknown): { id: string | null; status: string | null } {
	const data = dataPayload(value);
	const record = isRecord(data) && isRecord(data.run) ? data.run : data;
	if (!isRecord(record)) return { id: null, status: null };

	return {
		id: firstString(record, ['id', 'run_id', 'runId']),
		status: firstString(record, ['status', 'state'])
	};
}
