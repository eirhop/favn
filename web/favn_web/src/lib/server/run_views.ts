import type {
	AssetExecutionView,
	OutputView,
	RunDetailView,
	RunStatus,
	RunSummaryView,
	TimelineEventView
} from '$lib/run_view_types';

type JsonRecord = Record<string, unknown>;

function isRecord(value: unknown): value is JsonRecord {
	return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function dataPayload(payload: unknown): unknown {
	return isRecord(payload) && 'data' in payload ? payload.data : payload;
}

function runDetailPayload(payload: unknown): unknown {
	const value = dataPayload(payload);
	if (isRecord(value) && isRecord(value.run)) return value.run;
	return value;
}

function asString(value: unknown): string | null {
	return typeof value === 'string' && value.length > 0 ? value : null;
}

function asNumber(value: unknown): number | null {
	return typeof value === 'number' && Number.isFinite(value) ? value : null;
}

function asStringArray(value: unknown): string[] {
	return Array.isArray(value)
		? value.filter((item): item is string => typeof item === 'string')
		: [];
}

function asIntegerish(value: unknown): number | null {
	if (typeof value === 'number' && Number.isFinite(value)) return value;
	if (typeof value === 'string' && value.trim() !== '') {
		const parsed = Number(value);
		return Number.isFinite(parsed) ? parsed : null;
	}
	return null;
}

function normalizeStatus(value: unknown): RunStatus {
	const status = asString(value)?.toLowerCase();
	switch (status) {
		case 'ok':
		case 'succeeded':
		case 'success':
			return 'succeeded';
		case 'error':
		case 'failed':
		case 'timed_out':
		case 'timeout':
			return 'failed';
		case 'retrying':
		case 'running':
			return 'running';
		case 'cancelled':
		case 'canceled':
			return 'cancelled';
		case 'pending':
		case 'queued':
			return status;
		default:
			return 'unknown';
	}
}

function readableRef(value: string): string {
	const trimmed = value.trim();
	if (!trimmed) return trimmed;
	const withoutPrefix = trimmed.replace(/^(asset|pipeline|target|module):/i, '');
	const [module, suffix] = withoutPrefix.split(':');
	return suffix ? `${module} (${suffix})` : withoutPrefix;
}

function targetTypeFromRef(value: string | null): string | null {
	if (!value) return null;
	const suffix = value.split(':').at(-1)?.toLowerCase();
	if (suffix && suffix !== value.toLowerCase()) return suffix;
	if (/pipeline/i.test(value)) return 'pipeline';
	if (/asset/i.test(value)) return 'asset';
	return null;
}

function targetParts(record: JsonRecord): { label: string; type: string } {
	const target = isRecord(record.target) ? record.target : null;
	const targetId = target ? (asString(target.id) ?? asString(target.name)) : null;
	const submitRef = asString(record.submit_ref);
	const targetRefs = asStringArray(record.target_refs);
	const assetRef = asString(record.asset_ref);
	const pipeline = isRecord(record.pipeline) ? record.pipeline : null;
	const pipelineRef = pipeline ? (asString(pipeline.module) ?? asString(pipeline.name)) : null;
	const submitKind = asString(record.submit_kind);
	const rawLabel = targetId ?? submitRef ?? targetRefs[0] ?? assetRef ?? pipelineRef;
	const label = rawLabel ? readableRef(rawLabel) : 'Unknown target';
	const fallbackLabel = submitKind
		? `${submitKind[0].toUpperCase()}${submitKind.slice(1)} run`
		: label;
	const type =
		(target ? asString(target.type) : null) ??
		submitKind ??
		targetTypeFromRef(targetRefs[0] ?? null) ??
		targetTypeFromRef(assetRef) ??
		targetTypeFromRef(submitRef) ??
		(pipelineRef ? 'pipeline' : 'unknown');
	return { label: rawLabel ? label : fallbackLabel, type };
}

function firstString(record: JsonRecord, keys: string[]): string | null {
	for (const key of keys) {
		const value = asString(record[key]);
		if (value) return value;
	}
	return null;
}

function durationMs(record: JsonRecord): number | null {
	const direct =
		asNumber(record.duration_ms) ?? asNumber(record.elapsed_ms) ?? asNumber(record.durationMs);
	if (direct !== null) return direct;
	const startedAt = firstString(record, ['started_at', 'startedAt']);
	const finishedAt = firstString(record, ['finished_at', 'finishedAt']);
	if (!startedAt || !finishedAt) return null;
	const started = new Date(startedAt).getTime();
	const finished = new Date(finishedAt).getTime();
	return Number.isFinite(started) && Number.isFinite(finished) && finished >= started
		? finished - started
		: null;
}

function formatDuration(record: JsonRecord): string {
	const direct = firstString(record, ['duration', 'duration_ms_label']);
	if (direct) return direct;
	const ms = durationMs(record);
	if (ms === null) {
		const status = normalizeStatus(record.status);
		const startedAt = firstString(record, ['started_at', 'startedAt']);
		const started = shortTime(startedAt);
		if ((status === 'running' || status === 'pending' || status === 'queued') && started) {
			return `running since ${started}`;
		}
		return '—';
	}
	if (ms < 1000) return `${ms}ms`;
	const seconds = ms / 1000;
	if (seconds < 60) return `${seconds.toFixed(seconds < 10 ? 1 : 0)}s`;
	return `${Math.floor(seconds / 60)}m ${Math.round(seconds % 60)}s`;
}

function shortTime(value: string | null): string | null {
	if (!value) return null;
	const date = new Date(value);
	if (Number.isNaN(date.getTime())) return value;
	return date.toLocaleTimeString('en-GB', { hour12: false });
}

function normalizeOutputRecord(output: unknown, fallbackAsset: string, index: number): OutputView {
	const outputRecord = isRecord(output) ? output : {};
	const relation = firstString(outputRecord, ['relation', 'name', 'id']) ?? `output_${index + 1}`;
	const failed = normalizeStatus(outputRecord.status) === 'failed' || Boolean(outputRecord.error);
	return {
		relation,
		type: firstString(outputRecord, ['type', 'materialization']) ?? 'table',
		asset: firstString(outputRecord, ['asset', 'asset_id']) ?? fallbackAsset,
		connection: firstString(outputRecord, ['connection', 'connection_name']) ?? 'local',
		rows: asNumber(outputRecord.rows) ?? asNumber(outputRecord.row_count),
		updatedAt: shortTime(firstString(outputRecord, ['updated_at', 'created_at', 'createdAt'])),
		failed
	};
}

function listFromPayload(payload: unknown): unknown[] {
	const value = dataPayload(payload);
	if (Array.isArray(value)) return value;
	if (isRecord(value) && Array.isArray(value.items)) return value.items;
	return [];
}

function normalizeAssetList(record: JsonRecord, fallbackStatus: RunStatus): AssetExecutionView[] {
	const rawAssets = Array.isArray(record.assets)
		? record.assets
		: Array.isArray(record.asset_executions)
			? record.asset_executions
			: Array.isArray(record.asset_results)
				? record.asset_results
				: Array.isArray(record.node_results)
					? record.node_results
					: [];

	const assets = rawAssets.map((asset, index) => {
		const assetRecord = isRecord(asset) ? asset : {};
		const name =
			firstString(assetRecord, [
				'asset',
				'asset_id',
				'asset_ref',
				'target_ref',
				'node_ref',
				'module',
				'id',
				'name'
			]) ?? `asset_${index + 1}`;
		const rawAssetOutputs = Array.isArray(assetRecord.outputs) ? assetRecord.outputs : [];
		const outputs = rawAssetOutputs.map((output, outputIndex) =>
			normalizeOutputRecord(output, name, outputIndex)
		);
		const relation =
			firstString(assetRecord, ['output', 'relation']) ?? outputs[0]?.relation ?? null;
		const stageNumber = asIntegerish(assetRecord.stage) ?? asIntegerish(assetRecord.stage_number);
		return {
			id: firstString(assetRecord, ['id', 'asset_id']) ?? name,
			status: normalizeStatus(assetRecord.status ?? fallbackStatus),
			stage: firstString(assetRecord, ['stage_name']) ?? `Stage ${stageNumber ?? index + 1}`,
			stageNumber,
			asset: readableRef(name),
			module: firstString(assetRecord, ['module']),
			type: firstString(assetRecord, ['type', 'asset_type']) ?? 'unknown',
			startedAt: shortTime(firstString(assetRecord, ['started_at', 'startedAt'])),
			finishedAt: shortTime(firstString(assetRecord, ['finished_at', 'finishedAt'])),
			durationMs: durationMs(assetRecord),
			duration: formatDuration(assetRecord),
			attempt: asNumber(assetRecord.attempt) ?? 1,
			output: relation,
			outputs,
			error: firstString(assetRecord, ['error', 'error_message']),
			sql: firstString(assetRecord, ['sql', 'query']),
			operation: firstString(assetRecord, ['operation']),
			relation,
			connection:
				firstString(assetRecord, ['connection', 'connection_name']) ??
				outputs[0]?.connection ??
				null,
			database: firstString(assetRecord, ['database', 'database_path'])
		};
	});

	return assets;
}

function normalizeOutputs(record: JsonRecord, assets: AssetExecutionView[]): OutputView[] {
	const rawOutputs = Array.isArray(record.outputs)
		? record.outputs
		: Array.isArray(record.materializations)
			? record.materializations
			: Array.isArray(record.output_metadata)
				? record.output_metadata
				: [];
	const outputs = rawOutputs.map((output, index) =>
		normalizeOutputRecord(output, assets[0]?.asset ?? 'unknown', index)
	);

	if (outputs.length > 0) return outputs;

	return assets.flatMap((asset) => asset.outputs);
}

function normalizeTimeline(record: JsonRecord, status: RunStatus): TimelineEventView[] {
	const rawEvents = Array.isArray(record.events) ? record.events : [];
	const events = rawEvents.map((event, index) => {
		const eventRecord = isRecord(event) ? event : {};
		return {
			id: firstString(eventRecord, ['id', 'event_id']) ?? `event_${index + 1}`,
			timestamp: shortTime(firstString(eventRecord, ['timestamp', 'occurred_at', 'created_at'])),
			label: firstString(eventRecord, ['type', 'event', 'label']) ?? `Event ${index + 1}`,
			detail: firstString(eventRecord, ['message', 'detail', 'asset_id']) ?? status,
			assetId: firstString(eventRecord, ['asset_id', 'assetId'])
		};
	});

	if (events.length > 0) return events;

	const fallbacks: TimelineEventView[] = [
		{
			id: 'submitted',
			timestamp: shortTime(firstString(record, ['created_at', 'submitted_at', 'started_at'])),
			label: 'run_submitted',
			detail: firstString(record, ['submit_ref']) ?? 'Run accepted by orchestrator',
			assetId: null
		},
		{
			id: 'status',
			timestamp: shortTime(firstString(record, ['updated_at', 'finished_at', 'started_at'])),
			label: `run_${status}`,
			detail:
				firstString(record, ['terminal_reason', 'error', 'error_message']) ??
				`Latest projected run state${asIntegerish(record.event_seq) !== null ? ` · event #${asIntegerish(record.event_seq)}` : ''}`,
			assetId: null
		}
	];
	return fallbacks;
}

function shortHash(value: string | null): string | null {
	if (!value) return null;
	const [prefix, hash] = value.includes(':') ? value.split(':', 2) : ['', value];
	const short = hash.length > 12 ? hash.slice(0, 12) : hash;
	return prefix ? `${prefix}:${short}` : short;
}

function assetCountLabel(record: JsonRecord, assetsCompleted: number, assetsTotal: number): string {
	const direct = firstString(record, ['asset_count_label']);
	if (direct) return direct;
	if (assetsTotal > 0) return `${assetsCompleted}/${assetsTotal}`;
	const count = asNumber(record.asset_count);
	return count === null ? '—' : String(count);
}

export function normalizeRunSummaries(payload: unknown): RunSummaryView[] {
	return listFromPayload(payload).map((run, index) => {
		const record = isRecord(run) ? run : {};
		const target = targetParts(record);
		const assetsTotal =
			asNumber(record.assets_total) ??
			asNumber(record.asset_total) ??
			asNumber(record.asset_count) ??
			(Array.isArray(record.assets) ? record.assets.length : 1);
		const assetsCompleted =
			asNumber(record.assets_completed) ??
			asNumber(record.completed_asset_count) ??
			(normalizeStatus(record.status) === 'succeeded' ? assetsTotal : 0);
		return {
			id: firstString(record, ['id', 'run_id']) ?? `run-${index + 1}`,
			status: normalizeStatus(record.status),
			target: target.label,
			targetType: target.type,
			trigger: firstString(record, ['trigger', 'triggered_by']) ?? 'manual',
			startedAt: shortTime(firstString(record, ['started_at', 'startedAt', 'created_at'])),
			finishedAt: shortTime(firstString(record, ['finished_at', 'finishedAt'])),
			durationMs: durationMs(record),
			duration: formatDuration(record),
			assetCount: assetCountLabel(record, assetsCompleted, assetsTotal),
			assetsCompleted,
			assetsTotal,
			manifestVersionId: firstString(record, ['manifest_version_id', 'manifestVersionId']) ?? null,
			manifestContentHash: shortHash(
				firstString(record, ['manifest_content_hash', 'content_hash', 'manifest_hash'])
			),
			submitKind: firstString(record, ['submit_kind'])
		};
	});
}

export function normalizeRunDetail(payload: unknown, requestedRunId: string): RunDetailView {
	const value = runDetailPayload(payload);
	const record = isRecord(value) ? value : {};
	const target = targetParts(record);
	const status = normalizeStatus(record.status);
	const id = firstString(record, ['id', 'run_id']) ?? requestedRunId;
	const assets = normalizeAssetList(record, status);
	const outputs = normalizeOutputs(record, assets);
	const firstFailedAsset = assets.find((asset) => asset.status === 'failed' || asset.error);
	const errorMessage =
		firstString(record, ['error', 'error_message']) ?? firstFailedAsset?.error ?? null;
	const assetCounts = {
		succeeded: assets.filter((asset) => asset.status === 'succeeded').length,
		failed: assets.filter((asset) => asset.status === 'failed').length,
		skipped: assets.filter((asset) => asset.status === 'cancelled' || asset.status === 'unknown')
			.length,
		running: assets.filter((asset) => asset.status === 'running').length,
		pending: assets.filter((asset) => asset.status === 'pending' || asset.status === 'queued')
			.length
	};
	const assetsTotal =
		asNumber(record.assets_total) ?? asNumber(record.asset_total) ?? assets.length;
	const assetsCompleted =
		asNumber(record.assets_completed) ?? assetCounts.succeeded + assetCounts.failed;

	return {
		id,
		status,
		target: target.label,
		targetType: target.type,
		trigger: firstString(record, ['trigger', 'triggered_by']) ?? 'manual',
		startedAt: shortTime(firstString(record, ['started_at', 'startedAt', 'created_at'])),
		finishedAt: shortTime(firstString(record, ['finished_at', 'finishedAt'])),
		durationMs: durationMs(record),
		duration: formatDuration(record),
		assetCount: assetCountLabel(record, assetsCompleted, assetsTotal),
		assetsCompleted,
		assetsTotal,
		manifestVersionId: firstString(record, ['manifest_version_id', 'manifestVersionId']),
		manifestContentHash: shortHash(
			firstString(record, ['manifest_content_hash', 'content_hash', 'manifest_hash'])
		),
		submitKind: firstString(record, ['submit_kind']),
		raw: payload,
		error: errorMessage
			? { asset: firstFailedAsset?.asset ?? target.label, message: errorMessage }
			: null,
		assets,
		outputs,
		timeline: normalizeTimeline(record, status),
		metadata: [
			{ label: 'Run id', value: id },
			{ label: 'Submit kind', value: firstString(record, ['submit_kind']) ?? 'Not available' },
			{
				label: 'Manifest',
				value: firstString(record, ['manifest_version_id', 'manifestVersionId']) ?? 'Not available'
			},
			{
				label: 'Content hash',
				value:
					shortHash(
						firstString(record, ['manifest_content_hash', 'content_hash', 'manifest_hash'])
					) ?? 'Not available'
			},
			{ label: 'Schema version', value: String(asNumber(record.schema_version) ?? 1) },
			{ label: 'Runner contract', value: String(asNumber(record.runner_contract_version) ?? 1) },
			{ label: 'Target type', value: target.type },
			{ label: 'Storage', value: firstString(record, ['storage', 'storage_adapter']) ?? 'local' }
		],
		progressPercent:
			status === 'running' ? Math.round((assetsCompleted / Math.max(assetsTotal, 1)) * 100) : null,
		assetCounts,
		failedAssetId: firstFailedAsset?.id ?? null
	};
}
