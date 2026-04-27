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

function targetParts(value: unknown): { label: string; type: string } {
	if (!isRecord(value)) return { label: 'Unknown target', type: 'unknown' };
	const type = asString(value.type) ?? 'target';
	const id = asString(value.id) ?? asString(value.name) ?? 'unknown';
	return { label: id, type };
}

function firstString(record: JsonRecord, keys: string[]): string | null {
	for (const key of keys) {
		const value = asString(record[key]);
		if (value) return value;
	}
	return null;
}

function durationMs(record: JsonRecord): number | null {
	return asNumber(record.duration_ms) ?? asNumber(record.elapsed_ms) ?? asNumber(record.durationMs);
}

function formatDuration(record: JsonRecord): string {
	const direct = firstString(record, ['duration', 'duration_ms_label']);
	if (direct) return direct;
	const ms = durationMs(record);
	if (ms === null) return '—';
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

function normalizeAssetList(
	record: JsonRecord,
	fallbackTarget: string,
	fallbackStatus: RunStatus
): AssetExecutionView[] {
	const rawAssets = Array.isArray(record.assets)
		? record.assets
		: Array.isArray(record.asset_executions)
			? record.asset_executions
			: [];

	const assets = rawAssets.map((asset, index) => {
		const assetRecord = isRecord(asset) ? asset : {};
		const name =
			firstString(assetRecord, ['asset', 'asset_id', 'module', 'id', 'name']) ??
			`${fallbackTarget}#${index + 1}`;
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
			asset: name,
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

	if (assets.length > 0) return assets;

	return [
		{
			id: fallbackTarget,
			status: fallbackStatus,
			stage: 'Stage 1',
			stageNumber: 1,
			asset: fallbackTarget,
			module: null,
			type: fallbackTarget.startsWith('pipeline:') ? 'pipeline target' : 'asset',
			startedAt: shortTime(firstString(record, ['started_at', 'startedAt'])),
			finishedAt: shortTime(firstString(record, ['finished_at', 'finishedAt'])),
			durationMs: durationMs(record),
			duration: formatDuration(record),
			attempt: 1,
			output: fallbackStatus === 'succeeded' ? fallbackTarget.replace(/^asset:/, '') : null,
			outputs: [],
			error:
				fallbackStatus === 'failed'
					? 'Run failed before detailed asset errors were available.'
					: null,
			sql: null,
			operation: null,
			relation: fallbackStatus === 'succeeded' ? fallbackTarget.replace(/^asset:/, '') : null,
			connection: null,
			database: null
		}
	];
}

function normalizeOutputs(record: JsonRecord, assets: AssetExecutionView[]): OutputView[] {
	const rawOutputs = Array.isArray(record.outputs) ? record.outputs : [];
	const outputs = rawOutputs.map((output, index) =>
		normalizeOutputRecord(output, assets[0]?.asset ?? 'unknown', index)
	);

	if (outputs.length > 0) return outputs;

	return assets
		.filter((asset) => asset.output || asset.outputs.length > 0)
		.flatMap((asset) => {
			if (asset.outputs.length > 0) return asset.outputs;
			return [
				{
					relation: asset.output ?? asset.asset,
					type: 'table',
					asset: asset.asset,
					connection: asset.connection ?? 'local',
					rows: null,
					updatedAt: asset.startedAt,
					failed: asset.status === 'failed'
				}
			];
		});
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

	return [
		{
			id: 'submitted',
			timestamp: shortTime(firstString(record, ['created_at', 'submitted_at'])),
			label: 'run_submitted',
			detail: 'Run accepted by orchestrator',
			assetId: null
		},
		{
			id: 'status',
			timestamp: shortTime(firstString(record, ['updated_at', 'started_at'])),
			label: `run_${status}`,
			detail: 'Latest projected run state',
			assetId: null
		}
	];
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
		const target = targetParts(record.target);
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
			manifestVersionId: firstString(record, ['manifest_version_id', 'manifestVersionId'])
		};
	});
}

export function normalizeRunDetail(payload: unknown, requestedRunId: string): RunDetailView {
	const value = runDetailPayload(payload);
	const record = isRecord(value) ? value : {};
	const target = targetParts(record.target);
	const status = normalizeStatus(record.status);
	const id = firstString(record, ['id', 'run_id']) ?? requestedRunId;
	const assets = normalizeAssetList(record, target.label, status);
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
		raw: payload,
		error: errorMessage
			? { asset: firstFailedAsset?.asset ?? target.label, message: errorMessage }
			: null,
		assets,
		outputs,
		timeline: normalizeTimeline(record, status),
		metadata: [
			{ label: 'Run id', value: id },
			{
				label: 'Manifest',
				value: firstString(record, ['manifest_version_id', 'manifestVersionId']) ?? 'Not available'
			},
			{
				label: 'Content hash',
				value: firstString(record, ['content_hash', 'manifest_hash']) ?? 'Not available'
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
