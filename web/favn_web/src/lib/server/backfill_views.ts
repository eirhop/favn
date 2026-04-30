import type {
	AssetWindowStateView,
	BackfillPage,
	BackfillWindowView,
	CoverageBaselineView,
	PaginationView
} from '$lib/backfill_view_types';

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

function asNumber(value: unknown): number | null {
	if (typeof value === 'number' && Number.isFinite(value)) return value;
	if (typeof value === 'string' && value.trim() !== '') {
		const parsed = Number(value);
		return Number.isFinite(parsed) ? parsed : null;
	}
	return null;
}

function firstString(record: JsonRecord, keys: string[]): string | null {
	for (const key of keys) {
		const value = asString(record[key]);
		if (value) return value;
	}
	return null;
}

function listEnvelope(payload: unknown): { items: unknown[]; paginationSource: JsonRecord } {
	const data = dataPayload(payload);
	if (Array.isArray(data)) return { items: data, paginationSource: {} };
	if (!isRecord(data)) return { items: [], paginationSource: {} };

	const items = Array.isArray(data.items)
		? data.items
		: Array.isArray(data.windows)
			? data.windows
			: Array.isArray(data.baselines)
				? data.baselines
				: Array.isArray(data.states)
					? data.states
					: [];
	const pagination = isRecord(data.pagination) ? data.pagination : data;
	return { items, paginationSource: pagination };
}

export function normalizePagination(payload: unknown): PaginationView {
	const { items, paginationSource } = listEnvelope(payload);
	const limit = asNumber(paginationSource.limit);
	const offset = asNumber(paginationSource.offset) ?? 0;
	const total = asNumber(paginationSource.total) ?? asNumber(paginationSource.total_count);
	const hasNext =
		typeof paginationSource.has_next === 'boolean'
			? paginationSource.has_next
			: limit !== null && total !== null
				? offset + limit < total
				: false;
	const hasPrevious =
		typeof paginationSource.has_previous === 'boolean' ? paginationSource.has_previous : offset > 0;

	return {
		limit,
		offset,
		total: total ?? (items.length > 0 ? items.length : null),
		hasNext,
		hasPrevious
	};
}

function normalizeStatus(value: unknown): string {
	return asString(value)?.replace(/^:/, '').toLowerCase() ?? 'unknown';
}

export function normalizeBackfillWindows(payload: unknown): BackfillPage<BackfillWindowView> {
	const envelope = listEnvelope(payload);
	return {
		items: envelope.items.map((item, index) => {
			const record = isRecord(item) ? item : {};
			const status = normalizeStatus(record.status);
			const attemptCount = asNumber(record.attempt_count) ?? asNumber(record.attemptCount);
			return {
				backfillRunId: firstString(record, ['backfill_run_id', 'backfillRunId']) ?? 'unknown',
				pipelineModule: firstString(record, ['pipeline_module', 'pipelineModule']),
				manifestVersionId: firstString(record, ['manifest_version_id', 'manifestVersionId']),
				windowKind: firstString(record, ['window_kind', 'windowKind']),
				windowStartAt: firstString(record, ['window_start_at', 'windowStartAt']),
				windowEndAt: firstString(record, ['window_end_at', 'windowEndAt']),
				timezone: firstString(record, ['timezone']),
				windowKey: firstString(record, ['window_key', 'windowKey']) ?? `window-${index + 1}`,
				status,
				attemptCount,
				latestAttemptRunId: firstString(record, ['latest_attempt_run_id', 'latestAttemptRunId']),
				lastSuccessRunId: firstString(record, ['last_success_run_id', 'lastSuccessRunId']),
				updatedAt: firstString(record, ['updated_at', 'updatedAt']),
				childRunId: firstString(record, ['child_run_id', 'childRunId']),
				coverageBaselineId: firstString(record, ['coverage_baseline_id', 'coverageBaselineId']),
				lastError: firstString(record, ['last_error', 'lastError', 'error']),
				startedAt: firstString(record, ['started_at', 'startedAt']),
				finishedAt: firstString(record, ['finished_at', 'finishedAt']),
				createdAt: firstString(record, ['created_at', 'createdAt']),
				canRerun: ['failed', 'error'].includes(status) && (attemptCount ?? 0) > 0
			};
		}),
		pagination: normalizePagination(payload)
	};
}

export function normalizeCoverageBaselines(payload: unknown): BackfillPage<CoverageBaselineView> {
	const envelope = listEnvelope(payload);
	return {
		items: envelope.items.map((item, index) => {
			const record = isRecord(item) ? item : {};
			return {
				baselineId:
					firstString(record, ['baseline_id', 'baselineId', 'id']) ?? `baseline-${index + 1}`,
				pipelineModule: firstString(record, ['pipeline_module', 'pipelineModule']),
				sourceKey: firstString(record, ['source_key', 'sourceKey']),
				segmentKeyHash: firstString(record, ['segment_key_hash', 'segmentKeyHash']),
				windowKind: firstString(record, ['window_kind', 'windowKind']),
				timezone: firstString(record, ['timezone']),
				coverageUntil: firstString(record, ['coverage_until', 'coverageUntil']),
				createdByRunId: firstString(record, ['created_by_run_id', 'createdByRunId']),
				manifestVersionId: firstString(record, ['manifest_version_id', 'manifestVersionId']),
				status: firstString(record, ['status']),
				createdAt: firstString(record, ['created_at', 'createdAt']),
				updatedAt: firstString(record, ['updated_at', 'updatedAt'])
			};
		}),
		pagination: normalizePagination(payload)
	};
}

export function normalizeAssetWindowStates(payload: unknown): BackfillPage<AssetWindowStateView> {
	const envelope = listEnvelope(payload);
	return {
		items: envelope.items.map((item, index) => {
			const record = isRecord(item) ? item : {};
			return {
				assetRefModule: firstString(record, ['asset_ref_module', 'assetRefModule']),
				assetRefName: firstString(record, ['asset_ref_name', 'assetRefName']),
				pipelineModule: firstString(record, ['pipeline_module', 'pipelineModule']),
				manifestVersionId: firstString(record, ['manifest_version_id', 'manifestVersionId']),
				windowKind: firstString(record, ['window_kind', 'windowKind']),
				windowStartAt: firstString(record, ['window_start_at', 'windowStartAt']),
				windowEndAt: firstString(record, ['window_end_at', 'windowEndAt']),
				timezone: firstString(record, ['timezone']),
				windowKey: firstString(record, ['window_key', 'windowKey']) ?? `window-${index + 1}`,
				status: firstString(record, ['status']),
				latestRunId: firstString(record, ['latest_run_id', 'latestRunId']),
				updatedAt: firstString(record, ['updated_at', 'updatedAt'])
			};
		}),
		pagination: normalizePagination(payload)
	};
}
