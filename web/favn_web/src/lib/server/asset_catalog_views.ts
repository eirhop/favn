import type {
	AssetCatalogDetailView,
	AssetCatalogFilters,
	AssetCatalogItem,
	AssetCatalogListView,
	AssetCatalogRunAction,
	AssetCatalogRunSummary,
	AssetHealth
} from '$lib/asset_catalog_types';
import type { RunStatus } from '$lib/run_view_types';

type JsonRecord = Record<string, unknown>;

function isRecord(value: unknown): value is JsonRecord {
	return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function dataPayload(payload: unknown): unknown {
	return isRecord(payload) && 'data' in payload ? payload.data : payload;
}

function asString(value: unknown): string | null {
	return typeof value === 'string' && value.trim().length > 0 ? value.trim() : null;
}

function asNumber(value: unknown): number | null {
	return typeof value === 'number' && Number.isFinite(value) ? value : null;
}

function firstString(record: JsonRecord, keys: string[]): string | null {
	for (const key of keys) {
		const value = asString(record[key]);
		if (value) return value;
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

function healthFromRun(run: AssetCatalogRunSummary | null): AssetHealth {
	if (!run) return 'not_run';
	switch (run.status) {
		case 'succeeded':
			return 'healthy';
		case 'failed':
		case 'cancelled':
			return 'failed';
		case 'running':
		case 'pending':
		case 'queued':
			return 'running';
		default:
			return 'unknown';
	}
}

function runListFromPayload(payload: unknown): unknown[] {
	const value = dataPayload(payload);
	if (Array.isArray(value)) return value;
	if (isRecord(value) && Array.isArray(value.items)) return value.items;
	if (isRecord(value) && Array.isArray(value.runs)) return value.runs;
	return [];
}

function activeManifestData(payload: unknown): JsonRecord {
	const data = dataPayload(payload);
	return isRecord(data) ? data : {};
}

function activeManifestSummary(payload: unknown): unknown {
	const data = activeManifestData(payload);
	return isRecord(data.manifest) ? data.manifest : data.manifest;
}

function activeManifestTargets(payload: unknown): JsonRecord {
	const data = activeManifestData(payload);
	return isRecord(data.targets) ? data.targets : {};
}

function manifestVersionId(payload: unknown): string | null {
	const manifest = activeManifestSummary(payload);
	if (isRecord(manifest)) {
		return firstString(manifest, ['manifest_version_id', 'manifestVersionId', 'version_id', 'id']);
	}
	const targets = activeManifestTargets(payload);
	return firstString(targets, ['manifest_version_id', 'manifestVersionId']);
}

function manifestContentHash(payload: unknown): string | null {
	const manifest = activeManifestSummary(payload);
	if (!isRecord(manifest)) return null;
	return firstString(manifest, ['content_hash', 'manifest_content_hash', 'manifest_hash', 'hash']);
}

export function normalizeAssetRefParts(value: unknown): {
	ref: string | null;
	targetId: string | null;
	module: string | null;
	name: string | null;
} {
	const raw = asString(value);
	if (!raw) return { ref: null, targetId: null, module: null, name: null };

	const tuple = raw.match(/^\{\s*([^,{}]+)\s*,\s*:([^,{}]+)\s*\}$/);
	if (tuple) {
		const module = tuple[1].trim();
		const refName = tuple[2].trim();
		const name = refName === 'asset' ? (module.split('.').at(-1) ?? refName) : refName;
		return { ref: `${module}:${refName}`, targetId: `asset:${module}:${refName}`, module, name };
	}

	const withoutPrefix = raw.startsWith('asset:') ? raw.slice('asset:'.length) : raw;
	const separator = withoutPrefix.lastIndexOf(':');
	if (separator > 0 && separator < withoutPrefix.length - 1) {
		const module = withoutPrefix.slice(0, separator);
		const refName = withoutPrefix.slice(separator + 1).replace(/^:/, '');
		const name = refName === 'asset' ? (module.split('.').at(-1) ?? refName) : refName;
		return { ref: `${module}:${refName}`, targetId: `asset:${module}:${refName}`, module, name };
	}

	return {
		ref: withoutPrefix,
		targetId: raw.startsWith('asset:') ? raw : `asset:${withoutPrefix}`,
		module: withoutPrefix,
		name: withoutPrefix.split('.').at(-1) ?? withoutPrefix
	};
}

function normalizeTargetRecord(target: unknown, index: number): AssetCatalogItem {
	const record = isRecord(target) ? target : {};
	const directTargetId = firstString(record, ['target_id', 'targetId', 'id']);
	const label = firstString(record, ['label', 'name']) ?? directTargetId ?? `asset:${index + 1}`;
	const parts = normalizeAssetRefParts(directTargetId ?? label);
	const targetId = parts.targetId ?? directTargetId ?? `asset:${label}`;
	const ref = parts.ref ?? targetId.replace(/^asset:/, '');
	const module = firstString(record, ['module', 'asset_module']) ?? parts.module ?? ref;
	const name =
		firstString(record, ['asset_name', 'asset', 'name']) ??
		parts.name ??
		module.split('.').at(-1) ??
		ref;
	const kind = firstString(record, ['kind', 'type', 'asset_type']) ?? 'asset';
	const moduleParts = module
		.replace(/^Elixir\./, '')
		.split('.')
		.filter(Boolean);
	const domain = firstString(record, ['domain', 'category']) ?? moduleParts.at(-2) ?? null;

	return {
		ref,
		targetId,
		name,
		module,
		kind,
		domain,
		label,
		health: 'not_run',
		lastRun: null,
		upstreamCount: asNumber(record.upstream_count) ?? 0,
		downstreamCount: asNumber(record.downstream_count) ?? 0,
		manifestVersionId: null,
		manifestContentHash: null,
		runActions: [],
		rawTarget: target
	};
}

function assetTargets(activeManifestPayload: unknown): unknown[] {
	const data = activeManifestData(activeManifestPayload);
	if (Array.isArray(data.targets)) {
		return data.targets
			.filter((target) => isRecord(target) && target.type === 'asset')
			.map((target) => ({
				target_id: firstString(target, ['target_id', 'targetId', 'id']),
				label: firstString(target, ['label', 'name', 'id']),
				type: firstString(target, ['kind', 'asset_type']) ?? 'asset'
			}));
	}

	const targets = activeManifestTargets(activeManifestPayload);
	return Array.isArray(targets.assets) ? targets.assets : [];
}

function runTargetRecord(run: JsonRecord): JsonRecord | null {
	return isRecord(run.target) ? run.target : null;
}

function runTargetLabel(run: JsonRecord): string | null {
	const target = runTargetRecord(run);
	if (target) return firstString(target, ['id', 'target_id', 'label', 'name']);
	return firstString(run, ['target_id', 'targetId', 'target', 'asset_ref', 'assetRef']);
}

function runTargetType(run: JsonRecord): string | null {
	const target = runTargetRecord(run);
	if (target) return firstString(target, ['type']);
	return firstString(run, ['target_type', 'targetType', 'submit_kind']);
}

function targetAliases(
	asset: Pick<AssetCatalogItem, 'ref' | 'targetId' | 'label' | 'module' | 'name'>
): Set<string> {
	const aliases = new Set<string>();
	for (const value of [asset.ref, asset.targetId, asset.label, asset.module, asset.name]) {
		if (value) aliases.add(value);
		const normalized = normalizeAssetRefParts(value);
		if (normalized.ref) aliases.add(normalized.ref);
		if (normalized.targetId) aliases.add(normalized.targetId);
	}
	return aliases;
}

function runRefs(run: JsonRecord): Set<string> {
	const refs = new Set<string>();
	const add = (value: unknown) => {
		const text = asString(value);
		if (!text) return;
		refs.add(text);
		const normalized = normalizeAssetRefParts(text);
		if (normalized.ref) refs.add(normalized.ref);
		if (normalized.targetId) refs.add(normalized.targetId);
	};

	add(runTargetLabel(run));
	add(firstString(run, ['asset_ref', 'assetRef', 'target_id', 'targetId']));

	if (Array.isArray(run.target_refs)) run.target_refs.forEach(add);
	if (Array.isArray(run.targetRefs)) run.targetRefs.forEach(add);
	if (Array.isArray(run.asset_refs)) run.asset_refs.forEach(add);
	if (Array.isArray(run.assets)) {
		run.assets.forEach((asset) => {
			if (isRecord(asset))
				add(firstString(asset, ['asset_ref', 'assetRef', 'id', 'asset_id', 'module']));
		});
	}

	return refs;
}

export function runMatchesAsset(
	run: unknown,
	asset: Pick<AssetCatalogItem, 'ref' | 'targetId' | 'label' | 'module' | 'name'>
): boolean {
	if (!isRecord(run)) return false;
	const aliases = targetAliases(asset);
	for (const ref of runRefs(run)) {
		if (aliases.has(ref)) return true;
	}
	return false;
}

function normalizeRun(run: unknown): AssetCatalogRunSummary {
	const record = isRecord(run) ? run : {};
	return {
		id: firstString(record, ['id', 'run_id']) ?? 'unknown',
		status: normalizeStatus(record.status),
		target: runTargetLabel(record),
		targetType: runTargetType(record),
		startedAt: firstString(record, ['started_at', 'startedAt', 'created_at', 'submitted_at']),
		finishedAt: firstString(record, ['finished_at', 'finishedAt']),
		manifestVersionId: firstString(record, ['manifest_version_id', 'manifestVersionId']),
		raw: run
	};
}

function runTimestamp(run: AssetCatalogRunSummary): number {
	const value = run.finishedAt ?? run.startedAt;
	if (!value) return 0;
	const parsed = new Date(value).getTime();
	return Number.isFinite(parsed) ? parsed : 0;
}

function recentRunsForAsset(
	runsPayload: unknown,
	asset: AssetCatalogItem
): AssetCatalogRunSummary[] {
	return runListFromPayload(runsPayload)
		.filter((run) => runMatchesAsset(run, asset))
		.map(normalizeRun)
		.sort((a, b) => runTimestamp(b) - runTimestamp(a));
}

function runActions(asset: AssetCatalogItem): AssetCatalogRunAction[] {
	const targetId = asset.targetId ?? `asset:${asset.ref}`;
	return [
		{
			id: 'with_upstream',
			label: 'Run with upstream',
			available: true,
			method: 'POST',
			target: { type: 'asset', id: targetId },
			description:
				'Uses the current orchestrator asset run submission path. Backend dependency-scope controls are not exposed yet.'
		},
		{
			id: 'asset_only',
			label: 'Run asset only',
			available: false,
			method: 'POST',
			target: { type: 'asset', id: targetId },
			description:
				'Unavailable in this MVP because the orchestrator does not yet expose a separate asset-only dependency scope.'
		}
	];
}

export function normalizeAssetCatalogList(
	activeManifestPayload: unknown,
	runsPayload: unknown
): AssetCatalogListView {
	const versionId = manifestVersionId(activeManifestPayload);
	const contentHash = manifestContentHash(activeManifestPayload);
	const assets = assetTargets(activeManifestPayload).map((target, index) => {
		const asset = normalizeTargetRecord(target, index);
		const runs = recentRunsForAsset(runsPayload, asset);
		const lastRun = runs[0] ?? null;
		return {
			...asset,
			health: healthFromRun(lastRun),
			lastRun,
			runsCount: runs.length,
			manifestVersionId: versionId,
			manifestContentHash: contentHash,
			runActions: runActions(asset)
		};
	});

	return {
		assets,
		filters: {
			statuses: Array.from(new Set(assets.map((asset) => asset.health))).sort(),
			domains: Array.from(
				new Set(
					assets.map((asset) => asset.domain).filter((value): value is string => Boolean(value))
				)
			).sort(),
			kinds: Array.from(new Set(assets.map((asset) => asset.kind))).sort()
		},
		manifest: { versionId, contentHash, raw: activeManifestSummary(activeManifestPayload) },
		capabilityNotes: [
			{
				key: 'dependencies',
				message:
					'Active manifest targets expose runnable assets, but this endpoint does not expose dependency edges yet; upstream/downstream counts default to 0 unless provided by the backend.'
			},
			{
				key: 'asset_only_runs',
				message:
					'Asset-only run scope is shown as unavailable until the orchestrator exposes an explicit dependency-scope option.'
			}
		],
		raw: { activeManifest: activeManifestPayload, runs: runsPayload }
	};
}

export function filterAssetCatalogItems(
	assets: AssetCatalogItem[],
	filters: AssetCatalogFilters
): AssetCatalogItem[] {
	const status = asString(filters.status)?.toLowerCase();
	const domain = asString(filters.domain)?.toLowerCase();
	const kind = asString(filters.kind)?.toLowerCase();
	const text = asString(filters.text)?.toLowerCase();

	return assets.filter((asset) => {
		if (status && asset.health.toLowerCase() !== status) return false;
		if (domain && (asset.domain ?? '').toLowerCase() !== domain) return false;
		if (kind && asset.kind.toLowerCase() !== kind) return false;
		if (text) {
			const haystack = [
				asset.ref,
				asset.targetId,
				asset.name,
				asset.module,
				asset.kind,
				asset.domain,
				asset.label
			]
				.filter((value): value is string => Boolean(value))
				.join(' ')
				.toLowerCase();
			if (!haystack.includes(text)) return false;
		}
		return true;
	});
}

export function normalizeAssetCatalogDetail(
	activeManifestPayload: unknown,
	runsPayload: unknown,
	assetRef: string
): AssetCatalogDetailView | null {
	const list = normalizeAssetCatalogList(activeManifestPayload, runsPayload);
	const requested = normalizeAssetRefParts(assetRef);
	const wanted = new Set(
		[assetRef, requested.ref, requested.targetId].filter((value): value is string => Boolean(value))
	);
	const asset = list.assets.find((item) => {
		const aliases = targetAliases(item);
		for (const value of wanted) if (aliases.has(value)) return true;
		return false;
	});

	if (!asset) return null;

	const recentRuns = recentRunsForAsset(runsPayload, asset);
	return {
		asset: {
			...asset,
			lastRun: recentRuns[0] ?? asset.lastRun,
			health: healthFromRun(recentRuns[0] ?? asset.lastRun)
		},
		overview: [
			{ label: 'Ref', value: asset.ref },
			{ label: 'Target id', value: asset.targetId ?? 'Not available' },
			{ label: 'Module', value: asset.module },
			{ label: 'Name', value: asset.name },
			{ label: 'Kind', value: asset.kind },
			{ label: 'Domain', value: asset.domain ?? 'Not exposed by orchestrator' },
			{ label: 'Manifest version', value: asset.manifestVersionId ?? 'Not available' },
			{ label: 'Manifest hash', value: asset.manifestContentHash ?? 'Not available' }
		],
		recentRuns,
		dependencies: [],
		dependents: [],
		capabilityNotes: list.capabilityNotes,
		raw: { target: asset.rawTarget, manifest: list.manifest.raw, runs: runsPayload }
	};
}
