import type { RunStatus } from '$lib/run_view_types';

export type AssetHealth = 'healthy' | 'failed' | 'running' | 'not_run' | 'unknown';

export type AssetCatalogRunAction = {
	id: 'with_upstream' | 'asset_only';
	label: string;
	available: boolean;
	method: 'POST';
	target: { type: 'asset'; id: string };
	description: string;
};

export type AssetCatalogRunSummary = {
	id: string;
	status: RunStatus;
	target: string | null;
	targetType: string | null;
	startedAt: string | null;
	finishedAt: string | null;
	manifestVersionId: string | null;
	raw: unknown;
};

export type AssetMaterializationSummary = {
	relation: unknown | null;
	materialization: unknown | null;
	rowsWritten: number | null;
	rowsAffected: number | null;
	loadedAt: string | null;
	materializedAt: string | null;
	window: unknown | null;
	metadata: unknown | null;
};

export type AssetRuntimeConfigStatus = 'declared';

export type AssetRuntimeConfigEntry = {
	path: string;
	provider: string;
	key: string;
	secret: boolean;
	required: boolean;
	status: AssetRuntimeConfigStatus;
};

export type AssetCatalogItem = {
	ref: string;
	targetId?: string;
	name: string;
	module: string;
	kind: string;
	domain: string | null;
	label?: string;
	health: AssetHealth;
	lastRun?: AssetCatalogRunSummary | null;
	runsCount?: number;
	upstreamCount?: number;
	downstreamCount?: number;
	manifestVersionId?: string | null;
	manifestContentHash?: string | null;
	runtimeConfig?: AssetRuntimeConfigEntry[];
	relation?: unknown | null;
	materialization?: unknown | null;
	window?: unknown | null;
	metadata?: unknown | null;
	latestMaterialization?: AssetMaterializationSummary | null;
	runActions?: AssetCatalogRunAction[];
	rawTarget?: unknown;
};

export type AssetCatalogPageData = {
	activeManifestVersionId: string | null;
	assets: AssetCatalogItem[];
	loadError: string | null;
	manifest?: AssetCatalogListView['manifest'];
	capabilityNotes?: AssetCatalogCapabilityNote[];
	raw?: AssetCatalogListView['raw'];
};

export type AssetCatalogCapabilityNote = {
	key: string;
	message: string;
};

export type AssetCatalogListView = {
	assets: AssetCatalogItem[];
	filters: {
		statuses: AssetHealth[];
		domains: string[];
		kinds: string[];
	};
	manifest: {
		versionId: string | null;
		contentHash: string | null;
		raw: unknown;
	};
	capabilityNotes: AssetCatalogCapabilityNote[];
	raw: {
		activeManifest: unknown;
		runs: unknown;
	};
};

export type AssetCatalogDetailView = {
	asset: AssetCatalogItem;
	overview: Array<{ label: string; value: string }>;
	recentRuns: AssetCatalogRunSummary[];
	dependencies: AssetCatalogItem[];
	dependents: AssetCatalogItem[];
	capabilityNotes: AssetCatalogCapabilityNote[];
	raw: {
		target: unknown;
		manifest: unknown;
		runs: unknown;
	};
};

export type AssetDetailView = AssetCatalogDetailView | AssetCatalogItem | Record<string, unknown>;

export type AssetDetailPageData = {
	session?: { actor_id: string; provider: string } | null;
	activeManifestVersionId?: string | null;
	asset?: AssetDetailView;
	detail?: AssetCatalogDetailView;
	view?: AssetDetailView;
	assetDetail?: AssetDetailView;
	recentRuns?: AssetCatalogRunSummary[];
	runs?: AssetCatalogRunSummary[];
	runActions?: Record<string, unknown>;
	actions?: Record<string, unknown>;
	lineage?: Record<string, unknown>;
	capabilities?: Record<string, unknown>;
	lastRun?: AssetCatalogRunSummary | null;
	raw?: unknown;
};

export type AssetCatalogFilters = {
	status?: string | null;
	domain?: string | null;
	kind?: string | null;
	text?: string | null;
};
