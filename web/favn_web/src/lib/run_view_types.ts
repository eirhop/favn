export type RunStatus =
	| 'pending'
	| 'queued'
	| 'running'
	| 'succeeded'
	| 'failed'
	| 'cancelled'
	| 'unknown';

export type RunSummaryView = {
	id: string;
	status: RunStatus;
	target: string;
	targetType: string;
	trigger: string;
	startedAt: string | null;
	finishedAt: string | null;
	durationMs: number | null;
	duration: string;
	assetCount: string;
	assetsCompleted: number;
	assetsTotal: number;
	manifestVersionId: string | null;
	manifestContentHash: string | null;
	submitKind: string | null;
};

export type AssetExecutionView = {
	id: string;
	status: RunStatus;
	stage: string;
	stageNumber: number | null;
	asset: string;
	module: string | null;
	type: string;
	startedAt: string | null;
	finishedAt: string | null;
	durationMs: number | null;
	duration: string;
	attempt: number;
	output: string | null;
	outputs: OutputView[];
	error: string | null;
	sql: string | null;
	operation: string | null;
	relation: string | null;
	connection: string | null;
	database: string | null;
};

export type OutputView = {
	relation: string;
	type: string;
	asset: string;
	connection: string;
	rows: number | null;
	updatedAt: string | null;
	failed: boolean;
};

export type TimelineEventView = {
	id: string;
	timestamp: string | null;
	label: string;
	detail: string;
	assetId: string | null;
};

export type RunDetailView = RunSummaryView & {
	raw: unknown;
	error: { asset: string; message: string } | null;
	assets: AssetExecutionView[];
	outputs: OutputView[];
	timeline: TimelineEventView[];
	metadata: Array<{ label: string; value: string }>;
	progressPercent: number | null;
	assetCounts: {
		succeeded: number;
		failed: number;
		skipped: number;
		running: number;
		pending: number;
	};
	failedAssetId: string | null;
};
