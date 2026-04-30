export type PaginationView = {
	limit: number | null;
	offset: number | null;
	total: number | null;
	hasNext: boolean;
	hasPrevious: boolean;
};

export type BackfillWindowView = {
	backfillRunId: string;
	pipelineModule: string | null;
	manifestVersionId: string | null;
	windowKind: string | null;
	windowStartAt: string | null;
	windowEndAt: string | null;
	timezone: string | null;
	windowKey: string;
	status: string;
	attemptCount: number | null;
	latestAttemptRunId: string | null;
	lastSuccessRunId: string | null;
	updatedAt: string | null;
	childRunId: string | null;
	coverageBaselineId: string | null;
	lastError: string | null;
	startedAt: string | null;
	finishedAt: string | null;
	createdAt: string | null;
	canRerun: boolean;
};

export type CoverageBaselineView = {
	baselineId: string;
	pipelineModule: string | null;
	sourceKey: string | null;
	segmentKeyHash: string | null;
	windowKind: string | null;
	timezone: string | null;
	coverageUntil: string | null;
	createdByRunId: string | null;
	manifestVersionId: string | null;
	status: string | null;
	createdAt: string | null;
	updatedAt: string | null;
};

export type AssetWindowStateView = {
	assetRefModule: string | null;
	assetRefName: string | null;
	pipelineModule: string | null;
	manifestVersionId: string | null;
	windowKind: string | null;
	windowStartAt: string | null;
	windowEndAt: string | null;
	timezone: string | null;
	windowKey: string;
	status: string | null;
	latestRunId: string | null;
	updatedAt: string | null;
};

export type BackfillPage<T> = {
	items: T[];
	pagination: PaginationView;
};
