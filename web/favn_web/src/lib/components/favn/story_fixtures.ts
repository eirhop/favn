import type { RunDetailView, RunSummaryView } from '$lib/run_view_types';

export const failedRunSummary: RunSummaryView = {
	id: 'run_01JABCD12',
	status: 'failed',
	target: 'ImportCustomers',
	targetType: 'pipeline',
	trigger: 'manual',
	startedAt: '14:19:02',
	finishedAt: '14:19:05',
	durationMs: 3100,
	duration: '3.1s',
	assetCount: '2/5',
	assetsCompleted: 2,
	assetsTotal: 5,
	manifestVersionId: 'mfv_def456',
	manifestContentHash: 'sha256:def456789abc',
	submitKind: 'pipeline'
};

export const runningRunSummary: RunSummaryView = {
	id: 'run_01JABCD34',
	status: 'running',
	target: 'BuildWarehouse',
	targetType: 'pipeline',
	trigger: 'manual',
	startedAt: '14:36:51',
	finishedAt: null,
	durationMs: 42000,
	duration: '42s',
	assetCount: '4/10',
	assetsCompleted: 4,
	assetsTotal: 10,
	manifestVersionId: 'mfv_abc123',
	manifestContentHash: 'sha256:abc123456789',
	submitKind: 'pipeline'
};

export const succeededRunSummary: RunSummaryView = {
	id: 'run_01JABCDEF',
	status: 'succeeded',
	target: 'DailySalesPipeline',
	targetType: 'pipeline',
	trigger: 'manual',
	startedAt: '14:32:10',
	finishedAt: '14:32:22',
	durationMs: 12400,
	duration: '12.4s',
	assetCount: '8/8',
	assetsCompleted: 8,
	assetsTotal: 8,
	manifestVersionId: 'mfv_abc123',
	manifestContentHash: 'sha256:abc123456789',
	submitKind: 'pipeline'
};

export const sampleRuns: RunSummaryView[] = [
	succeededRunSummary,
	failedRunSummary,
	runningRunSummary
];

export const realPayloadRunSummary: RunSummaryView = {
	id: 'run_real_001',
	status: 'succeeded',
	target: 'Elixir.FavnReferenceWorkload.Warehouse.Ops.ReferenceWorkloadComplete (asset)',
	targetType: 'pipeline',
	trigger: 'manual',
	startedAt: '10:00:00',
	finishedAt: '10:00:03',
	durationMs: 3250,
	duration: '3.3s',
	assetCount: '—',
	assetsCompleted: 0,
	assetsTotal: 0,
	manifestVersionId: 'mfv_real_123',
	manifestContentHash: 'sha256:1234567890ab',
	submitKind: 'pipeline'
};

export const failedRunDetail: RunDetailView = {
	...failedRunSummary,
	raw: { id: failedRunSummary.id, status: 'failed' },
	error: {
		asset: 'Staging.CustomerOrders',
		message: 'DuckDB query failed: column "customer_id" not found'
	},
	assets: [
		{
			id: 'Raw.Crm.Customers',
			status: 'succeeded',
			stage: 'Stage 1',
			stageNumber: 1,
			asset: 'Raw.Crm.Customers',
			module: null,
			type: 'SQL',
			startedAt: '14:19:02',
			finishedAt: '14:19:03',
			durationMs: 430,
			duration: '430ms',
			attempt: 1,
			output: 'raw.crm_customers',
			outputs: [
				{
					relation: 'raw.crm_customers',
					type: 'table',
					asset: 'Raw.Crm.Customers',
					connection: 'local_duckdb',
					rows: 10000,
					updatedAt: '14:19:03',
					failed: false
				}
			],
			error: null,
			sql: 'create or replace table raw.crm_customers as select * from source_customers',
			operation: 'materialize table',
			relation: 'raw.crm_customers',
			connection: 'local_duckdb',
			database: '.favn/data/work.duckdb',
			window: null
		},
		{
			id: 'Staging.CustomerOrders',
			status: 'failed',
			stage: 'Stage 2',
			stageNumber: 2,
			asset: 'Staging.CustomerOrders',
			module: null,
			type: 'SQL',
			startedAt: '14:19:04',
			finishedAt: '14:19:05',
			durationMs: 812,
			duration: '812ms',
			attempt: 1,
			output: 'staging.customer_orders',
			outputs: [
				{
					relation: 'staging.customer_orders',
					type: 'table',
					asset: 'Staging.CustomerOrders',
					connection: 'local_duckdb',
					rows: null,
					updatedAt: null,
					failed: true
				}
			],
			error: 'column "customer_id" not found',
			sql: 'create or replace table staging.customer_orders as\nselect customer_id, order_date, total_amount\nfrom raw.crm_orders;',
			operation: 'materialize table',
			relation: 'staging.customer_orders',
			connection: 'local_duckdb',
			database: '.favn/data/work.duckdb',
			window: null
		},
		{
			id: 'Mart.CustomerRevenue',
			status: 'cancelled',
			stage: 'Stage 3',
			stageNumber: 3,
			asset: 'Mart.CustomerRevenue',
			module: null,
			type: 'SQL',
			startedAt: null,
			finishedAt: null,
			durationMs: null,
			duration: '—',
			attempt: 1,
			output: 'mart.customer_revenue',
			outputs: [],
			error: 'skipped',
			sql: null,
			operation: null,
			relation: 'mart.customer_revenue',
			connection: 'local_duckdb',
			database: '.favn/data/work.duckdb',
			window: null
		}
	],
	outputs: [
		{
			relation: 'raw.crm_customers',
			type: 'table',
			asset: 'Raw.Crm.Customers',
			connection: 'local_duckdb',
			rows: 10000,
			updatedAt: '14:19:03',
			failed: false
		},
		{
			relation: 'staging.customer_orders',
			type: 'table',
			asset: 'Staging.CustomerOrders',
			connection: 'local_duckdb',
			rows: null,
			updatedAt: null,
			failed: true
		}
	],
	timeline: [
		{
			id: 'evt_1',
			timestamp: '14:19:02',
			label: 'run_submitted',
			detail: 'ImportCustomers submitted by local-operator',
			assetId: null
		},
		{
			id: 'evt_2',
			timestamp: '14:19:05',
			label: 'asset_failed',
			detail: 'Staging.CustomerOrders · column "customer_id" not found',
			assetId: 'Staging.CustomerOrders'
		}
	],
	metadata: [
		{ label: 'Run id', value: failedRunSummary.id },
		{ label: 'Manifest', value: 'mfv_def456' },
		{ label: 'Content hash', value: 'sha256:abc123' },
		{ label: 'Schema version', value: '1' }
	],
	progressPercent: null,
	assetCounts: { succeeded: 1, failed: 1, skipped: 1, running: 0, pending: 0 },
	failedAssetId: 'Staging.CustomerOrders',
	windowInfo: {
		pipelinePolicy: null,
		requestedAnchorWindow: null,
		resolvedAnchorWindow: null,
		assetWindows: []
	}
};

export const realPayloadRunDetail: RunDetailView = {
	...realPayloadRunSummary,
	raw: {
		data: {
			run: {
				id: 'run_real_001',
				status: 'ok',
				submit_kind: 'pipeline',
				target_refs: ['Elixir.FavnReferenceWorkload.Warehouse.Ops.ReferenceWorkloadComplete:asset'],
				manifest_version_id: 'mfv_real_123',
				manifest_content_hash: 'sha256:1234567890abcdef1234567890abcdef',
				event_seq: 33
			}
		}
	},
	error: null,
	assets: [],
	outputs: [],
	timeline: [
		{
			id: 'submitted',
			timestamp: '10:00:00',
			label: 'run_submitted',
			detail: 'Elixir.FavnReferenceWorkload.Warehouse.Ops.ReferenceWorkloadComplete:asset',
			assetId: null
		},
		{
			id: 'status',
			timestamp: '10:00:03',
			label: 'run_succeeded',
			detail: 'Latest projected run state · event #33',
			assetId: null
		}
	],
	metadata: [
		{ label: 'Run id', value: 'run_real_001' },
		{ label: 'Submit kind', value: 'pipeline' },
		{ label: 'Manifest', value: 'mfv_real_123' },
		{ label: 'Content hash', value: 'sha256:1234567890ab' }
	],
	progressPercent: null,
	assetCounts: { succeeded: 0, failed: 0, skipped: 0, running: 0, pending: 0 },
	failedAssetId: null,
	windowInfo: {
		pipelinePolicy: null,
		requestedAnchorWindow: null,
		resolvedAnchorWindow: null,
		assetWindows: []
	}
};
