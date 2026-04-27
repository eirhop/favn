import type { AssetDetailPageData } from '$lib/asset_catalog_types';

const baseSession = { actor_id: 'local-operator', provider: 'local' };

export const successfulAssetWithRuns = {
	session: baseSession,
	activeManifestVersionId: 'mfv_2026_04_27',
	asset: {
		ref: 'MyApp.Assets.Mart.CustomerRevenue',
		targetId: 'asset:MyApp.Assets.Mart.CustomerRevenue',
		name: 'Customer revenue',
		status: 'succeeded',
		health: 'healthy',
		lastRun: {
			id: 'run_01HAPPY',
			status: 'succeeded',
			startedAt: '14:32:10',
			finishedAt: '14:32:22',
			duration: '12.4s'
		},
		manifestVersionId: 'mfv_2026_04_27',
		type: 'SQL asset',
		kind: 'table materialization',
		domain: 'Revenue',
		capabilities: {
			assetOnlyScopeAvailable: true,
			dependenciesAvailable: true,
			notes: ['SQL payload available', 'Owned relation metadata reported']
		},
		schema: {
			columns: [
				{ name: 'customer_id', type: 'varchar' },
				{ name: 'revenue', type: 'decimal' },
				{ name: 'reported_at', type: 'timestamp' }
			]
		},
		lineage: {
			upstream: ['MyApp.Assets.Raw.CustomerOrders', 'MyApp.Assets.Raw.Customers'],
			downstream: ['MyApp.Assets.Mart.ExecutiveDashboard'],
			dependenciesAvailable: true
		},
		recentRuns: [
			{
				id: 'run_01HAPPY',
				status: 'succeeded',
				startedAt: '14:32:10',
				finishedAt: '14:32:22',
				duration: '12.4s',
				trigger: 'manual'
			},
			{
				id: 'run_01PREV',
				status: 'succeeded',
				startedAt: 'Yesterday 08:00',
				finishedAt: 'Yesterday 08:01',
				duration: '58s',
				trigger: 'schedule'
			}
		],
		raw: {
			ref: 'MyApp.Assets.Mart.CustomerRevenue',
			manifest_version_id: 'mfv_2026_04_27',
			materialization: 'table'
		}
	},
	runActions: { asset: '?/run_asset', withDependencies: '?/run_with_dependencies' }
} as unknown as AssetDetailPageData;

export const failedAssetWithLatestRun = {
	...successfulAssetWithRuns,
	asset: {
		...(successfulAssetWithRuns.asset as Record<string, unknown>),
		ref: 'MyApp.Assets.Staging.CustomerOrders',
		targetId: 'asset:MyApp.Assets.Staging.CustomerOrders',
		name: 'Customer orders staging',
		status: 'failed',
		health: 'failed',
		lastRun: {
			id: 'run_01FAILED',
			status: 'failed',
			startedAt: '14:19:04',
			finishedAt: '14:19:05',
			duration: '812ms',
			error: 'column "customer_id" not found'
		},
		recentRuns: [
			{
				id: 'run_01FAILED',
				status: 'failed',
				startedAt: '14:19:04',
				finishedAt: '14:19:05',
				duration: '812ms',
				trigger: 'manual',
				error: 'column "customer_id" not found'
			}
		],
		raw: { ref: 'MyApp.Assets.Staging.CustomerOrders', latest_status: 'failed' }
	}
} as unknown as AssetDetailPageData;

export const assetWithUnavailableScopes = {
	...successfulAssetWithRuns,
	asset: {
		...(successfulAssetWithRuns.asset as Record<string, unknown>),
		ref: 'MyApp.Sources.ExternalStripeInvoices',
		targetId: 'source:stripe.invoices',
		name: 'Stripe invoices source',
		status: 'unknown',
		health: 'unknown',
		lastRun: null,
		type: 'Source',
		kind: 'external relation',
		domain: 'Billing',
		capabilities: {
			assetOnlyScopeAvailable: false,
			dependenciesAvailable: false,
			notes: ['Source nodes are observed by the planner, not executed directly']
		},
		schema: null,
		lineage: { upstream: [], downstream: [], dependenciesAvailable: false },
		recentRuns: [],
		raw: { ref: 'MyApp.Sources.ExternalStripeInvoices', runnable: false }
	},
	runActions: { asset: '?/run_asset', withDependencies: '?/run_with_dependencies' }
} as unknown as AssetDetailPageData;
