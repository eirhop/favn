import type { AssetCatalogItem, AssetCatalogPageData } from '$lib/asset_catalog_types';

type StoryAsset = AssetCatalogItem & {
	ref: string;
	name: string;
	friendlyName: string;
	module: string;
	health: string;
	kind: string;
	domain: string;
	lastRunAt: string | null;
	runsCount: number;
	tags: string[];
	storagePath: string;
};

export const mixedCatalogAssets: StoryAsset[] = [
	{
		ref: 'Elixir.Favn.Demo.Warehouse.Raw.Crm.CustomerProfiles',
		name: 'CustomerProfiles',
		friendlyName: 'Customer profiles',
		module: 'Favn.Demo.Warehouse.Raw.Crm.CustomerProfiles',
		health: 'healthy',
		kind: 'sql table',
		domain: 'customer',
		lastRunAt: '2026-04-27 09:14:22',
		runsCount: 42,
		tags: ['crm', 'raw', 'pii-reviewed'],
		storagePath: 'duckdb://local_warehouse/raw.crm.customer_profiles'
	},
	{
		ref: 'Elixir.Favn.Demo.Warehouse.Staging.Sales.ExtremelyLongNestedNamespace.OrderLineItemsEnrichedForFinanceControls',
		name: 'OrderLineItemsEnrichedForFinanceControls',
		friendlyName: 'Order line items enriched for finance controls',
		module:
			'Favn.Demo.Warehouse.Staging.Sales.ExtremelyLongNestedNamespace.OrderLineItemsEnrichedForFinanceControls',
		health: 'failed',
		kind: 'sql view',
		domain: 'sales',
		lastRunAt: '2026-04-27 09:12:08',
		runsCount: 18,
		tags: ['sales', 'finance', 'gold'],
		storagePath:
			'duckdb://local_warehouse/staging.sales.order_line_items_enriched_for_finance_controls'
	},
	{
		ref: 'Elixir.Favn.Demo.Operations.Inventory.RebuildSafetyStockLevels',
		name: 'RebuildSafetyStockLevels',
		friendlyName: 'Rebuild safety stock levels',
		module: 'Favn.Demo.Operations.Inventory.RebuildSafetyStockLevels',
		health: 'running',
		kind: 'elixir asset',
		domain: 'operations',
		lastRunAt: '2026-04-27 09:18:51',
		runsCount: 7,
		tags: ['inventory', 'ops'],
		storagePath: 'memory://runner/operations.inventory.safety_stock_levels'
	},
	{
		ref: 'Elixir.Favn.Demo.External.Marketing.AdSpendSnapshot',
		name: 'AdSpendSnapshot',
		friendlyName: 'Ad spend snapshot',
		module: 'Favn.Demo.External.Marketing.AdSpendSnapshot',
		health: 'unknown',
		kind: 'source',
		domain: 'marketing',
		lastRunAt: null,
		runsCount: 0,
		tags: ['source', 'marketing'],
		storagePath: 'snowflake://marketing.public.ad_spend_snapshot'
	}
];

export const mixedAssetCatalog: AssetCatalogPageData & {
	activeManifestVersionId: string;
	assets: AssetCatalogItem[];
	loadError: null;
} = {
	activeManifestVersionId: 'mfv_01HW8YJ7P8M9K9N-control-room',
	assets: mixedCatalogAssets,
	loadError: null
};

export const noActiveManifestCatalog: AssetCatalogPageData & {
	activeManifestVersionId: null;
	assets: AssetCatalogItem[];
	loadError: null;
} = {
	activeManifestVersionId: null,
	assets: [],
	loadError: null
};
