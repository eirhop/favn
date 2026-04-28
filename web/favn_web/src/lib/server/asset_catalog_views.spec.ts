import { describe, expect, it } from 'vitest';
import {
	filterAssetCatalogItems,
	normalizeAssetCatalogDetail,
	normalizeAssetCatalogList,
	normalizeAssetRefParts,
	runMatchesAsset
} from './asset_catalog_views';

const activeManifestPayload = {
	data: {
		manifest: {
			manifest_version_id: 'mfv_123',
			content_hash: 'sha256:abc',
			asset_count: 3,
			pipeline_count: 1
		},
		targets: {
			manifest_version_id: 'mfv_123',
			assets: [
				{
					target_id: 'asset:Elixir.FavnDemo.Raw.Orders:asset',
					label: '{Elixir.FavnDemo.Raw.Orders, :asset}'
				},
				{
					target_id: 'asset:Elixir.FavnDemo.Gold.OrderSummary:asset',
					label: 'Elixir.FavnDemo.Gold.OrderSummary:asset'
				},
				{
					target_id: 'asset:Elixir.FavnDemo.Source.Customers:asset',
					label: '{Elixir.FavnDemo.Source.Customers, :asset}',
					kind: 'source',
					domain: 'crm'
				}
			],
			pipelines: [
				{
					target_id: 'pipeline:Elixir.FavnDemo.Pipelines.Daily',
					label: 'Elixir.FavnDemo.Pipelines.Daily'
				}
			]
		}
	}
};

const runsPayload = {
	data: {
		items: [
			{
				id: 'run_orders_ok',
				status: 'ok',
				target: { type: 'asset', id: 'asset:Elixir.FavnDemo.Raw.Orders:asset' },
				started_at: '2026-04-27T10:00:00Z',
				finished_at: '2026-04-27T10:01:00Z',
				manifest_version_id: 'mfv_123'
			},
			{
				id: 'run_summary_failed',
				status: 'error',
				target_refs: ['Elixir.FavnDemo.Gold.OrderSummary:asset'],
				started_at: '2026-04-27T11:00:00Z',
				manifest_version_id: 'mfv_123'
			},
			{
				id: 'run_pipeline_mentions_orders',
				status: 'running',
				target: { type: 'pipeline', id: 'pipeline:Elixir.FavnDemo.Pipelines.Daily' },
				target_refs: [
					'{Elixir.FavnDemo.Raw.Orders, :asset}',
					'{Elixir.FavnDemo.Gold.OrderSummary, :asset}'
				],
				started_at: '2026-04-27T12:00:00Z',
				manifest_version_id: 'mfv_123'
			}
		]
	}
};

describe('asset catalog view normalizers', () => {
	it('normalizes assets from the real active manifest target envelope', () => {
		const catalog = normalizeAssetCatalogList(activeManifestPayload, runsPayload);

		expect(catalog.manifest).toEqual(
			expect.objectContaining({ versionId: 'mfv_123', contentHash: 'sha256:abc' })
		);
		expect(catalog.assets).toHaveLength(3);
		expect(catalog.assets[0]).toEqual(
			expect.objectContaining({
				ref: 'Elixir.FavnDemo.Raw.Orders:asset',
				targetId: 'asset:Elixir.FavnDemo.Raw.Orders:asset',
				name: 'Orders',
				module: 'Elixir.FavnDemo.Raw.Orders',
				kind: 'asset',
				domain: 'Raw',
				manifestVersionId: 'mfv_123',
				manifestContentHash: 'sha256:abc',
				upstreamCount: 0,
				downstreamCount: 0
			})
		);
		expect(catalog.assets[0].runActions).toEqual([
			expect.objectContaining({ id: 'with_upstream', available: true }),
			expect.objectContaining({ id: 'asset_only', available: false })
		]);
	});

	it('handles an empty active manifest honestly', () => {
		const catalog = normalizeAssetCatalogList(
			{
				data: {
					manifest: { manifest_version_id: 'empty', content_hash: 'hash' },
					targets: { assets: [], pipelines: [] }
				}
			},
			{ data: { items: [] } }
		);

		expect(catalog.assets).toEqual([]);
		expect(catalog.filters).toEqual({ statuses: [], domains: [], kinds: [] });
		expect(catalog.capabilityNotes.map((note) => note.key)).toContain('dependencies');
	});

	it('prefers asset target manifest metadata when exposed by the backend', () => {
		const catalog = normalizeAssetCatalogList(
			{
				data: {
					manifest: { manifest_version_id: 'mfv_active', content_hash: 'sha256:active' },
					targets: {
						assets: [
							{
								target_id: 'asset:Elixir.FavnDemo.Raw.Orders:asset',
								manifest_version_id: 'mfv_target',
								content_hash: 'sha256:target'
							}
						],
						pipelines: []
					}
				}
			},
			{ data: { items: [] } }
		);

		expect(catalog.assets[0]).toEqual(
			expect.objectContaining({
				manifestVersionId: 'mfv_target',
				manifestContentHash: 'sha256:target'
			})
		);
	});

	it('normalizes runtime config refs as declarations without checking web process env', () => {
		const previousSegment = process.env.SOURCE_SYSTEM_SEGMENT_ID;
		const previousToken = process.env.SOURCE_SYSTEM_TOKEN;
		process.env.SOURCE_SYSTEM_SEGMENT_ID = 'segment-123';
		process.env.SOURCE_SYSTEM_TOKEN = 'super-secret-token';

		try {
			const catalog = normalizeAssetCatalogList(
				{
					data: {
						manifest: { manifest_version_id: 'mfv_active', content_hash: 'sha256:active' },
						targets: {
							assets: [
								{
									target_id: 'asset:Elixir.FavnDemo.Raw.Orders:asset',
									runtime_config: {
										source_system: {
											segment_id: {
												provider: ':env',
												key: 'SOURCE_SYSTEM_SEGMENT_ID',
												'secret?': false,
												'required?': true
											},
											token: {
												provider: 'env',
												key: 'SOURCE_SYSTEM_TOKEN',
												secret: true,
												required: true,
												value: 'super-secret-token'
											}
										}
									}
								}
							],
							pipelines: []
						}
					}
				},
				{ data: { items: [] } }
			);

			expect(catalog.assets[0].runtimeConfig).toEqual([
				expect.objectContaining({
					path: 'source_system.segment_id',
					provider: 'env',
					key: 'SOURCE_SYSTEM_SEGMENT_ID',
					secret: false,
					required: true,
					status: 'declared'
				}),
				expect.objectContaining({
					path: 'source_system.token',
					provider: 'env',
					key: 'SOURCE_SYSTEM_TOKEN',
					secret: true,
					required: true,
					status: 'declared'
				})
			]);
			expect(catalog.assets[0].runtimeConfig?.map((entry) => entry.status)).toEqual([
				'declared',
				'declared'
			]);
			expect(JSON.stringify(catalog.assets[0])).not.toContain('segment-123');
			expect(JSON.stringify(catalog.assets[0])).not.toContain('super-secret-token');
			expect(JSON.stringify(catalog.assets[0].runtimeConfig)).not.toContain('present');
			expect(JSON.stringify(catalog.assets[0].runtimeConfig)).not.toContain('missing');
		} finally {
			if (previousSegment === undefined) delete process.env.SOURCE_SYSTEM_SEGMENT_ID;
			else process.env.SOURCE_SYSTEM_SEGMENT_ID = previousSegment;
			if (previousToken === undefined) delete process.env.SOURCE_SYSTEM_TOKEN;
			else process.env.SOURCE_SYSTEM_TOKEN = previousToken;
		}
	});

	it('normalizes robust asset labels and target ids', () => {
		expect(normalizeAssetRefParts('{Elixir.Foo.Bar, :asset}')).toEqual({
			ref: 'Elixir.Foo.Bar:asset',
			targetId: 'asset:Elixir.Foo.Bar:asset',
			module: 'Elixir.Foo.Bar',
			name: 'Bar'
		});
		expect(normalizeAssetRefParts('Elixir.Foo.Bar:asset')).toEqual({
			ref: 'Elixir.Foo.Bar:asset',
			targetId: 'asset:Elixir.Foo.Bar:asset',
			module: 'Elixir.Foo.Bar',
			name: 'Bar'
		});
		expect(normalizeAssetRefParts('asset:Elixir.Foo.Bar:asset')).toEqual({
			ref: 'Elixir.Foo.Bar:asset',
			targetId: 'asset:Elixir.Foo.Bar:asset',
			module: 'Elixir.Foo.Bar',
			name: 'Bar'
		});
	});

	it('matches runs by target id, target_refs, asset_ref, and normalized labels', () => {
		const catalog = normalizeAssetCatalogList(activeManifestPayload, runsPayload);
		const orders = catalog.assets.find((asset) => asset.module.endsWith('.Raw.Orders'))!;
		const summary = catalog.assets.find((asset) => asset.module.endsWith('.Gold.OrderSummary'))!;

		expect(runMatchesAsset({ target: { type: 'asset', id: orders.targetId } }, orders)).toBe(true);
		expect(runMatchesAsset({ target_refs: ['{Elixir.FavnDemo.Raw.Orders, :asset}'] }, orders)).toBe(
			true
		);
		expect(runMatchesAsset({ asset_ref: 'Elixir.FavnDemo.Gold.OrderSummary:asset' }, summary)).toBe(
			true
		);
		expect(runMatchesAsset({ target_refs: ['asset:Elixir.Other.Asset:asset'] }, orders)).toBe(
			false
		);
	});

	it('builds detail views with recent matching runs and honest missing metadata notes', () => {
		const detail = normalizeAssetCatalogDetail(
			activeManifestPayload,
			runsPayload,
			'asset:Elixir.FavnDemo.Gold.OrderSummary:asset'
		);

		expect(detail).not.toBeNull();
		expect(detail!.asset.health).toBe('running');
		expect(detail!.recentRuns.map((run) => run.id)).toEqual([
			'run_pipeline_mentions_orders',
			'run_summary_failed'
		]);
		expect(detail!.dependencies).toEqual([]);
		expect(detail!.dependents).toEqual([]);
		expect(detail!.overview).toContainEqual({ label: 'Domain', value: 'Gold' });
		expect(detail!.capabilityNotes.map((note) => note.key)).toContain('asset_only_runs');
	});

	it('filters by status, domain, kind, and text', () => {
		const catalog = normalizeAssetCatalogList(activeManifestPayload, runsPayload);

		expect(
			filterAssetCatalogItems(catalog.assets, { status: 'not_run' }).map((asset) => asset.module)
		).toEqual(['Elixir.FavnDemo.Source.Customers']);
		expect(
			filterAssetCatalogItems(catalog.assets, { domain: 'crm' }).map((asset) => asset.module)
		).toEqual(['Elixir.FavnDemo.Source.Customers']);
		expect(
			filterAssetCatalogItems(catalog.assets, { kind: 'source' }).map((asset) => asset.module)
		).toEqual(['Elixir.FavnDemo.Source.Customers']);
		expect(
			filterAssetCatalogItems(catalog.assets, { text: 'order' }).map((asset) => asset.module)
		).toEqual(['Elixir.FavnDemo.Raw.Orders', 'Elixir.FavnDemo.Gold.OrderSummary']);
	});
});
