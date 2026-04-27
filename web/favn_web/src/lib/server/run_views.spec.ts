import { describe, expect, it } from 'vitest';
import { normalizeRunDetail, normalizeRunSummaries } from './run_views';

describe('run view normalizers', () => {
	it('normalizes run lists from the orchestrator items envelope', () => {
		expect(
			normalizeRunSummaries({
				data: {
					items: [
						{
							id: 'run_ok',
							status: 'ok',
							target: { type: 'pipeline', id: 'DailySalesPipeline' },
							assets_completed: 3,
							assets_total: 3,
							manifest_version_id: 'mfv_abc123'
						}
					]
				}
			})
		).toEqual([
			expect.objectContaining({
				id: 'run_ok',
				status: 'succeeded',
				target: 'DailySalesPipeline',
				assetCount: '3/3',
				manifestVersionId: 'mfv_abc123'
			})
		]);
	});

	it('unwraps real run detail envelope shape', () => {
		const detail = normalizeRunDetail(
			{
				data: {
					run: {
						id: 'run_001',
						status: 'error',
						target: { type: 'pipeline', id: 'ImportCustomers' },
						manifest_version_id: 'mfv_abc123',
						assets: [
							{ id: 'Raw.Customers', status: 'ok', output: 'raw.customers' },
							{ id: 'Staging.Customers', status: 'error', error: 'column missing' }
						],
						events: [{ id: 'evt_1', type: 'asset_failed', message: 'column missing' }]
					}
				}
			},
			'run_001'
		);

		expect(detail).toEqual(
			expect.objectContaining({
				id: 'run_001',
				status: 'failed',
				target: 'ImportCustomers',
				manifestVersionId: 'mfv_abc123',
				failedAssetId: 'Staging.Customers'
			})
		);
		expect(detail.assets.map((asset) => asset.status)).toEqual(['succeeded', 'failed']);
		expect(detail.outputs).toEqual([
			expect.objectContaining({ relation: 'raw.customers', failed: false })
		]);
		expect(detail.timeline).toEqual([expect.objectContaining({ label: 'asset_failed' })]);
	});

	it('keeps prototype detail envelope support', () => {
		const detail = normalizeRunDetail(
			{
				data: {
					id: 'run_002',
					status: 'succeeded',
					target: { type: 'asset', id: 'Raw.Customers' }
				}
			},
			'run_002'
		);

		expect(detail.status).toBe('succeeded');
		expect(detail.target).toBe('Raw.Customers');
	});

	it('maps domain and UI statuses to inspector statuses', () => {
		const statuses = normalizeRunSummaries({
			data: {
				items: [
					{ id: 'ok', status: 'ok' },
					{ id: 'error', status: 'error' },
					{ id: 'timed_out', status: 'timed_out' },
					{ id: 'retrying', status: 'retrying' },
					{ id: 'succeeded', status: 'succeeded' },
					{ id: 'failed', status: 'failed' },
					{ id: 'running', status: 'running' },
					{ id: 'cancelled', status: 'cancelled' },
					{ id: 'canceled', status: 'canceled' }
				]
			}
		}).map((run) => run.status);

		expect(statuses).toEqual([
			'succeeded',
			'failed',
			'failed',
			'running',
			'succeeded',
			'failed',
			'running',
			'cancelled',
			'cancelled'
		]);
	});

	it('provides asset/output fallback data when optional lists are missing', () => {
		const detail = normalizeRunDetail(
			{
				data: {
					run: {
						id: 'run_no_assets',
						status: 'ok',
						target: { type: 'asset', id: 'Raw.Customers' }
					}
				}
			},
			'run_no_assets'
		);

		expect(detail.assets).toEqual([
			expect.objectContaining({ asset: 'Raw.Customers', status: 'succeeded' })
		]);
		expect(detail.outputs).toEqual([
			expect.objectContaining({ relation: 'Raw.Customers', asset: 'Raw.Customers' })
		]);
	});
});
