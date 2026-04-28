import { describe, expect, it } from 'vitest';
import { parseSubmitPayload } from './run_submit_payload';
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
				manifestVersionId: 'mfv_abc123',
				manifestContentHash: null
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
		expect(detail.outputs).toEqual([]);
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

	it('normalizes real orchestrator run payloads without inventing outputs', () => {
		const detail = normalizeRunDetail(
			{
				data: {
					run: {
						id: 'run_real_001',
						status: 'ok',
						started_at: '2026-04-27T10:00:00.000Z',
						finished_at: '2026-04-27T10:00:03.250Z',
						manifest_version_id: 'mfv_real_123',
						manifest_content_hash: 'sha256:1234567890abcdef1234567890abcdef',
						submit_kind: 'pipeline',
						target_refs: [
							'Elixir.FavnReferenceWorkload.Warehouse.Ops.ReferenceWorkloadComplete:asset'
						],
						event_seq: 33
					}
				}
			},
			'run_real_001'
		);

		expect(detail).toEqual(
			expect.objectContaining({
				id: 'run_real_001',
				status: 'succeeded',
				target: 'Elixir.FavnReferenceWorkload.Warehouse.Ops.ReferenceWorkloadComplete (asset)',
				targetType: 'pipeline',
				durationMs: 3250,
				duration: '3.3s',
				manifestVersionId: 'mfv_real_123',
				manifestContentHash: 'sha256:1234567890ab',
				submitKind: 'pipeline'
			})
		);
		expect(detail.target).not.toBe('Unknown target');
		expect(detail.assets).toEqual([]);
		expect(detail.outputs).toEqual([]);
		expect(detail.timeline).toEqual([
			expect.objectContaining({ label: 'run_submitted' }),
			expect.objectContaining({
				label: 'run_succeeded',
				detail: 'Latest projected run state · event #33'
			})
		]);
	});

	it('uses submit kind as a useful list target when list DTOs omit target refs', () => {
		const summaries = normalizeRunSummaries({
			data: {
				items: [
					{
						id: 'run_list_real',
						status: 'ok',
						submit_kind: 'pipeline',
						started_at: '2026-04-27T10:00:00.000Z',
						finished_at: '2026-04-27T10:00:00.499Z'
					}
				]
			}
		});

		expect(summaries[0]).toEqual(
			expect.objectContaining({
				target: 'Pipeline run',
				targetType: 'pipeline',
				duration: '499ms'
			})
		);
		expect(summaries[0].target).not.toBe('Unknown target');
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

	it('does not invent asset/output fallback data when optional lists are missing', () => {
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

		expect(detail.assets).toEqual([]);
		expect(detail.outputs).toEqual([]);
		expect(detail.timeline).toContainEqual(expect.objectContaining({ label: 'run_succeeded' }));
	});

	it('normalizes pipeline and asset runtime window context defensively', () => {
		const detail = normalizeRunDetail(
			{
				data: {
					run: {
						id: 'run_windowed',
						status: 'ok',
						submit_kind: 'pipeline',
						pipeline: {
							module: 'Elixir.FavnDemo.Pipelines.Daily',
							window_policy: {
								kind: 'day',
								anchor: 'previous_complete',
								timezone: 'Europe/Oslo',
								allow_full_load: false
							},
							anchor_window: {
								mode: 'single',
								kind: 'day',
								value: '2026-04-27',
								timezone: 'Europe/Oslo'
							},
							window: ':day:2026-04-27'
						},
						assets: [
							{
								id: 'Raw.Orders',
								status: 'ok',
								window: { kind: 'day', value: '2026-04-27', timezone: 'Europe/Oslo' }
							}
						]
					}
				}
			},
			'run_windowed'
		);

		expect(detail.windowInfo).toEqual({
			pipelinePolicy: 'day · previous_complete · Europe/Oslo · full load blocked',
			requestedAnchorWindow: 'day · 2026-04-27 · Europe/Oslo',
			resolvedAnchorWindow: 'day:2026-04-27',
			assetWindows: [{ asset: 'Raw.Orders', window: 'day · 2026-04-27 · Europe/Oslo' }]
		});
		expect(detail.assets[0].window).toBe('day · 2026-04-27 · Europe/Oslo');
	});

	it('accepts window payloads for pipeline submissions only', () => {
		expect(
			parseSubmitPayload({
				target: { type: 'pipeline', id: 'DailySales' },
				window: { mode: 'single', kind: 'day', value: '2026-04-27', timezone: 'Europe/Oslo' }
			})
		).toEqual({
			target: { type: 'pipeline', id: 'DailySales' },
			window: { mode: 'single', kind: 'day', value: '2026-04-27', timezone: 'Europe/Oslo' }
		});

		expect(
			parseSubmitPayload({
				target: { type: 'asset', id: 'Raw.Orders' },
				window: { mode: 'single', kind: 'day', value: '2026-04-27' }
			})
		).toBeNull();
		expect(
			parseSubmitPayload({
				target: { type: 'pipeline', id: 'DailySales' },
				window: { mode: 'range', kind: 'day', value: '2026-04-27' }
			})
		).toBeNull();
	});
});
