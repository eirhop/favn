import { describe, expect, it } from 'vitest';
import {
	normalizeAssetWindowStates,
	normalizeBackfillWindows,
	normalizeCoverageBaselines
} from './backfill_views';

describe('backfill view normalization', () => {
	it('normalizes window rows and preserves pagination metadata', () => {
		const page = normalizeBackfillWindows({
			data: {
				items: [
					{
						backfill_run_id: 'bf_001',
						window_key: 'day:2026-04-01',
						status: 'failed',
						attempt_count: 1,
						latest_attempt_run_id: 'run_child_001'
					}
				],
				pagination: { limit: 1, offset: 1, total: 3 }
			}
		});

		expect(page.items[0]).toMatchObject({
			backfillRunId: 'bf_001',
			windowKey: 'day:2026-04-01',
			status: 'failed',
			canRerun: true
		});
		expect(page.pagination).toMatchObject({
			limit: 1,
			offset: 1,
			total: 3,
			hasNext: true,
			hasPrevious: true
		});
	});

	it('normalizes coverage baselines', () => {
		expect(
			normalizeCoverageBaselines({ data: { baselines: [{ baseline_id: 'baseline_1' }] } }).items[0]
		).toMatchObject({ baselineId: 'baseline_1' });
	});

	it('normalizes asset/window states', () => {
		expect(
			normalizeAssetWindowStates({
				data: { states: [{ asset_ref_name: 'Orders', window_key: 'd1' }] }
			}).items[0]
		).toMatchObject({ assetRefName: 'Orders', windowKey: 'd1' });
	});
});
