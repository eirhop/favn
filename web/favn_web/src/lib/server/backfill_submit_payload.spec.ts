import { describe, expect, it } from 'vitest';
import { parseBackfillSubmitPayload } from './backfill_submit_payload';

describe('parseBackfillSubmitPayload', () => {
	it('accepts the issue 182 submit contract', () => {
		expect(
			parseBackfillSubmitPayload({
				target: { type: 'pipeline', id: 'pipeline:daily_sales' },
				manifest_selection: { mode: 'active' },
				range: { from: '2026-04-01', to: '2026-04-07', kind: 'day', timezone: 'Etc/UTC' },
				coverage_baseline_id: 'baseline_123',
				max_attempts: 2
			})
		).toMatchObject({
			target: { type: 'pipeline', id: 'pipeline:daily_sales' },
			manifest_selection: { mode: 'active' },
			range: { from: '2026-04-01', to: '2026-04-07', kind: 'day', timezone: 'Etc/UTC' },
			coverage_baseline_id: 'baseline_123',
			max_attempts: 2
		});
	});

	it('rejects unsupported targets, manifest modes, ranges, and option types', () => {
		expect(parseBackfillSubmitPayload({ target: { type: 'asset', id: 'asset:a' } })).toBeNull();
		expect(
			parseBackfillSubmitPayload({
				target: { type: 'pipeline', id: 'pipeline:a' },
				manifest_selection: { mode: 'latest' },
				range: { from: 'a', to: 'b', kind: 'day', timezone: 'Etc/UTC' }
			})
		).toBeNull();
		expect(
			parseBackfillSubmitPayload({
				target: { type: 'pipeline', id: 'pipeline:a' },
				manifest_selection: { mode: 'active' },
				range: { from: 'a', to: 'b', kind: 'week', timezone: 'Etc/UTC' }
			})
		).toBeNull();
	});
});
