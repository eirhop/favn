import { describe, expect, it } from 'vitest';
import {
	buildBackfillSubmitPayload,
	compatibleCoverageBaselines,
	coverageBaselineOptionLabel,
	extractSubmittedBackfill
} from './backfill_submission';
import type { PipelineTargetView } from './pipeline_run_submission';

const pipeline: PipelineTargetView = {
	targetId: 'pipeline:DailySales',
	label: 'Daily sales',
	module: 'DailySales',
	windowPolicy: null
};

describe('buildBackfillSubmitPayload', () => {
	it('builds the conservative backfill payload', () => {
		expect(
			buildBackfillSubmitPayload({
				pipeline,
				from: '2026-04-01',
				to: '2026-04-07',
				kind: 'day',
				timezone: 'Etc/UTC',
				coverageBaselineId: 'baseline_123'
			})
		).toEqual({
			ok: true,
			payload: {
				target: { type: 'pipeline', id: 'pipeline:DailySales' },
				manifest_selection: { mode: 'active' },
				range: {
					from: '2026-04-01',
					to: '2026-04-07',
					kind: 'day',
					timezone: 'Etc/UTC'
				},
				coverage_baseline_id: 'baseline_123'
			}
		});
	});

	it('rejects missing pipeline and range inputs', () => {
		expect(
			buildBackfillSubmitPayload({
				pipeline: null,
				from: '2026-04-01',
				to: '2026-04-07',
				kind: 'day',
				timezone: 'Etc/UTC'
			})
		).toEqual({ ok: false, error: 'Choose a pipeline to backfill.' });

		expect(
			buildBackfillSubmitPayload({
				pipeline,
				from: '',
				to: '2026-04-07',
				kind: 'day',
				timezone: ''
			})
		).toEqual({ ok: false, error: 'Enter the range start.' });
	});
});

describe('extractSubmittedBackfill', () => {
	it('extracts run identity from accepted payloads', () => {
		expect(
			extractSubmittedBackfill({ data: { run: { run_id: 'bf_001', status: 'queued' } } })
		).toEqual({
			id: 'bf_001',
			status: 'queued'
		});
	});
});

describe('coverage baseline helpers', () => {
	it('filters baselines by pipeline, kind, and available timezone', () => {
		const baselines = [
			{
				baselineId: 'baseline_match',
				pipelineModule: 'DailySales',
				sourceKey: null,
				segmentKeyHash: null,
				windowKind: 'day',
				timezone: 'Etc/UTC',
				coverageUntil: '2026-04-01',
				createdByRunId: null,
				manifestVersionId: null,
				status: 'active',
				createdAt: null,
				updatedAt: null
			},
			{
				baselineId: 'baseline_other_kind',
				pipelineModule: 'DailySales',
				sourceKey: null,
				segmentKeyHash: null,
				windowKind: 'month',
				timezone: 'Etc/UTC',
				coverageUntil: null,
				createdByRunId: null,
				manifestVersionId: null,
				status: 'active',
				createdAt: null,
				updatedAt: null
			},
			{
				baselineId: 'baseline_other_tz',
				pipelineModule: 'DailySales',
				sourceKey: null,
				segmentKeyHash: null,
				windowKind: 'day',
				timezone: 'Europe/Oslo',
				coverageUntil: null,
				createdByRunId: null,
				manifestVersionId: null,
				status: 'active',
				createdAt: null,
				updatedAt: null
			}
		];

		expect(
			compatibleCoverageBaselines({ baselines, pipeline, kind: 'day', timezone: 'Etc/UTC' }).map(
				(baseline) => baseline.baselineId
			)
		).toEqual(['baseline_match']);
	});

	it('builds useful baseline labels', () => {
		expect(
			coverageBaselineOptionLabel({
				baselineId: 'baseline_match',
				pipelineModule: 'DailySales',
				sourceKey: null,
				segmentKeyHash: null,
				windowKind: 'day',
				timezone: 'Etc/UTC',
				coverageUntil: '2026-04-01',
				createdByRunId: null,
				manifestVersionId: null,
				status: 'active',
				createdAt: null,
				updatedAt: null
			})
		).toBe('baseline_match · DailySales · day · Etc/UTC · coverage until 2026-04-01');
	});
});
