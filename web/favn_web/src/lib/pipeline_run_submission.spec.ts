import { describe, expect, it } from 'vitest';
import {
	buildPipelineRunPayload,
	extractSubmittedRun,
	normalizePipelineTargets,
	normalizePipelineWindowPolicy
} from './pipeline_run_submission';

describe('pipeline run submission helpers', () => {
	it('normalizes window policies defensively', () => {
		expect(
			normalizePipelineWindowPolicy({
				kind: ':daily',
				anchor: 'previous_complete',
				timezone: 'Europe/Oslo',
				allow_full_load: true
			})
		).toEqual({
			kind: 'day',
			anchor: 'previous_complete',
			timezone: 'Europe/Oslo',
			allowFullLoad: true
		});

		expect(normalizePipelineWindowPolicy({ kind: 'weekly' })).toBeNull();
		expect(normalizePipelineWindowPolicy(null)).toBeNull();
	});

	it('normalizes pipeline targets from active manifest target data', () => {
		const pipelines = normalizePipelineTargets({
			data: {
				manifest: { manifest_version_id: 'mfv_123' },
				targets: {
					pipelines: [
						{
							target_id: 'pipeline:Elixir.FavnDemo.Pipelines.Daily',
							label: 'Daily sales',
							window: {
								kind: 'day',
								anchor: 'previous_complete',
								timezone: 'Etc/UTC'
							}
						},
						{
							target_id: 'pipeline:Elixir.FavnDemo.Pipelines.Smoke',
							label: 'Smoke test'
						}
					]
				}
			}
		});

		expect(pipelines).toEqual([
			expect.objectContaining({
				targetId: 'pipeline:Elixir.FavnDemo.Pipelines.Daily',
				label: 'Daily sales',
				windowPolicy: expect.objectContaining({ kind: 'day', timezone: 'Etc/UTC' })
			}),
			expect.objectContaining({
				targetId: 'pipeline:Elixir.FavnDemo.Pipelines.Smoke',
				windowPolicy: null
			})
		]);
	});

	it('constructs safe pipeline run payloads', () => {
		const pipeline = {
			targetId: 'pipeline:DailySales',
			label: 'DailySales',
			module: null,
			windowPolicy: {
				kind: 'day' as const,
				anchor: 'previous_complete',
				timezone: 'Etc/UTC',
				allowFullLoad: false
			}
		};

		expect(
			buildPipelineRunPayload({
				pipeline,
				windowValue: '2026-04-27',
				timezone: 'Europe/Oslo',
				fullLoad: false
			})
		).toEqual({
			ok: true,
			payload: {
				target: { type: 'pipeline', id: 'pipeline:DailySales' },
				window: { mode: 'single', kind: 'day', value: '2026-04-27', timezone: 'Europe/Oslo' }
			}
		});

		expect(
			buildPipelineRunPayload({ pipeline, windowValue: '', timezone: '', fullLoad: false })
		).toEqual({ ok: false, error: 'Enter a day window value.' });

		expect(
			buildPipelineRunPayload({ pipeline, windowValue: '', timezone: '', fullLoad: true })
		).toEqual({ ok: false, error: 'This pipeline requires a window; full load is not allowed.' });
	});

	it('extracts submitted run id and status from response payloads', () => {
		expect(extractSubmittedRun({ data: { run: { id: 'run_123', status: 'queued' } } })).toEqual({
			id: 'run_123',
			status: 'queued'
		});
	});
});
