<script module lang="ts">
	import { defineMeta } from '@storybook/addon-svelte-csf';
	import { expect, within } from 'storybook/test';
	import BackfillsPage from './BackfillsPage.svelte';
	import type { CoverageBaselineView } from '$lib/backfill_view_types';
	import type { PipelineTargetView } from '$lib/pipeline_run_submission';

	const pipelineTargets: PipelineTargetView[] = [
		{
			targetId: 'pipeline:DailySales',
			label: 'Daily sales',
			module: 'DailySales',
			windowPolicy: {
				kind: 'day',
				anchor: 'previous_complete',
				timezone: 'Etc/UTC',
				allowFullLoad: false
			}
		}
	];
	const baseline: CoverageBaselineView = {
		baselineId: 'baseline_123',
		pipelineModule: 'DailySales',
		sourceKey: 'daily_sales',
		segmentKeyHash: 'abc123',
		windowKind: 'day',
		timezone: 'Etc/UTC',
		coverageUntil: '2026-04-01',
		createdByRunId: 'run_001',
		manifestVersionId: 'manifest_v2',
		status: 'active',
		createdAt: '2026-04-01T00:00:00Z',
		updatedAt: '2026-04-01T00:00:00Z'
	};
	const { Story } = defineMeta({
		title: 'Favn/Backfills',
		component: BackfillsPage,
		parameters: { layout: 'fullscreen' }
	});
</script>

<Story
	name="Submit Ready"
	args={{ pipelineTargets, coverageBaselines: [baseline] }}
	play={async ({ canvasElement }) => {
		const canvas = within(canvasElement);
		await expect(canvas.getByRole('heading', { name: 'Backfills' })).toBeInTheDocument();
		await expect(canvas.getByText('Daily sales')).toBeInTheDocument();
	}}
/>
<Story name="No Pipeline Targets" args={{ pipelineTargets: [], coverageBaselines: [] }} />
<Story
	name="Submit Backend Error"
	args={{ pipelineTargets, coverageBaselines: [], loadError: 'HTTP 502' }}
/>
