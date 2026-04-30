<script module lang="ts">
	import { defineMeta } from '@storybook/addon-svelte-csf';
	import BackfillDetailPage from './BackfillDetailPage.svelte';
	import type { BackfillPage, BackfillWindowView } from '$lib/backfill_view_types';
	import type { RunDetailView } from '$lib/run_view_types';

	const windowsPage: BackfillPage<BackfillWindowView> = {
		items: [
			{
				backfillRunId: 'bf_001',
				pipelineModule: 'DailySales',
				manifestVersionId: 'manifest_v2',
				windowKind: 'day',
				windowStartAt: '2026-04-01T00:00:00Z',
				windowEndAt: '2026-04-02T00:00:00Z',
				timezone: 'Etc/UTC',
				windowKey: 'day:2026-04-01',
				status: 'failed',
				attemptCount: 1,
				latestAttemptRunId: 'run_child_001',
				lastSuccessRunId: null,
				updatedAt: '2026-04-01T00:05:00Z',
				childRunId: 'run_child_001',
				coverageBaselineId: 'baseline_123',
				lastError: 'relation missing',
				startedAt: '2026-04-01T00:00:00Z',
				finishedAt: '2026-04-01T00:05:00Z',
				createdAt: '2026-04-01T00:00:00Z',
				canRerun: true
			}
		],
		pagination: { limit: 50, offset: 0, total: 1, hasNext: false, hasPrevious: false }
	};
	const run: RunDetailView = {
		id: 'bf_001',
		status: 'failed',
		target: 'Daily sales',
		targetType: 'pipeline',
		trigger: 'manual',
		startedAt: '2026-04-01T00:00:00Z',
		finishedAt: null,
		durationMs: null,
		duration: 'running since 00:00',
		assetCount: '0/1',
		assetsCompleted: 0,
		assetsTotal: 1,
		manifestVersionId: 'manifest_v2',
		manifestContentHash: null,
		submitKind: 'backfill',
		raw: {},
		error: null,
		assets: [],
		outputs: [],
		timeline: [],
		metadata: [],
		progressPercent: null,
		assetCounts: { succeeded: 0, failed: 1, skipped: 0, running: 0, pending: 0 },
		failedAssetId: null,
		windowInfo: {
			pipelinePolicy: null,
			requestedAnchorWindow: null,
			resolvedAnchorWindow: null,
			assetWindows: []
		}
	};
	const { Story } = defineMeta({
		title: 'Favn/Backfills/Detail',
		component: BackfillDetailPage,
		parameters: { layout: 'fullscreen' }
	});
</script>

<Story name="Failed Rerunnable Window" args={{ run, windowsPage }} />
<Story
	name="Empty Windows"
	args={{
		run,
		windowsPage: { ...windowsPage, items: [], pagination: { ...windowsPage.pagination, total: 0 } }
	}}
/>
