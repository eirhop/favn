<script module lang="ts">
	import { defineMeta } from '@storybook/addon-svelte-csf';
	import CoverageBaselinesPage from './CoverageBaselinesPage.svelte';
	import type { BackfillPage, CoverageBaselineView } from '$lib/backfill_view_types';
	const baselinesPage: BackfillPage<CoverageBaselineView> = {
		items: [
			{
				baselineId: 'baseline_123',
				pipelineModule: 'DailySales',
				sourceKey: 'daily-sales',
				segmentKeyHash: 'abc123',
				windowKind: 'day',
				timezone: 'Etc/UTC',
				coverageUntil: '2026-04-01',
				createdByRunId: 'run_001',
				manifestVersionId: 'manifest_v2',
				status: 'active',
				createdAt: '2026-04-01T00:00:00Z',
				updatedAt: '2026-04-01T00:00:00Z'
			}
		],
		pagination: { limit: 50, offset: 0, total: 1, hasNext: false, hasPrevious: false }
	};
	const { Story } = defineMeta({
		title: 'Favn/Backfills/Coverage Baselines',
		component: CoverageBaselinesPage,
		parameters: { layout: 'fullscreen' }
	});
</script>

<Story name="Data" args={{ baselinesPage }} />
<Story
	name="Empty"
	args={{
		baselinesPage: {
			...baselinesPage,
			items: [],
			pagination: { ...baselinesPage.pagination, total: 0 }
		}
	}}
/>
