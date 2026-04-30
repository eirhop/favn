<script module lang="ts">
	import { defineMeta } from '@storybook/addon-svelte-csf';
	import AssetWindowStatesPage from './AssetWindowStatesPage.svelte';
	import type { AssetWindowStateView, BackfillPage } from '$lib/backfill_view_types';
	const statesPage: BackfillPage<AssetWindowStateView> = {
		items: [
			{
				assetRefModule: 'Mart',
				assetRefName: 'Revenue',
				pipelineModule: 'DailySales',
				manifestVersionId: 'manifest_v2',
				windowKind: 'day',
				windowStartAt: '2026-04-01T00:00:00Z',
				windowEndAt: '2026-04-02T00:00:00Z',
				timezone: 'Etc/UTC',
				windowKey: 'day:2026-04-01',
				status: 'succeeded',
				latestRunId: 'run_child_002',
				updatedAt: '2026-04-02T00:00:00Z'
			}
		],
		pagination: { limit: 50, offset: 0, total: 1, hasNext: false, hasPrevious: false }
	};
	const { Story } = defineMeta({
		title: 'Favn/Backfills/Asset Window States',
		component: AssetWindowStatesPage,
		parameters: { layout: 'fullscreen' }
	});
</script>

<Story name="Data" args={{ statesPage }} />
<Story
	name="Empty"
	args={{
		statesPage: { ...statesPage, items: [], pagination: { ...statesPage.pagination, total: 0 } }
	}}
/>
