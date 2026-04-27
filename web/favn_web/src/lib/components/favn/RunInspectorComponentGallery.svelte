<script lang="ts">
	import StatusBadge from './StatusBadge.svelte';
	import RunsTable from './RunsTable.svelte';
	import RunSummaryCards from './RunSummaryCards.svelte';
	import ErrorPanel from './ErrorPanel.svelte';
	import AssetExecutionTable from './AssetExecutionTable.svelte';
	import OutputRelationsTable from './OutputRelationsTable.svelte';
	import RunTimeline from './RunTimeline.svelte';
	import ManifestSummaryCard from './ManifestSummaryCard.svelte';
	import AssetDetailSheet from './AssetDetailSheet.svelte';
	import { failedRunDetail, sampleRuns } from './story_fixtures';

	type Variant =
		| 'status-badges'
		| 'runs-table'
		| 'summary-cards'
		| 'error-panel'
		| 'asset-table'
		| 'outputs-table'
		| 'timeline'
		| 'manifest-card'
		| 'asset-sheet';

	let { variant = 'status-badges' } = $props<{ variant?: Variant }>();
	let failedAsset = $derived(failedRunDetail.assets.find((asset) => asset.status === 'failed'));
</script>

<div class="min-h-screen bg-slate-50 p-6 text-slate-950">
	{#if variant === 'status-badges'}
		<div class="flex flex-wrap gap-3">
			<StatusBadge status="queued" />
			<StatusBadge status="running" />
			<StatusBadge status="succeeded" />
			<StatusBadge status="failed" />
			<StatusBadge status="cancelled" />
		</div>
	{:else if variant === 'runs-table'}
		<RunsTable runs={sampleRuns} />
	{:else if variant === 'summary-cards'}
		<RunSummaryCards run={failedRunDetail} />
	{:else if variant === 'error-panel'}
		<ErrorPanel
			asset={failedRunDetail.error?.asset ?? ''}
			message={failedRunDetail.error?.message ?? ''}
		/>
	{:else if variant === 'asset-table'}
		<AssetExecutionTable assets={failedRunDetail.assets} />
	{:else if variant === 'outputs-table'}
		<OutputRelationsTable outputs={failedRunDetail.outputs} />
	{:else if variant === 'timeline'}
		<RunTimeline events={failedRunDetail.timeline} />
	{:else if variant === 'manifest-card'}
		<ManifestSummaryCard metadata={failedRunDetail.metadata} />
	{:else if variant === 'asset-sheet' && failedAsset}
		<AssetDetailSheet
			asset={failedAsset}
			runId={failedRunDetail.id}
			events={failedRunDetail.timeline}
		/>
	{/if}
</div>
