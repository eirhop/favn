<script lang="ts">
	import AppShell from '$lib/components/favn/AppShell.svelte';
	import BackfillsPage from '$lib/components/favn/BackfillsPage.svelte';
	let { data } = $props<{ data: import('./$types').PageData }>();
</script>

<svelte:head><title>Backfills · Favn</title></svelte:head>

<AppShell session={data.session} activeManifestVersionId={data.activeManifestVersionId}>
	<BackfillsPage
		pipelineTargets={data.pipelineTargets}
		coverageBaselines={data.coverageBaselines.items}
		loadError={data.loadError}
	/>
</AppShell>
