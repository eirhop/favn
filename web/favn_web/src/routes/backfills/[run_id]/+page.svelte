<script lang="ts">
	import AppShell from '$lib/components/favn/AppShell.svelte';
	import BackfillDetailPage from '$lib/components/favn/BackfillDetailPage.svelte';
	let { data } = $props<{ data: import('./$types').PageData }>();
</script>

<svelte:head><title>Backfill {data.run.id} · Favn</title></svelte:head>

<AppShell session={data.session} activeManifestVersionId={data.activeManifestVersionId}>
	<BackfillDetailPage run={data.run} windowsPage={data.windowsPage} loadError={data.loadError} />
</AppShell>
