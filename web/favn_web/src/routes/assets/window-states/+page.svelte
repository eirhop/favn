<script lang="ts">
	import AppShell from '$lib/components/favn/AppShell.svelte';
	import AssetWindowStatesPage from '$lib/components/favn/AssetWindowStatesPage.svelte';
	let { data } = $props<{ data: import('./$types').PageData }>();
</script>

<svelte:head><title>Asset window states · Favn</title></svelte:head>

<AppShell session={data.session} activeManifestVersionId={data.activeManifestVersionId}>
	<AssetWindowStatesPage statesPage={data.statesPage} loadError={data.loadError} />
</AppShell>
