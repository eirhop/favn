<script lang="ts">
	import AppShell from '$lib/components/favn/AppShell.svelte';
	import CoverageBaselinesPage from '$lib/components/favn/CoverageBaselinesPage.svelte';
	let { data } = $props<{ data: import('./$types').PageData }>();
</script>

<svelte:head><title>Coverage baselines · Favn</title></svelte:head>

<AppShell session={data.session} activeManifestVersionId={data.activeManifestVersionId}>
	<CoverageBaselinesPage baselinesPage={data.baselinesPage} loadError={data.loadError} />
</AppShell>
