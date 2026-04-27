<script lang="ts">
	import AppShell from '$lib/components/favn/AppShell.svelte';
	import RunsPage from '$lib/components/favn/RunsPage.svelte';

	let { data } = $props<{
		data: import('./$types').PageData;
	}>();
</script>

<AppShell session={data.session} activeManifestVersionId={data.activeManifestVersionId}>
	<RunsPage runs={data.runs} loadError={data.loadError} />
</AppShell>
