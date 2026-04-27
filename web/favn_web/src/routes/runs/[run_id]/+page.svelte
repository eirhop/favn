<script lang="ts">
	import AppShell from '$lib/components/favn/AppShell.svelte';
	import RunDetailPage from '$lib/components/favn/RunDetailPage.svelte';

	let { data } = $props<{
		data: import('./$types').PageData;
	}>();
</script>

<AppShell session={data.session} activeManifestVersionId={data.activeManifestVersionId}>
	<RunDetailPage run={data.run} />
</AppShell>
