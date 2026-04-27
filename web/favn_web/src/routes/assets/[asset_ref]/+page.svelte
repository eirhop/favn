<script lang="ts">
	import AppShell from '$lib/components/favn/AppShell.svelte';
	import AssetDetailPage from '$lib/components/favn/AssetDetailPage.svelte';

	let { data } = $props<{
		data: import('./$types').PageData;
	}>();
</script>

<AppShell session={data.session} activeManifestVersionId={data.activeManifestVersionId}>
	<AssetDetailPage {data} />
</AppShell>
