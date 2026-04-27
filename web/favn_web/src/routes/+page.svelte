<script lang="ts">
	import DashboardPage from '$lib/components/favn/DashboardPage.svelte';

	let { data } = $props<{
		data: {
			session: { actor_id: string; provider: string };
			runs: Array<{ id: string; status: string | null; target: string | null }>;
			schedules: Array<{ id: string; enabled: boolean | null; target: string | null }>;
			activeManifestVersionId: string | null;
		};
	}>();
</script>

<DashboardPage
	session={data.session}
	runs={data.runs}
	schedules={data.schedules}
	activeManifestVersionId={data.activeManifestVersionId}
/>
