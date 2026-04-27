<script lang="ts">
	import LoginPanel from '$lib/components/favn/LoginPanel.svelte';

	let { data, form } = $props<{
		data: { localAdminConfigured: boolean };
		form?: { message?: string; username?: string };
	}>();
</script>

<LoginPanel {form} localAdminConfigured={data.localAdminConfigured} />
