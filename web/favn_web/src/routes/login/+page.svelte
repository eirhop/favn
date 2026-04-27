<script lang="ts">
	import LoginPanel from '$lib/components/favn/LoginPanel.svelte';

	let { form } = $props<{
		form?: { message?: string; username?: string };
	}>();
</script>

<LoginPanel {form} />
