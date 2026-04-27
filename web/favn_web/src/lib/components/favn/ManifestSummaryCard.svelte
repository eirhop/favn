<script lang="ts">
	import * as Card from '$lib/components/ui/card';

	let { metadata } = $props<{ metadata: Array<{ label: string; value: string }> }>();
</script>

<Card.Root>
	<Card.Header>
		<Card.Title>Manifest</Card.Title>
		<Card.Description>Read-only run context for reproducibility.</Card.Description>
	</Card.Header>
	<Card.Content class="grid gap-3 md:grid-cols-2">
		{#each metadata as item (item.label)}
			<div class="rounded-lg border p-3">
				<p class="text-xs text-slate-500">{item.label}</p>
				<p class="font-mono text-sm">{item.value}</p>
			</div>
		{/each}
	</Card.Content>
</Card.Root>
